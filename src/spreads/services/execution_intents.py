from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from spreads.db.decorators import with_storage
from spreads.services.alpaca import (
    create_alpaca_client_from_env,
    resolve_trading_environment,
)
from spreads.services.execution import (
    submit_opportunity_execution,
    submit_position_close_by_id,
)
from spreads.storage.serializers import parse_datetime

AUTO_EXECUTION_MODES = {"paper", "live"}
ACTIVE_INTENT_STATES = {"pending", "claimed", "submitted", "partially_filled"}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _intent_payload(intent: dict[str, Any]) -> dict[str, Any]:
    payload = intent.get("payload")
    if isinstance(payload, dict):
        return dict(payload)
    payload_json = intent.get("payload_json")
    if isinstance(payload_json, dict):
        return dict(payload_json)
    return {}


def _update_intent(
    execution_store: Any,
    intent: dict[str, Any],
    *,
    state: str,
    payload_updates: dict[str, Any] | None = None,
    execution_attempt_id: str | None = None,
    updated_at: str | None = None,
) -> dict[str, Any]:
    payload = _intent_payload(intent)
    if payload_updates:
        payload.update(payload_updates)
    return execution_store.upsert_execution_intent(
        execution_intent_id=str(intent["execution_intent_id"]),
        bot_id=str(intent["bot_id"]),
        automation_id=str(intent["automation_id"]),
        opportunity_decision_id=_as_text(intent.get("opportunity_decision_id")),
        strategy_position_id=_as_text(intent.get("strategy_position_id")),
        execution_attempt_id=execution_attempt_id
        if execution_attempt_id is not None
        else _as_text(intent.get("execution_attempt_id")),
        action_type=str(intent["action_type"]),
        slot_key=str(intent["slot_key"]),
        claim_token=_as_text(intent.get("claim_token")),
        policy_ref=dict(intent.get("policy_ref") or {}),
        config_hash=str(intent.get("config_hash") or ""),
        state=state,
        expires_at=_as_text(intent.get("expires_at")),
        superseded_by_id=_as_text(intent.get("superseded_by_id")),
        payload=payload,
        created_at=str(intent["created_at"]),
        updated_at=updated_at or _utc_now(),
    )


def _append_event(
    execution_store: Any,
    *,
    execution_intent_id: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
) -> None:
    execution_store.append_execution_intent_event(
        execution_intent_id=execution_intent_id,
        event_type=event_type,
        event_at=_utc_now(),
        payload=payload,
    )


def _attempt_state(attempt: dict[str, Any] | None) -> str:
    if attempt is None:
        return "claimed"
    status = str(attempt.get("status") or "").strip().lower()
    if status in {"partially_filled"}:
        return "partially_filled"
    if status in {"filled"}:
        return "filled"
    if status in {"canceled", "cancelled"}:
        return "canceled"
    if status in {"failed", "rejected"}:
        return "failed"
    if status in {"expired", "revoked"}:
        return status
    return "claimed"


def _auto_execution_gate(
    *,
    intent: dict[str, Any],
    trading_environment: str,
) -> tuple[bool, str | None]:
    payload = _intent_payload(intent)
    approval_mode = str(payload.get("approval_mode") or "manual").strip().lower()
    execution_mode = str(payload.get("execution_mode") or "paper").strip().lower()
    if approval_mode != "auto":
        return False, "manual_approval_required"
    if execution_mode not in AUTO_EXECUTION_MODES:
        return False, "unsupported_execution_mode"
    if execution_mode == "paper" and trading_environment != "paper":
        return False, "paper_execution_requires_paper_environment"
    return True, None


@with_storage()
def submit_execution_intent(
    *,
    db_target: str,
    execution_intent_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    signal_store = storage.signals
    if not execution_store.intent_schema_ready():
        raise ValueError("Execution intent tables are not available yet.")
    intent = execution_store.get_execution_intent(execution_intent_id)
    if intent is None:
        raise ValueError(f"Unknown execution_intent_id: {execution_intent_id}")
    state = str(intent.get("state") or "")
    if state not in {"pending", "claimed"}:
        return {
            "action": "submit_execution_intent",
            "changed": False,
            "message": "Execution intent is no longer pending submission.",
            "execution_intent": intent,
        }

    claim_token = str(intent.get("claim_token") or uuid4().hex)
    claimed_intent = _update_intent(
        execution_store,
        dict(intent),
        state="claimed",
        payload_updates={"dispatch_status": "claimed"},
        updated_at=_utc_now(),
    )
    if not _as_text(claimed_intent.get("claim_token")):
        claimed_intent = execution_store.upsert_execution_intent(
            execution_intent_id=str(claimed_intent["execution_intent_id"]),
            bot_id=str(claimed_intent["bot_id"]),
            automation_id=str(claimed_intent["automation_id"]),
            opportunity_decision_id=_as_text(
                claimed_intent.get("opportunity_decision_id")
            ),
            strategy_position_id=_as_text(claimed_intent.get("strategy_position_id")),
            execution_attempt_id=_as_text(claimed_intent.get("execution_attempt_id")),
            action_type=str(claimed_intent["action_type"]),
            slot_key=str(claimed_intent["slot_key"]),
            claim_token=claim_token,
            policy_ref=dict(claimed_intent.get("policy_ref") or {}),
            config_hash=str(claimed_intent.get("config_hash") or ""),
            state="claimed",
            expires_at=_as_text(claimed_intent.get("expires_at")),
            superseded_by_id=_as_text(claimed_intent.get("superseded_by_id")),
            payload=_intent_payload(claimed_intent),
            created_at=str(claimed_intent["created_at"]),
            updated_at=_utc_now(),
        )
    _append_event(
        execution_store,
        execution_intent_id=execution_intent_id,
        event_type="claimed",
        payload={"claim_token": claim_token},
    )

    try:
        if intent.get("opportunity_decision_id"):
            decision = signal_store.get_opportunity_decision(
                str(intent["opportunity_decision_id"])
            )
            if decision is None:
                raise ValueError(
                    f"Missing opportunity decision for execution intent {execution_intent_id}"
                )
            result = submit_opportunity_execution(
                db_target=db_target,
                opportunity_id=str(decision["opportunity_id"]),
                request_metadata={
                    "execution_intent_id": execution_intent_id,
                    "bot_id": intent.get("bot_id"),
                    "automation_id": intent.get("automation_id"),
                },
                storage=storage,
            )
        elif intent.get("strategy_position_id"):
            payload = _intent_payload(intent)
            result = submit_position_close_by_id(
                db_target=db_target,
                position_id=str(intent["strategy_position_id"]),
                limit_price=(
                    None
                    if payload.get("limit_price") in (None, "")
                    else float(payload["limit_price"])
                ),
                request_metadata={
                    "execution_intent_id": execution_intent_id,
                    "bot_id": intent.get("bot_id"),
                    "automation_id": intent.get("automation_id"),
                },
                storage=storage,
            )
        else:
            raise ValueError(
                f"Execution intent {execution_intent_id} is missing its source reference"
            )
    except Exception as exc:
        failed_intent = _update_intent(
            execution_store,
            dict(claimed_intent),
            state="failed",
            payload_updates={"dispatch_status": "failed", "error": str(exc)},
            updated_at=_utc_now(),
        )
        _append_event(
            execution_store,
            execution_intent_id=execution_intent_id,
            event_type="failed",
            payload={"error": str(exc)},
        )
        return {
            "action": "submit_execution_intent",
            "changed": False,
            "message": str(exc),
            "execution_intent": failed_intent,
        }

    attempt = result.get("attempt") if isinstance(result.get("attempt"), dict) else None
    linked_attempt_id = (
        None if attempt is None else _as_text(attempt.get("execution_attempt_id"))
    )
    next_state = _attempt_state(attempt)
    linked_intent = _update_intent(
        execution_store,
        dict(claimed_intent),
        state=next_state,
        execution_attempt_id=linked_attempt_id,
        payload_updates={
            "dispatch_status": next_state,
            **(
                {}
                if linked_attempt_id is None
                else {"execution_attempt_id": linked_attempt_id}
            ),
        },
        updated_at=_utc_now(),
    )
    _append_event(
        execution_store,
        execution_intent_id=execution_intent_id,
        event_type="queued_for_submission"
        if linked_attempt_id is not None
        else "submit_noop",
        payload={
            "execution_attempt_id": linked_attempt_id,
            "attempt_status": None if attempt is None else attempt.get("status"),
            "changed": bool(result.get("changed", False)),
        },
    )
    return {
        "action": "submit_execution_intent",
        "changed": True,
        "result": result,
        "execution_intent": linked_intent,
    }


@with_storage()
def dispatch_pending_execution_intents(
    *,
    db_target: str,
    limit: int = 25,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    if not execution_store.intent_schema_ready():
        return {"status": "skipped", "reason": "execution_intent_schema_unavailable"}

    client = create_alpaca_client_from_env()
    trading_environment = resolve_trading_environment(client.trading_base_url)
    intents = [
        dict(row)
        for row in execution_store.list_execution_intents(
            states=["pending"],
            limit=max(int(limit), 1) * 5,
        )
    ]
    intents.sort(
        key=lambda row: parse_datetime(_as_text(row.get("created_at")))
        or datetime.min.replace(tzinfo=UTC)
    )
    submitted = 0
    skipped = 0
    expired = 0
    failed = 0
    reviewed = 0
    results: list[dict[str, Any]] = []
    for intent in intents:
        if reviewed >= max(int(limit), 1):
            break
        reviewed += 1
        execution_intent_id = str(intent["execution_intent_id"])
        expires_at = parse_datetime(_as_text(intent.get("expires_at")))
        if expires_at is not None and expires_at <= datetime.now(UTC):
            updated = _update_intent(
                execution_store,
                intent,
                state="expired",
                payload_updates={"dispatch_status": "expired"},
                updated_at=_utc_now(),
            )
            _append_event(
                execution_store,
                execution_intent_id=execution_intent_id,
                event_type="expired",
                payload={"reason": "expired_before_dispatch"},
            )
            expired += 1
            results.append(
                {
                    "execution_intent_id": execution_intent_id,
                    "status": "expired",
                    "intent": updated,
                }
            )
            continue

        allowed, reason = _auto_execution_gate(
            intent=intent,
            trading_environment=trading_environment,
        )
        if not allowed:
            if reason == "paper_execution_requires_paper_environment":
                updated = _update_intent(
                    execution_store,
                    intent,
                    state="failed",
                    payload_updates={"dispatch_status": reason},
                    updated_at=_utc_now(),
                )
                _append_event(
                    execution_store,
                    execution_intent_id=execution_intent_id,
                    event_type="failed",
                    payload={
                        "reason": reason,
                        "trading_environment": trading_environment,
                    },
                )
                failed += 1
                results.append(
                    {
                        "execution_intent_id": execution_intent_id,
                        "status": "failed",
                        "intent": updated,
                    }
                )
            else:
                skipped += 1
                results.append(
                    {
                        "execution_intent_id": execution_intent_id,
                        "status": "pending",
                        "reason": reason,
                    }
                )
            continue

        result = submit_execution_intent(
            db_target=db_target,
            execution_intent_id=execution_intent_id,
            storage=storage,
        )
        final_intent = (
            result.get("execution_intent")
            if isinstance(result.get("execution_intent"), dict)
            else None
        )
        final_state = (
            None if final_intent is None else str(final_intent.get("state") or "")
        )
        if final_state == "failed":
            failed += 1
        else:
            submitted += 1
        results.append(
            {
                "execution_intent_id": execution_intent_id,
                "status": final_state or "submitted",
                "result": result,
            }
        )

    return {
        "status": "ok",
        "trading_environment": trading_environment,
        "reviewed": reviewed,
        "submitted": submitted,
        "skipped": skipped,
        "expired": expired,
        "failed": failed,
        "results": results[:25],
    }


__all__ = ["dispatch_pending_execution_intents", "submit_execution_intent"]
