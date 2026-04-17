from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from core.db.decorators import with_storage
from core.services.alpaca import (
    create_alpaca_client_from_env,
    resolve_trading_environment,
)
from core.services.execution import (
    submit_opportunity_execution,
    submit_position_close_by_id,
)
from core.storage.serializers import parse_datetime

from .maintenance import (
    _auto_execution_gate,
    _backfill_strategy_position_links,
    _cleanup_slot_conflicts,
    _cleanup_stale_automation_opportunities,
    _cleanup_terminal_intent_history,
    _intent_execution_policy,
    _intent_exit_policy,
)
from .repricing import _manage_submitted_open_intents
from .shared import (
    _append_event,
    _as_text,
    _attempt_state,
    _intent_payload,
    _update_intent,
    _utc_now,
)


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

    source_intent = dict(claimed_intent)
    payload = _intent_payload(source_intent)
    policy_ref = dict(source_intent.get("policy_ref") or {})
    execution_policy = _intent_execution_policy(source_intent)
    exit_policy = _intent_exit_policy(source_intent)

    try:
        if source_intent.get("opportunity_decision_id"):
            decision = signal_store.get_opportunity_decision(
                str(source_intent["opportunity_decision_id"])
            )
            if decision is None:
                raise ValueError(
                    f"Missing opportunity decision for execution intent {execution_intent_id}"
                )
            request_metadata = {
                "execution_intent_id": execution_intent_id,
                "bot_id": source_intent.get("bot_id"),
                "automation_id": source_intent.get("automation_id"),
                "strategy_config_id": policy_ref.get("strategy_config_id"),
                "strategy_id": policy_ref.get("strategy_id"),
                "config_hash": source_intent.get("config_hash"),
            }
            if execution_policy is not None:
                request_metadata["execution_policy"] = execution_policy
            if exit_policy is not None:
                request_metadata["exit_policy"] = exit_policy
            result = submit_opportunity_execution(
                db_target=db_target,
                opportunity_id=str(decision["opportunity_id"]),
                limit_price=(
                    None
                    if payload.get("limit_price") in (None, "")
                    else float(payload["limit_price"])
                ),
                request_metadata=request_metadata,
                storage=storage,
            )
        elif source_intent.get("strategy_position_id"):
            result = submit_position_close_by_id(
                db_target=db_target,
                position_id=str(source_intent["strategy_position_id"]),
                limit_price=(
                    None
                    if payload.get("limit_price") in (None, "")
                    else float(payload["limit_price"])
                ),
                request_metadata={
                    "execution_intent_id": execution_intent_id,
                    "bot_id": source_intent.get("bot_id"),
                    "automation_id": source_intent.get("automation_id"),
                    "strategy_config_id": policy_ref.get("strategy_config_id"),
                    "strategy_id": policy_ref.get("strategy_id"),
                    "config_hash": source_intent.get("config_hash"),
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

    batch_limit = max(int(limit), 1)
    market_date = datetime.now(UTC).date().isoformat()
    client = create_alpaca_client_from_env()
    trading_environment = resolve_trading_environment(client.trading_base_url)
    opportunity_cleanup = _cleanup_stale_automation_opportunities(
        signal_store=storage.signals,
        job_store=storage.jobs,
        market_date=market_date,
        limit=batch_limit,
    )
    intent_cleanup = _cleanup_terminal_intent_history(
        execution_store,
        limit=batch_limit,
    )
    position_linkage = _backfill_strategy_position_links(
        execution_store,
        limit=batch_limit,
    )
    slot_cleanup = _cleanup_slot_conflicts(
        execution_store,
        limit=batch_limit,
    )
    active_management = _manage_submitted_open_intents(
        db_target=db_target,
        storage=storage,
        execution_store=execution_store,
        limit=batch_limit,
    )
    intents = [
        dict(row)
        for row in execution_store.list_execution_intents(
            states=["pending"],
            limit=batch_limit * 5,
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
        if reviewed >= batch_limit:
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
            if reason in {"bot_entry_cutoff_reached", "bot_live_disabled"}:
                updated = _update_intent(
                    execution_store,
                    intent,
                    state="revoked",
                    payload_updates={
                        "dispatch_status": "revoked",
                        "revoke_reason": reason,
                    },
                    updated_at=_utc_now(),
                )
                _append_event(
                    execution_store,
                    execution_intent_id=execution_intent_id,
                    event_type="revoked",
                    payload={"reason": reason},
                )
                skipped += 1
                results.append(
                    {
                        "execution_intent_id": execution_intent_id,
                        "status": "revoked",
                        "intent": updated,
                    }
                )
            elif reason == "paper_execution_requires_paper_environment":
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
        "opportunity_cleanup": opportunity_cleanup,
        "intent_cleanup": intent_cleanup,
        "position_linkage": position_linkage,
        "slot_cleanup": slot_cleanup,
        "active_management": active_management,
        "reviewed": reviewed,
        "submitted": submitted,
        "skipped": skipped,
        "expired": expired,
        "failed": failed,
        "results": results[:25],
    }


__all__ = ["dispatch_pending_execution_intents", "submit_execution_intent"]
