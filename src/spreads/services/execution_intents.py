from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from spreads.db.decorators import with_storage
from spreads.services.alpaca import (
    create_alpaca_client_from_env,
    resolve_trading_environment,
)
from spreads.services.deployment_policy import (
    DEPLOYMENT_MODE_LIVE_AUTO,
    DEPLOYMENT_MODE_PAPER_AUTO,
)
from spreads.services.execution import (
    refresh_execution_attempt,
    submit_opportunity_execution,
    submit_position_close_by_id,
)
from spreads.services.option_structures import (
    net_premium_kind,
    normalize_strategy_family,
)
from spreads.storage.serializers import parse_datetime

AUTO_EXECUTION_MODES = {"paper", "live"}
ACTIVE_INTENT_STATES = {"pending", "claimed", "submitted", "partially_filled"}
OPEN_POSITION_STATES = {"open", "partial_open", "partial_close"}
WORKING_REPRICE_ATTEMPT_STATUSES = {
    "accepted",
    "accepted_for_bidding",
    "new",
    "pending_new",
    "replaced",
}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _intent_payload(intent: dict[str, Any]) -> dict[str, Any]:
    payload = intent.get("payload")
    if isinstance(payload, dict):
        return dict(payload)
    payload_json = intent.get("payload_json")
    if isinstance(payload_json, dict):
        return dict(payload_json)
    return {}


def _intent_action_type(
    intent: dict[str, Any], attempt: dict[str, Any] | None = None
) -> str:
    action_type = str(intent.get("action_type") or "").strip().lower()
    if action_type:
        return action_type
    request = {} if attempt is None else _attempt_request(attempt)
    trade_intent = (
        str(
            request.get("trade_intent")
            or (None if attempt is None else attempt.get("trade_intent"))
            or "open"
        )
        .strip()
        .lower()
    )
    return "close" if trade_intent == "close" else "open"


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


def _attempt_request(attempt: dict[str, Any]) -> dict[str, Any]:
    payload = attempt.get("request")
    return dict(payload) if isinstance(payload, dict) else {}


def _reprice_count(intent: dict[str, Any]) -> int:
    payload = _intent_payload(intent)
    try:
        return int(payload.get("reprice_count") or 0)
    except (TypeError, ValueError):
        return 0


def _submitted_age_seconds(attempt: dict[str, Any]) -> float | None:
    submitted_at = parse_datetime(_as_text(attempt.get("submitted_at")))
    if submitted_at is None:
        submitted_at = parse_datetime(_as_text(attempt.get("requested_at")))
    if submitted_at is None:
        return None
    return max((datetime.now(UTC) - submitted_at).total_seconds(), 0.0)


def _next_reprice_limit(
    intent: dict[str, Any], attempt: dict[str, Any]
) -> float | None:
    request = _attempt_request(attempt)
    candidate = (
        request.get("candidate") if isinstance(request.get("candidate"), dict) else {}
    )
    execution_policy = (
        request.get("execution_policy")
        if isinstance(request.get("execution_policy"), dict)
        else {}
    )
    current_limit = _coerce_float(attempt.get("requested_limit_price"))
    if current_limit is None:
        current_limit = _coerce_float(attempt.get("limit_price"))
    if current_limit is None:
        return None
    midpoint_value = _coerce_float(
        candidate.get("midpoint_credit")
        or candidate.get("midpoint_debit")
        or candidate.get("midpoint_value")
    )
    natural_value = _coerce_float(
        candidate.get("natural_credit")
        or candidate.get("natural_debit")
        or candidate.get("natural_value")
    )
    max_credit_concession = max(
        _coerce_float(execution_policy.get("max_credit_concession")) or 0.02,
        0.0,
    )
    step = 0.01
    premium_kind = net_premium_kind(
        normalize_strategy_family(
            attempt.get("strategy_family") or attempt.get("strategy")
        )
    )
    if _intent_action_type(intent, attempt) == "close":
        if premium_kind == "credit":
            premium_kind = "debit"
        elif premium_kind == "debit":
            premium_kind = "credit"
    if premium_kind == "debit":
        ceiling = current_limit + max_credit_concession
        target = min(round(current_limit + step, 2), round(ceiling, 2))
        if natural_value is not None:
            target = min(target, round(max(natural_value, current_limit), 2))
        if target <= current_limit:
            return None
        return target

    floor = round(max(current_limit - max_credit_concession, 0.01), 2)
    target = max(round(current_limit - step, 2), floor)
    if natural_value is not None:
        target = min(target, round(current_limit - step, 2))
    if target >= current_limit:
        return None
    return max(target, 0.01)


def _replacement_intent_id() -> str:
    return f"execution_intent:{uuid4().hex}"


def _create_replacement_intent(
    execution_store: Any,
    *,
    intent: dict[str, Any],
    attempt: dict[str, Any],
) -> dict[str, Any] | None:
    next_limit = _next_reprice_limit(intent, attempt)
    if next_limit is None:
        updated = _update_intent(
            execution_store,
            intent,
            state="failed",
            payload_updates={"dispatch_status": "reprice_exhausted"},
            updated_at=_utc_now(),
        )
        _append_event(
            execution_store,
            execution_intent_id=str(intent["execution_intent_id"]),
            event_type="reprice_exhausted",
            payload={"execution_attempt_id": attempt.get("execution_attempt_id")},
        )
        return updated
    now = _utc_now()
    replacement_id = _replacement_intent_id()
    payload = _intent_payload(intent)
    payload.update(
        {
            "limit_price": next_limit,
            "reprice_count": _reprice_count(intent) + 1,
            "dispatch_status": "pending",
            "supersedes_execution_intent_id": str(intent["execution_intent_id"]),
            "previous_execution_attempt_id": _as_text(
                attempt.get("execution_attempt_id")
            ),
        }
    )
    execution_store.upsert_execution_intent(
        execution_intent_id=replacement_id,
        bot_id=str(intent["bot_id"]),
        automation_id=str(intent["automation_id"]),
        opportunity_decision_id=_as_text(intent.get("opportunity_decision_id")),
        strategy_position_id=_as_text(intent.get("strategy_position_id")),
        execution_attempt_id=None,
        action_type=str(intent["action_type"]),
        slot_key=str(intent["slot_key"]),
        claim_token=None,
        policy_ref=dict(intent.get("policy_ref") or {}),
        config_hash=str(intent.get("config_hash") or ""),
        state="pending",
        expires_at=_as_text(intent.get("expires_at")),
        superseded_by_id=None,
        payload=payload,
        created_at=now,
        updated_at=now,
    )
    updated = execution_store.upsert_execution_intent(
        execution_intent_id=str(intent["execution_intent_id"]),
        bot_id=str(intent["bot_id"]),
        automation_id=str(intent["automation_id"]),
        opportunity_decision_id=_as_text(intent.get("opportunity_decision_id")),
        strategy_position_id=_as_text(intent.get("strategy_position_id")),
        execution_attempt_id=_as_text(attempt.get("execution_attempt_id")),
        action_type=str(intent["action_type"]),
        slot_key=str(intent["slot_key"]),
        claim_token=_as_text(intent.get("claim_token")),
        policy_ref=dict(intent.get("policy_ref") or {}),
        config_hash=str(intent.get("config_hash") or ""),
        state="canceled",
        expires_at=_as_text(intent.get("expires_at")),
        superseded_by_id=replacement_id,
        payload={
            **_intent_payload(intent),
            "dispatch_status": "canceled_for_reprice",
            "replacement_execution_intent_id": replacement_id,
        },
        created_at=str(intent["created_at"]),
        updated_at=now,
    )
    _append_event(
        execution_store,
        execution_intent_id=str(intent["execution_intent_id"]),
        event_type="replaced",
        payload={
            "replacement_execution_intent_id": replacement_id,
            "next_limit_price": next_limit,
        },
    )
    _append_event(
        execution_store,
        execution_intent_id=replacement_id,
        event_type="created",
        payload={
            "reprice_count": payload.get("reprice_count"),
            "limit_price": next_limit,
            "replaces_execution_intent_id": str(intent["execution_intent_id"]),
        },
    )
    return updated


def _manage_submitted_open_intents(
    *,
    db_target: str,
    storage: Any,
    execution_store: Any,
    limit: int,
    stale_after_seconds: int = 60,
) -> dict[str, Any]:
    intents = [
        dict(row)
        for row in execution_store.list_execution_intents(
            states=["submitted"],
            limit=max(int(limit), 1) * 5,
        )
    ]
    reviewed = 0
    repriced = 0
    canceled = 0
    refreshed = 0
    results: list[dict[str, Any]] = []
    client = create_alpaca_client_from_env()
    for intent in intents:
        if reviewed >= max(int(limit), 1):
            break
        reviewed += 1
        action_type = _intent_action_type(intent)
        execution_attempt_id = _as_text(intent.get("execution_attempt_id"))
        if execution_attempt_id is None:
            continue
        refreshed_result = refresh_execution_attempt(
            db_target=db_target,
            execution_attempt_id=execution_attempt_id,
            storage=storage,
        )
        refreshed_attempt = (
            refreshed_result.get("attempt")
            if isinstance(refreshed_result.get("attempt"), dict)
            else None
        )
        if refreshed_attempt is None:
            continue
        refreshed += 1
        status = str(refreshed_attempt.get("status") or "").strip().lower()
        if status in {
            "filled",
            "failed",
            "canceled",
            "cancelled",
            "expired",
            "rejected",
        }:
            if status in {"canceled", "cancelled"}:
                replacement = _create_replacement_intent(
                    execution_store,
                    intent=intent,
                    attempt=refreshed_attempt,
                )
                if replacement is not None:
                    repriced += 1
                    results.append(
                        {
                            "execution_intent_id": str(intent["execution_intent_id"]),
                            "status": "replaced",
                            "replacement_execution_intent_id": replacement.get(
                                "superseded_by_id"
                            ),
                        }
                    )
            continue
        if status not in WORKING_REPRICE_ATTEMPT_STATUSES:
            continue
        if action_type == "open":
            active, inactive_reason = _opportunity_is_active_for_intent(
                storage.signals,
                intent,
                execution_attempt_id=execution_attempt_id,
            )
        else:
            active, inactive_reason = _position_is_active_for_intent(
                execution_store,
                intent,
            )
        age_seconds = _submitted_age_seconds(refreshed_attempt)
        if age_seconds is None or age_seconds < float(max(stale_after_seconds, 1)):
            continue
        broker_order_id = _as_text(refreshed_attempt.get("broker_order_id"))
        if broker_order_id is None:
            continue
        client.cancel_order(broker_order_id)
        canceled += 1
        _append_event(
            execution_store,
            execution_intent_id=str(intent["execution_intent_id"]),
            event_type="cancel_requested_for_reprice",
            payload={
                "execution_attempt_id": execution_attempt_id,
                "broker_order_id": broker_order_id,
                "age_seconds": age_seconds,
            },
        )
        post_cancel = refresh_execution_attempt(
            db_target=db_target,
            execution_attempt_id=execution_attempt_id,
            storage=storage,
        )
        post_cancel_attempt = (
            post_cancel.get("attempt")
            if isinstance(post_cancel.get("attempt"), dict)
            else refreshed_attempt
        )
        post_cancel_status = (
            str(post_cancel_attempt.get("status") or "").strip().lower()
        )
        if not active and post_cancel_status in {"canceled", "cancelled"}:
            updated = execution_store.upsert_execution_intent(
                execution_intent_id=str(intent["execution_intent_id"]),
                bot_id=str(intent["bot_id"]),
                automation_id=str(intent["automation_id"]),
                opportunity_decision_id=_as_text(intent.get("opportunity_decision_id")),
                strategy_position_id=_as_text(intent.get("strategy_position_id")),
                execution_attempt_id=execution_attempt_id,
                action_type=str(intent["action_type"]),
                slot_key=str(intent["slot_key"]),
                claim_token=_as_text(intent.get("claim_token")),
                policy_ref=dict(intent.get("policy_ref") or {}),
                config_hash=str(intent.get("config_hash") or ""),
                state="revoked",
                expires_at=_as_text(intent.get("expires_at")),
                superseded_by_id=None,
                payload={
                    **_intent_payload(intent),
                    "dispatch_status": "revoked",
                    "revoke_reason": inactive_reason,
                },
                created_at=str(intent["created_at"]),
                updated_at=_utc_now(),
            )
            _append_event(
                execution_store,
                execution_intent_id=str(intent["execution_intent_id"]),
                event_type="revoked",
                payload={
                    "reason": inactive_reason,
                    "execution_attempt_id": execution_attempt_id,
                },
            )
            results.append(
                {
                    "execution_intent_id": str(intent["execution_intent_id"]),
                    "status": updated.get("state"),
                    "reason": inactive_reason,
                }
            )
            continue
        if post_cancel_status in {"canceled", "cancelled"}:
            replacement = _create_replacement_intent(
                execution_store,
                intent=intent,
                attempt=post_cancel_attempt,
            )
            if replacement is not None:
                repriced += 1
                results.append(
                    {
                        "execution_intent_id": str(intent["execution_intent_id"]),
                        "status": "replaced",
                        "replacement_execution_intent_id": replacement.get(
                            "superseded_by_id"
                        ),
                    }
                )
                continue
        results.append(
            {
                "execution_intent_id": str(intent["execution_intent_id"]),
                "status": post_cancel_status,
                "execution_attempt_id": execution_attempt_id,
            }
        )
    return {
        "reviewed": reviewed,
        "refreshed": refreshed,
        "cancel_requested": canceled,
        "repriced": repriced,
        "results": results,
    }


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


def _intent_execution_policy(intent: dict[str, Any]) -> dict[str, Any] | None:
    payload = _intent_payload(intent)
    approval_mode = str(payload.get("approval_mode") or "manual").strip().lower()
    execution_mode = str(payload.get("execution_mode") or "paper").strip().lower()
    if approval_mode != "auto":
        return None
    if execution_mode == "paper":
        return {"deployment_mode": DEPLOYMENT_MODE_PAPER_AUTO}
    if execution_mode == "live":
        return {"deployment_mode": DEPLOYMENT_MODE_LIVE_AUTO}
    return None


def _resolve_intent_opportunity_id(
    signal_store: Any,
    intent: dict[str, Any],
) -> str | None:
    opportunity_decision_id = _as_text(intent.get("opportunity_decision_id"))
    if opportunity_decision_id:
        decision = signal_store.get_opportunity_decision(opportunity_decision_id)
        if decision is not None:
            return _as_text(decision.get("opportunity_id"))
    return _as_text(_intent_payload(intent).get("opportunity_id"))


def _opportunity_is_active_for_intent(
    signal_store: Any,
    intent: dict[str, Any],
    *,
    execution_attempt_id: str | None = None,
) -> tuple[bool, str | None]:
    opportunity_id = _resolve_intent_opportunity_id(signal_store, intent)
    if opportunity_id is None:
        return False, "opportunity_missing"
    opportunity = signal_store.get_opportunity(opportunity_id)
    if opportunity is None:
        return False, "opportunity_missing"
    lifecycle_state = str(opportunity.get("lifecycle_state") or "")
    eligibility_state = str(opportunity.get("eligibility_state") or "")
    if lifecycle_state not in {"candidate", "ready", "blocked"}:
        return False, "opportunity_inactive"
    if eligibility_state != "live":
        return False, "opportunity_not_live"
    consumed = _as_text(opportunity.get("consumed_by_execution_attempt_id"))
    if execution_attempt_id and consumed not in {None, "", execution_attempt_id}:
        return False, "opportunity_consumed_elsewhere"
    return True, None


def _position_is_active_for_intent(
    execution_store: Any,
    intent: dict[str, Any],
) -> tuple[bool, str | None]:
    strategy_position_id = _as_text(intent.get("strategy_position_id"))
    if strategy_position_id is None:
        return False, "position_missing"
    position = execution_store.get_position(strategy_position_id)
    if position is None:
        return False, "position_missing"
    status = str(position.get("status") or "")
    if status not in OPEN_POSITION_STATES:
        return False, "position_closed"
    return True, None


def _cleanup_slot_conflicts(
    execution_store: Any,
    *,
    limit: int,
) -> dict[str, Any]:
    rows = [
        dict(row)
        for row in execution_store.list_execution_intents(
            limit=max(int(limit), 1) * 20,
        )
    ]
    slots: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        slot_key = str(row.get("slot_key") or "")
        if not slot_key:
            continue
        slots.setdefault(slot_key, []).append(row)

    revoked = 0
    results: list[dict[str, Any]] = []
    for slot_key, intents in slots.items():
        intents.sort(
            key=lambda row: parse_datetime(_as_text(row.get("created_at")))
            or datetime.min.replace(tzinfo=UTC),
            reverse=True,
        )
        anchor_id: str | None = None
        for intent in intents:
            state = str(intent.get("state") or "")
            intent_id = str(intent["execution_intent_id"])
            if anchor_id is None and state in ACTIVE_INTENT_STATES.union({"filled"}):
                anchor_id = intent_id
                continue
            if anchor_id is None:
                continue
            if state not in {"pending", "claimed"}:
                continue
            if _as_text(intent.get("execution_attempt_id")):
                continue
            updated = execution_store.upsert_execution_intent(
                execution_intent_id=intent_id,
                bot_id=str(intent["bot_id"]),
                automation_id=str(intent["automation_id"]),
                opportunity_decision_id=_as_text(intent.get("opportunity_decision_id")),
                strategy_position_id=_as_text(intent.get("strategy_position_id")),
                execution_attempt_id=None,
                action_type=str(intent["action_type"]),
                slot_key=slot_key,
                claim_token=_as_text(intent.get("claim_token")),
                policy_ref=dict(intent.get("policy_ref") or {}),
                config_hash=str(intent.get("config_hash") or ""),
                state="revoked",
                expires_at=_as_text(intent.get("expires_at")),
                superseded_by_id=anchor_id,
                payload={
                    **_intent_payload(intent),
                    "dispatch_status": "revoked",
                    "revoked_by_execution_intent_id": anchor_id,
                },
                created_at=str(intent["created_at"]),
                updated_at=_utc_now(),
            )
            _append_event(
                execution_store,
                execution_intent_id=intent_id,
                event_type="revoked",
                payload={
                    "reason": "slot_conflict",
                    "anchor_execution_intent_id": anchor_id,
                },
            )
            revoked += 1
            results.append(
                {
                    "execution_intent_id": intent_id,
                    "slot_key": slot_key,
                    "status": updated.get("state"),
                    "anchor_execution_intent_id": anchor_id,
                }
            )
    return {"revoked": revoked, "results": results[:25]}


def _backfill_strategy_position_links(
    execution_store: Any, *, limit: int
) -> dict[str, Any]:
    linked = 0
    results: list[dict[str, Any]] = []
    positions = [
        dict(row)
        for row in execution_store.list_positions(limit=max(int(limit), 1) * 10)
    ]
    for position in positions:
        position_id = str(position.get("position_id") or "")
        open_execution_attempt_id = _as_text(position.get("open_execution_attempt_id"))
        if not position_id or open_execution_attempt_id is None:
            continue
        attempt = execution_store.get_attempt(open_execution_attempt_id)
        if attempt is None:
            continue
        request = (
            attempt.get("request") if isinstance(attempt.get("request"), dict) else {}
        )
        execution_intent_id = _as_text(request.get("execution_intent_id"))
        if execution_intent_id is None:
            continue
        intent = execution_store.get_execution_intent(execution_intent_id)
        if intent is None:
            continue
        if _as_text(intent.get("strategy_position_id")) == position_id:
            continue
        updated = execution_store.upsert_execution_intent(
            execution_intent_id=str(intent["execution_intent_id"]),
            bot_id=str(intent["bot_id"]),
            automation_id=str(intent["automation_id"]),
            opportunity_decision_id=_as_text(intent.get("opportunity_decision_id")),
            strategy_position_id=position_id,
            execution_attempt_id=_as_text(intent.get("execution_attempt_id")),
            action_type=str(intent["action_type"]),
            slot_key=str(intent["slot_key"]),
            claim_token=_as_text(intent.get("claim_token")),
            policy_ref=dict(intent.get("policy_ref") or {}),
            config_hash=str(intent.get("config_hash") or ""),
            state=str(intent.get("state") or ""),
            expires_at=_as_text(intent.get("expires_at")),
            superseded_by_id=_as_text(intent.get("superseded_by_id")),
            payload={
                **_intent_payload(intent),
                "strategy_position_id": position_id,
            },
            created_at=str(intent["created_at"]),
            updated_at=_utc_now(),
        )
        linked += 1
        results.append(
            {
                "execution_intent_id": str(intent["execution_intent_id"]),
                "strategy_position_id": position_id,
                "state": updated.get("state"),
            }
        )
    return {"linked": linked, "results": results[:25]}


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
            payload = _intent_payload(intent)
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
                limit_price=(
                    None
                    if payload.get("limit_price") in (None, "")
                    else float(payload["limit_price"])
                ),
                request_metadata={
                    "execution_intent_id": execution_intent_id,
                    "bot_id": intent.get("bot_id"),
                    "automation_id": intent.get("automation_id"),
                    **(
                        {}
                        if _intent_execution_policy(intent) is None
                        else {"execution_policy": _intent_execution_policy(intent)}
                    ),
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
    position_linkage = _backfill_strategy_position_links(
        execution_store,
        limit=max(int(limit), 1),
    )
    slot_cleanup = _cleanup_slot_conflicts(
        execution_store,
        limit=max(int(limit), 1),
    )
    active_management = _manage_submitted_open_intents(
        db_target=db_target,
        storage=storage,
        execution_store=execution_store,
        limit=max(int(limit), 1),
    )
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
