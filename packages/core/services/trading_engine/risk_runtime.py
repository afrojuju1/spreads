from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.money import option_contract_notional
from core.services.admission_lifecycle import normalize_lifecycle_admission
from core.services.execution_lifecycle import OPEN_ATTEMPT_STATUS_LIST
from core.services.risk.admission import (
    CLOSE_RECONCILIATION_MAX_AGE_SECONDS,
    validate_close_execution,
)
from core.value_coercion import as_text, coerce_float
from core.storage.serializers import parse_datetime

OPEN_POSITION_STATUSES = ("open", "partial_close")


def position_status(position: Mapping[str, Any]) -> str:
    return str(position.get("position_status") or position.get("status") or "").lower()


def position_is_open(position: Mapping[str, Any]) -> bool:
    return position_status(position) in OPEN_POSITION_STATUSES


def close_slot_key(position_id: str) -> str:
    return f"manage:{position_id}:close"


def close_intent_id(*, position_id: str, trading_strategy_id: str) -> str:
    return f"execution_intent:manage:{trading_strategy_id}:{position_id}"


def has_open_close_attempt(execution_store: Any, *, position_id: str) -> bool:
    return bool(
        execution_store.list_open_attempts_for_position(
            position_id=position_id,
            statuses=list(OPEN_ATTEMPT_STATUS_LIST),
        )
    )


def has_active_close_intent(execution_store: Any, *, position_id: str) -> bool:
    if not execution_store.intent_schema_ready():
        return False
    from core.services.execution_intents.shared import ACTIVE_INTENT_STATES

    return bool(
        execution_store.list_execution_intents(
            slot_key=close_slot_key(position_id),
            states=sorted(ACTIVE_INTENT_STATES),
            limit=1,
        )
    )


def position_close_block_reason(position: Mapping[str, Any], *, now: datetime) -> str | None:
    position_payload = dict(position)
    status = position_status(position_payload)
    if status and status not in OPEN_POSITION_STATUSES:
        return "position_not_open"

    remaining_quantity = coerce_float(position_payload.get("remaining_quantity")) or 0.0
    if remaining_quantity <= 0:
        return "no_remaining_quantity"

    reconciliation_status = as_text(position_payload.get("reconciliation_status"))
    if reconciliation_status != "matched":
        return "awaiting_broker_reconciliation"

    last_reconciled_at = parse_datetime(as_text(position_payload.get("last_reconciled_at")))
    if last_reconciled_at is None:
        return "awaiting_broker_reconciliation"

    reconciliation_age_seconds = (now - last_reconciled_at.astimezone(UTC)).total_seconds()
    if reconciliation_age_seconds > CLOSE_RECONCILIATION_MAX_AGE_SECONDS:
        return "broker_reconciliation_stale"

    try:
        validate_close_execution(
            position=position_payload,
            quantity=max(int(remaining_quantity), 1),
            now=now,
            max_reconciliation_age_seconds=CLOSE_RECONCILIATION_MAX_AGE_SECONDS,
        )
    except ValueError as exc:
        error_text = str(exc)
        if "broker symbols" in error_text:
            return "close_symbols_missing"
        return "close_validation_blocked"
    return None


def evaluate_close_admission(
    execution_store: Any,
    *,
    position: Mapping[str, Any],
    now: datetime,
    extra_blocker: str | None = None,
) -> dict[str, Any]:
    position_id = as_text(position.get("position_id"))
    reason: str | None = None
    if extra_blocker is not None:
        reason = extra_blocker
    elif position_id is not None and has_open_close_attempt(execution_store, position_id=position_id):
        reason = "close_already_open"
    elif position_id is not None and not execution_store.intent_schema_ready():
        reason = "execution_intent_schema_unavailable"
    elif position_id is not None and has_active_close_intent(execution_store, position_id=position_id):
        reason = "close_intent_already_open"
    else:
        reason = position_close_block_reason(position, now=now)

    close_decision = position.get("close_decision") if isinstance(position.get("close_decision"), Mapping) else {}
    close_decision_id = as_text(close_decision.get("close_decision_id"))
    remaining_quantity = coerce_float(position.get("remaining_quantity")) or 0.0
    requested_quantity = None if remaining_quantity <= 0 else max(int(remaining_quantity), 1)
    limit_price = coerce_float(close_decision.get("limit_price"))
    status = "approved" if reason is None else "blocked"
    return normalize_lifecycle_admission(
        {
            "status": status,
            "reason": "approved" if reason is None else reason,
            "message": "Close request passed position, reconciliation, and active-close validation."
            if reason is None
            else "Close request is blocked before intent creation.",
            "evaluated_at": now.isoformat().replace("+00:00", "Z") if now.tzinfo is not None else now.isoformat(),
            "admissible_quantity": requested_quantity if reason is None else 0,
        },
        admission_kind="position_close",
        source_object_type="close_decision" if close_decision_id is not None else "position",
        source_object_id=close_decision_id or position_id,
        session_date=as_text(position.get("session_date") or position.get("market_date") or position.get("market_date_opened")),
        requested_quantity=requested_quantity,
        requested_notional=option_contract_notional(limit_price, requested_quantity),
        max_loss=None,
        policy_snapshot=position.get("risk_policy") if isinstance(position.get("risk_policy"), Mapping) else {},
        capability_snapshot={
            "position_status": position_status(position),
            "reconciliation_status": as_text(position.get("reconciliation_status")),
            "active_close_attempt": bool(position_id is not None and has_open_close_attempt(execution_store, position_id=position_id)),
            "active_close_intent": bool(position_id is not None and has_active_close_intent(execution_store, position_id=position_id)),
            "intent_schema_ready": bool(execution_store.intent_schema_ready()),
        },
        metrics={
            "remaining_quantity": remaining_quantity,
            "requested_limit_price": limit_price,
        },
        reason_codes=["approved" if reason is None else reason],
        blockers=[] if reason is None else [reason],
        evidence={
            "position_id": position_id,
            "close_decision_id": close_decision_id,
            "admission_boundary": "close_admission",
        },
        decided_at=now.isoformat().replace("+00:00", "Z") if now.tzinfo is not None else now.isoformat(),
    )


__all__ = [
    "OPEN_POSITION_STATUSES",
    "close_intent_id",
    "close_slot_key",
    "evaluate_close_admission",
    "has_active_close_intent",
    "has_open_close_attempt",
    "position_close_block_reason",
    "position_is_open",
    "position_status",
]
