from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.services.execution_lifecycle import OPEN_ATTEMPT_STATUS_LIST
from core.services.risk_manager import (
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


def close_execution_block_reason(
    execution_store: Any,
    *,
    position: Mapping[str, Any],
    now: datetime,
) -> str | None:
    position_id = as_text(position.get("position_id"))
    if position_id is not None and has_open_close_attempt(execution_store, position_id=position_id):
        return "close_already_open"
    return position_close_block_reason(position, now=now)


def close_intent_block_reason(execution_store: Any, *, position_id: str) -> str | None:
    if not execution_store.intent_schema_ready():
        return "execution_intent_schema_unavailable"
    if has_active_close_intent(execution_store, position_id=position_id):
        return "close_intent_already_open"
    return None


__all__ = [
    "OPEN_POSITION_STATUSES",
    "close_execution_block_reason",
    "close_intent_block_reason",
    "close_intent_id",
    "close_slot_key",
    "has_active_close_intent",
    "has_open_close_attempt",
    "position_close_block_reason",
    "position_is_open",
    "position_status",
]
