from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.services.risk_manager import (
    CLOSE_RECONCILIATION_MAX_AGE_SECONDS,
    validate_close_execution,
)
from core.storage.serializers import parse_datetime

OPEN_POSITION_STATUSES = ("open", "partial_close")


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


def position_status(position: Mapping[str, Any]) -> str:
    return str(position.get("position_status") or position.get("status") or "").lower()


def position_is_open(position: Mapping[str, Any]) -> bool:
    return position_status(position) in OPEN_POSITION_STATUSES


def position_close_block_reason(position: Mapping[str, Any], *, now: datetime) -> str | None:
    position_payload = dict(position)
    status = position_status(position_payload)
    if status and status not in OPEN_POSITION_STATUSES:
        return "position_not_open"

    remaining_quantity = _coerce_float(position_payload.get("remaining_quantity")) or 0.0
    if remaining_quantity <= 0:
        return "no_remaining_quantity"

    reconciliation_status = _as_text(position_payload.get("reconciliation_status"))
    if reconciliation_status != "matched":
        return "awaiting_broker_reconciliation"

    last_reconciled_at = parse_datetime(_as_text(position_payload.get("last_reconciled_at")))
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


__all__ = [
    "OPEN_POSITION_STATUSES",
    "position_close_block_reason",
    "position_is_open",
    "position_status",
]
