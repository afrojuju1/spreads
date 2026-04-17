from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from core.services.value_coercion import as_text as _as_text
from core.storage.serializers import parse_datetime

STATUS_RANK = {
    "healthy": 0,
    "idle": 0,
    "unknown": 1,
    "degraded": 2,
    "blocked": 3,
    "halted": 4,
}


class OpsLookupError(LookupError):
    pass


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    text = _as_text(value)
    if text is None:
        return None
    try:
        parsed = parse_datetime(text)
    except ValueError:
        return None
    if parsed is None:
        return None
    return parsed.astimezone(UTC)


def _combine_statuses(*statuses: str | None) -> str:
    normalized = [
        str(status or "unknown").strip().lower()
        for status in statuses
        if status is not None
    ]
    if not normalized:
        return "unknown"
    return max(normalized, key=lambda status: STATUS_RANK.get(status, 1))


def _attention(*, severity: str, code: str, message: str) -> dict[str, str]:
    return {
        "severity": severity,
        "code": code,
        "message": message,
    }


def _control_status(control: dict[str, Any] | Any) -> str:
    mode = str(control.get("mode") or "unknown")
    if mode == "halted":
        return "halted"
    if mode == "degraded":
        return "degraded"
    if mode == "normal":
        return "healthy"
    return "unknown"


def _session_status(status: Any) -> str:
    normalized = str(status or "unknown").strip().lower()
    if normalized == "failed":
        return "blocked"
    if normalized in {"healthy", "idle", "degraded"}:
        return normalized
    return "unknown"
