from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.services.value_coercion import (
    as_text as _as_text,
    coerce_int as _coerce_int,
)
from core.storage.serializers import parse_datetime

STATUS_RANK = {
    "healthy": 0,
    "idle": 0,
    "unknown": 1,
    "degraded": 2,
    "blocked": 3,
    "halted": 4,
}
OPS_INCIDENT_WINDOW_SECONDS = 24 * 60 * 60
JOB_RUN_QUEUE_STALE_AFTER_SECONDS = 15 * 60
JOB_RUN_HEARTBEAT_STALE_AFTER_SECONDS = 10 * 60
RECENT_FAILURE_LIMIT = 10


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


def _stream_quote_events_saved(capture: Mapping[str, Any] | None) -> int:
    if not isinstance(capture, Mapping):
        return 0
    return (
        _coerce_int(capture.get("stream_quote_events_saved"))
        or _coerce_int(capture.get("websocket_quote_events_saved"))
        or 0
    )


def _stream_trade_events_saved(capture: Mapping[str, Any] | None) -> int:
    if not isinstance(capture, Mapping):
        return 0
    return (
        _coerce_int(capture.get("stream_trade_events_saved"))
        or _coerce_int(capture.get("websocket_trade_events_saved"))
        or 0
    )


def _seconds_since(value: Any, *, now: datetime) -> float | None:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return None
    return max((now - parsed).total_seconds(), 0.0)


def _seconds_until(value: Any, *, now: datetime) -> float | None:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return None
    return (parsed - now).total_seconds()


def _is_recent(
    value: Any,
    *,
    now: datetime,
    within_seconds: int = OPS_INCIDENT_WINDOW_SECONDS,
) -> bool:
    age_seconds = _seconds_since(value, now=now)
    return age_seconds is not None and age_seconds <= within_seconds


def _lease_status(lease: Mapping[str, Any] | None, *, now: datetime) -> str:
    if lease is None:
        return "blocked"
    remaining = _seconds_until(lease.get("expires_at"), now=now)
    if remaining is None or remaining <= 0:
        return "blocked"
    if remaining <= 30:
        return "degraded"
    return "healthy"


def _activity_at(row: Mapping[str, Any]) -> str | None:
    for key in (
        "finished_at",
        "heartbeat_at",
        "started_at",
        "slot_at",
        "scheduled_for",
        "requested_at",
        "updated_at",
    ):
        value = _as_text(row.get(key))
        if value:
            return value
    return None


def _sorted_by_activity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: _parse_timestamp(_activity_at(row))
        or datetime.fromtimestamp(0, UTC),
        reverse=True,
    )


def _run_duration_seconds(run: Mapping[str, Any]) -> float | None:
    started_at = _parse_timestamp(run.get("started_at"))
    finished_at = _parse_timestamp(run.get("finished_at"))
    if started_at is None or finished_at is None:
        return None
    duration_seconds = (finished_at - started_at).total_seconds()
    if duration_seconds < 0:
        return None
    return round(duration_seconds, 3)
