from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from core.services.value_coercion import coerce_int as _coerce_int

from .shared import _seconds_since

BROKER_SYNC_STALE_AFTER_SECONDS = 15 * 60


def broker_sync_payload(
    state: Mapping[str, Any] | None,
    *,
    now: datetime,
    market_session: Mapping[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    if state is None:
        return (
            "blocked",
            {
                "status": "missing",
                "raw_status": None,
                "updated_at": None,
                "summary": {},
                "error_text": None,
                "age_seconds": None,
            },
        )
    payload = dict(state)
    summary = payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
    age_seconds = _seconds_since(payload.get("updated_at"), now=now)
    status = str(payload.get("status") or "unknown")
    normalized = "unknown"
    if status == "healthy":
        normalized = "healthy"
    elif status == "degraded":
        normalized = "degraded"
    elif status == "failed":
        normalized = "blocked"
    open_position_count = _coerce_int(summary.get("open_position_count")) or 0
    queued_attempt_count = _coerce_int(summary.get("queued_attempt_count")) or 0
    requires_freshness = bool((market_session or {}).get("is_open")) or bool(
        open_position_count or queued_attempt_count
    )
    freshness = "current"
    if (
        age_seconds is not None
        and age_seconds > BROKER_SYNC_STALE_AFTER_SECONDS
        and normalized == "healthy"
    ):
        freshness = "stale"
        normalized = "degraded" if requires_freshness else "idle"
    payload["raw_status"] = status
    payload["status"] = normalized
    payload["age_seconds"] = age_seconds
    payload["freshness"] = freshness
    payload["requires_freshness"] = requires_freshness
    payload["market_session"] = dict(market_session or {})
    return normalized, payload


__all__ = [
    "BROKER_SYNC_STALE_AFTER_SECONDS",
    "broker_sync_payload",
]
