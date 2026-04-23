from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from core.jobs.orchestration import NEW_YORK, _market_schedule


def market_session_context(
    *,
    now: datetime,
    calendar_name: str = "NYSE",
) -> dict[str, Any]:
    local_now = now.astimezone(NEW_YORK)
    market_window = _market_schedule(calendar_name, local_now.date())
    if market_window is None:
        return {
            "calendar": calendar_name,
            "status": "closed",
            "is_open": False,
            "market_open_at": None,
            "market_close_at": None,
        }
    market_open, market_close = market_window
    return {
        "calendar": calendar_name,
        "status": "open" if market_open <= local_now < market_close else "closed",
        "is_open": market_open <= local_now < market_close,
        "market_open_at": market_open.astimezone(UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "market_close_at": market_close.astimezone(UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
    }


__all__ = ["market_session_context"]
