from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from core.services.market_dates import NEW_YORK, market_session_window


def build_collection_schedule_summary(
    *,
    now: datetime | None = None,
    market_calendar: str = "NYSE",
    interval_seconds: int = 300,
    session_start_offset_minutes: int = 0,
    session_end_offset_minutes: int = 0,
) -> dict[str, Any]:
    current = datetime.now(UTC) if now is None else now.astimezone(UTC)
    local_now = current.astimezone(NEW_YORK)
    normalized_interval_seconds = max(int(interval_seconds or 0), 1)
    interval_minutes = normalized_interval_seconds / 60.0
    market_window = market_session_window(market_calendar, local_now.date())
    if market_window is None:
        return {
            "calendar": market_calendar,
            "state": "off_day",
            "interval_seconds": normalized_interval_seconds,
            "interval_minutes": interval_minutes,
            "session_start_offset_minutes": int(session_start_offset_minutes),
            "session_end_offset_minutes": int(session_end_offset_minutes),
            "market_open_at": None,
            "market_close_at": None,
            "session_start_at": None,
            "session_end_at": None,
            "recovery_deadline_at": None,
            "expected_current_slot_at": None,
            "expected_last_slot_at": None,
            "next_slot_at": None,
        }

    market_open, market_close = market_window
    session_start = market_open + timedelta(minutes=int(session_start_offset_minutes))
    session_end = market_close + timedelta(minutes=int(session_end_offset_minutes))
    recovery_deadline = session_end + timedelta(seconds=normalized_interval_seconds)

    def _slot_at_or_before(cutoff: datetime) -> datetime | None:
        if cutoff < session_start:
            return None
        elapsed_seconds = int(max((cutoff - session_start).total_seconds(), 0))
        slot_count = elapsed_seconds // normalized_interval_seconds
        return session_start + timedelta(seconds=slot_count * normalized_interval_seconds)

    expected_current_slot = _slot_at_or_before(min(local_now, session_end))
    expected_last_slot = _slot_at_or_before(session_end)
    next_slot = (
        None
        if expected_current_slot is None
        else expected_current_slot + timedelta(seconds=normalized_interval_seconds)
    )
    if next_slot is not None and next_slot > session_end:
        next_slot = None

    if local_now < session_start:
        state = "pending"
    elif local_now <= session_end:
        state = "active"
    elif local_now <= recovery_deadline:
        state = "settling"
    else:
        state = "complete"

    return {
        "calendar": market_calendar,
        "state": state,
        "interval_seconds": normalized_interval_seconds,
        "interval_minutes": interval_minutes,
        "session_start_offset_minutes": int(session_start_offset_minutes),
        "session_end_offset_minutes": int(session_end_offset_minutes),
        "market_open_at": market_open.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "market_close_at": market_close.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "session_start_at": session_start.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "session_end_at": session_end.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "recovery_deadline_at": recovery_deadline.astimezone(UTC)
        .isoformat()
        .replace("+00:00", "Z"),
        "expected_current_slot_at": None
        if expected_current_slot is None
        else expected_current_slot.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "expected_last_slot_at": None
        if expected_last_slot is None
        else expected_last_slot.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "next_slot_at": None
        if next_slot is None
        else next_slot.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    }


__all__ = ["build_collection_schedule_summary"]
