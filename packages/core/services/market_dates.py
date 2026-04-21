from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

NEW_YORK = ZoneInfo("America/New_York")


def resolve_market_date(value: str) -> str:
    if value == "today":
        return datetime.now(NEW_YORK).date().isoformat()
    return value


def market_session_window(
    calendar_name: str,
    session_day: date,
) -> tuple[datetime, datetime] | None:
    calendar = mcal.get_calendar(calendar_name)
    schedule = calendar.schedule(
        start_date=session_day.isoformat(),
        end_date=session_day.isoformat(),
    )
    if schedule.empty:
        return None
    session = schedule.iloc[0]
    market_open = session["market_open"].to_pydatetime().astimezone(NEW_YORK)
    market_close = session["market_close"].to_pydatetime().astimezone(NEW_YORK)
    return market_open, market_close


__all__ = ["NEW_YORK", "market_session_window", "resolve_market_date"]
