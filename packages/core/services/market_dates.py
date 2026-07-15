from __future__ import annotations

from datetime import date, datetime
from functools import lru_cache
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

NEW_YORK = ZoneInfo("America/New_York")


def resolve_market_date(value: str) -> str:
    if value == "today":
        return datetime.now(NEW_YORK).date().isoformat()
    return value


@lru_cache(maxsize=1024)
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


@lru_cache(maxsize=32)
def market_session_windows(
    calendar_name: str,
    start_day: date,
    end_day: date,
) -> tuple[tuple[date, datetime, datetime], ...]:
    calendar = mcal.get_calendar(calendar_name)
    schedule = calendar.schedule(
        start_date=start_day.isoformat(),
        end_date=end_day.isoformat(),
    )
    return tuple(
        (
            session_label.date(),
            session["market_open"].to_pydatetime().astimezone(NEW_YORK),
            session["market_close"].to_pydatetime().astimezone(NEW_YORK),
        )
        for session_label, session in schedule.iterrows()
    )


__all__ = ["NEW_YORK", "market_session_window", "market_session_windows", "resolve_market_date"]
