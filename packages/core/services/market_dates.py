from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

NEW_YORK = ZoneInfo("America/New_York")


def resolve_market_date(value: str) -> str:
    if value == "today":
        return datetime.now(NEW_YORK).date().isoformat()
    return value


__all__ = ["NEW_YORK", "resolve_market_date"]
