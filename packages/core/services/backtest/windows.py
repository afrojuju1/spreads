from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from core.storage.serializers import parse_date


@dataclass(frozen=True, slots=True)
class BacktestWindow:
    start_date: date
    end_date: date
    market_dates: tuple[str, ...]
    start_at: datetime
    end_at: datetime


def normalize_backtest_window(
    start_date: str | date | None,
    end_date: str | date | None,
    *,
    max_days: int,
) -> BacktestWindow:
    if start_date is None:
        raise ValueError("start_date is required for a bounded backtest")
    start_day = parse_date(start_date)
    end_day = parse_date(end_date or start_day)
    if end_day < start_day:
        raise ValueError("end_date must be on or after start_date")
    day_count = (end_day - start_day).days + 1
    if day_count > max(int(max_days), 1):
        raise ValueError(f"backtest window is {day_count} days; max_days is {max_days}")
    market_dates = tuple((start_day + timedelta(days=offset)).isoformat() for offset in range(day_count))
    return BacktestWindow(
        start_date=start_day,
        end_date=end_day,
        market_dates=market_dates,
        start_at=datetime.combine(start_day, datetime.min.time(), tzinfo=UTC),
        end_at=datetime.combine(end_day + timedelta(days=1), datetime.min.time(), tzinfo=UTC),
    )


__all__ = ["BacktestWindow", "normalize_backtest_window"]
