from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

YFINANCE_EARNINGS_PAGE_LIMIT = 100


@dataclass(frozen=True)
class YFinanceEarningsCalendarPage:
    offset: int
    limit: int
    rows: tuple[dict[str, object], ...]

    @property
    def row_count(self) -> int:
        return len(self.rows)


class YFinanceEarningsCalendarAdapter:
    """Bulk-only yfinance earnings calendar helper for data-lane refresh jobs."""

    source_name = "yfinance_earnings_calendar"
    source_confidence = "medium"

    def fetch_pages(
        self,
        *,
        window_start: date | datetime,
        window_end: date | datetime,
        market_cap: int | str | None = None,
        filter_most_active: bool = True,
        page_limit: int = YFINANCE_EARNINGS_PAGE_LIMIT,
        force: bool = False,
    ) -> tuple[YFinanceEarningsCalendarPage, ...]:
        start_date = _as_date(window_start)
        end_date = _as_date(window_end)
        limit = max(1, min(int(page_limit), YFINANCE_EARNINGS_PAGE_LIMIT))
        calendars = _build_yfinance_calendars(start=start_date, end=end_date)

        pages: list[YFinanceEarningsCalendarPage] = []
        offset = 0
        while True:
            frame = calendars.get_earnings_calendar(
                market_cap=market_cap,
                filter_most_active=filter_most_active,
                start=start_date,
                end=end_date,
                limit=limit,
                offset=offset,
                force=force,
            )
            rows = _dataframe_rows(frame)
            pages.append(YFinanceEarningsCalendarPage(offset=offset, limit=limit, rows=rows))
            if len(rows) < limit:
                break
            offset += limit
        return tuple(pages)

    def fetch_rows(
        self,
        *,
        window_start: date | datetime,
        window_end: date | datetime,
        market_cap: int | str | None = None,
        filter_most_active: bool = True,
        page_limit: int = YFINANCE_EARNINGS_PAGE_LIMIT,
        force: bool = False,
    ) -> tuple[dict[str, object], ...]:
        pages = self.fetch_pages(
            window_start=window_start,
            window_end=window_end,
            market_cap=market_cap,
            filter_most_active=filter_most_active,
            page_limit=page_limit,
            force=force,
        )
        return tuple(row for page in pages for row in page.rows)


def _build_yfinance_calendars(*, start: date, end: date) -> Any:
    import yfinance as yf

    return yf.Calendars(start=start, end=end)


def _as_date(value: date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    return value


def _dataframe_rows(frame: Any) -> tuple[dict[str, object], ...]:
    if frame is None or bool(getattr(frame, "empty", False)):
        return ()

    if not hasattr(frame, "to_dict"):
        raise TypeError(f"Unsupported yfinance earnings calendar response: {type(frame)!r}")

    rows = frame.reset_index().to_dict(orient="records")
    return tuple(_clean_row(row) for row in rows if isinstance(row, dict))


def _clean_row(row: dict[object, object]) -> dict[str, object]:
    return {str(key): cleaned for key, value in row.items() if (cleaned := _clean_value(value)) is not None}


def _clean_value(value: object) -> object | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except (TypeError, ValueError):
            pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            pass
    return value
