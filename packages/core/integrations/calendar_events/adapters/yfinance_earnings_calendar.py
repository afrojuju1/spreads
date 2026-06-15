from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from core.storage.serializers import parse_datetime as _parse_datetime
from core.value_coercion import utc_now_iso as _utc_now_iso

from ..consensus import render_earnings_timestamp
from ..models import CalendarEventRecord

YFINANCE_EARNINGS_PAGE_LIMIT = 100
_SYMBOL_KEYS = ("Symbol", "symbol", "Ticker", "ticker", "index", "Index")
_EARNINGS_DATE_KEYS = (
    "Event Start Date",
    "eventStartDate",
    "event_start_date",
    "Earnings Date",
    "earningsDate",
    "earnings_date",
    "Report Date",
    "reportDate",
    "report_date",
    "Date",
    "date",
)


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

    def fetch_page(
        self,
        *,
        window_start: date | datetime,
        window_end: date | datetime,
        offset: int,
        market_cap: int | str | None = None,
        filter_most_active: bool = True,
        page_limit: int = YFINANCE_EARNINGS_PAGE_LIMIT,
        force: bool = False,
    ) -> YFinanceEarningsCalendarPage:
        start_date = _as_date(window_start)
        end_date = _as_date(window_end)
        limit = max(1, min(int(page_limit), YFINANCE_EARNINGS_PAGE_LIMIT))
        calendars = _build_yfinance_calendars(start=start_date, end=end_date)
        return _fetch_page(
            calendars,
            start_date=start_date,
            end_date=end_date,
            market_cap=market_cap,
            filter_most_active=filter_most_active,
            limit=limit,
            offset=max(int(offset), 0),
            force=force,
        )

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
            page = _fetch_page(
                calendars,
                start_date=start_date,
                end_date=end_date,
                market_cap=market_cap,
                filter_most_active=filter_most_active,
                limit=limit,
                offset=offset,
                force=force,
            )
            pages.append(page)
            if page.row_count < limit:
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

    def records_from_rows(
        self,
        rows: tuple[dict[str, object], ...],
        *,
        fetched_at: str | None = None,
    ) -> list[CalendarEventRecord]:
        ingested_at = fetched_at or _utc_now_iso()
        records: list[CalendarEventRecord] = []
        for row in rows:
            symbol = _row_text(row, *_SYMBOL_KEYS)
            scheduled_at = _row_scheduled_at(row)
            if symbol is None or scheduled_at is None:
                continue
            event_date = scheduled_at[:10]
            records.append(
                CalendarEventRecord(
                    event_id=f"{self.source_name}:{symbol}:{event_date}",
                    event_type="earnings",
                    symbol=symbol,
                    asset_scope=None,
                    scheduled_at=scheduled_at,
                    window_start=scheduled_at,
                    window_end=scheduled_at,
                    source=self.source_name,
                    source_confidence=self.source_confidence,
                    status="scheduled",
                    payload_json=json.dumps(row, separators=(",", ":"), default=str),
                    ingested_at=ingested_at,
                    source_updated_at=ingested_at,
                )
            )
        return records


def _build_yfinance_calendars(*, start: date, end: date) -> Any:
    import yfinance as yf

    return yf.Calendars(start=start, end=end)


def _fetch_page(
    calendars: Any,
    *,
    start_date: date,
    end_date: date,
    market_cap: int | str | None,
    filter_most_active: bool,
    limit: int,
    offset: int,
    force: bool,
) -> YFinanceEarningsCalendarPage:
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
    return YFinanceEarningsCalendarPage(offset=offset, limit=limit, rows=rows)


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


def _row_text(row: dict[str, object], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        rendered = str(value).strip()
        if rendered:
            return rendered.upper()
    return None


def _row_scheduled_at(row: dict[str, object]) -> str | None:
    value = next((row.get(key) for key in _EARNINGS_DATE_KEYS if row.get(key) is not None), None)
    if value is None:
        return None
    rendered = str(value).strip()
    if not rendered:
        return None
    try:
        parsed = _parse_datetime(rendered)
    except (TypeError, ValueError):
        return None
    if "T" in rendered or ":" in rendered:
        return parsed.isoformat()
    return render_earnings_timestamp(parsed.date().isoformat(), "unknown")
