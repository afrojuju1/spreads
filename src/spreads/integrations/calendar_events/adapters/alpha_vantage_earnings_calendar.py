from __future__ import annotations

import csv
import io
import json
import urllib.parse
import urllib.request
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from .base import BaseCalendarEventAdapter
from ..config import EARNINGS_PRE_EVENT_LOOKAHEAD_DAYS
from ..models import CalendarEventQuery, CalendarEventRecord

NEW_YORK = ZoneInfo("America/New_York")


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _parse_datetime(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00") if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _earnings_timestamp(date_str: str, session_label: str | None) -> str:
    session = (session_label or "").strip().lower()
    local_time = time(12, 0)
    if session in {"bmo", "before_open"} or "before" in session:
        local_time = time(9, 0)
    elif session in {"amc", "after_close"} or "after" in session:
        local_time = time(16, 15)
    local_dt = datetime.combine(date.fromisoformat(date_str), local_time, tzinfo=NEW_YORK)
    return local_dt.astimezone(UTC).isoformat()


def _row_value(row: dict[str, str], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        rendered = str(value).strip()
        if rendered:
            return rendered
    return None


def _horizon_label(start_dt: datetime, end_dt: datetime) -> str:
    span_days = max((end_dt.date() - start_dt.date()).days, 0)
    if span_days > 180:
        return "12month"
    if span_days > 90:
        return "6month"
    return "3month"


class AlphaVantageEarningsCalendarAdapter(BaseCalendarEventAdapter):
    source_name = "alpha_vantage_earnings_calendar"
    source_confidence = "medium"
    base_url = "https://www.alphavantage.co/query"

    def __init__(self, *, api_key: str) -> None:
        self.api_key = api_key

    def applies_to(self, query: CalendarEventQuery) -> bool:
        return query.underlying_type == "single_name_equity" and bool(self.api_key)

    def scope_key(self, query: CalendarEventQuery) -> str:
        return query.symbol.upper()

    def coverage_query(self, query: CalendarEventQuery) -> CalendarEventQuery:
        start_dt = max(_parse_datetime(query.window_start), datetime.now(UTC))
        end_dt = max(
            _parse_datetime(query.window_end),
            start_dt + timedelta(days=EARNINGS_PRE_EVENT_LOOKAHEAD_DAYS),
        )
        return replace(
            query,
            window_start=start_dt.isoformat(),
            window_end=end_dt.isoformat(),
        )

    def fetch(self, query: CalendarEventQuery) -> list[CalendarEventRecord]:
        start_dt = _parse_datetime(query.window_start)
        end_dt = _parse_datetime(query.window_end)
        params = {
            "function": "EARNINGS_CALENDAR",
            "symbol": query.symbol.upper(),
            "horizon": _horizon_label(start_dt, end_dt),
            "apikey": self.api_key,
        }
        url = self.base_url + "?" + urllib.parse.urlencode(params)
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "calendar-events/1.0"},
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = response.read().decode("utf-8")

        rows = list(csv.DictReader(io.StringIO(payload)))
        fetched_at = _utc_now_iso()
        records: list[CalendarEventRecord] = []
        for row in rows:
            symbol = _row_value(row, "symbol")
            report_date = _row_value(row, "reportDate", "report_date", "date")
            if symbol != query.symbol.upper() or report_date is None:
                continue
            session_label = _row_value(
                row,
                "reportTime",
                "report_time",
                "when",
                "time",
            )
            scheduled_at = _earnings_timestamp(report_date, session_label)
            records.append(
                CalendarEventRecord(
                    event_id=f"{self.source_name}:{symbol}:{report_date}",
                    event_type="earnings",
                    symbol=symbol,
                    asset_scope=None,
                    scheduled_at=scheduled_at,
                    window_start=scheduled_at,
                    window_end=scheduled_at,
                    source=self.source_name,
                    source_confidence=self.source_confidence,
                    status="scheduled",
                    payload_json=json.dumps(row, separators=(",", ":")),
                    ingested_at=fetched_at,
                    source_updated_at=fetched_at,
                )
            )
        return records
