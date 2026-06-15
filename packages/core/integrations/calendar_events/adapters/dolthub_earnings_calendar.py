from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime, time
from datetime import timedelta
from zoneinfo import ZoneInfo

from core.integrations.http_client import VendorHttpClient
from core.value_coercion import utc_now_iso as _utc_now_iso
from core.storage.serializers import parse_datetime as _parse_datetime

from .base import BaseCalendarEventAdapter
from ..config import EARNINGS_POST_EVENT_SETTLED_DAYS, EARNINGS_PRE_EVENT_LOOKAHEAD_DAYS
from ..models import CalendarEventQuery, CalendarEventRecord

NEW_YORK = ZoneInfo("America/New_York")
DOLTHUB_EARNINGS_HTTP = VendorHttpClient(timeout_seconds=20, user_agent="calendar-events/1.0")


def _earnings_timestamp(date_str: str, session_label: str | None) -> str:
    session = (session_label or "").strip().lower()
    local_time = time(12, 0)
    if "before market open" in session:
        local_time = time(9, 0)
    elif "after market close" in session:
        local_time = time(16, 15)
    local_dt = datetime.combine(date.fromisoformat(date_str), local_time, tzinfo=NEW_YORK)
    return local_dt.astimezone(UTC).isoformat()


class DoltHubEarningsCalendarAdapter(BaseCalendarEventAdapter):
    source_name = "dolt_earnings_calendar"
    source_confidence = "low"
    base_url = "https://www.dolthub.com/api/v1alpha1/post-no-preference/earnings"

    def applies_to(self, query: CalendarEventQuery) -> bool:
        return query.underlying_type == "single_name_equity"

    def scope_key(self, query: CalendarEventQuery) -> str:
        return query.symbol.upper()

    def coverage_query(self, query: CalendarEventQuery) -> CalendarEventQuery:
        start_dt = _parse_datetime(query.window_start)
        end_dt = _parse_datetime(query.window_end)
        return replace(
            query,
            window_start=(start_dt - timedelta(days=EARNINGS_POST_EVENT_SETTLED_DAYS)).isoformat(),
            window_end=max(
                end_dt,
                start_dt + timedelta(days=EARNINGS_PRE_EVENT_LOOKAHEAD_DAYS),
            ).isoformat(),
        )

    def fetch(self, query: CalendarEventQuery) -> list[CalendarEventRecord]:
        symbol = query.symbol.upper().replace("'", "''")
        start_date = query.window_start[:10]
        end_date = query.window_end[:10]
        sql = (
            "select act_symbol, date, `when` "
            "from earnings_calendar "
            f"where act_symbol='{symbol}' "
            f"and date >= '{start_date}' "
            f"and date <= '{end_date}' "
            "order by date asc"
        )
        payload = DOLTHUB_EARNINGS_HTTP.request_json("GET", self.base_url, "", params={"q": sql})

        rows = payload.get("rows", [])
        fetched_at = _utc_now_iso()
        records: list[CalendarEventRecord] = []
        for row in rows:
            raw_date = row["date"]
            when_label = row.get("when")
            scheduled_at = _earnings_timestamp(raw_date, when_label)
            record_key = (when_label or "unknown").replace(" ", "_").lower()
            records.append(
                CalendarEventRecord(
                    event_id=f"{self.source_name}:{symbol}:{raw_date}:{record_key}",
                    event_type="earnings",
                    symbol=query.symbol.upper(),
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

    def fetch_bulk(
        self,
        *,
        window_start: str,
        window_end: str,
    ) -> list[CalendarEventRecord]:
        start_date = window_start[:10]
        end_date = window_end[:10]
        sql = (
            "select act_symbol, date, `when` "
            "from earnings_calendar "
            f"where date >= '{start_date}' "
            f"and date <= '{end_date}' "
            "order by date asc, act_symbol asc"
        )
        payload = DOLTHUB_EARNINGS_HTTP.request_json("GET", self.base_url, "", params={"q": sql})

        rows = payload.get("rows", [])
        fetched_at = _utc_now_iso()
        records: list[CalendarEventRecord] = []
        for row in rows:
            symbol = str(row.get("act_symbol") or "").upper().strip()
            raw_date = row.get("date")
            if not symbol or raw_date is None:
                continue
            raw_date_text = str(raw_date)[:10]
            when_label = row.get("when")
            scheduled_at = _earnings_timestamp(raw_date_text, None if when_label is None else str(when_label))
            record_key = (str(when_label or "unknown")).replace(" ", "_").lower()
            records.append(
                CalendarEventRecord(
                    event_id=f"{self.source_name}:{symbol}:{raw_date_text}:{record_key}",
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
