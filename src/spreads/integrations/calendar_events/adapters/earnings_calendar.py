from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo

from .base import BaseCalendarEventAdapter
from ..models import CalendarEventQuery, CalendarEventRecord

NEW_YORK = ZoneInfo("America/New_York")


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _earnings_timestamp(date_str: str, session_label: str | None) -> str:
    session = (session_label or "").strip().lower()
    local_time = time(12, 0)
    if "before market open" in session:
        local_time = time(9, 0)
    elif "after market close" in session:
        local_time = time(16, 15)
    local_dt = datetime.combine(date.fromisoformat(date_str), local_time, tzinfo=NEW_YORK)
    return local_dt.astimezone(UTC).isoformat()


class EarningsCalendarAdapter(BaseCalendarEventAdapter):
    source_name = "dolt_earnings_calendar"
    source_confidence = "low"
    base_url = "https://www.dolthub.com/api/v1alpha1/post-no-preference/earnings"

    def applies_to(self, query: CalendarEventQuery) -> bool:
        return query.underlying_type == "single_name_equity"

    def scope_key(self, query: CalendarEventQuery) -> str:
        return query.symbol.upper()

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
        url = self.base_url + "?" + urllib.parse.urlencode({"q": sql})
        request = urllib.request.Request(url, headers={"User-Agent": "calendar-events/1.0"})
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.load(response)

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
