from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo

from .base import BaseCalendarEventAdapter
from ..config import ALPACA_CORPORATE_ACTION_TYPES
from ..models import CalendarEventQuery, CalendarEventRecord

NEW_YORK = ZoneInfo("America/New_York")


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _singularize(group_name: str) -> str:
    if group_name.endswith("ies"):
        return group_name[:-3] + "y"
    if group_name.endswith("s"):
        return group_name[:-1]
    return group_name


def _scheduled_at_for_action(event_type: str, payload: dict[str, object]) -> str | None:
    date_value = None
    local_time = time(9, 0)
    if event_type in {"cash_dividend", "stock_dividend"}:
        date_value = payload.get("ex_date") or payload.get("record_date") or payload.get("process_date")
    else:
        date_value = (
            payload.get("ex_date")
            or payload.get("process_date")
            or payload.get("record_date")
            or payload.get("payable_date")
        )
    if not isinstance(date_value, str):
        return None
    local_dt = datetime.combine(date.fromisoformat(date_value), local_time, tzinfo=NEW_YORK)
    return local_dt.astimezone(UTC).isoformat()


class AlpacaCorporateActionsAdapter(BaseCalendarEventAdapter):
    source_name = "alpaca_corporate_actions"
    source_confidence = "medium"

    def __init__(self, *, key_id: str, secret_key: str, base_url: str = "https://data.alpaca.markets") -> None:
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "APCA-API-KEY-ID": key_id,
            "APCA-API-SECRET-KEY": secret_key,
            "Accept": "application/json",
            "User-Agent": "calendar-events/1.0",
        }

    def applies_to(self, query: CalendarEventQuery) -> bool:
        return True

    def scope_key(self, query: CalendarEventQuery) -> str:
        return query.symbol.upper()

    def fetch(self, query: CalendarEventQuery) -> list[CalendarEventRecord]:
        records: list[CalendarEventRecord] = []
        page_token: str | None = None
        fetched_at = _utc_now_iso()
        while True:
            params = {
                "symbols": query.symbol.upper(),
                "start": query.window_start[:10],
                "end": query.window_end[:10],
                "types": ",".join(ALPACA_CORPORATE_ACTION_TYPES),
                "limit": 1000,
            }
            if page_token:
                params["page_token"] = page_token
            url = f"{self.base_url}/v1/corporate-actions?{urllib.parse.urlencode(params)}"
            request = urllib.request.Request(url, headers=self.headers)
            with urllib.request.urlopen(request, timeout=20) as response:
                payload = json.load(response)

            grouped = payload.get("corporate_actions", {})
            if isinstance(grouped, dict):
                for group_name, items in grouped.items():
                    event_type = _singularize(group_name)
                    if not isinstance(items, list):
                        continue
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        scheduled_at = _scheduled_at_for_action(event_type, item)
                        if not scheduled_at:
                            continue
                        records.append(
                            CalendarEventRecord(
                                event_id=str(item.get("id") or f"{self.source_name}:{query.symbol}:{scheduled_at}:{event_type}"),
                                event_type=event_type,
                                symbol=query.symbol.upper(),
                                asset_scope=None,
                                scheduled_at=scheduled_at,
                                window_start=scheduled_at,
                                window_end=scheduled_at,
                                source=self.source_name,
                                source_confidence=self.source_confidence,
                                status="scheduled",
                                payload_json=json.dumps(item, separators=(",", ":")),
                                ingested_at=fetched_at,
                                source_updated_at=fetched_at,
                            )
                        )

            page_token = payload.get("next_page_token")
            if not page_token:
                break

        return records
