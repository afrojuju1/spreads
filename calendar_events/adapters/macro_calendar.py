from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from calendar_events.adapters.base import BaseCalendarEventAdapter
from calendar_events.config import MACRO_ASSET_SCOPE
from calendar_events.models import CalendarEventQuery, CalendarEventRecord


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


class MacroCalendarAdapter(BaseCalendarEventAdapter):
    source_name = "macro_calendar"
    source_confidence = "high"
    refresh_always = True

    def __init__(self, path: Path) -> None:
        self.path = path

    def applies_to(self, query: CalendarEventQuery) -> bool:
        return query.underlying_type == MACRO_ASSET_SCOPE

    def scope_key(self, query: CalendarEventQuery) -> str:
        return MACRO_ASSET_SCOPE

    def fetch(self, query: CalendarEventQuery) -> list[CalendarEventRecord]:
        payload = json.loads(self.path.read_text())
        events = payload.get("events", [])
        source_updated_at = datetime.fromtimestamp(self.path.stat().st_mtime, tz=UTC).isoformat()
        ingested_at = _utc_now_iso()
        window_start = datetime.fromisoformat(query.window_start)
        window_end = datetime.fromisoformat(query.window_end)
        records: list[CalendarEventRecord] = []

        for event in events:
            if not isinstance(event, dict):
                continue
            timezone_name = event.get("timezone", "America/New_York")
            local_tz = ZoneInfo(timezone_name)
            scheduled_at = datetime.combine(
                datetime.fromisoformat(event["date"]).date(),
                datetime.strptime(event["time"], "%H:%M").time(),
                tzinfo=local_tz,
            ).astimezone(UTC)
            if not (window_start <= scheduled_at <= window_end):
                continue
            scheduled_at_iso = scheduled_at.isoformat()
            records.append(
                CalendarEventRecord(
                    event_id=str(event["event_id"]),
                    event_type=str(event["event_type"]),
                    symbol=None,
                    asset_scope=MACRO_ASSET_SCOPE,
                    scheduled_at=scheduled_at_iso,
                    window_start=scheduled_at_iso,
                    window_end=scheduled_at_iso,
                    source=self.source_name,
                    source_confidence=self.source_confidence,
                    status="scheduled",
                    payload_json=json.dumps(event, separators=(",", ":")),
                    ingested_at=ingested_at,
                    source_updated_at=source_updated_at,
                )
            )

        return records
