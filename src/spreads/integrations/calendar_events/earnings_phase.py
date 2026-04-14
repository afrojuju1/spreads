from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Iterable

from .config import (
    EARNINGS_POST_EVENT_FRESH_DAYS,
    EARNINGS_POST_EVENT_SETTLED_DAYS,
    EARNINGS_PRE_EVENT_LOOKAHEAD_DAYS,
)
from .models import CalendarEventRecord, EarningsEventPhase, EventSessionTiming, SourceConfidence


def _parse_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    normalized = value.replace("Z", "+00:00") if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _days_between(start: datetime, end: datetime) -> int:
    return (end.date() - start.date()).days


def _record_payload(record: CalendarEventRecord) -> dict[str, object]:
    if not record.payload_json:
        return {}
    try:
        payload = json.loads(record.payload_json)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _session_timing(record: CalendarEventRecord) -> EventSessionTiming:
    raw = str(_record_payload(record).get("when") or "").strip().lower()
    if "before market open" in raw:
        return "before_open"
    if "after market close" in raw:
        return "after_close"
    if raw:
        return "during_market"
    return "unknown"


def _timing_confidence(
    source_confidence: SourceConfidence,
    *,
    session_timing: EventSessionTiming,
) -> SourceConfidence:
    if session_timing == "unknown":
        return "unknown"
    return source_confidence


@dataclass(frozen=True)
class EarningsPhaseSnapshot:
    phase: EarningsEventPhase = "clean"
    event_date: str | None = None
    session_timing: EventSessionTiming = "unknown"
    cohort_key: str | None = None
    days_to_event: int | None = None
    days_since_event: int | None = None
    timing_confidence: SourceConfidence = "unknown"
    horizon_crosses_report: bool = False


def resolve_earnings_phase_snapshot(
    *,
    records: Iterable[CalendarEventRecord],
    as_of: str | datetime,
    horizon_end: str | datetime,
) -> EarningsPhaseSnapshot:
    start_dt = _parse_datetime(as_of)
    horizon_end_dt = _parse_datetime(horizon_end)
    earnings_records = sorted(
        (record for record in records if record.event_type == "earnings"),
        key=lambda item: _parse_datetime(item.scheduled_at),
    )
    if not earnings_records:
        return EarningsPhaseSnapshot()

    next_record: CalendarEventRecord | None = None
    previous_record: CalendarEventRecord | None = None
    for record in earnings_records:
        scheduled_at = _parse_datetime(record.scheduled_at)
        if scheduled_at >= start_dt:
            next_record = record
            break
        previous_record = record

    phase: EarningsEventPhase = "clean"
    anchor_record: CalendarEventRecord | None = None
    days_to_event: int | None = None
    days_since_event: int | None = None
    horizon_crosses_report = False

    if next_record is not None:
        next_scheduled_at = _parse_datetime(next_record.scheduled_at)
        days_to_event = _days_between(start_dt, next_scheduled_at)
        horizon_crosses_report = next_scheduled_at <= horizon_end_dt
        if horizon_crosses_report:
            phase = "through_event"
            anchor_record = next_record
        elif days_to_event <= EARNINGS_PRE_EVENT_LOOKAHEAD_DAYS:
            phase = "pre_event_runup"
            anchor_record = next_record

    if phase == "clean" and previous_record is not None:
        previous_scheduled_at = _parse_datetime(previous_record.scheduled_at)
        days_since_event = _days_between(previous_scheduled_at, start_dt)
        if days_since_event <= EARNINGS_POST_EVENT_FRESH_DAYS:
            phase = "post_event_fresh"
            anchor_record = previous_record
        elif days_since_event <= EARNINGS_POST_EVENT_SETTLED_DAYS:
            phase = "post_event_settled"
            anchor_record = previous_record

    if anchor_record is None:
        return EarningsPhaseSnapshot()

    session_timing = _session_timing(anchor_record)
    event_date = anchor_record.scheduled_at[:10]
    return EarningsPhaseSnapshot(
        phase=phase,
        event_date=event_date,
        session_timing=session_timing,
        cohort_key=f"{event_date}:{session_timing}:{phase}",
        days_to_event=days_to_event,
        days_since_event=days_since_event,
        timing_confidence=_timing_confidence(
            anchor_record.source_confidence,
            session_timing=session_timing,
        ),
        horizon_crosses_report=horizon_crosses_report,
    )


__all__ = [
    "EarningsPhaseSnapshot",
    "resolve_earnings_phase_snapshot",
]
