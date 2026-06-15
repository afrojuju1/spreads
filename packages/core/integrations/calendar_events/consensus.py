from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from core.storage.serializers import parse_datetime as _parse_datetime
from core.value_coercion import utc_now_iso as _utc_now_iso

from .config import SOURCE_CONFIDENCE_RANK
from .models import CalendarEventRecord, EarningsEventConsensusRecord, EventSessionTiming, SourceConfidence

NEW_YORK = ZoneInfo("America/New_York")
YFINANCE_EARNINGS_SOURCE = "yfinance_earnings_calendar"
DEFAULT_CONSENSUS_STALE_HOURS = 24


def record_payload(record: CalendarEventRecord) -> dict[str, object]:
    if not record.payload_json:
        return {}
    try:
        payload = json.loads(record.payload_json)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def compact_payload(payload: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in payload.items() if value not in (None, "", (), [], {})}


def earnings_session_timing(record: CalendarEventRecord) -> EventSessionTiming:
    payload = record_payload(record)
    raw = str(payload.get("when") or payload.get("reportTime") or payload.get("report_time") or payload.get("time") or "").strip().lower()
    if raw in {"bmo", "before_open"} or "before" in raw:
        return "before_open"
    if raw in {"amc", "after_close"} or "after" in raw:
        return "after_close"
    if raw:
        return "during_market"
    return "unknown"


def render_earnings_timestamp(date_str: str, timing: str) -> str:
    local_time = time(12, 0)
    if timing == "before_open":
        local_time = time(9, 0)
    elif timing == "after_close":
        local_time = time(16, 15)
    local_dt = datetime.combine(date.fromisoformat(date_str), local_time, tzinfo=NEW_YORK)
    return local_dt.astimezone(UTC).isoformat()


def fresh_earnings_records(
    records: list[CalendarEventRecord],
    *,
    covered_sources: set[str],
) -> list[CalendarEventRecord]:
    return [record for record in records if record.event_type == "earnings" and record.source in covered_sources]


def reconcile_earnings_records(
    records: list[CalendarEventRecord],
    *,
    covered_sources: set[str],
) -> tuple[list[CalendarEventRecord], dict[str, dict[str, object]]]:
    fresh_records = sorted(
        fresh_earnings_records(records, covered_sources=covered_sources),
        key=lambda item: _parse_datetime(item.scheduled_at),
    )
    if not fresh_records:
        return [], {}

    clusters: list[list[CalendarEventRecord]] = []
    for record in fresh_records:
        event_date = _parse_datetime(record.scheduled_at).date()
        if not clusters:
            clusters.append([record])
            continue
        last_date = _parse_datetime(clusters[-1][-1].scheduled_at).date()
        if abs((event_date - last_date).days) <= 3:
            clusters[-1].append(record)
        else:
            clusters.append([record])

    canonical_records: list[CalendarEventRecord] = []
    consensus_by_date: dict[str, dict[str, object]] = {}
    for cluster in clusters:
        by_date: dict[str, list[CalendarEventRecord]] = {}
        for record in cluster:
            by_date.setdefault(record.scheduled_at[:10], []).append(record)
        event_date, date_records = max(
            by_date.items(),
            key=lambda item: (
                len(item[1]),
                max(SOURCE_CONFIDENCE_RANK.get(record.source_confidence, 0) for record in item[1]),
                max(_parse_datetime(record.source_updated_at) for record in item[1]),
                item[0],
            ),
        )
        date_sources = sorted({record.source for record in date_records})
        cluster_sources = sorted({record.source for record in cluster})
        conflicting_sources = tuple(source for source in cluster_sources if source not in date_sources)
        timing_counts = Counter(timing for timing in (earnings_session_timing(record) for record in date_records) if timing != "unknown")
        canonical_timing = max(timing_counts.items(), key=lambda item: (item[1], item[0]))[0] if timing_counts else "unknown"
        best_record = max(
            date_records,
            key=lambda item: (
                SOURCE_CONFIDENCE_RANK.get(item.source_confidence, 0),
                _parse_datetime(item.source_updated_at),
                item.source,
            ),
        )
        canonical_record = best_record
        if canonical_timing != "unknown":
            canonical_record = replace(
                best_record,
                scheduled_at=render_earnings_timestamp(event_date, canonical_timing),
                window_start=render_earnings_timestamp(event_date, canonical_timing),
                window_end=render_earnings_timestamp(event_date, canonical_timing),
            )
        canonical_records.append(canonical_record)

        if len(cluster_sources) == 1:
            consensus_status = "single_source"
            timing_confidence = "low"
        elif len(by_date) > 1:
            consensus_status = "conflict"
            timing_confidence = "medium" if canonical_timing != "unknown" else "low"
        elif len(timing_counts) == 1 and sum(timing_counts.values()) == len(date_records):
            consensus_status = "consensus"
            timing_confidence = "high"
        else:
            consensus_status = "date_only"
            timing_confidence = "medium"

        consensus_by_date[event_date] = {
            "primary_source": best_record.source,
            "supporting_sources": tuple(date_sources),
            "conflicting_sources": conflicting_sources,
            "consensus_status": consensus_status,
            "timing_confidence": timing_confidence,
            "session_timing": canonical_timing,
        }

    canonical_records.sort(key=lambda item: _parse_datetime(item.scheduled_at))
    return canonical_records, consensus_by_date


def build_earnings_event_consensus(
    records: list[CalendarEventRecord],
    *,
    covered_sources: set[str] | None = None,
    computed_at: str | None = None,
    stale_after: str | None = None,
) -> list[EarningsEventConsensusRecord]:
    earnings_records = [record for record in records if record.event_type == "earnings" and record.symbol]
    if not earnings_records:
        return []

    effective_sources = covered_sources or {record.source for record in earnings_records}
    computed_at_value = computed_at or _utc_now_iso()
    stale_after_value = stale_after or (_parse_datetime(computed_at_value) + timedelta(hours=DEFAULT_CONSENSUS_STALE_HOURS)).isoformat()
    records_by_symbol: dict[str, list[CalendarEventRecord]] = {}
    for record in earnings_records:
        records_by_symbol.setdefault(str(record.symbol).upper(), []).append(record)

    consensus_records: list[EarningsEventConsensusRecord] = []
    for symbol, symbol_records in sorted(records_by_symbol.items()):
        canonical_records, consensus_by_date = reconcile_earnings_records(symbol_records, covered_sources=effective_sources)
        for canonical_record in canonical_records:
            event_date = canonical_record.scheduled_at[:10]
            consensus = consensus_by_date.get(event_date, {})
            source_records = _nearby_source_records(symbol_records, event_date=event_date, covered_sources=effective_sources)
            supporting_sources = tuple(consensus.get("supporting_sources") or ())
            conflicting_sources = tuple(consensus.get("conflicting_sources") or ())
            consensus_records.append(
                EarningsEventConsensusRecord(
                    consensus_id=f"earnings_event_consensus:{symbol}:{event_date}",
                    symbol=symbol,
                    event_date=event_date,
                    scheduled_at=canonical_record.scheduled_at,
                    session_timing=str(consensus.get("session_timing") or "unknown"),  # type: ignore[arg-type]
                    event_status=canonical_record.status,
                    primary_source=str(consensus.get("primary_source")) if consensus.get("primary_source") else None,
                    supporting_sources=supporting_sources,
                    conflicting_sources=conflicting_sources,
                    consensus_status=str(consensus.get("consensus_status") or "missing"),  # type: ignore[arg-type]
                    source_confidence=_consensus_source_confidence(
                        primary_source=str(consensus.get("primary_source") or ""),
                        supporting_sources=supporting_sources,
                        best_record=canonical_record,
                    ),
                    timing_confidence=str(consensus.get("timing_confidence") or "unknown"),  # type: ignore[arg-type]
                    provider_payload=_provider_payload(canonical_record=canonical_record, source_records=source_records),
                    computed_at=computed_at_value,
                    stale_after=stale_after_value,
                )
            )
    consensus_records.sort(key=lambda item: (item.event_date, item.symbol))
    return consensus_records


def _nearby_source_records(
    records: list[CalendarEventRecord],
    *,
    event_date: str,
    covered_sources: set[str],
) -> list[CalendarEventRecord]:
    anchor = date.fromisoformat(event_date)
    return [
        record
        for record in records
        if record.source in covered_sources and abs((_parse_datetime(record.scheduled_at).date() - anchor).days) <= 3
    ]


def _provider_payload(
    *,
    canonical_record: CalendarEventRecord,
    source_records: list[CalendarEventRecord],
) -> dict[str, object]:
    return {
        "canonical_event_id": canonical_record.event_id,
        "records": [
            {
                "event_id": record.event_id,
                "source": record.source,
                "source_confidence": record.source_confidence,
                "scheduled_at": record.scheduled_at,
                "status": record.status,
                "payload": record_payload(record),
            }
            for record in sorted(source_records, key=lambda item: (item.source, item.scheduled_at, item.event_id))
        ],
    }


def _consensus_source_confidence(
    *,
    primary_source: str,
    supporting_sources: tuple[str, ...],
    best_record: CalendarEventRecord,
) -> SourceConfidence:
    if YFINANCE_EARNINGS_SOURCE in supporting_sources and len(supporting_sources) >= 2:
        return "high"
    if len(supporting_sources) >= 2 or primary_source == YFINANCE_EARNINGS_SOURCE:
        return "medium"
    return best_record.source_confidence
