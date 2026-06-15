from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from .adapters.alpaca_corporate_actions import AlpacaCorporateActionsAdapter
from .adapters.base import BaseCalendarEventAdapter
from .adapters.macro_calendar import MacroCalendarAdapter
from .config import (
    DEFAULT_MACRO_CALENDAR_PATH,
    DIVIDEND_EVENT_TYPES,
    MACRO_ASSET_SCOPE,
    OPTIONAL_SOURCES_BY_UNDERLYING,
    REQUIRED_SOURCES_BY_UNDERLYING,
    SOURCE_CONFIDENCE_RANK,
    SOURCE_FRESHNESS_HOURS,
)
from .consensus import compact_payload as _compact_payload
from .earnings_phase import resolve_earnings_phase_snapshot
from .models import (
    CalendarEventContext,
    CalendarEventQuery,
    CalendarEventReason,
    CalendarEventRecord,
    EarningsEventConsensusRecord,
)
from .store import CalendarEventStore
from core.runtime.config import default_database_url
from core.storage.serializers import parse_datetime as _parse_datetime
from core.value_coercion import as_text as _as_text, utc_now_iso as _utc_now_iso

EARNINGS_CONSENSUS_SOURCE = "earnings_event_consensus"


def _aggregate_confidence(confidences: list[str]) -> str:
    if not confidences:
        return "unknown"
    ranked = sorted(confidences, key=lambda item: SOURCE_CONFIDENCE_RANK.get(item, 0))
    return ranked[0]


def _days_until(window_start: datetime, scheduled_at: datetime) -> int:
    return (scheduled_at.date() - window_start.date()).days


def _earnings_when(session_timing: str) -> str | None:
    if session_timing == "before_open":
        return "before market open"
    if session_timing == "after_close":
        return "after market close"
    if session_timing == "during_market":
        return "during market"
    return None


def _provider_record_summaries(record: EarningsEventConsensusRecord) -> list[dict[str, object]]:
    provider_records = record.provider_payload.get("records")
    if not isinstance(provider_records, list):
        return []
    summaries: list[dict[str, object]] = []
    for provider_record in provider_records:
        if not isinstance(provider_record, dict):
            continue
        summaries.append(
            _compact_payload(
                {
                    "eventId": provider_record.get("event_id"),
                    "source": provider_record.get("source"),
                    "sourceConfidence": provider_record.get("source_confidence"),
                    "scheduledAt": provider_record.get("scheduled_at"),
                    "status": provider_record.get("status"),
                }
            )
        )
    return summaries


def _consensus_payload(record: EarningsEventConsensusRecord) -> dict[str, object]:
    return _compact_payload(
        {
            "source": EARNINGS_CONSENSUS_SOURCE,
            "eventDate": record.event_date,
            "scheduledAt": record.scheduled_at,
            "when": _earnings_when(record.session_timing),
            "sessionTiming": record.session_timing,
            "eventStatus": record.event_status,
            "consensusStatus": record.consensus_status,
            "sourceConfidence": record.source_confidence,
            "timingConfidence": record.timing_confidence,
            "primarySource": record.primary_source,
            "supportingSources": list(record.supporting_sources),
            "conflictingSources": list(record.conflicting_sources),
            "computedAt": record.computed_at,
            "staleAfter": record.stale_after,
            "canonicalEventId": record.provider_payload.get("canonical_event_id"),
            "providerRecords": _provider_record_summaries(record),
        }
    )


def _calendar_record_from_consensus(record: EarningsEventConsensusRecord) -> CalendarEventRecord:
    payload = _consensus_payload(record)
    return CalendarEventRecord(
        event_id=record.consensus_id,
        event_type="earnings",
        symbol=record.symbol,
        asset_scope=None,
        scheduled_at=record.scheduled_at,
        window_start=record.scheduled_at,
        window_end=record.scheduled_at,
        source=EARNINGS_CONSENSUS_SOURCE,
        source_confidence=record.source_confidence,
        status=record.event_status,
        payload_json=json.dumps(payload, sort_keys=True),
        ingested_at=record.computed_at,
        source_updated_at=record.computed_at,
    )


def _earnings_consensus_by_date(records: list[EarningsEventConsensusRecord]) -> dict[str, EarningsEventConsensusRecord]:
    return {record.event_date: record for record in records}


def _earnings_consensus_reasons(
    records: list[EarningsEventConsensusRecord],
    *,
    as_of: str,
) -> list[CalendarEventReason]:
    reasons: list[CalendarEventReason] = []
    as_of_dt = _parse_datetime(as_of)
    for record in records:
        stale_after = _parse_datetime(record.stale_after)
        if stale_after is not None and as_of_dt is not None and stale_after < as_of_dt:
            reasons.append(
                CalendarEventReason(
                    code="earnings_consensus_stale",
                    event_type="earnings_source_state",
                    severity="medium",
                    message=f"Earnings consensus stale for {record.symbol} after {record.stale_after}",
                    scheduled_at=record.scheduled_at,
                    source=EARNINGS_CONSENSUS_SOURCE,
                )
            )
        if str(record.consensus_status or "").strip().lower() == "conflict":
            reasons.append(
                CalendarEventReason(
                    code="earnings_date_conflict",
                    event_type="earnings_source_state",
                    severity="high",
                    message=f"Earnings consensus has conflicting source dates for {record.symbol} on {record.event_date}",
                    scheduled_at=record.scheduled_at,
                    source=EARNINGS_CONSENSUS_SOURCE,
                )
            )
    return reasons


def _build_reason(record: CalendarEventRecord) -> CalendarEventReason:
    if record.event_type == "earnings":
        return CalendarEventReason(
            code="earnings_before_expiry",
            event_type=record.event_type,
            severity="high",
            message=f"Earnings scheduled before expiry on {record.scheduled_at[:10]}",
            scheduled_at=record.scheduled_at,
            source=record.source,
        )
    if record.event_type in DIVIDEND_EVENT_TYPES:
        return CalendarEventReason(
            code="ex_dividend_before_expiry",
            event_type=record.event_type,
            severity="high",
            message=f"Ex-dividend date before expiry on {record.scheduled_at[:10]}",
            scheduled_at=record.scheduled_at,
            source=record.source,
        )
    if record.event_type.startswith("macro_"):
        return CalendarEventReason(
            code="macro_event_in_window",
            event_type=record.event_type,
            severity="medium",
            message=f"Macro event in holding window: {record.event_type} on {record.scheduled_at[:10]}",
            scheduled_at=record.scheduled_at,
            source=record.source,
        )
    return CalendarEventReason(
        code="corporate_action_before_expiry",
        event_type=record.event_type,
        severity="medium",
        message=f"Corporate action before expiry: {record.event_type} on {record.scheduled_at[:10]}",
        scheduled_at=record.scheduled_at,
        source=record.source,
    )


def _latest_updated_at(records: list[EarningsEventConsensusRecord]) -> str | None:
    if not records:
        return None
    return max(record.computed_at for record in records)


def _source_confidences_from_consensus(records: list[EarningsEventConsensusRecord]) -> list[str]:
    return [record.source_confidence for record in records if record.source_confidence]


class CalendarEventResolver:
    def __init__(self, *, store: CalendarEventStore, adapters: list[BaseCalendarEventAdapter]) -> None:
        self.store = store
        self.adapters = adapters

    def resolve_calendar_context(
        self,
        *,
        symbol: str,
        strategy: str,
        window_start: str,
        window_end: str,
        underlying_type: str,
        refresh: bool = False,
    ) -> CalendarEventContext:
        query = CalendarEventQuery(
            symbol=symbol.upper(),
            strategy=strategy,
            window_start=window_start,
            window_end=window_end,
            underlying_type=underlying_type,
        )
        required_sources = REQUIRED_SOURCES_BY_UNDERLYING.get(underlying_type, set())
        optional_sources = OPTIONAL_SOURCES_BY_UNDERLYING.get(underlying_type, set())

        source_updates: list[str] = []
        covered_sources: list[str] = []
        confidences: list[str] = []
        reasons: list[CalendarEventReason] = []
        missing_required = False

        for adapter in self.adapters:
            if not adapter.applies_to(query):
                continue
            adapter_query = adapter.coverage_query(query)
            freshness_hours = SOURCE_FRESHNESS_HOURS.get(adapter.source_name, 24)
            scope_key = adapter.scope_key(query)
            has_fresh_coverage = self.store.has_fresh_coverage(
                source=adapter.source_name,
                scope_key=scope_key,
                coverage_start=adapter_query.window_start,
                coverage_end=adapter_query.window_end,
                freshness_hours=freshness_hours,
            )

            fetch_error: Exception | None = None
            if refresh or not has_fresh_coverage or adapter.refresh_always:
                try:
                    records = adapter.fetch(adapter_query)
                    self.store.upsert_events(records)
                    self.store.set_refresh_state(
                        source=adapter.source_name,
                        scope_key=scope_key,
                        coverage_start=adapter_query.window_start,
                        coverage_end=adapter_query.window_end,
                        refreshed_at=_utc_now_iso(),
                    )
                except Exception as exc:
                    fetch_error = exc

            has_fresh_coverage = self.store.has_fresh_coverage(
                source=adapter.source_name,
                scope_key=scope_key,
                coverage_start=adapter_query.window_start,
                coverage_end=adapter_query.window_end,
                freshness_hours=freshness_hours,
            )
            if has_fresh_coverage:
                if adapter.contributes_to_coverage:
                    covered_sources.append(adapter.source_name)
                    confidences.append(adapter.source_confidence)
                    state = self.store.get_refresh_state(
                        source=adapter.source_name,
                        scope_key=scope_key,
                    )
                    if state is not None:
                        source_updates.append(state["refreshed_at"])
                continue

            if not adapter.contributes_to_coverage:
                continue
            code = f"{adapter.source_name}_unavailable"
            severity = "high" if adapter.source_name in required_sources else "low"
            reasons.append(
                CalendarEventReason(
                    code=code,
                    event_type="source_unavailable",
                    severity=severity,
                    message=f"Calendar source unavailable: {adapter.source_name}",
                    source=adapter.source_name,
                )
            )
            if adapter.source_name in required_sources:
                missing_required = True
            elif adapter.source_name in optional_sources and fetch_error is not None:
                source_updates.append(_utc_now_iso())

        records = self.store.query_events(
            symbol=query.symbol,
            asset_scope=MACRO_ASSET_SCOPE if underlying_type == MACRO_ASSET_SCOPE else None,
            window_start=window_start,
            window_end=window_end,
        )
        consensus_records: list[EarningsEventConsensusRecord] = []
        if underlying_type == "single_name_equity":
            consensus_records = self.store.query_earnings_event_consensus(
                symbol=query.symbol,
                window_start=window_start,
                window_end=window_end,
            )
            if consensus_records:
                covered_sources.append(EARNINGS_CONSENSUS_SOURCE)
                source_updates.append(_latest_updated_at(consensus_records) or "")
                confidences.extend(_source_confidences_from_consensus(consensus_records))

        canonical_window_earnings = [_calendar_record_from_consensus(record) for record in consensus_records]
        normalized_records = [record for record in records if record.event_type != "earnings"] + canonical_window_earnings
        normalized_records.sort(key=lambda item: _parse_datetime(item.scheduled_at))
        reasons.extend(_build_reason(record) for record in normalized_records)
        reasons.extend(_earnings_consensus_reasons(consensus_records, as_of=window_start))

        macro_events = [record for record in normalized_records if record.event_type.startswith("macro_")]
        assignment_risk = any(record.event_type in DIVIDEND_EVENT_TYPES for record in normalized_records)
        macro_regime = None
        if macro_events:
            macro_regime = ",".join(sorted({record.event_type for record in macro_events}))

        days_to_nearest_event = None
        if normalized_records:
            start_dt = datetime.fromisoformat(window_start)
            nearest = min(datetime.fromisoformat(record.scheduled_at) for record in normalized_records)
            days_to_nearest_event = _days_until(start_dt, nearest)

        if missing_required:
            status = "unknown"
        elif normalized_records:
            status = "penalized"
        else:
            status = "clean"

        consensus_by_date = _earnings_consensus_by_date(consensus_records)
        earnings_snapshot = resolve_earnings_phase_snapshot(
            records=canonical_window_earnings,
            as_of=query.window_start,
            horizon_end=query.window_end,
        )
        anchor_consensus = consensus_by_date.get(str(earnings_snapshot.event_date)) if earnings_snapshot.event_date else None
        earnings_session_timing = str(
            anchor_consensus.session_timing if anchor_consensus is not None else earnings_snapshot.session_timing or "unknown"
        )
        earnings_timing_confidence = str(
            anchor_consensus.timing_confidence if anchor_consensus is not None else earnings_snapshot.timing_confidence or "unknown"
        )
        earnings_primary_source = _as_text(None if anchor_consensus is None else anchor_consensus.primary_source)
        supporting_sources = () if anchor_consensus is None else tuple(anchor_consensus.supporting_sources)
        earnings_consensus_status = str(anchor_consensus.consensus_status if anchor_consensus is not None else "missing")
        earnings_enrichment = {} if anchor_consensus is None else _consensus_payload(anchor_consensus)
        filtered_source_updates = [value for value in source_updates if value]

        return CalendarEventContext(
            status=status,
            reasons=tuple(reasons),
            days_to_nearest_event=days_to_nearest_event,
            events_before_expiry=len(normalized_records),
            assignment_risk=assignment_risk,
            macro_regime=macro_regime,
            source_confidence=_aggregate_confidence(confidences),
            sources=tuple(sorted(covered_sources)),
            last_updated=max(filtered_source_updates) if filtered_source_updates else None,
            earnings_phase=earnings_snapshot.phase,
            earnings_event_date=earnings_snapshot.event_date,
            earnings_session_timing=earnings_session_timing,
            earnings_cohort_key=(
                None
                if earnings_snapshot.event_date is None
                else f"{earnings_snapshot.event_date}:{earnings_session_timing}:{earnings_snapshot.phase}"
            ),
            earnings_days_to_event=earnings_snapshot.days_to_event,
            earnings_days_since_event=earnings_snapshot.days_since_event,
            earnings_timing_confidence=earnings_timing_confidence,
            earnings_horizon_crosses_report=earnings_snapshot.horizon_crosses_report,
            earnings_primary_source=earnings_primary_source,
            earnings_supporting_sources=supporting_sources,
            earnings_consensus_status=earnings_consensus_status,
            earnings_enrichment=earnings_enrichment,
        )


def build_calendar_event_resolver(
    *,
    key_id: str,
    secret_key: str,
    data_base_url: str,
    database_url: str | None = None,
    macro_calendar_path: Path | None = None,
) -> CalendarEventResolver:
    store = CalendarEventStore(database_url or default_database_url())
    adapters: list[BaseCalendarEventAdapter] = [
        AlpacaCorporateActionsAdapter(
            key_id=key_id,
            secret_key=secret_key,
            base_url=data_base_url,
        ),
        MacroCalendarAdapter(macro_calendar_path or DEFAULT_MACRO_CALENDAR_PATH),
    ]
    return CalendarEventResolver(store=store, adapters=adapters)
