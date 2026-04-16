from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace
from datetime import UTC, date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from .adapters.alpaca_corporate_actions import AlpacaCorporateActionsAdapter
from .adapters.alpha_vantage_earnings_calendar import (
    AlphaVantageEarningsCalendarAdapter,
)
from .adapters.base import BaseCalendarEventAdapter
from .adapters.earnings_calendar import EarningsCalendarAdapter
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
from .earnings_phase import resolve_earnings_phase_snapshot
from .models import (
    CalendarEventContext,
    CalendarEventQuery,
    CalendarEventReason,
    CalendarEventRecord,
)
from .store import CalendarEventStore
from core.runtime.config import default_alpha_vantage_api_key, default_database_url

NEW_YORK = ZoneInfo("America/New_York")


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _aggregate_confidence(confidences: list[str]) -> str:
    if not confidences:
        return "unknown"
    ranked = sorted(confidences, key=lambda item: SOURCE_CONFIDENCE_RANK.get(item, 0))
    return ranked[0]


def _as_text(value: object) -> str | None:
    rendered = str(value or "").strip()
    return rendered or None


def _parse_datetime(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00") if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _record_payload(record: CalendarEventRecord) -> dict[str, object]:
    if not record.payload_json:
        return {}
    try:
        payload = json.loads(record.payload_json)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _earnings_session_timing(record: CalendarEventRecord) -> str:
    payload = _record_payload(record)
    raw = str(
        payload.get("when")
        or payload.get("reportTime")
        or payload.get("report_time")
        or payload.get("time")
        or ""
    ).strip().lower()
    if raw in {"bmo", "before_open"} or "before" in raw:
        return "before_open"
    if raw in {"amc", "after_close"} or "after" in raw:
        return "after_close"
    if raw:
        return "during_market"
    return "unknown"


def _render_earnings_timestamp(date_str: str, timing: str) -> str:
    local_time = time(12, 0)
    if timing == "before_open":
        local_time = time(9, 0)
    elif timing == "after_close":
        local_time = time(16, 15)
    local_dt = datetime.combine(date.fromisoformat(date_str), local_time, tzinfo=NEW_YORK)
    return local_dt.astimezone(UTC).isoformat()


def _fresh_earnings_records(
    records: list[CalendarEventRecord],
    *,
    covered_sources: set[str],
) -> list[CalendarEventRecord]:
    return [
        record
        for record in records
        if record.event_type == "earnings" and record.source in covered_sources
    ]


def _reconcile_earnings_records(
    records: list[CalendarEventRecord],
    *,
    covered_sources: set[str],
) -> tuple[list[CalendarEventRecord], dict[str, dict[str, object]]]:
    fresh_records = sorted(
        _fresh_earnings_records(records, covered_sources=covered_sources),
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
                max(
                    SOURCE_CONFIDENCE_RANK.get(record.source_confidence, 0)
                    for record in item[1]
                ),
                max(_parse_datetime(record.source_updated_at) for record in item[1]),
                item[0],
            ),
        )
        date_sources = sorted({record.source for record in date_records})
        cluster_sources = sorted({record.source for record in cluster})
        timing_counts = Counter(
            timing
            for timing in (_earnings_session_timing(record) for record in date_records)
            if timing != "unknown"
        )
        canonical_timing = (
            max(timing_counts.items(), key=lambda item: (item[1], item[0]))[0]
            if timing_counts
            else "unknown"
        )
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
                scheduled_at=_render_earnings_timestamp(event_date, canonical_timing),
                window_start=_render_earnings_timestamp(event_date, canonical_timing),
                window_end=_render_earnings_timestamp(event_date, canonical_timing),
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
            "consensus_status": consensus_status,
            "timing_confidence": timing_confidence,
            "session_timing": canonical_timing,
        }

    canonical_records.sort(key=lambda item: _parse_datetime(item.scheduled_at))
    return canonical_records, consensus_by_date


def _days_until(window_start: datetime, scheduled_at: datetime) -> int:
    return (scheduled_at.date() - window_start.date()).days


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
        earnings_query = query

        for adapter in self.adapters:
            if not adapter.applies_to(query):
                continue
            adapter_query = adapter.coverage_query(query)
            if adapter.source_name == "dolt_earnings_calendar":
                earnings_query = adapter_query
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
                covered_sources.append(adapter.source_name)
                confidences.append(adapter.source_confidence)
                state = self.store.get_refresh_state(source=adapter.source_name, scope_key=scope_key)
                if state is not None:
                    source_updates.append(state["refreshed_at"])
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
        covered_source_set = set(covered_sources)
        canonical_window_earnings, _window_consensus = _reconcile_earnings_records(
            records,
            covered_sources=covered_source_set,
        )
        normalized_records = [
            record for record in records if record.event_type != "earnings"
        ] + canonical_window_earnings
        normalized_records.sort(key=lambda item: _parse_datetime(item.scheduled_at))
        reasons.extend(_build_reason(record) for record in normalized_records)

        macro_events = [
            record
            for record in normalized_records
            if record.event_type.startswith("macro_")
        ]
        assignment_risk = any(
            record.event_type in DIVIDEND_EVENT_TYPES for record in normalized_records
        )
        macro_regime = None
        if macro_events:
            macro_regime = ",".join(sorted({record.event_type for record in macro_events}))

        days_to_nearest_event = None
        if normalized_records:
            start_dt = datetime.fromisoformat(window_start)
            nearest = min(
                datetime.fromisoformat(record.scheduled_at)
                for record in normalized_records
            )
            days_to_nearest_event = _days_until(start_dt, nearest)

        if missing_required:
            status = "unknown"
        elif normalized_records:
            status = "penalized"
        else:
            status = "clean"

        canonical_earnings_records: list[CalendarEventRecord] = []
        earnings_consensus_by_date: dict[str, dict[str, object]] = {}
        if underlying_type == "single_name_equity":
            earnings_records = self.store.query_events(
                symbol=query.symbol,
                asset_scope=None,
                window_start=earnings_query.window_start,
                window_end=earnings_query.window_end,
            )
            canonical_earnings_records, earnings_consensus_by_date = (
                _reconcile_earnings_records(
                    earnings_records,
                    covered_sources=covered_source_set,
                )
            )
        earnings_snapshot = resolve_earnings_phase_snapshot(
            records=canonical_earnings_records,
            as_of=query.window_start,
            horizon_end=query.window_end,
        )
        anchor_consensus = (
            earnings_consensus_by_date.get(str(earnings_snapshot.event_date))
            if earnings_snapshot.event_date
            else None
        )
        earnings_session_timing = str(
            (anchor_consensus or {}).get("session_timing")
            or earnings_snapshot.session_timing
            or "unknown"
        )
        earnings_timing_confidence = str(
            (anchor_consensus or {}).get("timing_confidence")
            or earnings_snapshot.timing_confidence
            or "unknown"
        )
        earnings_primary_source = _as_text(
            None if anchor_consensus is None else anchor_consensus.get("primary_source")
        )
        supporting_sources = (
            ()
            if anchor_consensus is None
            else tuple(anchor_consensus.get("supporting_sources") or ())
        )
        earnings_consensus_status = str(
            (anchor_consensus or {}).get("consensus_status") or "missing"
        )

        return CalendarEventContext(
            status=status,
            reasons=tuple(reasons),
            days_to_nearest_event=days_to_nearest_event,
            events_before_expiry=len(normalized_records),
            assignment_risk=assignment_risk,
            macro_regime=macro_regime,
            source_confidence=_aggregate_confidence(confidences),
            sources=tuple(sorted(covered_sources)),
            last_updated=max(source_updates) if source_updates else None,
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
        )


def build_calendar_event_resolver(
    *,
    key_id: str,
    secret_key: str,
    data_base_url: str,
    database_url: str | None = None,
    macro_calendar_path: Path | None = None,
    alpha_vantage_api_key: str | None = None,
) -> CalendarEventResolver:
    store = CalendarEventStore(database_url or default_database_url())
    adapters: list[BaseCalendarEventAdapter] = [EarningsCalendarAdapter()]
    resolved_alpha_vantage_api_key = (
        alpha_vantage_api_key or default_alpha_vantage_api_key()
    )
    if resolved_alpha_vantage_api_key:
        adapters.append(
            AlphaVantageEarningsCalendarAdapter(
                api_key=resolved_alpha_vantage_api_key
            )
        )
    adapters.extend(
        [
            AlpacaCorporateActionsAdapter(
                key_id=key_id,
                secret_key=secret_key,
                base_url=data_base_url,
            ),
            MacroCalendarAdapter(macro_calendar_path or DEFAULT_MACRO_CALENDAR_PATH),
        ]
    )
    return CalendarEventResolver(store=store, adapters=adapters)
