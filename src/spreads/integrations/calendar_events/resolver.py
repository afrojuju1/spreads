from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from .adapters.alpaca_corporate_actions import AlpacaCorporateActionsAdapter
from .adapters.base import BaseCalendarEventAdapter
from .adapters.earnings_calendar import EarningsCalendarAdapter
from .adapters.macro_calendar import MacroCalendarAdapter
from .config import (
    DEFAULT_CALENDAR_DB_PATH,
    DEFAULT_MACRO_CALENDAR_PATH,
    DIVIDEND_EVENT_TYPES,
    MACRO_ASSET_SCOPE,
    OPTIONAL_SOURCES_BY_UNDERLYING,
    REQUIRED_SOURCES_BY_UNDERLYING,
    SOURCE_CONFIDENCE_RANK,
    SOURCE_FRESHNESS_HOURS,
)
from .models import (
    CalendarEventContext,
    CalendarEventQuery,
    CalendarEventReason,
    CalendarEventRecord,
)
from .store import CalendarEventStore


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _aggregate_confidence(confidences: list[str]) -> str:
    if not confidences:
        return "unknown"
    ranked = sorted(confidences, key=lambda item: SOURCE_CONFIDENCE_RANK.get(item, 0))
    return ranked[0]


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

        for adapter in self.adapters:
            if not adapter.applies_to(query):
                continue
            freshness_hours = SOURCE_FRESHNESS_HOURS.get(adapter.source_name, 24)
            scope_key = adapter.scope_key(query)
            has_fresh_coverage = self.store.has_fresh_coverage(
                source=adapter.source_name,
                scope_key=scope_key,
                coverage_start=window_start,
                coverage_end=window_end,
                freshness_hours=freshness_hours,
            )

            fetch_error: Exception | None = None
            if refresh or not has_fresh_coverage or adapter.refresh_always:
                try:
                    records = adapter.fetch(query)
                    self.store.upsert_events(records)
                    self.store.set_refresh_state(
                        source=adapter.source_name,
                        scope_key=scope_key,
                        coverage_start=window_start,
                        coverage_end=window_end,
                        refreshed_at=_utc_now_iso(),
                    )
                except Exception as exc:
                    fetch_error = exc

            has_fresh_coverage = self.store.has_fresh_coverage(
                source=adapter.source_name,
                scope_key=scope_key,
                coverage_start=window_start,
                coverage_end=window_end,
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
        reasons.extend(_build_reason(record) for record in records)

        macro_events = [record for record in records if record.event_type.startswith("macro_")]
        assignment_risk = any(record.event_type in DIVIDEND_EVENT_TYPES for record in records)
        macro_regime = None
        if macro_events:
            macro_regime = ",".join(sorted({record.event_type for record in macro_events}))

        days_to_nearest_event = None
        if records:
            start_dt = datetime.fromisoformat(window_start)
            nearest = min(datetime.fromisoformat(record.scheduled_at) for record in records)
            days_to_nearest_event = _days_until(start_dt, nearest)

        if missing_required:
            status = "unknown"
        elif records:
            status = "penalized"
        else:
            status = "clean"

        return CalendarEventContext(
            status=status,
            reasons=tuple(reasons),
            days_to_nearest_event=days_to_nearest_event,
            events_before_expiry=len(records),
            assignment_risk=assignment_risk,
            macro_regime=macro_regime,
            source_confidence=_aggregate_confidence(confidences),
            sources=tuple(sorted(covered_sources)),
            last_updated=max(source_updates) if source_updates else None,
        )


def build_calendar_event_resolver(
    *,
    key_id: str,
    secret_key: str,
    data_base_url: str,
    db_path: Path | None = None,
    macro_calendar_path: Path | None = None,
) -> CalendarEventResolver:
    store = CalendarEventStore(db_path or DEFAULT_CALENDAR_DB_PATH)
    adapters: list[BaseCalendarEventAdapter] = [
        EarningsCalendarAdapter(),
        AlpacaCorporateActionsAdapter(key_id=key_id, secret_key=secret_key, base_url=data_base_url),
        MacroCalendarAdapter(macro_calendar_path or DEFAULT_MACRO_CALENDAR_PATH),
    ]
    return CalendarEventResolver(store=store, adapters=adapters)
