from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
from typing import Iterator

from sqlalchemy import and_, delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from core.value_coercion import utc_now
from core.storage.calendar_models import (
    CalendarEventModel,
    CalendarEventRefreshStateModel,
    EarningsEventConsensusModel,
    ProviderFetchAuditModel,
)
from core.storage.db import build_session_factory
from core.storage.serializers import parse_date as _parse_date, parse_datetime as _parse_datetime, render_value as _render_value

from .consensus import build_earnings_event_consensus
from .models import CalendarEventRecord, EarningsEventConsensusRecord, ProviderFetchAuditRecord
from .provider_cache import sanitize_provider_json


class CalendarEventStore:
    def __init__(self, database_url: str) -> None:
        self.path = database_url
        self.engine, self.session_factory = build_session_factory(database_url)
        with self.session_factory() as session:
            session.execute(select(1))

    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def close(self) -> None:
        self.engine.dispose()

    def upsert_events(self, records: list[CalendarEventRecord]) -> None:
        if not records:
            return
        records_by_id = {record.event_id: record for record in records}
        statement = insert(CalendarEventModel).values(
            [
                {
                    "event_id": record.event_id,
                    "event_type": record.event_type,
                    "symbol": record.symbol,
                    "asset_scope": record.asset_scope,
                    "scheduled_at": _parse_datetime(record.scheduled_at),
                    "window_start": _parse_datetime(record.window_start),
                    "window_end": _parse_datetime(record.window_end),
                    "source": record.source,
                    "source_confidence": record.source_confidence,
                    "status": record.status,
                    "payload_json": record.payload_json,
                    "ingested_at": _parse_datetime(record.ingested_at),
                    "source_updated_at": _parse_datetime(record.source_updated_at),
                }
                for record in records_by_id.values()
            ]
        )
        upsert = statement.on_conflict_do_update(
            index_elements=[CalendarEventModel.event_id],
            set_={
                "event_type": statement.excluded.event_type,
                "symbol": statement.excluded.symbol,
                "asset_scope": statement.excluded.asset_scope,
                "scheduled_at": statement.excluded.scheduled_at,
                "window_start": statement.excluded.window_start,
                "window_end": statement.excluded.window_end,
                "source": statement.excluded.source,
                "source_confidence": statement.excluded.source_confidence,
                "status": statement.excluded.status,
                "payload_json": statement.excluded.payload_json,
                "ingested_at": statement.excluded.ingested_at,
                "source_updated_at": statement.excluded.source_updated_at,
            },
        )
        with self.session_scope() as session:
            session.execute(upsert)

    def upsert_provider_fetch_audit(self, records: list[ProviderFetchAuditRecord]) -> None:
        if not records:
            return
        statement = insert(ProviderFetchAuditModel).values(
            [
                {
                    "audit_id": record.audit_id,
                    "provider": record.provider,
                    "endpoint": record.endpoint,
                    "params_hash": record.params_hash,
                    "params_json": sanitize_provider_json(dict(record.params_json)),
                    "coverage_start": _parse_datetime(record.coverage_start),
                    "coverage_end": _parse_datetime(record.coverage_end),
                    "page_key": record.page_key,
                    "status": record.status,
                    "cache_hit": record.cache_hit,
                    "payload_hash": record.payload_hash,
                    "row_count": record.row_count,
                    "fetched_at": _parse_datetime(record.fetched_at),
                    "expires_at": _parse_datetime(record.expires_at),
                    "backoff_until": _parse_datetime(record.backoff_until),
                    "error_code": record.error_code,
                    "error_message": record.error_message,
                    "created_at": _parse_datetime(record.created_at),
                }
                for record in records
            ]
        )
        upsert = statement.on_conflict_do_update(
            index_elements=[ProviderFetchAuditModel.audit_id],
            set_={
                "provider": statement.excluded.provider,
                "endpoint": statement.excluded.endpoint,
                "params_hash": statement.excluded.params_hash,
                "params_json": statement.excluded.params_json,
                "coverage_start": statement.excluded.coverage_start,
                "coverage_end": statement.excluded.coverage_end,
                "page_key": statement.excluded.page_key,
                "status": statement.excluded.status,
                "cache_hit": statement.excluded.cache_hit,
                "payload_hash": statement.excluded.payload_hash,
                "row_count": statement.excluded.row_count,
                "fetched_at": statement.excluded.fetched_at,
                "expires_at": statement.excluded.expires_at,
                "backoff_until": statement.excluded.backoff_until,
                "error_code": statement.excluded.error_code,
                "error_message": statement.excluded.error_message,
                "created_at": statement.excluded.created_at,
            },
        )
        with self.session_scope() as session:
            session.execute(upsert)

    def latest_provider_fetch_audit(
        self,
        *,
        provider: str,
        endpoint: str,
        params_hash: str,
        page_key: str | None = None,
    ) -> dict[str, object] | None:
        statement = (
            select(ProviderFetchAuditModel)
            .where(ProviderFetchAuditModel.provider == provider)
            .where(ProviderFetchAuditModel.endpoint == endpoint)
            .where(ProviderFetchAuditModel.params_hash == params_hash)
            .order_by(ProviderFetchAuditModel.fetched_at.desc())
            .limit(1)
        )
        if page_key is None:
            statement = statement.where(ProviderFetchAuditModel.page_key.is_(None))
        else:
            statement = statement.where(ProviderFetchAuditModel.page_key == page_key)
        with self.session_factory() as session:
            row = session.scalar(statement)
        if row is None:
            return None
        return {
            "audit_id": row.audit_id,
            "provider": row.provider,
            "endpoint": row.endpoint,
            "params_hash": row.params_hash,
            "params_json": dict(row.params_json or {}),
            "coverage_start": _render_value(row.coverage_start),
            "coverage_end": _render_value(row.coverage_end),
            "page_key": row.page_key,
            "status": row.status,
            "cache_hit": row.cache_hit,
            "payload_hash": row.payload_hash,
            "row_count": row.row_count,
            "fetched_at": _render_value(row.fetched_at),
            "expires_at": _render_value(row.expires_at),
            "backoff_until": _render_value(row.backoff_until),
            "error_code": row.error_code,
            "error_message": row.error_message,
            "created_at": _render_value(row.created_at),
        }

    def set_refresh_state(
        self,
        *,
        source: str,
        scope_key: str,
        coverage_start: str,
        coverage_end: str,
        refreshed_at: str,
    ) -> None:
        statement = insert(CalendarEventRefreshStateModel).values(
            {
                "source": source,
                "scope_key": scope_key,
                "coverage_start": _parse_datetime(coverage_start),
                "coverage_end": _parse_datetime(coverage_end),
                "refreshed_at": _parse_datetime(refreshed_at),
            }
        )
        upsert = statement.on_conflict_do_update(
            index_elements=[
                CalendarEventRefreshStateModel.source,
                CalendarEventRefreshStateModel.scope_key,
            ],
            set_={
                "coverage_start": statement.excluded.coverage_start,
                "coverage_end": statement.excluded.coverage_end,
                "refreshed_at": statement.excluded.refreshed_at,
            },
        )
        with self.session_scope() as session:
            session.execute(upsert)

    def get_refresh_state(self, *, source: str, scope_key: str) -> dict[str, str] | None:
        statement = select(CalendarEventRefreshStateModel).where(
            and_(
                CalendarEventRefreshStateModel.source == source,
                CalendarEventRefreshStateModel.scope_key == scope_key,
            )
        )
        with self.session_factory() as session:
            row = session.scalar(statement)
        if row is None:
            return None
        return {
            "source": row.source,
            "scope_key": row.scope_key,
            "coverage_start": str(_render_value(row.coverage_start)),
            "coverage_end": str(_render_value(row.coverage_end)),
            "refreshed_at": str(_render_value(row.refreshed_at)),
        }

    def has_fresh_coverage(
        self,
        *,
        source: str,
        scope_key: str,
        coverage_start: str,
        coverage_end: str,
        freshness_hours: int,
    ) -> bool:
        row = self.get_refresh_state(source=source, scope_key=scope_key)
        if row is None:
            return False
        stored_coverage_start = _parse_datetime(row["coverage_start"])
        stored_coverage_end = _parse_datetime(row["coverage_end"])
        requested_coverage_start = _parse_datetime(coverage_start)
        requested_coverage_end = _parse_datetime(coverage_end)
        if stored_coverage_start > requested_coverage_start or stored_coverage_end < requested_coverage_end:
            return False
        if freshness_hours <= 0:
            return True
        refreshed_at = _parse_datetime(row["refreshed_at"])
        return refreshed_at >= utc_now() - timedelta(hours=freshness_hours)

    def query_events(
        self,
        *,
        symbol: str,
        asset_scope: str | None,
        window_start: str,
        window_end: str,
    ) -> list[CalendarEventRecord]:
        statement = (
            select(CalendarEventModel)
            .where(CalendarEventModel.scheduled_at >= _parse_datetime(window_start))
            .where(CalendarEventModel.scheduled_at <= _parse_datetime(window_end))
            .where(
                (CalendarEventModel.symbol == symbol)
                if not asset_scope
                else ((CalendarEventModel.symbol == symbol) | (CalendarEventModel.asset_scope == asset_scope))
            )
            .order_by(CalendarEventModel.scheduled_at.asc())
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [_calendar_event_record_from_row(row) for row in rows]

    def query_earnings_events(
        self,
        *,
        window_start: str,
        window_end: str,
        symbol: str | None = None,
        sources: set[str] | None = None,
    ) -> list[CalendarEventRecord]:
        statement = (
            select(CalendarEventModel)
            .where(CalendarEventModel.event_type == "earnings")
            .where(CalendarEventModel.scheduled_at >= _parse_datetime(window_start))
            .where(CalendarEventModel.scheduled_at <= _parse_datetime(window_end))
            .order_by(CalendarEventModel.symbol.asc(), CalendarEventModel.scheduled_at.asc())
        )
        if symbol:
            statement = statement.where(CalendarEventModel.symbol == symbol.upper())
        if sources:
            statement = statement.where(CalendarEventModel.source.in_(sorted(sources)))
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [_calendar_event_record_from_row(row) for row in rows]

    def upsert_earnings_event_consensus(self, records: list[EarningsEventConsensusRecord]) -> None:
        if not records:
            return
        statement = insert(EarningsEventConsensusModel).values([_earnings_event_consensus_values(record) for record in records])
        upsert = statement.on_conflict_do_update(
            index_elements=[EarningsEventConsensusModel.consensus_id],
            set_={
                "symbol": statement.excluded.symbol,
                "event_date": statement.excluded.event_date,
                "scheduled_at": statement.excluded.scheduled_at,
                "session_timing": statement.excluded.session_timing,
                "event_status": statement.excluded.event_status,
                "primary_source": statement.excluded.primary_source,
                "supporting_sources_json": statement.excluded.supporting_sources_json,
                "conflicting_sources_json": statement.excluded.conflicting_sources_json,
                "consensus_status": statement.excluded.consensus_status,
                "source_confidence": statement.excluded.source_confidence,
                "timing_confidence": statement.excluded.timing_confidence,
                "provider_payload_json": statement.excluded.provider_payload_json,
                "computed_at": statement.excluded.computed_at,
                "stale_after": statement.excluded.stale_after,
            },
        )
        with self.session_scope() as session:
            session.execute(upsert)

    def rebuild_earnings_event_consensus(
        self,
        *,
        window_start: str,
        window_end: str,
        sources: set[str] | None = None,
        computed_at: str | None = None,
        stale_after: str | None = None,
    ) -> list[EarningsEventConsensusRecord]:
        events = self.query_earnings_events(window_start=window_start, window_end=window_end, sources=sources)
        consensus_records = build_earnings_event_consensus(
            events,
            covered_sources=sources,
            computed_at=computed_at,
            stale_after=stale_after,
        )
        start_date = _parse_datetime(window_start).date()
        end_date = _parse_datetime(window_end).date()
        delete_statement = delete(EarningsEventConsensusModel).where(
            and_(
                EarningsEventConsensusModel.event_date >= start_date,
                EarningsEventConsensusModel.event_date <= end_date,
            )
        )
        with self.session_scope() as session:
            session.execute(delete_statement)
            if consensus_records:
                statement = insert(EarningsEventConsensusModel).values(
                    [_earnings_event_consensus_values(record) for record in consensus_records]
                )
                session.execute(statement)
        return consensus_records

    def query_earnings_event_consensus(
        self,
        *,
        window_start: str,
        window_end: str,
        symbol: str | None = None,
    ) -> list[EarningsEventConsensusRecord]:
        statement = (
            select(EarningsEventConsensusModel)
            .where(EarningsEventConsensusModel.event_date >= _parse_datetime(window_start).date())
            .where(EarningsEventConsensusModel.event_date <= _parse_datetime(window_end).date())
            .order_by(EarningsEventConsensusModel.event_date.asc(), EarningsEventConsensusModel.symbol.asc())
        )
        if symbol:
            statement = statement.where(EarningsEventConsensusModel.symbol == symbol.upper())
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [
            EarningsEventConsensusRecord(
                consensus_id=row.consensus_id,
                symbol=row.symbol,
                event_date=str(_render_value(row.event_date)),
                scheduled_at=str(_render_value(row.scheduled_at)),
                session_timing=row.session_timing,  # type: ignore[arg-type]
                event_status=row.event_status,
                primary_source=row.primary_source,
                supporting_sources=tuple(row.supporting_sources_json or ()),
                conflicting_sources=tuple(row.conflicting_sources_json or ()),
                consensus_status=row.consensus_status,  # type: ignore[arg-type]
                source_confidence=row.source_confidence,  # type: ignore[arg-type]
                timing_confidence=row.timing_confidence,  # type: ignore[arg-type]
                provider_payload=dict(row.provider_payload_json or {}),
                computed_at=str(_render_value(row.computed_at)),
                stale_after=str(_render_value(row.stale_after)),
            )
            for row in rows
        ]


def _calendar_event_record_from_row(row: CalendarEventModel) -> CalendarEventRecord:
    return CalendarEventRecord(
        event_id=row.event_id,
        event_type=row.event_type,
        symbol=row.symbol,
        asset_scope=row.asset_scope,
        scheduled_at=str(_render_value(row.scheduled_at)),
        window_start=str(_render_value(row.window_start)),
        window_end=str(_render_value(row.window_end)),
        source=row.source,
        source_confidence=row.source_confidence,
        status=row.status,
        payload_json=row.payload_json,
        ingested_at=str(_render_value(row.ingested_at)),
        source_updated_at=str(_render_value(row.source_updated_at)),
    )


def _earnings_event_consensus_values(record: EarningsEventConsensusRecord) -> dict[str, object]:
    return {
        "consensus_id": record.consensus_id,
        "symbol": record.symbol,
        "event_date": _parse_date(record.event_date),
        "scheduled_at": _parse_datetime(record.scheduled_at),
        "session_timing": record.session_timing,
        "event_status": record.event_status,
        "primary_source": record.primary_source,
        "supporting_sources_json": list(record.supporting_sources),
        "conflicting_sources_json": list(record.conflicting_sources),
        "consensus_status": record.consensus_status,
        "source_confidence": record.source_confidence,
        "timing_confidence": record.timing_confidence,
        "provider_payload_json": dict(record.provider_payload),
        "computed_at": _parse_datetime(record.computed_at),
        "stale_after": _parse_datetime(record.stale_after),
    }
