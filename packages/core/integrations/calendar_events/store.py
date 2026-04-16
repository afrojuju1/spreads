from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Iterator

from sqlalchemy import and_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from core.storage.calendar_models import CalendarEventModel, CalendarEventRefreshStateModel
from core.storage.db import build_session_factory

from .models import CalendarEventRecord


def _parse_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    normalized = value.replace("Z", "+00:00") if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _render_datetime(value: datetime) -> str:
    rendered = value.isoformat()
    return rendered.replace("+00:00", "Z") if rendered.endswith("+00:00") else rendered


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
                for record in records
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
            "coverage_start": _render_datetime(row.coverage_start),
            "coverage_end": _render_datetime(row.coverage_end),
            "refreshed_at": _render_datetime(row.refreshed_at),
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
        if row["coverage_start"] > coverage_start or row["coverage_end"] < coverage_end:
            return False
        if freshness_hours <= 0:
            return True
        refreshed_at = _parse_datetime(row["refreshed_at"])
        return refreshed_at >= datetime.now(UTC) - timedelta(hours=freshness_hours)

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
                else (
                    (CalendarEventModel.symbol == symbol)
                    | (CalendarEventModel.asset_scope == asset_scope)
                )
            )
            .order_by(CalendarEventModel.scheduled_at.asc())
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [
            CalendarEventRecord(
                event_id=row.event_id,
                event_type=row.event_type,
                symbol=row.symbol,
                asset_scope=row.asset_scope,
                scheduled_at=_render_datetime(row.scheduled_at),
                window_start=_render_datetime(row.window_start),
                window_end=_render_datetime(row.window_end),
                source=row.source,
                source_confidence=row.source_confidence,
                status=row.status,
                payload_json=row.payload_json,
                ingested_at=_render_datetime(row.ingested_at),
                source_updated_at=_render_datetime(row.source_updated_at),
            )
            for row in rows
        ]
