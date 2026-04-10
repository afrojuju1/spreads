from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import select

from spreads.storage.base import RepositoryBase
from spreads.storage.event_models import EventLogModel
from spreads.storage.records import EventLogRecord
from spreads.storage.serializers import parse_date, parse_datetime


class EventRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("event_log")

    def create_event(
        self,
        *,
        event_id: str,
        event_class: str,
        event_type: str,
        topic: str,
        occurred_at: str,
        ingested_at: str,
        source: str,
        entity_type: str,
        entity_key: str,
        payload: dict[str, Any],
        schema_version: str,
        producer_version: str,
        session_date: str | date | None = None,
        market_session: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> EventLogRecord:
        with self.session_scope() as session:
            row = EventLogModel(
                event_id=event_id,
                event_class=event_class,
                event_type=event_type,
                topic=topic,
                occurred_at=parse_datetime(occurred_at),
                ingested_at=parse_datetime(ingested_at),
                source=source,
                entity_type=entity_type,
                entity_key=entity_key,
                session_date=None if session_date is None else parse_date(session_date),
                market_session=market_session,
                payload_json=dict(payload),
                schema_version=schema_version,
                producer_version=producer_version,
                correlation_id=correlation_id,
                causation_id=causation_id,
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return self.row(row)

    def create_events(
        self,
        rows: list[dict[str, Any]],
    ) -> list[EventLogRecord]:
        if not rows:
            return []
        with self.session_scope() as session:
            models: list[EventLogModel] = []
            for payload in rows:
                row = EventLogModel(
                    event_id=str(payload["event_id"]),
                    event_class=str(payload["event_class"]),
                    event_type=str(payload["event_type"]),
                    topic=str(payload["topic"]),
                    occurred_at=parse_datetime(payload["occurred_at"]),
                    ingested_at=parse_datetime(payload["ingested_at"]),
                    source=str(payload["source"]),
                    entity_type=str(payload["entity_type"]),
                    entity_key=str(payload["entity_key"]),
                    session_date=(
                        None
                        if payload.get("session_date") is None
                        else parse_date(payload["session_date"])
                    ),
                    market_session=payload.get("market_session"),
                    payload_json=dict(payload.get("payload") or {}),
                    schema_version=str(payload["schema_version"]),
                    producer_version=str(payload["producer_version"]),
                    correlation_id=payload.get("correlation_id"),
                    causation_id=payload.get("causation_id"),
                )
                session.add(row)
                models.append(row)
            session.flush()
            for row in models:
                session.refresh(row)
            return self.rows(models)

    def list_events(
        self,
        *,
        event_class: str | None = None,
        topic: str | None = None,
        entity_type: str | None = None,
        entity_key: str | None = None,
        session_date: str | None = None,
        correlation_id: str | None = None,
        occurred_from: str | None = None,
        occurred_to: str | None = None,
        limit: int = 200,
    ) -> list[EventLogRecord]:
        statement = select(EventLogModel)
        if event_class:
            statement = statement.where(EventLogModel.event_class == event_class)
        if topic:
            statement = statement.where(EventLogModel.topic == topic)
        if entity_type:
            statement = statement.where(EventLogModel.entity_type == entity_type)
        if entity_key:
            statement = statement.where(EventLogModel.entity_key == entity_key)
        if session_date:
            statement = statement.where(EventLogModel.session_date == date.fromisoformat(session_date))
        if correlation_id:
            statement = statement.where(EventLogModel.correlation_id == correlation_id)
        occurred_from_dt = parse_datetime(occurred_from)
        if occurred_from_dt is not None:
            statement = statement.where(EventLogModel.occurred_at >= occurred_from_dt)
        occurred_to_dt = parse_datetime(occurred_to)
        if occurred_to_dt is not None:
            statement = statement.where(EventLogModel.occurred_at <= occurred_to_dt)
        statement = statement.order_by(EventLogModel.occurred_at.asc(), EventLogModel.event_id.asc()).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)
