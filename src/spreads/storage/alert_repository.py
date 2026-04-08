from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from typing import Any, Iterator

from sqlalchemy import delete, func, inspect, select
from sqlalchemy.orm import Session

from spreads.storage.alert_models import AlertEventModel, AlertStateModel
from spreads.storage.db import build_session_factory
from spreads.storage.records import AlertEventRecord, AlertStateRecord
from spreads.storage.serializers import (
    parse_date,
    parse_datetime,
    to_alert_event_record,
    to_alert_state_record,
)


class AlertRepository:
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

    def schema_ready(self) -> bool:
        tables = set(inspect(self.engine).get_table_names(schema="public"))
        required = {"alert_events", "alert_state"}
        return required.issubset(tables)

    def get_alert_state(self, dedupe_key: str) -> AlertStateRecord | None:
        with self.session_factory() as session:
            row = session.get(AlertStateModel, dedupe_key)
        if row is None:
            return None
        return to_alert_state_record(row)

    def upsert_alert_state(
        self,
        *,
        dedupe_key: str,
        last_alert_at: str | datetime,
        last_cycle_id: str,
        last_alert_type: str,
        state: dict[str, Any],
    ) -> None:
        with self.session_scope() as session:
            row = session.get(AlertStateModel, dedupe_key)
            if row is None:
                row = AlertStateModel(dedupe_key=dedupe_key)
                session.add(row)
            parsed_last_alert_at = parse_datetime(last_alert_at)
            if parsed_last_alert_at is None:
                raise ValueError("last_alert_at is required")
            row.last_alert_at = parsed_last_alert_at
            row.last_cycle_id = last_cycle_id
            row.last_alert_type = last_alert_type
            row.state_json = state

    def create_alert_event(
        self,
        *,
        created_at: str | datetime,
        session_date: str | date,
        label: str,
        cycle_id: str,
        symbol: str,
        alert_type: str,
        dedupe_key: str,
        status: str,
        delivery_target: str,
        payload: dict[str, Any],
        response: dict[str, Any] | None = None,
        error_text: str | None = None,
    ) -> AlertEventRecord:
        with self.session_scope() as session:
            parsed_created_at = parse_datetime(created_at)
            if parsed_created_at is None:
                raise ValueError("created_at is required")
            row = AlertEventModel(
                created_at=parsed_created_at,
                session_date=parse_date(session_date),
                label=label,
                cycle_id=cycle_id,
                symbol=symbol,
                alert_type=alert_type,
                dedupe_key=dedupe_key,
                status=status,
                delivery_target=delivery_target,
                payload_json=payload,
                response_json=response,
                error_text=error_text,
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return to_alert_event_record(row)

    def mark_alert_event_status(
        self,
        *,
        alert_id: int,
        status: str,
        response: dict[str, Any] | None = None,
        error_text: str | None = None,
    ) -> AlertEventRecord:
        with self.session_scope() as session:
            row = session.get(AlertEventModel, alert_id)
            if row is None:
                raise ValueError(f"Unknown alert_id: {alert_id}")
            row.status = status
            row.response_json = response
            row.error_text = error_text
            session.flush()
            session.refresh(row)
            return to_alert_event_record(row)

    def list_alert_events(
        self,
        *,
        session_date: str | None = None,
        label: str | None = None,
        symbol: str | None = None,
        limit: int = 100,
    ) -> list[AlertEventRecord]:
        statement = select(AlertEventModel)
        if session_date:
            statement = statement.where(AlertEventModel.session_date == date.fromisoformat(session_date))
        if label:
            statement = statement.where(AlertEventModel.label == label)
        if symbol:
            statement = statement.where(AlertEventModel.symbol == symbol.upper())
        statement = statement.order_by(AlertEventModel.created_at.desc(), AlertEventModel.alert_id.desc()).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [to_alert_event_record(row) for row in rows]

    def count_alert_events(
        self,
        *,
        session_date: str | None = None,
        label: str | None = None,
    ) -> int:
        statement = select(func.count()).select_from(AlertEventModel)
        if session_date:
            statement = statement.where(AlertEventModel.session_date == date.fromisoformat(session_date))
        if label:
            statement = statement.where(AlertEventModel.label == label)
        with self.session_factory() as session:
            count = session.scalar(statement)
        return int(count or 0)

    def get_alert_event(self, alert_id: int) -> AlertEventRecord | None:
        with self.session_factory() as session:
            row = session.get(AlertEventModel, alert_id)
        if row is None:
            return None
        return to_alert_event_record(row)

    def truncate_all(self) -> None:
        with self.session_scope() as session:
            session.execute(delete(AlertEventModel))
            session.execute(delete(AlertStateModel))

    def close(self) -> None:
        self.engine.dispose()
