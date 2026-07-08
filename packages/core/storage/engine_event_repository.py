from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.engine.events import ENGINE_EVENT_STREAM, EngineEvent
from core.engine.ids import engine_outbox_id
from core.storage.base import RepositoryBase
from core.storage.engine_event_models import EngineEventModel, EngineOutboxModel
from core.storage.records import EngineEventRecord, EngineOutboxRecord
from core.storage.serializers import parse_date, parse_datetime, render_value


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _optional_date(value: Any) -> Any:
    if value in (None, ""):
        return None
    return parse_date(value)


def _optional_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    return parse_datetime(value)


class EngineEventRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("engine_events", "engine_outbox")

    def _event_row(self, row: EngineEventModel) -> EngineEventRecord:
        return self.row(row, aliases={"payload_json": "payload", "metadata_json": "metadata"})

    def _outbox_row(self, row: EngineOutboxModel) -> EngineOutboxRecord:
        return self.row(row, aliases={"payload_json": "payload", "headers_json": "headers"})

    def append_engine_event(
        self,
        event: EngineEvent,
        *,
        publish: bool = True,
        stream: str = ENGINE_EVENT_STREAM,
        subject: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> EngineEventRecord:
        if not self.schema_ready():
            raise RuntimeError("Engine event schema is not ready. Run Alembic migrations before appending engine events.")

        now = _utc_now()
        idempotency_key = event.resolved_idempotency_key()
        event_id = event.resolved_engine_event_id()
        recorded_at = _optional_datetime(event.recorded_at) or now
        occurred_at = parse_datetime(event.occurred_at)
        if occurred_at is None:
            raise ValueError("Engine event requires occurred_at")

        payload = render_value(event.payload)
        metadata = render_value(event.metadata)
        outbox_subject = subject or event.subject()
        event_payload = {
            "engine_event_id": event_id,
            "run_id": event.run_id,
            "workflow_id": event.workflow_id,
            "workflow_run_id": event.workflow_run_id,
            "event_type": event.event_type,
            "event_version": event.event_version,
            "aggregate_type": event.aggregate_type,
            "aggregate_id": event.aggregate_id,
            "aggregate_version": event.aggregate_version,
            "lifecycle_object": event.lifecycle_object,
            "from_state": event.from_state,
            "to_state": event.to_state,
            "trading_strategy_id": event.trading_strategy_id,
            "trade_signal_id": event.trade_signal_id,
            "trade_decision_id": event.trade_decision_id,
            "execution_intent_id": event.execution_intent_id,
            "execution_attempt_id": event.execution_attempt_id,
            "broker_order_id": event.broker_order_id,
            "position_id": event.position_id,
            "session_date": _optional_date(event.session_date),
            "market_session": event.market_session,
            "correlation_id": event.correlation_id,
            "causation_id": event.causation_id,
            "idempotency_key": idempotency_key,
            "payload_json": payload,
            "metadata_json": metadata,
            "occurred_at": occurred_at,
            "recorded_at": recorded_at,
        }
        with self.session_scope() as session:
            inserted = session.execute(
                pg_insert(EngineEventModel)
                .values(**event_payload)
                .on_conflict_do_nothing(index_elements=["idempotency_key"])
                .returning(EngineEventModel.engine_event_id)
            ).scalar_one_or_none()
            resolved_event_id = inserted or session.scalar(
                select(EngineEventModel.engine_event_id).where(EngineEventModel.idempotency_key == idempotency_key)
            )
            if resolved_event_id is None:
                raise RuntimeError(f"Unable to resolve engine event for idempotency key {idempotency_key!r}")
            if publish:
                outbox_id = engine_outbox_id(str(resolved_event_id), stream=stream, subject=outbox_subject)
                outbox_payload = {
                    "engine_event_id": resolved_event_id,
                    "event_type": event.event_type,
                    "event_version": event.event_version,
                    "aggregate_type": event.aggregate_type,
                    "aggregate_id": event.aggregate_id,
                    "aggregate_version": event.aggregate_version,
                    "workflow_id": event.workflow_id,
                    "workflow_run_id": event.workflow_run_id,
                    "correlation_id": event.correlation_id,
                    "causation_id": event.causation_id,
                    "occurred_at": render_value(occurred_at),
                    "recorded_at": render_value(recorded_at),
                    "payload": payload,
                    "metadata": metadata,
                }
                session.execute(
                    pg_insert(EngineOutboxModel)
                    .values(
                        engine_outbox_id=outbox_id,
                        engine_event_id=resolved_event_id,
                        stream=stream,
                        subject=outbox_subject,
                        event_type=event.event_type,
                        aggregate_type=event.aggregate_type,
                        aggregate_id=event.aggregate_id,
                        payload_json=outbox_payload,
                        headers_json=dict(headers or {}),
                        publish_state="pending",
                        attempt_count=0,
                        next_attempt_at=None,
                        last_attempt_at=None,
                        published_at=None,
                        error_text=None,
                        created_at=now,
                        updated_at=now,
                    )
                    .on_conflict_do_nothing(index_elements=["engine_outbox_id"])
                )
            row = session.get(EngineEventModel, resolved_event_id)
            if row is None:
                raise RuntimeError(f"Engine event {resolved_event_id!r} disappeared during append")
            return self._event_row(row)

    def get_engine_event(self, engine_event_id: str) -> EngineEventRecord | None:
        with self.session_factory() as session:
            row = session.get(EngineEventModel, engine_event_id)
            if row is None:
                return None
            return self._event_row(row)

    def list_pending_outbox(self, *, limit: int = 100, now: datetime | None = None) -> list[EngineOutboxRecord]:
        observed_at = now or _utc_now()
        with self.session_factory() as session:
            rows = session.scalars(
                select(EngineOutboxModel)
                .where(
                    EngineOutboxModel.publish_state == "pending",
                    or_(EngineOutboxModel.next_attempt_at.is_(None), EngineOutboxModel.next_attempt_at <= observed_at),
                )
                .order_by(EngineOutboxModel.created_at, EngineOutboxModel.engine_outbox_id)
                .limit(limit)
            ).all()
            return [self._outbox_row(row) for row in rows]

    def mark_outbox_published(self, engine_outbox_id: str, *, published_at: datetime | None = None) -> EngineOutboxRecord | None:
        observed_at = published_at or _utc_now()
        with self.session_scope() as session:
            session.execute(
                update(EngineOutboxModel)
                .where(EngineOutboxModel.engine_outbox_id == engine_outbox_id)
                .values(
                    publish_state="published",
                    published_at=observed_at,
                    last_attempt_at=observed_at,
                    updated_at=observed_at,
                    error_text=None,
                )
            )
            row = session.get(EngineOutboxModel, engine_outbox_id)
            if row is None:
                return None
            return self._outbox_row(row)

    def mark_outbox_failed(
        self,
        engine_outbox_id: str,
        *,
        error_text: str,
        retry_after_seconds: int = 30,
        failed_at: datetime | None = None,
    ) -> EngineOutboxRecord | None:
        observed_at = failed_at or _utc_now()
        next_attempt_at = observed_at + timedelta(seconds=max(retry_after_seconds, 0))
        with self.session_scope() as session:
            row = session.get(EngineOutboxModel, engine_outbox_id)
            if row is None:
                return None
            row.publish_state = "pending"
            row.attempt_count = int(row.attempt_count or 0) + 1
            row.last_attempt_at = observed_at
            row.next_attempt_at = next_attempt_at
            row.updated_at = observed_at
            row.error_text = error_text
            session.add(row)
            session.flush()
            session.refresh(row)
            return self._outbox_row(row)
