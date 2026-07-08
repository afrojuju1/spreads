from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import BigInteger, Date, DateTime, ForeignKey, Identity, Index, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.storage.db import Base


class EngineEventModel(Base):
    __tablename__ = "engine_events"
    __table_args__ = (
        Index("ux_engine_events_sequence", "sequence", unique=True),
        Index("ux_engine_events_idempotency_key", "idempotency_key", unique=True),
        Index("idx_engine_events_aggregate", "aggregate_type", "aggregate_id", "sequence"),
        Index("idx_engine_events_workflow", "workflow_id", "sequence"),
        Index("idx_engine_events_type_occurred", "event_type", "occurred_at"),
        Index("idx_engine_events_correlation", "correlation_id", "sequence"),
        Index("idx_engine_events_intent", "execution_intent_id", "sequence"),
        Index("idx_engine_events_attempt", "execution_attempt_id", "sequence"),
        Index("idx_engine_events_position", "position_id", "sequence"),
        Index("idx_engine_events_session", "session_date", "sequence"),
    )

    engine_event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    sequence: Mapped[int] = mapped_column(BigInteger, Identity(always=False), nullable=False)
    run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    workflow_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    workflow_run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    event_version: Mapped[int] = mapped_column(Integer, nullable=False)
    aggregate_type: Mapped[str] = mapped_column(Text, nullable=False)
    aggregate_id: Mapped[str] = mapped_column(Text, nullable=False)
    aggregate_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lifecycle_object: Mapped[str | None] = mapped_column(Text, nullable=True)
    from_state: Mapped[str | None] = mapped_column(Text, nullable=True)
    to_state: Mapped[str | None] = mapped_column(Text, nullable=True)
    trading_strategy_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    trade_signal_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    trade_decision_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    execution_intent_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    execution_attempt_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    broker_order_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    position_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    session_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    market_session: Mapped[str | None] = mapped_column(Text, nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    causation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class EngineOutboxModel(Base):
    __tablename__ = "engine_outbox"
    __table_args__ = (
        Index("idx_engine_outbox_pending", "publish_state", "next_attempt_at", "created_at"),
        Index("idx_engine_outbox_event", "engine_event_id"),
        Index("idx_engine_outbox_subject", "subject", "created_at"),
        Index("idx_engine_outbox_aggregate", "aggregate_type", "aggregate_id", "created_at"),
    )

    engine_outbox_id: Mapped[str] = mapped_column(Text, primary_key=True)
    engine_event_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("engine_events.engine_event_id", ondelete="CASCADE"),
        nullable=False,
    )
    stream: Mapped[str] = mapped_column(Text, nullable=False)
    subject: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    aggregate_type: Mapped[str] = mapped_column(Text, nullable=False)
    aggregate_id: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    headers_json: Mapped[dict[str, str]] = mapped_column(JSONB, nullable=False, default=dict)
    publish_state: Mapped[str] = mapped_column(Text, nullable=False, default="pending")
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
