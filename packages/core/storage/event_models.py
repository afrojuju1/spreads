from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import Date, DateTime, Index, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.storage.db import Base


class EventLogModel(Base):
    __tablename__ = "event_log"
    __table_args__ = (
        Index("idx_event_log_occurred_at", "occurred_at"),
        Index("idx_event_log_class_occurred", "event_class", "occurred_at"),
        Index("idx_event_log_topic_occurred", "topic", "occurred_at"),
        Index("idx_event_log_entity_occurred", "entity_type", "entity_key", "occurred_at"),
        Index("idx_event_log_session_occurred", "session_date", "occurred_at"),
        Index("idx_event_log_correlation_occurred", "correlation_id", "occurred_at"),
    )

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    event_class: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    entity_key: Mapped[str] = mapped_column(Text, nullable=False)
    session_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    market_session: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    schema_version: Mapped[str] = mapped_column(Text, nullable=False)
    producer_version: Mapped[str] = mapped_column(Text, nullable=False)
    correlation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    causation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
