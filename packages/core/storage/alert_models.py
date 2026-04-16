from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import BigInteger, Date, DateTime, Index, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.storage.db import Base


class AlertEventModel(Base):
    __tablename__ = "alert_events"
    __table_args__ = (
        Index(
            "idx_alert_events_kind_session_label_created_at",
            "record_kind",
            "session_date",
            "label",
            "created_at",
        ),
        Index("idx_alert_events_kind_symbol_created_at", "record_kind", "symbol", "created_at"),
        Index(
            "idx_alert_events_kind_dedupe_target_created_at",
            "record_kind",
            "dedupe_key",
            "delivery_target",
            "created_at",
        ),
        Index(
            "idx_alert_events_kind_status_next_attempt_at",
            "record_kind",
            "status",
            "next_attempt_at",
        ),
        Index(
            "idx_alert_events_kind_status_claimed_at",
            "record_kind",
            "status",
            "claimed_at",
        ),
    )

    alert_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    record_kind: Mapped[str] = mapped_column(Text, nullable=False, default="delivery")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    session_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    cycle_id: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    alert_type: Mapped[str] = mapped_column(Text, nullable=False)
    dedupe_key: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    delivery_target: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    planner_job_run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    delivery_job_run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    worker_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    state_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    response_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)
