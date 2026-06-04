from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import Date, DateTime, ForeignKey, Index, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.storage.db import Base


class LiveSessionSlotModel(Base):
    __tablename__ = "live_session_slots"
    __table_args__ = (
        Index("idx_live_session_slots_session_slot", "session_id", "slot_at", unique=True),
        Index("idx_live_session_slots_status_updated", "status", "updated_at"),
        Index("idx_live_session_slots_job_key_session_date", "job_key", "session_date"),
    )

    session_slot_id: Mapped[str] = mapped_column(Text, primary_key=True)
    job_key: Mapped[str] = mapped_column(Text, nullable=False)
    session_id: Mapped[str] = mapped_column(Text, nullable=False)
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    slot_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    scheduled_for: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    job_run_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("job_runs.job_run_id", ondelete="SET NULL"),
        nullable=True,
    )
    capture_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    recovery_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    slot_details_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    queued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
