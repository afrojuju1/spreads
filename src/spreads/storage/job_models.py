from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from spreads.storage.db import Base


class JobDefinitionModel(Base):
    __tablename__ = "job_definitions"
    __table_args__ = (
        Index("idx_job_definitions_enabled_type", "enabled", "job_type"),
    )

    job_key: Mapped[str] = mapped_column(Text, primary_key=True)
    job_type: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    schedule_type: Mapped[str] = mapped_column(Text, nullable=False)
    schedule_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    market_calendar: Mapped[str] = mapped_column(Text, nullable=False, default="NYSE")
    singleton_scope: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class JobRunModel(Base):
    __tablename__ = "job_runs"
    __table_args__ = (
        Index("idx_job_runs_job_key_scheduled_for", "job_key", "scheduled_for"),
        Index("idx_job_runs_status_scheduled_for", "status", "scheduled_for"),
    )

    job_run_id: Mapped[str] = mapped_column(Text, primary_key=True)
    job_key: Mapped[str] = mapped_column(
        Text,
        ForeignKey("job_definitions.job_key", ondelete="CASCADE"),
        nullable=False,
    )
    arq_job_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    job_type: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    scheduled_for: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    worker_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    result_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)


class JobLeaseModel(Base):
    __tablename__ = "job_leases"
    __table_args__ = (
        Index("idx_job_leases_expires_at", "expires_at"),
    )

    lease_key: Mapped[str] = mapped_column(Text, primary_key=True)
    job_run_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("job_runs.job_run_id", ondelete="SET NULL"),
        nullable=True,
    )
    owner: Mapped[str] = mapped_column(Text, nullable=False)
    acquired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    lease_state_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
