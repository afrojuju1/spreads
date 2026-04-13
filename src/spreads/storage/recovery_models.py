from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Index, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from spreads.storage.db import Base


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


class MarketRecorderTargetModel(Base):
    __tablename__ = "market_recorder_targets"
    __table_args__ = (
        Index(
            "ux_market_recorder_targets_owner_reason_symbol",
            "owner_kind",
            "owner_key",
            "reason",
            "option_symbol",
            unique=True,
        ),
        Index("idx_market_recorder_targets_expires_at", "expires_at"),
        Index("idx_market_recorder_targets_session_reason", "session_id", "reason"),
    )

    capture_target_id: Mapped[str] = mapped_column(Text, primary_key=True)
    owner_kind: Mapped[str] = mapped_column(Text, nullable=False)
    owner_key: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    session_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    session_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    label: Mapped[str | None] = mapped_column(Text, nullable=True)
    profile: Mapped[str | None] = mapped_column(Text, nullable=True)
    underlying_symbol: Mapped[str | None] = mapped_column(Text, nullable=True)
    strategy: Mapped[str | None] = mapped_column(Text, nullable=True)
    leg_role: Mapped[str | None] = mapped_column(Text, nullable=True)
    option_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    quote_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    trade_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    feed: Mapped[str] = mapped_column(Text, nullable=False, default="opra")
    data_base_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
