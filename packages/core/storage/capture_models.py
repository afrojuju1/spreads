from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import Boolean, Date, DateTime, Index, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.storage.db import Base


class CaptureTargetModel(Base):
    __tablename__ = "capture_targets"
    __table_args__ = (
        Index(
            "ux_capture_targets_owner_reason_symbol",
            "owner_kind",
            "owner_key",
            "reason",
            "option_symbol",
            unique=True,
        ),
        Index("idx_capture_targets_active_priority", "expires_at", "priority", "updated_at"),
        Index("idx_capture_targets_session_reason", "session_id", "reason"),
    )

    capture_target_id: Mapped[str] = mapped_column(Text, primary_key=True)
    owner_kind: Mapped[str] = mapped_column(Text, nullable=False)
    owner_key: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
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


class CaptureSummaryModel(Base):
    __tablename__ = "capture_summaries"
    __table_args__ = (
        Index("idx_capture_summaries_source_captured", "source", "captured_at"),
        Index("idx_capture_summaries_status_captured", "status", "captured_at"),
    )

    capture_summary_id: Mapped[str] = mapped_column(Text, primary_key=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    active_target_count: Mapped[int] = mapped_column(Integer, nullable=False)
    selected_target_count: Mapped[int] = mapped_column(Integer, nullable=False)
    capture_group_count: Mapped[int] = mapped_column(Integer, nullable=False)
    quote_rows_saved: Mapped[int] = mapped_column(Integer, nullable=False)
    trade_rows_saved: Mapped[int] = mapped_column(Integer, nullable=False)
    target_limit: Mapped[int | None] = mapped_column(Integer, nullable=True)
    target_counts_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    group_summary_json: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    error_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
