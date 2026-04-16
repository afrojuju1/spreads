from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Index, PrimaryKeyConstraint, Text
from sqlalchemy.orm import Mapped, mapped_column

from core.storage.db import Base


class CalendarEventModel(Base):
    __tablename__ = "calendar_events"
    __table_args__ = (
        Index("idx_calendar_events_symbol", "symbol", "scheduled_at"),
        Index("idx_calendar_events_asset_scope", "asset_scope", "scheduled_at"),
    )

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str | None] = mapped_column(Text, nullable=True)
    asset_scope: Mapped[str | None] = mapped_column(Text, nullable=True)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    source_confidence: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CalendarEventRefreshStateModel(Base):
    __tablename__ = "calendar_event_refresh_state"
    __table_args__ = (
        PrimaryKeyConstraint("source", "scope_key"),
    )

    source: Mapped[str] = mapped_column(Text, nullable=False)
    scope_key: Mapped[str] = mapped_column(Text, nullable=False)
    coverage_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    coverage_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    refreshed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
