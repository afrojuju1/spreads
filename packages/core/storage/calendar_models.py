from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import Boolean, Date, DateTime, Index, Integer, PrimaryKeyConstraint, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB
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


class ProviderFetchAuditModel(Base):
    __tablename__ = "provider_fetch_audit"
    __table_args__ = (
        Index("idx_provider_fetch_audit_provider_endpoint_fetched", "provider", "endpoint", "fetched_at"),
        Index("idx_provider_fetch_audit_provider_params", "provider", "endpoint", "params_hash", "page_key"),
        Index("idx_provider_fetch_audit_window", "coverage_start", "coverage_end"),
    )

    audit_id: Mapped[str] = mapped_column(Text, primary_key=True)
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    endpoint: Mapped[str] = mapped_column(Text, nullable=False)
    params_hash: Mapped[str] = mapped_column(Text, nullable=False)
    params_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    coverage_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    coverage_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    page_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    cache_hit: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    payload_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    backoff_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class EarningsEventConsensusModel(Base):
    __tablename__ = "earnings_event_consensus"
    __table_args__ = (
        UniqueConstraint("symbol", "event_date", name="uq_earnings_event_consensus_symbol_date"),
        Index("idx_earnings_event_consensus_symbol_date", "symbol", "event_date"),
        Index("idx_earnings_event_consensus_stale_after", "stale_after"),
        Index("idx_earnings_event_consensus_status_date", "consensus_status", "event_date"),
    )

    consensus_id: Mapped[str] = mapped_column(Text, primary_key=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    event_date: Mapped[date] = mapped_column(Date, nullable=False)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    session_timing: Mapped[str] = mapped_column(Text, nullable=False)
    event_status: Mapped[str] = mapped_column(Text, nullable=False)
    primary_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    supporting_sources_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list, server_default=text("'[]'::jsonb"))
    conflicting_sources_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list, server_default=text("'[]'::jsonb"))
    consensus_status: Mapped[str] = mapped_column(Text, nullable=False)
    source_confidence: Mapped[str] = mapped_column(Text, nullable=False)
    timing_confidence: Mapped[str] = mapped_column(Text, nullable=False)
    provider_payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    stale_after: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
