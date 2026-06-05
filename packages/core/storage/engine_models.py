from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import BigInteger, Boolean, Date, DateTime, Float, ForeignKey, Index, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.storage.db import Base


class TickerSourceRunModel(Base):
    __tablename__ = "ticker_source_runs"
    __table_args__ = (
        Index("idx_ticker_source_runs_source_generated", "ticker_source_id", "generated_at"),
        Index("idx_ticker_source_runs_status_generated", "status", "generated_at"),
    )

    ticker_source_run_id: Mapped[str] = mapped_column(Text, primary_key=True)
    ticker_source_type: Mapped[str] = mapped_column(Text, nullable=False)
    ticker_source_id: Mapped[str] = mapped_column(Text, nullable=False)
    job_run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    config_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    observed_count: Mapped[int] = mapped_column(Integer, nullable=False)
    selected_count: Mapped[int] = mapped_column(Integer, nullable=False)
    excluded_count: Mapped[int] = mapped_column(Integer, nullable=False)
    summary_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TickerSourceObservationModel(Base):
    __tablename__ = "ticker_source_observations"
    __table_args__ = (
        Index("ux_ticker_source_observations_run_symbol", "ticker_source_run_id", "symbol", unique=True),
        Index("idx_ticker_source_observations_source_state", "ticker_source_id", "observation_state", "created_at"),
        Index("idx_ticker_source_observations_symbol_created", "symbol", "created_at"),
    )

    ticker_source_observation_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ticker_source_run_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("ticker_source_runs.ticker_source_run_id", ondelete="CASCADE"),
        nullable=False,
    )
    ticker_source_id: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    observation_state: Mapped[str] = mapped_column(Text, nullable=False)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    company: Mapped[str | None] = mapped_column(Text, nullable=True)
    sector: Mapped[str | None] = mapped_column(Text, nullable=True)
    industry: Mapped[str | None] = mapped_column(Text, nullable=True)
    country: Mapped[str | None] = mapped_column(Text, nullable=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    market_cap: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    daily_volume: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    move_percent: Mapped[float | None] = mapped_column(Float, nullable=True)
    relative_volume: Mapped[float | None] = mapped_column(Float, nullable=True)
    reason_codes_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TickerSourceStateModel(Base):
    __tablename__ = "ticker_source_state"
    __table_args__ = (
        Index("idx_ticker_source_state_active_rank", "ticker_source_id", "active", "last_rank"),
        Index("idx_ticker_source_state_symbol_updated", "symbol", "updated_at"),
    )

    ticker_source_id: Mapped[str] = mapped_column(Text, primary_key=True)
    symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    first_selected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_selected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    seen_count: Mapped[int] = mapped_column(Integer, nullable=False)
    selected_count: Mapped[int] = mapped_column(Integer, nullable=False)
    consecutive_seen_count: Mapped[int] = mapped_column(Integer, nullable=False)
    consecutive_missing_count: Mapped[int] = mapped_column(Integer, nullable=False)
    last_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    best_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    best_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_state: Mapped[str] = mapped_column(Text, nullable=False)
    last_ticker_source_run_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("ticker_source_runs.ticker_source_run_id", ondelete="SET NULL"),
        nullable=True,
    )
    last_observation_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("ticker_source_observations.ticker_source_observation_id", ondelete="SET NULL"),
        nullable=True,
    )
    last_metrics_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CandidateRunModel(Base):
    __tablename__ = "candidate_runs"
    __table_args__ = (
        Index("idx_candidate_runs_strategy_generated", "trading_strategy_id", "routine", "generated_at"),
        Index("idx_candidate_runs_ticker_source_generated", "ticker_source_id", "generated_at"),
    )

    candidate_run_id: Mapped[str] = mapped_column(Text, primary_key=True)
    run_key: Mapped[str] = mapped_column(Text, nullable=False)
    trading_strategy_id: Mapped[str] = mapped_column(Text, nullable=False)
    trade_structure: Mapped[str] = mapped_column(Text, nullable=False)
    routine: Mapped[str] = mapped_column(Text, nullable=False)
    ticker_source_run_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("ticker_source_runs.ticker_source_run_id", ondelete="SET NULL"),
        nullable=True,
    )
    ticker_source_kind: Mapped[str] = mapped_column(Text, nullable=False)
    ticker_source_id: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    config_hash: Mapped[str] = mapped_column(Text, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    symbol_count: Mapped[int] = mapped_column(Integer, nullable=False)
    candidate_count: Mapped[int] = mapped_column(Integer, nullable=False)
    summary_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TradeCandidateModel(Base):
    __tablename__ = "trade_candidates"
    __table_args__ = (
        Index("ux_trade_candidates_run_identity", "candidate_run_id", "underlying_symbol", "candidate_identity", unique=True),
        Index("idx_trade_candidates_strategy_state", "trading_strategy_id", "routine", "candidate_state"),
        Index("idx_trade_candidates_underlying_updated", "underlying_symbol", "updated_at"),
    )

    trade_candidate_id: Mapped[str] = mapped_column(Text, primary_key=True)
    candidate_run_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("candidate_runs.candidate_run_id", ondelete="CASCADE"),
        nullable=False,
    )
    trading_strategy_id: Mapped[str] = mapped_column(Text, nullable=False)
    trade_structure: Mapped[str] = mapped_column(Text, nullable=False)
    routine: Mapped[str] = mapped_column(Text, nullable=False)
    config_hash: Mapped[str] = mapped_column(Text, nullable=False)
    underlying_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    root_symbol: Mapped[str | None] = mapped_column(Text, nullable=True)
    candidate_identity: Mapped[str] = mapped_column(Text, nullable=False)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    expiration_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    selection_state: Mapped[str | None] = mapped_column(Text, nullable=True)
    candidate_state: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    legs_json: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False, default=list)
    execution_shape_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    economics_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    risk_hints_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    reason_codes_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    blockers_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    candidate_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
