from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import BigInteger, Date, DateTime, Float, ForeignKey, Index, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.storage.db import Base


class SourceRunModel(Base):
    __tablename__ = "source_runs"
    __table_args__ = (
        Index("idx_source_runs_source_generated", "source_ref", "generated_at"),
        Index("idx_source_runs_status_generated", "status", "generated_at"),
    )

    source_run_id: Mapped[str] = mapped_column(Text, primary_key=True)
    source_type: Mapped[str] = mapped_column(Text, nullable=False)
    source_ref: Mapped[str] = mapped_column(Text, nullable=False)
    source_job_run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    config_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    symbol_count: Mapped[int] = mapped_column(Integer, nullable=False)
    summary_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class SourceTickerModel(Base):
    __tablename__ = "source_tickers"
    __table_args__ = (
        Index("ux_source_tickers_run_symbol", "source_run_id", "symbol", unique=True),
        Index("idx_source_tickers_symbol_created", "symbol", "created_at"),
    )

    source_ticker_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_run_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("source_runs.source_run_id", ondelete="CASCADE"),
        nullable=False,
    )
    source_ref: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    reason_codes_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CandidateRunModel(Base):
    __tablename__ = "candidate_runs"
    __table_args__ = (
        Index("idx_candidate_runs_strategy_generated", "trading_strategy_id", "routine", "generated_at"),
        Index("idx_candidate_runs_source_generated", "source_ref", "generated_at"),
    )

    candidate_run_id: Mapped[str] = mapped_column(Text, primary_key=True)
    run_key: Mapped[str] = mapped_column(Text, nullable=False)
    trading_strategy_id: Mapped[str] = mapped_column(Text, nullable=False)
    trade_structure: Mapped[str] = mapped_column(Text, nullable=False)
    routine: Mapped[str] = mapped_column(Text, nullable=False)
    source_run_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("source_runs.source_run_id", ondelete="SET NULL"),
        nullable=True,
    )
    source_type: Mapped[str] = mapped_column(Text, nullable=False)
    source_ref: Mapped[str] = mapped_column(Text, nullable=False)
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
