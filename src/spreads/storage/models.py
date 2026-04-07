from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import BigInteger, Date, DateTime, Float, ForeignKey, Index, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from spreads.storage.db import Base


class ScanRunModel(Base):
    __tablename__ = "scan_runs"
    __table_args__ = (
        Index("idx_scan_runs_symbol_generated_at", "symbol", "generated_at"),
    )

    run_id: Mapped[str] = mapped_column(Text, primary_key=True)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    strategy: Mapped[str] = mapped_column(Text, nullable=False, default="call_credit")
    session_label: Mapped[str | None] = mapped_column(Text, nullable=True)
    profile: Mapped[str] = mapped_column(Text, nullable=False)
    spot_price: Mapped[float] = mapped_column(Float, nullable=False)
    candidate_count: Mapped[int] = mapped_column(Integer, nullable=False)
    output_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    filters_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    setup_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    setup_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    setup_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    candidates: Mapped[list["ScanCandidateModel"]] = relationship(
        back_populates="run",
        cascade="all, delete-orphan",
        order_by="ScanCandidateModel.rank",
    )


class ScanCandidateModel(Base):
    __tablename__ = "scan_candidates"
    __table_args__ = (
        Index("idx_scan_candidates_run_id", "run_id"),
    )

    run_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("scan_runs.run_id", ondelete="CASCADE"),
        primary_key=True,
    )
    rank: Mapped[int] = mapped_column(Integer, primary_key=True)
    strategy: Mapped[str] = mapped_column(Text, nullable=False, default="call_credit")
    expiration_date: Mapped[date] = mapped_column(Date, nullable=False)
    short_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    long_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    short_strike: Mapped[float] = mapped_column(Float, nullable=False)
    long_strike: Mapped[float] = mapped_column(Float, nullable=False)
    width: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    midpoint_credit: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    natural_credit: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    breakeven: Mapped[float] = mapped_column(Float, nullable=False)
    max_profit: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    max_loss: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    quality_score: Mapped[float] = mapped_column(Float, nullable=False)
    return_on_risk: Mapped[float] = mapped_column(Float, nullable=False)
    short_otm_pct: Mapped[float] = mapped_column(Float, nullable=False)
    calendar_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    setup_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    expected_move: Mapped[float | None] = mapped_column(Float, nullable=True)
    short_vs_expected_move: Mapped[float | None] = mapped_column(Float, nullable=True)

    run: Mapped[ScanRunModel] = relationship(back_populates="candidates")


class OptionQuoteEventModel(Base):
    __tablename__ = "option_quote_events"
    __table_args__ = (
        Index("idx_option_quote_events_cycle_id", "cycle_id"),
        Index("idx_option_quote_events_symbol_captured_at", "option_symbol", "captured_at"),
    )

    quote_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    cycle_id: Mapped[str] = mapped_column(Text, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    underlying_symbol: Mapped[str | None] = mapped_column(Text, nullable=True)
    strategy: Mapped[str | None] = mapped_column(Text, nullable=True)
    profile: Mapped[str | None] = mapped_column(Text, nullable=True)
    option_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    leg_role: Mapped[str] = mapped_column(Text, nullable=False)
    bid: Mapped[float] = mapped_column(Float, nullable=False)
    ask: Mapped[float] = mapped_column(Float, nullable=False)
    midpoint: Mapped[float] = mapped_column(Float, nullable=False)
    bid_size: Mapped[int] = mapped_column(Integer, nullable=False)
    ask_size: Mapped[int] = mapped_column(Integer, nullable=False)
    quote_timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source: Mapped[str] = mapped_column(Text, nullable=False, default="alpaca_websocket")
