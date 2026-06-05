from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, Boolean, DateTime, Float, Identity, Index, Integer, PrimaryKeyConstraint, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.storage.db import Base


class OptionQuoteTickModel(Base):
    __tablename__ = "option_quote_ticks"
    __table_args__ = (
        PrimaryKeyConstraint("captured_at", "quote_tick_id"),
        Index("idx_option_quote_ticks_symbol_captured", "option_symbol", "captured_at"),
        Index("idx_option_quote_ticks_label_captured", "label", "captured_at"),
        Index("idx_option_quote_ticks_cycle", "cycle_id"),
        Index("idx_option_quote_ticks_captured_brin", "captured_at", postgresql_using="brin"),
        {"postgresql_partition_by": "RANGE (captured_at)"},
    )

    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    quote_tick_id: Mapped[int] = mapped_column(BigInteger, Identity(), nullable=False)
    cycle_id: Mapped[str] = mapped_column(Text, nullable=False)
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
    source_timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source: Mapped[str] = mapped_column(Text, nullable=False, default="alpaca_websocket")


class OptionTradeTickModel(Base):
    __tablename__ = "option_trade_ticks"
    __table_args__ = (
        PrimaryKeyConstraint("captured_at", "trade_tick_id"),
        Index("idx_option_trade_ticks_symbol_captured", "option_symbol", "captured_at"),
        Index("idx_option_trade_ticks_underlying_captured", "underlying_symbol", "captured_at"),
        Index("idx_option_trade_ticks_label_captured", "label", "captured_at"),
        Index("idx_option_trade_ticks_cycle", "cycle_id"),
        Index("idx_option_trade_ticks_captured_brin", "captured_at", postgresql_using="brin"),
        {"postgresql_partition_by": "RANGE (captured_at)"},
    )

    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    trade_tick_id: Mapped[int] = mapped_column(BigInteger, Identity(), nullable=False)
    cycle_id: Mapped[str] = mapped_column(Text, nullable=False)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    underlying_symbol: Mapped[str | None] = mapped_column(Text, nullable=True)
    strategy: Mapped[str | None] = mapped_column(Text, nullable=True)
    profile: Mapped[str | None] = mapped_column(Text, nullable=True)
    option_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    leg_role: Mapped[str] = mapped_column(Text, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    premium: Mapped[float] = mapped_column(Float, nullable=False)
    exchange_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    conditions_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    source_timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    included_in_score: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    exclusion_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False, default="alpaca_websocket")
