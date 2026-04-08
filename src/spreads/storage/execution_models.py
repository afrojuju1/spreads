from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import BigInteger, Date, DateTime, Float, ForeignKey, Index, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from spreads.storage.db import Base


class ExecutionAttemptModel(Base):
    __tablename__ = "execution_attempts"
    __table_args__ = (
        Index("idx_execution_attempts_session_requested", "session_id", "requested_at"),
        Index("idx_execution_attempts_status_requested", "status", "requested_at"),
        Index("idx_execution_attempts_candidate_requested", "candidate_id", "requested_at"),
    )

    execution_attempt_id: Mapped[str] = mapped_column(Text, primary_key=True)
    session_id: Mapped[str] = mapped_column(Text, nullable=False)
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    cycle_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("collector_cycles.cycle_id", ondelete="SET NULL"),
        nullable=True,
    )
    candidate_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("collector_cycle_candidates.candidate_id", ondelete="SET NULL"),
        nullable=True,
    )
    bucket: Mapped[str | None] = mapped_column(Text, nullable=True)
    candidate_generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    job_run_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("job_runs.job_run_id", ondelete="SET NULL"),
        nullable=True,
    )
    underlying_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    strategy: Mapped[str] = mapped_column(Text, nullable=False)
    expiration_date: Mapped[date] = mapped_column(Date, nullable=False)
    short_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    long_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    limit_price: Mapped[float] = mapped_column(Float, nullable=False)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    broker: Mapped[str] = mapped_column(Text, nullable=False, default="alpaca")
    broker_order_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    client_order_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    request_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    candidate_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)


class ExecutionOrderModel(Base):
    __tablename__ = "execution_orders"
    __table_args__ = (
        Index("idx_execution_orders_attempt_updated", "execution_attempt_id", "updated_at"),
        Index("idx_execution_orders_parent", "parent_broker_order_id"),
        Index("ux_execution_orders_broker_order_id", "broker_order_id", unique=True),
    )

    execution_order_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    execution_attempt_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("execution_attempts.execution_attempt_id", ondelete="CASCADE"),
        nullable=False,
    )
    broker: Mapped[str] = mapped_column(Text, nullable=False, default="alpaca")
    broker_order_id: Mapped[str] = mapped_column(Text, nullable=False)
    parent_broker_order_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    client_order_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    order_status: Mapped[str] = mapped_column(Text, nullable=False)
    order_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    time_in_force: Mapped[str | None] = mapped_column(Text, nullable=True)
    order_class: Mapped[str | None] = mapped_column(Text, nullable=True)
    side: Mapped[str | None] = mapped_column(Text, nullable=True)
    symbol: Mapped[str | None] = mapped_column(Text, nullable=True)
    leg_symbol: Mapped[str | None] = mapped_column(Text, nullable=True)
    leg_side: Mapped[str | None] = mapped_column(Text, nullable=True)
    position_intent: Mapped[str | None] = mapped_column(Text, nullable=True)
    quantity: Mapped[float | None] = mapped_column(Float, nullable=True)
    limit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    filled_qty: Mapped[float | None] = mapped_column(Float, nullable=True)
    filled_avg_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    order_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class ExecutionFillModel(Base):
    __tablename__ = "execution_fills"
    __table_args__ = (
        Index("idx_execution_fills_attempt_filled", "execution_attempt_id", "filled_at"),
        Index("idx_execution_fills_order", "broker_order_id"),
        Index("ux_execution_fills_broker_fill_id", "broker_fill_id", unique=True),
    )

    execution_fill_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    execution_attempt_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("execution_attempts.execution_attempt_id", ondelete="CASCADE"),
        nullable=False,
    )
    execution_order_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("execution_orders.execution_order_id", ondelete="SET NULL"),
        nullable=True,
    )
    broker: Mapped[str] = mapped_column(Text, nullable=False, default="alpaca")
    broker_fill_id: Mapped[str] = mapped_column(Text, nullable=False)
    broker_order_id: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[str | None] = mapped_column(Text, nullable=True)
    fill_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    cumulative_quantity: Mapped[float | None] = mapped_column(Float, nullable=True)
    remaining_quantity: Mapped[float | None] = mapped_column(Float, nullable=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    filled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    fill_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
