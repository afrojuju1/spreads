from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.storage.db import Base


class ExecutionAttemptModel(Base):
    __tablename__ = "execution_attempts"
    __table_args__ = (
        Index("idx_execution_attempts_session_requested", "session_id", "requested_at"),
        Index(
            "idx_execution_attempts_pipeline_requested", "pipeline_id", "requested_at"
        ),
        Index("idx_execution_attempts_status_requested", "status", "requested_at"),
        Index(
            "idx_execution_attempts_candidate_requested", "candidate_id", "requested_at"
        ),
        Index(
            "idx_execution_attempts_runtime_position_requested",
            "position_id",
            "requested_at",
        ),
        Index(
            "idx_execution_attempts_opportunity_requested",
            "opportunity_id",
            "requested_at",
        ),
        Index(
            "idx_execution_attempts_risk_decision_requested",
            "risk_decision_id",
            "requested_at",
        ),
    )

    execution_attempt_id: Mapped[str] = mapped_column(Text, primary_key=True)
    session_id: Mapped[str] = mapped_column(Text, nullable=False)
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    pipeline_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    market_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    cycle_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("collector_cycles.cycle_id", ondelete="SET NULL"),
        nullable=True,
    )
    opportunity_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("opportunities.opportunity_id", ondelete="SET NULL"),
        nullable=True,
    )
    risk_decision_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("risk_decisions.risk_decision_id", ondelete="SET NULL"),
        nullable=True,
    )
    candidate_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("collector_cycle_candidates.candidate_id", ondelete="SET NULL"),
        nullable=True,
    )
    attempt_context: Mapped[str | None] = mapped_column(Text, nullable=True)
    candidate_generated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
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
    trade_intent: Mapped[str] = mapped_column(Text, nullable=False, default="open")
    position_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("portfolio_positions.position_id", ondelete="SET NULL"),
        nullable=True,
    )
    root_symbol: Mapped[str | None] = mapped_column(Text, nullable=True)
    strategy_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    style_profile: Mapped[str | None] = mapped_column(Text, nullable=True)
    horizon_intent: Mapped[str | None] = mapped_column(Text, nullable=True)
    product_class: Mapped[str | None] = mapped_column(Text, nullable=True)
    requested_quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    requested_limit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    limit_price: Mapped[float] = mapped_column(Float, nullable=False)
    requested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    submitted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    status: Mapped[str] = mapped_column(Text, nullable=False)
    broker: Mapped[str] = mapped_column(Text, nullable=False, default="alpaca")
    broker_order_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    client_order_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    request_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    candidate_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)


class ExecutionIntentModel(Base):
    __tablename__ = "execution_intents"
    __table_args__ = (
        Index("idx_execution_intents_bot_created", "bot_id", "created_at"),
        Index("idx_execution_intents_slot_state", "slot_key", "state"),
        Index("idx_execution_intents_opportunity_decision", "opportunity_decision_id"),
        Index("idx_execution_intents_strategy_position", "strategy_position_id"),
        Index("idx_execution_intents_execution_attempt", "execution_attempt_id"),
    )

    execution_intent_id: Mapped[str] = mapped_column(Text, primary_key=True)
    bot_id: Mapped[str] = mapped_column(Text, nullable=False)
    automation_id: Mapped[str] = mapped_column(Text, nullable=False)
    opportunity_decision_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey(
            "opportunity_decisions.opportunity_decision_id", ondelete="SET NULL"
        ),
        nullable=True,
    )
    strategy_position_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("portfolio_positions.position_id", ondelete="SET NULL"),
        nullable=True,
    )
    execution_attempt_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("execution_attempts.execution_attempt_id", ondelete="SET NULL"),
        nullable=True,
    )
    action_type: Mapped[str] = mapped_column(Text, nullable=False)
    slot_key: Mapped[str] = mapped_column(Text, nullable=False)
    claim_token: Mapped[str | None] = mapped_column(Text, nullable=True)
    policy_ref_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    config_hash: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    superseded_by_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class ExecutionIntentEventModel(Base):
    __tablename__ = "execution_intent_events"
    __table_args__ = (
        Index(
            "idx_execution_intent_events_intent_event_at",
            "execution_intent_id",
            "event_at",
        ),
    )

    execution_intent_event_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    execution_intent_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("execution_intents.execution_intent_id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    event_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )


class ExecutionOrderModel(Base):
    __tablename__ = "execution_orders"
    __table_args__ = (
        Index(
            "idx_execution_orders_attempt_updated", "execution_attempt_id", "updated_at"
        ),
        Index("idx_execution_orders_parent", "parent_broker_order_id"),
        Index("ux_execution_orders_broker_order_id", "broker_order_id", unique=True),
    )

    execution_order_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
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
    submitted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    order_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class ExecutionFillModel(Base):
    __tablename__ = "execution_fills"
    __table_args__ = (
        Index(
            "idx_execution_fills_attempt_filled", "execution_attempt_id", "filled_at"
        ),
        Index("idx_execution_fills_order", "broker_order_id"),
        Index("ux_execution_fills_broker_fill_id", "broker_fill_id", unique=True),
    )

    execution_fill_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
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


class PortfolioPositionModel(Base):
    __tablename__ = "portfolio_positions"
    __table_args__ = (
        Index("idx_portfolio_positions_pipeline_updated", "pipeline_id", "updated_at"),
        Index("idx_portfolio_positions_pipeline_status", "pipeline_id", "status"),
        Index(
            "ux_portfolio_positions_open_attempt",
            "open_execution_attempt_id",
            unique=True,
        ),
    )

    position_id: Mapped[str] = mapped_column(Text, primary_key=True)
    pipeline_id: Mapped[str] = mapped_column(Text, nullable=False)
    source_opportunity_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("opportunities.opportunity_id", ondelete="SET NULL"),
        nullable=True,
    )
    open_execution_attempt_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("execution_attempts.execution_attempt_id", ondelete="CASCADE"),
        nullable=False,
    )
    root_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    strategy_family: Mapped[str] = mapped_column(Text, nullable=False)
    style_profile: Mapped[str | None] = mapped_column(Text, nullable=True)
    horizon_intent: Mapped[str | None] = mapped_column(Text, nullable=True)
    product_class: Mapped[str | None] = mapped_column(Text, nullable=True)
    market_date_opened: Mapped[date] = mapped_column(Date, nullable=False)
    market_date_closed: Mapped[date | None] = mapped_column(Date, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    legs_json: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB, nullable=False, default=list
    )
    economics_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    strategy_metrics_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    requested_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    opened_quantity: Mapped[float] = mapped_column(Float, nullable=False)
    remaining_quantity: Mapped[float] = mapped_column(Float, nullable=False)
    entry_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_pnl: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    unrealized_pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    close_mark: Mapped[float | None] = mapped_column(Float, nullable=True)
    close_mark_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    close_marked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_broker_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    exit_policy_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    risk_policy_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict
    )
    source_job_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_job_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_job_run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_exit_evaluated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_exit_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_reconciled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    reconciliation_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    reconciliation_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    opened_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    closed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class PositionCloseModel(Base):
    __tablename__ = "position_closes"
    __table_args__ = (
        Index("idx_position_closes_position_closed", "position_id", "closed_at"),
        Index(
            "ux_position_closes_execution_attempt", "execution_attempt_id", unique=True
        ),
    )

    position_close_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    position_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("portfolio_positions.position_id", ondelete="CASCADE"),
        nullable=False,
    )
    execution_attempt_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("execution_attempts.execution_attempt_id", ondelete="CASCADE"),
        nullable=False,
    )
    closed_quantity: Mapped[float] = mapped_column(Float, nullable=False)
    exit_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_pnl: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    broker_order_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
