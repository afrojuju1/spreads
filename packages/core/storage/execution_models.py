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
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from core.storage.db import Base


class ExecutionAttemptModel(Base):
    __tablename__ = "execution_attempts"
    __table_args__ = (
        Index("idx_execution_attempts_session_requested", "session_id", "requested_at"),
        Index(
            "idx_execution_attempts_strategy_requested",
            "trading_strategy_id",
            "requested_at",
        ),
        Index(
            "idx_execution_attempts_strategy_market_requested",
            "trading_strategy_id",
            "market_date",
            "requested_at",
        ),
        Index(
            "idx_execution_attempts_session_structure_requested",
            "session_id",
            "structure_identity",
            "requested_at",
        ),
        Index("idx_execution_attempts_status_requested", "status", "requested_at"),
        Index("idx_execution_attempts_source_object", "source_object_type", "source_object_id"),
        Index("idx_execution_attempts_trade_signal", "trade_signal_id"),
        Index("idx_execution_attempts_trade_decision", "trade_decision_id"),
        Index("idx_execution_attempts_admission_decision", "admission_decision_id"),
        Index("idx_execution_attempts_execution_intent", "execution_intent_id"),
        Index(
            "ux_execution_attempts_execution_intent",
            "execution_intent_id",
            unique=True,
            postgresql_where=text("execution_intent_id IS NOT NULL"),
        ),
        Index(
            "idx_execution_attempts_runtime_position_requested",
            "position_id",
            "requested_at",
        ),
    )

    execution_attempt_id: Mapped[str] = mapped_column(Text, primary_key=True)
    session_id: Mapped[str] = mapped_column(Text, nullable=False)
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    trading_strategy_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    market_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    cycle_id: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    source_object_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_object_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    trade_signal_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("trade_signals.trade_signal_id", ondelete="SET NULL"),
        nullable=True,
    )
    trade_decision_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("trade_decisions.trade_decision_id", ondelete="SET NULL"),
        nullable=True,
    )
    admission_decision_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey(
            "trade_admissions.admission_decision_id",
            name="execution_attempts_admission_decision_id_fkey",
            ondelete="SET NULL",
            use_alter=True,
        ),
        nullable=True,
    )
    execution_intent_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("execution_intents.execution_intent_id", ondelete="RESTRICT"),
        nullable=True,
    )
    attempt_context: Mapped[str | None] = mapped_column(Text, nullable=True)
    candidate_generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    job_run_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("job_runs.job_run_id", ondelete="SET NULL"),
        nullable=True,
    )
    underlying_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    strategy: Mapped[str] = mapped_column(Text, nullable=False)
    expiration_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    structure_identity: Mapped[str | None] = mapped_column(Text, nullable=True)
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
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    broker: Mapped[str] = mapped_column(Text, nullable=False, default="alpaca")
    broker_order_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    client_order_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    request_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    candidate_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    legs_json: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False, default=list)
    order_payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    economics_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)

    orders: Mapped[list["ExecutionOrderModel"]] = relationship(
        "ExecutionOrderModel",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by=lambda: (ExecutionOrderModel.updated_at.desc(), ExecutionOrderModel.execution_order_id.desc()),
    )
    fills: Mapped[list["ExecutionFillModel"]] = relationship(
        "ExecutionFillModel",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by=lambda: (ExecutionFillModel.filled_at.desc(), ExecutionFillModel.execution_fill_id.desc()),
    )


class ExecutionIntentModel(Base):
    __tablename__ = "execution_intents"
    __table_args__ = (
        Index(
            "idx_execution_intents_strategy_created",
            "trading_strategy_id",
            "created_at",
        ),
        Index("idx_execution_intents_slot_state", "slot_key", "state"),
        Index("idx_execution_intents_trade_signal", "trade_signal_id"),
        Index("idx_execution_intents_trade_decision", "trade_decision_id"),
        Index("idx_execution_intents_admission_decision", "admission_decision_id"),
        Index("idx_execution_intents_close_decision", "close_decision_id"),
        Index("idx_execution_intents_position", "position_id"),
        Index("idx_execution_intents_workflow", "workflow_id"),
        Index("idx_execution_intents_supersedes", "supersedes_execution_intent_id"),
        Index(
            "ux_execution_intents_supersedes",
            "supersedes_execution_intent_id",
            unique=True,
            postgresql_where=text("supersedes_execution_intent_id IS NOT NULL"),
        ),
        Index(
            "ux_execution_intents_admission_decision",
            "admission_decision_id",
            unique=True,
            postgresql_where=text("admission_decision_id IS NOT NULL"),
        ),
    )

    execution_intent_id: Mapped[str] = mapped_column(Text, primary_key=True)
    trading_strategy_id: Mapped[str] = mapped_column(Text, nullable=False)
    trade_signal_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("trade_signals.trade_signal_id", ondelete="SET NULL"),
        nullable=True,
    )
    trade_decision_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("trade_decisions.trade_decision_id", ondelete="SET NULL"),
        nullable=True,
    )
    admission_decision_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("trade_admissions.admission_decision_id", ondelete="RESTRICT"),
        nullable=True,
    )
    close_decision_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("trade_close_decisions.close_decision_id", ondelete="RESTRICT"),
        nullable=True,
    )
    position_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("portfolio_positions.position_id", ondelete="RESTRICT"),
        nullable=True,
    )
    intent_kind: Mapped[str] = mapped_column(Text, nullable=False)
    slot_key: Mapped[str] = mapped_column(Text, nullable=False)
    claim_token: Mapped[str | None] = mapped_column(Text, nullable=True)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    workflow_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    workflow_run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    policy_ref_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    config_hash: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    supersedes_execution_intent_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("execution_intents.execution_intent_id", ondelete="RESTRICT"),
        nullable=True,
    )
    state_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
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


class PortfolioPositionModel(Base):
    __tablename__ = "portfolio_positions"
    __table_args__ = (
        Index(
            "idx_portfolio_positions_strategy_status",
            "trading_strategy_id",
            "status",
        ),
        Index(
            "idx_portfolio_positions_strategy_opened",
            "trading_strategy_id",
            "market_date_opened",
            "updated_at",
        ),
        Index(
            "idx_portfolio_positions_strategy_closed",
            "trading_strategy_id",
            "market_date_closed",
            "updated_at",
        ),
        Index("idx_portfolio_positions_source_object", "source_object_type", "source_object_id"),
        Index("idx_portfolio_positions_trade_signal", "trade_signal_id"),
        Index("idx_portfolio_positions_trade_decision", "trade_decision_id"),
        Index("idx_portfolio_positions_admission_decision", "admission_decision_id"),
        Index(
            "ux_portfolio_positions_open_attempt",
            "open_execution_attempt_id",
            unique=True,
        ),
    )

    position_id: Mapped[str] = mapped_column(Text, primary_key=True)
    trading_strategy_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_object_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_object_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    trade_signal_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("trade_signals.trade_signal_id", ondelete="SET NULL"),
        nullable=True,
    )
    trade_decision_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("trade_decisions.trade_decision_id", ondelete="SET NULL"),
        nullable=True,
    )
    admission_decision_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("trade_admissions.admission_decision_id", ondelete="SET NULL"),
        nullable=True,
    )
    opening_execution_intent_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey(
            "execution_intents.execution_intent_id",
            name="fk_portfolio_positions_opening_execution_intent",
            ondelete="SET NULL",
            use_alter=True,
        ),
        nullable=True,
    )
    open_execution_attempt_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey(
            "execution_attempts.execution_attempt_id",
            name="portfolio_positions_open_execution_attempt_id_fkey",
            ondelete="CASCADE",
            use_alter=True,
        ),
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
    legs_json: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False, default=list)
    economics_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    strategy_metrics_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    requested_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    opened_quantity: Mapped[float] = mapped_column(Float, nullable=False)
    remaining_quantity: Mapped[float] = mapped_column(Float, nullable=False)
    entry_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_pnl: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    unrealized_pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    close_mark: Mapped[float | None] = mapped_column(Float, nullable=True)
    close_mark_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    close_marked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_broker_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    exit_policy_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    risk_policy_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    config_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_job_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_job_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_job_run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_exit_evaluated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_exit_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_reconciled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reconciliation_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    reconciliation_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class PositionCloseModel(Base):
    __tablename__ = "position_closes"
    __table_args__ = (
        Index("idx_position_closes_position_closed", "position_id", "closed_at"),
        Index("idx_position_closes_closed_position", "closed_at", "position_id"),
        Index("ux_position_closes_execution_attempt", "execution_attempt_id", unique=True),
    )

    position_close_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
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
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
