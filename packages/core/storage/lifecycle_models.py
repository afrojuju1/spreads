from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import BigInteger, Date, DateTime, Float, ForeignKey, Index, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.storage.db import Base

TARGET_LIFECYCLE_TABLES: tuple[dict[str, str], ...] = (
    {"name": "trade_signals", "role": "signal fact"},
    {"name": "trade_decisions", "role": "decision fact"},
    {"name": "trade_execution_intents", "role": "dispatch request fact"},
    {"name": "trade_admissions", "role": "pre-attempt admission fact"},
    {"name": "trade_execution_attempts", "role": "broker submission attempt fact"},
    {"name": "trade_broker_orders", "role": "broker order snapshot fact"},
    {"name": "trade_broker_fills", "role": "broker fill fact"},
    {"name": "trade_positions", "role": "position projection"},
    {"name": "trade_close_decisions", "role": "close decision fact"},
    {"name": "trade_position_closes", "role": "close impact fact"},
    {
        "name": "trade_reconciliation_observations",
        "role": "broker/local reconciliation fact",
    },
    {"name": "trade_lifecycle_events", "role": "lifecycle event fact"},
)


class TradeSignalModel(Base):
    __tablename__ = "trade_signals"
    __table_args__ = (
        Index("ux_trade_signals_idempotency_key", "idempotency_key", unique=True),
        Index("idx_trade_signals_source", "source_kind", "source_id"),
        Index("idx_trade_signals_session_state", "session_date", "signal_state"),
        Index("idx_trade_signals_underlying_updated", "underlying_symbol", "updated_at"),
    )

    trade_signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    source_kind: Mapped[str] = mapped_column(Text, nullable=False)
    source_id: Mapped[str] = mapped_column(Text, nullable=False)
    account_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    market_session: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    underlying_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    root_symbol: Mapped[str | None] = mapped_column(Text, nullable=True)
    asset_class: Mapped[str | None] = mapped_column(Text, nullable=True)
    strategy_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    product_class: Mapped[str | None] = mapped_column(Text, nullable=True)
    horizon: Mapped[str | None] = mapped_column(Text, nullable=True)
    style_profile: Mapped[str | None] = mapped_column(Text, nullable=True)
    signal_state: Mapped[str] = mapped_column(Text, nullable=False)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    legs_json: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False, default=list)
    execution_shape_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    economics_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    reason_codes_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    blockers_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    metrics_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TradeDecisionModel(Base):
    __tablename__ = "trade_decisions"
    __table_args__ = (
        Index("ux_trade_decisions_run_signal", "run_key", "trade_signal_id", unique=True),
        Index("idx_trade_decisions_bot_decided", "bot_id", "automation_id", "decided_at"),
        Index("idx_trade_decisions_state_decided", "decision_state", "decided_at"),
    )

    trade_decision_id: Mapped[str] = mapped_column(Text, primary_key=True)
    trade_signal_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("trade_signals.trade_signal_id", ondelete="CASCADE"),
        nullable=False,
    )
    bot_id: Mapped[str] = mapped_column(Text, nullable=False)
    automation_id: Mapped[str] = mapped_column(Text, nullable=False)
    strategy_config_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    config_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    run_key: Mapped[str] = mapped_column(Text, nullable=False)
    scope_key: Mapped[str] = mapped_column(Text, nullable=False)
    decision_state: Mapped[str] = mapped_column(Text, nullable=False)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    selected_quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    selected_execution_shape_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    reason_codes_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    blockers_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    metrics_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    supersedes_decision_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    superseded_by_decision_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TradeExecutionIntentModel(Base):
    __tablename__ = "trade_execution_intents"
    __table_args__ = (
        Index("ux_trade_execution_intents_idempotency_key", "idempotency_key", unique=True),
        Index("idx_trade_execution_intents_slot_state", "slot_key", "intent_state"),
        Index("idx_trade_execution_intents_source", "source_object_type", "source_object_id"),
        Index("idx_trade_execution_intents_created", "created_at"),
    )

    execution_intent_id: Mapped[str] = mapped_column(Text, primary_key=True)
    intent_kind: Mapped[str] = mapped_column(Text, nullable=False)
    source_object_type: Mapped[str] = mapped_column(Text, nullable=False)
    source_object_id: Mapped[str] = mapped_column(Text, nullable=False)
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
    position_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    bot_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    automation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    account_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    slot_key: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    intent_state: Mapped[str] = mapped_column(Text, nullable=False)
    claim_token: Mapped[str | None] = mapped_column(Text, nullable=True)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    supersedes_intent_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    superseded_by_intent_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    policy_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    config_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TradeAdmissionModel(Base):
    __tablename__ = "trade_admissions"
    __table_args__ = (
        Index("idx_trade_admissions_intent", "execution_intent_id"),
        Index("idx_trade_admissions_state_decided", "admission_state", "decided_at"),
        Index("idx_trade_admissions_signal_decided", "trade_signal_id", "decided_at"),
    )

    admission_decision_id: Mapped[str] = mapped_column(Text, primary_key=True)
    execution_intent_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("trade_execution_intents.execution_intent_id", ondelete="CASCADE"),
        nullable=False,
    )
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
    position_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    admission_kind: Mapped[str] = mapped_column(Text, nullable=False)
    admission_state: Mapped[str] = mapped_column(Text, nullable=False)
    account_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    requested_quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    requested_notional: Mapped[float | None] = mapped_column(Float, nullable=True)
    max_loss: Mapped[float | None] = mapped_column(Float, nullable=True)
    policy_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    capability_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    metrics_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    reason_codes_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    blockers_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    execution_attempt_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TradeExecutionAttemptModel(Base):
    __tablename__ = "trade_execution_attempts"
    __table_args__ = (
        Index("idx_trade_execution_attempts_intent", "execution_intent_id"),
        Index("idx_trade_execution_attempts_state_requested", "attempt_state", "requested_at"),
        Index("ux_trade_execution_attempts_client_order_id", "client_order_id", unique=True),
    )

    execution_attempt_id: Mapped[str] = mapped_column(Text, primary_key=True)
    execution_intent_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("trade_execution_intents.execution_intent_id", ondelete="CASCADE"),
        nullable=False,
    )
    admission_decision_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("trade_admissions.admission_decision_id", ondelete="SET NULL"),
        nullable=True,
    )
    attempt_kind: Mapped[str] = mapped_column(Text, nullable=False)
    attempt_state: Mapped[str] = mapped_column(Text, nullable=False)
    account_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    broker: Mapped[str] = mapped_column(Text, nullable=False)
    execution_runtime: Mapped[str] = mapped_column(Text, nullable=False)
    client_order_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    primary_broker_order_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    requested_quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    requested_limit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    canonical_legs_json: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False, default=list)
    order_payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    policy_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    economics_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    source_job_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    queued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    error_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    supersedes_attempt_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    superseded_by_attempt_id: Mapped[str | None] = mapped_column(Text, nullable=True)


class TradeBrokerOrderModel(Base):
    __tablename__ = "trade_broker_orders"
    __table_args__ = (
        Index("idx_trade_broker_orders_attempt_updated", "execution_attempt_id", "updated_at"),
        Index("ux_trade_broker_orders_broker_order_id", "broker_order_id", unique=True),
    )

    trade_broker_order_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    execution_attempt_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("trade_execution_attempts.execution_attempt_id", ondelete="CASCADE"),
        nullable=False,
    )
    broker: Mapped[str] = mapped_column(Text, nullable=False)
    broker_order_id: Mapped[str] = mapped_column(Text, nullable=False)
    parent_broker_order_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    client_order_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    broker_status: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_order_state: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str | None] = mapped_column(Text, nullable=True)
    asset_class: Mapped[str | None] = mapped_column(Text, nullable=True)
    side: Mapped[str | None] = mapped_column(Text, nullable=True)
    position_intent: Mapped[str | None] = mapped_column(Text, nullable=True)
    order_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    time_in_force: Mapped[str | None] = mapped_column(Text, nullable=True)
    order_class: Mapped[str | None] = mapped_column(Text, nullable=True)
    quantity: Mapped[float | None] = mapped_column(Float, nullable=True)
    filled_quantity: Mapped[float | None] = mapped_column(Float, nullable=True)
    remaining_quantity: Mapped[float | None] = mapped_column(Float, nullable=True)
    limit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    filled_avg_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw_payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)


class TradeBrokerFillModel(Base):
    __tablename__ = "trade_broker_fills"
    __table_args__ = (
        Index("idx_trade_broker_fills_attempt_filled", "execution_attempt_id", "filled_at"),
        Index("ux_trade_broker_fills_broker_fill_id", "broker_fill_id", unique=True),
    )

    trade_broker_fill_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    execution_attempt_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("trade_execution_attempts.execution_attempt_id", ondelete="CASCADE"),
        nullable=False,
    )
    trade_broker_order_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("trade_broker_orders.trade_broker_order_id", ondelete="SET NULL"),
        nullable=True,
    )
    broker: Mapped[str] = mapped_column(Text, nullable=False)
    broker_fill_id: Mapped[str] = mapped_column(Text, nullable=False)
    broker_order_id: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[str | None] = mapped_column(Text, nullable=True)
    position_intent: Mapped[str | None] = mapped_column(Text, nullable=True)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    value: Mapped[float | None] = mapped_column(Float, nullable=True)
    filled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw_payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)


class TradePositionModel(Base):
    __tablename__ = "trade_positions"
    __table_args__ = (
        Index("idx_trade_positions_account_state", "account_id", "position_state"),
        Index("idx_trade_positions_session_state", "session_date", "position_state"),
        Index("idx_trade_positions_underlying_updated", "underlying_symbol", "updated_at"),
    )

    position_id: Mapped[str] = mapped_column(Text, primary_key=True)
    account_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    market_session: Mapped[str] = mapped_column(Text, nullable=False)
    source_trade_signal_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    opening_trade_decision_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    opening_execution_intent_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    opening_execution_attempt_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    position_state: Mapped[str] = mapped_column(Text, nullable=False)
    underlying_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    root_symbol: Mapped[str | None] = mapped_column(Text, nullable=True)
    strategy_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    product_class: Mapped[str | None] = mapped_column(Text, nullable=True)
    canonical_legs_json: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False, default=list)
    opened_quantity: Mapped[float] = mapped_column(Float, nullable=False)
    remaining_quantity: Mapped[float] = mapped_column(Float, nullable=False)
    entry_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_pnl: Mapped[float] = mapped_column(Float, nullable=False)
    unrealized_pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    mark: Mapped[float | None] = mapped_column(Float, nullable=True)
    mark_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    marked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    risk_policy_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    exit_policy_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    reconciliation_state: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_reconciled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reconciliation_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TradeCloseDecisionModel(Base):
    __tablename__ = "trade_close_decisions"
    __table_args__ = (
        Index("idx_trade_close_decisions_position_decided", "position_id", "decided_at"),
        Index("idx_trade_close_decisions_state_decided", "decision_state", "decided_at"),
    )

    close_decision_id: Mapped[str] = mapped_column(Text, primary_key=True)
    position_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("trade_positions.position_id", ondelete="CASCADE"),
        nullable=False,
    )
    decision_state: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    quantity_to_close: Mapped[float | None] = mapped_column(Float, nullable=True)
    limit_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    limit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    mark_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    policy_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    reason_codes_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    blockers_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    metrics_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    execution_intent_id: Mapped[str | None] = mapped_column(Text, nullable=True)


class TradePositionCloseModel(Base):
    __tablename__ = "trade_position_closes"
    __table_args__ = (
        Index("idx_trade_position_closes_position_closed", "position_id", "closed_at"),
        Index("idx_trade_position_closes_attempt", "execution_attempt_id"),
    )

    position_close_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    position_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("trade_positions.position_id", ondelete="CASCADE"),
        nullable=False,
    )
    close_decision_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("trade_close_decisions.close_decision_id", ondelete="SET NULL"),
        nullable=True,
    )
    execution_intent_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    execution_attempt_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    closed_quantity: Mapped[float] = mapped_column(Float, nullable=False)
    exit_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_pnl: Mapped[float] = mapped_column(Float, nullable=False)
    broker_order_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    close_state: Mapped[str] = mapped_column(Text, nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TradeReconciliationObservationModel(Base):
    __tablename__ = "trade_reconciliation_observations"
    __table_args__ = (
        Index("idx_trade_reconciliation_object_observed", "object_type", "object_id", "observed_at"),
        Index("idx_trade_reconciliation_state_observed", "reconciliation_state", "observed_at"),
    )

    reconciliation_observation_id: Mapped[str] = mapped_column(Text, primary_key=True)
    account_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    broker: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    object_type: Mapped[str] = mapped_column(Text, nullable=False)
    object_id: Mapped[str] = mapped_column(Text, nullable=False)
    broker_order_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    client_order_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    position_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    reconciliation_state: Mapped[str] = mapped_column(Text, nullable=False)
    reason_codes_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    raw_payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    repair_action: Mapped[str | None] = mapped_column(Text, nullable=True)
    repair_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    repair_result: Mapped[str | None] = mapped_column(Text, nullable=True)


class TradeLifecycleEventModel(Base):
    __tablename__ = "trade_lifecycle_events"
    __table_args__ = (
        Index("idx_trade_lifecycle_events_object", "object_type", "object_id", "occurred_at"),
        Index("idx_trade_lifecycle_events_correlation", "correlation_id", "occurred_at"),
        Index("idx_trade_lifecycle_events_type", "event_type", "occurred_at"),
    )

    lifecycle_event_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    object_type: Mapped[str] = mapped_column(Text, nullable=False)
    object_id: Mapped[str] = mapped_column(Text, nullable=False)
    from_state: Mapped[str | None] = mapped_column(Text, nullable=True)
    to_state: Mapped[str | None] = mapped_column(Text, nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    causation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
