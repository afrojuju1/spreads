"""add target trade lifecycle schema

Revision ID: 20260603_0042
Revises: 20260602_0041
Create Date: 2026-06-03 10:55:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260603_0042"
down_revision = "20260602_0041"
branch_labels = None
depends_on = None


def _jsonb() -> postgresql.JSONB:
    return postgresql.JSONB(astext_type=sa.Text())


def upgrade() -> None:
    op.create_table(
        "trade_signals",
        sa.Column("trade_signal_id", sa.Text(), primary_key=True),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("source_kind", sa.Text(), nullable=False),
        sa.Column("source_id", sa.Text(), nullable=False),
        sa.Column("account_id", sa.Text(), nullable=True),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("market_session", sa.Text(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("underlying_symbol", sa.Text(), nullable=False),
        sa.Column("root_symbol", sa.Text(), nullable=True),
        sa.Column("asset_class", sa.Text(), nullable=True),
        sa.Column("strategy_family", sa.Text(), nullable=True),
        sa.Column("product_class", sa.Text(), nullable=True),
        sa.Column("horizon", sa.Text(), nullable=True),
        sa.Column("style_profile", sa.Text(), nullable=True),
        sa.Column("signal_state", sa.Text(), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=True),
        sa.Column("score", sa.Float(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("legs_json", _jsonb(), nullable=False),
        sa.Column("execution_shape_json", _jsonb(), nullable=False),
        sa.Column("economics_json", _jsonb(), nullable=False),
        sa.Column("reason_codes_json", _jsonb(), nullable=False),
        sa.Column("blockers_json", _jsonb(), nullable=False),
        sa.Column("evidence_json", _jsonb(), nullable=False),
        sa.Column("metrics_json", _jsonb(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ux_trade_signals_idempotency_key",
        "trade_signals",
        ["idempotency_key"],
        unique=True,
    )
    op.create_index(
        "idx_trade_signals_source",
        "trade_signals",
        ["source_kind", "source_id"],
    )
    op.create_index(
        "idx_trade_signals_session_state",
        "trade_signals",
        ["session_date", "signal_state"],
    )
    op.create_index(
        "idx_trade_signals_underlying_updated",
        "trade_signals",
        ["underlying_symbol", "updated_at"],
    )

    op.create_table(
        "trade_decisions",
        sa.Column("trade_decision_id", sa.Text(), primary_key=True),
        sa.Column(
            "trade_signal_id",
            sa.Text(),
            sa.ForeignKey("trade_signals.trade_signal_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("bot_id", sa.Text(), nullable=False),
        sa.Column("automation_id", sa.Text(), nullable=False),
        sa.Column("strategy_config_id", sa.Text(), nullable=True),
        sa.Column("config_hash", sa.Text(), nullable=True),
        sa.Column("run_key", sa.Text(), nullable=False),
        sa.Column("scope_key", sa.Text(), nullable=False),
        sa.Column("decision_state", sa.Text(), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=True),
        sa.Column("score", sa.Float(), nullable=True),
        sa.Column("selected_quantity", sa.Integer(), nullable=True),
        sa.Column("selected_execution_shape_json", _jsonb(), nullable=False),
        sa.Column("reason_codes_json", _jsonb(), nullable=False),
        sa.Column("blockers_json", _jsonb(), nullable=False),
        sa.Column("evidence_json", _jsonb(), nullable=False),
        sa.Column("metrics_json", _jsonb(), nullable=False),
        sa.Column("supersedes_decision_id", sa.Text(), nullable=True),
        sa.Column("superseded_by_decision_id", sa.Text(), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ux_trade_decisions_run_signal",
        "trade_decisions",
        ["run_key", "trade_signal_id"],
        unique=True,
    )
    op.create_index(
        "idx_trade_decisions_bot_decided",
        "trade_decisions",
        ["bot_id", "automation_id", "decided_at"],
    )
    op.create_index(
        "idx_trade_decisions_state_decided",
        "trade_decisions",
        ["decision_state", "decided_at"],
    )

    op.create_table(
        "trade_execution_intents",
        sa.Column("execution_intent_id", sa.Text(), primary_key=True),
        sa.Column("intent_kind", sa.Text(), nullable=False),
        sa.Column("source_object_type", sa.Text(), nullable=False),
        sa.Column("source_object_id", sa.Text(), nullable=False),
        sa.Column(
            "trade_signal_id",
            sa.Text(),
            sa.ForeignKey("trade_signals.trade_signal_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "trade_decision_id",
            sa.Text(),
            sa.ForeignKey("trade_decisions.trade_decision_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("position_id", sa.Text(), nullable=True),
        sa.Column("bot_id", sa.Text(), nullable=True),
        sa.Column("automation_id", sa.Text(), nullable=True),
        sa.Column("account_id", sa.Text(), nullable=True),
        sa.Column("slot_key", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("intent_state", sa.Text(), nullable=False),
        sa.Column("claim_token", sa.Text(), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("supersedes_intent_id", sa.Text(), nullable=True),
        sa.Column("superseded_by_intent_id", sa.Text(), nullable=True),
        sa.Column("payload_json", _jsonb(), nullable=False),
        sa.Column("policy_snapshot_json", _jsonb(), nullable=False),
        sa.Column("config_hash", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ux_trade_execution_intents_idempotency_key",
        "trade_execution_intents",
        ["idempotency_key"],
        unique=True,
    )
    op.create_index(
        "idx_trade_execution_intents_slot_state",
        "trade_execution_intents",
        ["slot_key", "intent_state"],
    )
    op.create_index(
        "idx_trade_execution_intents_source",
        "trade_execution_intents",
        ["source_object_type", "source_object_id"],
    )
    op.create_index(
        "idx_trade_execution_intents_created",
        "trade_execution_intents",
        ["created_at"],
    )

    op.create_table(
        "trade_admissions",
        sa.Column("admission_decision_id", sa.Text(), primary_key=True),
        sa.Column(
            "execution_intent_id",
            sa.Text(),
            sa.ForeignKey(
                "trade_execution_intents.execution_intent_id", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        sa.Column(
            "trade_signal_id",
            sa.Text(),
            sa.ForeignKey("trade_signals.trade_signal_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "trade_decision_id",
            sa.Text(),
            sa.ForeignKey("trade_decisions.trade_decision_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("position_id", sa.Text(), nullable=True),
        sa.Column("admission_kind", sa.Text(), nullable=False),
        sa.Column("admission_state", sa.Text(), nullable=False),
        sa.Column("account_id", sa.Text(), nullable=True),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("requested_quantity", sa.Integer(), nullable=True),
        sa.Column("requested_notional", sa.Float(), nullable=True),
        sa.Column("max_loss", sa.Float(), nullable=True),
        sa.Column("policy_snapshot_json", _jsonb(), nullable=False),
        sa.Column("capability_snapshot_json", _jsonb(), nullable=False),
        sa.Column("metrics_json", _jsonb(), nullable=False),
        sa.Column("reason_codes_json", _jsonb(), nullable=False),
        sa.Column("blockers_json", _jsonb(), nullable=False),
        sa.Column("evidence_json", _jsonb(), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("execution_attempt_id", sa.Text(), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("idx_trade_admissions_intent", "trade_admissions", ["execution_intent_id"])
    op.create_index(
        "idx_trade_admissions_state_decided",
        "trade_admissions",
        ["admission_state", "decided_at"],
    )
    op.create_index(
        "idx_trade_admissions_signal_decided",
        "trade_admissions",
        ["trade_signal_id", "decided_at"],
    )

    op.create_table(
        "trade_execution_attempts",
        sa.Column("execution_attempt_id", sa.Text(), primary_key=True),
        sa.Column(
            "execution_intent_id",
            sa.Text(),
            sa.ForeignKey(
                "trade_execution_intents.execution_intent_id", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        sa.Column(
            "admission_decision_id",
            sa.Text(),
            sa.ForeignKey("trade_admissions.admission_decision_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("attempt_kind", sa.Text(), nullable=False),
        sa.Column("attempt_state", sa.Text(), nullable=False),
        sa.Column("account_id", sa.Text(), nullable=True),
        sa.Column("broker", sa.Text(), nullable=False),
        sa.Column("execution_runtime", sa.Text(), nullable=False),
        sa.Column("client_order_id", sa.Text(), nullable=True),
        sa.Column("primary_broker_order_id", sa.Text(), nullable=True),
        sa.Column("requested_quantity", sa.Integer(), nullable=True),
        sa.Column("requested_limit_price", sa.Float(), nullable=True),
        sa.Column("canonical_legs_json", _jsonb(), nullable=False),
        sa.Column("order_payload_json", _jsonb(), nullable=False),
        sa.Column("policy_snapshot_json", _jsonb(), nullable=False),
        sa.Column("economics_json", _jsonb(), nullable=False),
        sa.Column("source_job_json", _jsonb(), nullable=False),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("queued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("error_code", sa.Text(), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("supersedes_attempt_id", sa.Text(), nullable=True),
        sa.Column("superseded_by_attempt_id", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_trade_execution_attempts_intent",
        "trade_execution_attempts",
        ["execution_intent_id"],
    )
    op.create_index(
        "idx_trade_execution_attempts_state_requested",
        "trade_execution_attempts",
        ["attempt_state", "requested_at"],
    )
    op.create_index(
        "ux_trade_execution_attempts_client_order_id",
        "trade_execution_attempts",
        ["client_order_id"],
        unique=True,
    )

    op.create_table(
        "trade_broker_orders",
        sa.Column("trade_broker_order_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "execution_attempt_id",
            sa.Text(),
            sa.ForeignKey(
                "trade_execution_attempts.execution_attempt_id", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        sa.Column("broker", sa.Text(), nullable=False),
        sa.Column("broker_order_id", sa.Text(), nullable=False),
        sa.Column("parent_broker_order_id", sa.Text(), nullable=True),
        sa.Column("client_order_id", sa.Text(), nullable=True),
        sa.Column("broker_status", sa.Text(), nullable=False),
        sa.Column("normalized_order_state", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("asset_class", sa.Text(), nullable=True),
        sa.Column("side", sa.Text(), nullable=True),
        sa.Column("position_intent", sa.Text(), nullable=True),
        sa.Column("order_type", sa.Text(), nullable=True),
        sa.Column("time_in_force", sa.Text(), nullable=True),
        sa.Column("order_class", sa.Text(), nullable=True),
        sa.Column("quantity", sa.Float(), nullable=True),
        sa.Column("filled_quantity", sa.Float(), nullable=True),
        sa.Column("remaining_quantity", sa.Float(), nullable=True),
        sa.Column("limit_price", sa.Float(), nullable=True),
        sa.Column("filled_avg_price", sa.Float(), nullable=True),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_payload_json", _jsonb(), nullable=False),
    )
    op.create_index(
        "idx_trade_broker_orders_attempt_updated",
        "trade_broker_orders",
        ["execution_attempt_id", "updated_at"],
    )
    op.create_index(
        "ux_trade_broker_orders_broker_order_id",
        "trade_broker_orders",
        ["broker_order_id"],
        unique=True,
    )

    op.create_table(
        "trade_broker_fills",
        sa.Column("trade_broker_fill_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "execution_attempt_id",
            sa.Text(),
            sa.ForeignKey(
                "trade_execution_attempts.execution_attempt_id", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        sa.Column(
            "trade_broker_order_id",
            sa.BigInteger(),
            sa.ForeignKey("trade_broker_orders.trade_broker_order_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("broker", sa.Text(), nullable=False),
        sa.Column("broker_fill_id", sa.Text(), nullable=False),
        sa.Column("broker_order_id", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=True),
        sa.Column("position_intent", sa.Text(), nullable=True),
        sa.Column("quantity", sa.Float(), nullable=False),
        sa.Column("price", sa.Float(), nullable=True),
        sa.Column("value", sa.Float(), nullable=True),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_payload_json", _jsonb(), nullable=False),
    )
    op.create_index(
        "idx_trade_broker_fills_attempt_filled",
        "trade_broker_fills",
        ["execution_attempt_id", "filled_at"],
    )
    op.create_index(
        "ux_trade_broker_fills_broker_fill_id",
        "trade_broker_fills",
        ["broker_fill_id"],
        unique=True,
    )

    op.create_table(
        "trade_positions",
        sa.Column("position_id", sa.Text(), primary_key=True),
        sa.Column("account_id", sa.Text(), nullable=True),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("market_session", sa.Text(), nullable=False),
        sa.Column("source_trade_signal_id", sa.Text(), nullable=True),
        sa.Column("opening_trade_decision_id", sa.Text(), nullable=True),
        sa.Column("opening_execution_intent_id", sa.Text(), nullable=True),
        sa.Column("opening_execution_attempt_id", sa.Text(), nullable=True),
        sa.Column("position_state", sa.Text(), nullable=False),
        sa.Column("underlying_symbol", sa.Text(), nullable=False),
        sa.Column("root_symbol", sa.Text(), nullable=True),
        sa.Column("strategy_family", sa.Text(), nullable=True),
        sa.Column("product_class", sa.Text(), nullable=True),
        sa.Column("canonical_legs_json", _jsonb(), nullable=False),
        sa.Column("opened_quantity", sa.Float(), nullable=False),
        sa.Column("remaining_quantity", sa.Float(), nullable=False),
        sa.Column("entry_value", sa.Float(), nullable=True),
        sa.Column("realized_pnl", sa.Float(), nullable=False),
        sa.Column("unrealized_pnl", sa.Float(), nullable=True),
        sa.Column("mark", sa.Float(), nullable=True),
        sa.Column("mark_source", sa.Text(), nullable=True),
        sa.Column("marked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("risk_policy_snapshot_json", _jsonb(), nullable=False),
        sa.Column("exit_policy_snapshot_json", _jsonb(), nullable=False),
        sa.Column("reconciliation_state", sa.Text(), nullable=True),
        sa.Column("last_reconciled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reconciliation_note", sa.Text(), nullable=True),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_trade_positions_account_state",
        "trade_positions",
        ["account_id", "position_state"],
    )
    op.create_index(
        "idx_trade_positions_session_state",
        "trade_positions",
        ["session_date", "position_state"],
    )
    op.create_index(
        "idx_trade_positions_underlying_updated",
        "trade_positions",
        ["underlying_symbol", "updated_at"],
    )

    op.create_table(
        "trade_close_decisions",
        sa.Column("close_decision_id", sa.Text(), primary_key=True),
        sa.Column(
            "position_id",
            sa.Text(),
            sa.ForeignKey("trade_positions.position_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("decision_state", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("quantity_to_close", sa.Float(), nullable=True),
        sa.Column("limit_source", sa.Text(), nullable=True),
        sa.Column("limit_price", sa.Float(), nullable=True),
        sa.Column("mark_source", sa.Text(), nullable=True),
        sa.Column("policy_snapshot_json", _jsonb(), nullable=False),
        sa.Column("reason_codes_json", _jsonb(), nullable=False),
        sa.Column("blockers_json", _jsonb(), nullable=False),
        sa.Column("evidence_json", _jsonb(), nullable=False),
        sa.Column("metrics_json", _jsonb(), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("execution_intent_id", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_trade_close_decisions_position_decided",
        "trade_close_decisions",
        ["position_id", "decided_at"],
    )
    op.create_index(
        "idx_trade_close_decisions_state_decided",
        "trade_close_decisions",
        ["decision_state", "decided_at"],
    )

    op.create_table(
        "trade_position_closes",
        sa.Column("position_close_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "position_id",
            sa.Text(),
            sa.ForeignKey("trade_positions.position_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "close_decision_id",
            sa.Text(),
            sa.ForeignKey("trade_close_decisions.close_decision_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("execution_intent_id", sa.Text(), nullable=True),
        sa.Column("execution_attempt_id", sa.Text(), nullable=True),
        sa.Column("closed_quantity", sa.Float(), nullable=False),
        sa.Column("exit_value", sa.Float(), nullable=True),
        sa.Column("realized_pnl", sa.Float(), nullable=False),
        sa.Column("broker_order_id", sa.Text(), nullable=True),
        sa.Column("close_state", sa.Text(), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_trade_position_closes_position_closed",
        "trade_position_closes",
        ["position_id", "closed_at"],
    )
    op.create_index(
        "idx_trade_position_closes_attempt",
        "trade_position_closes",
        ["execution_attempt_id"],
    )

    op.create_table(
        "trade_reconciliation_observations",
        sa.Column("reconciliation_observation_id", sa.Text(), primary_key=True),
        sa.Column("account_id", sa.Text(), nullable=True),
        sa.Column("broker", sa.Text(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("object_type", sa.Text(), nullable=False),
        sa.Column("object_id", sa.Text(), nullable=False),
        sa.Column("broker_order_id", sa.Text(), nullable=True),
        sa.Column("client_order_id", sa.Text(), nullable=True),
        sa.Column("position_id", sa.Text(), nullable=True),
        sa.Column("reconciliation_state", sa.Text(), nullable=False),
        sa.Column("reason_codes_json", _jsonb(), nullable=False),
        sa.Column("evidence_json", _jsonb(), nullable=False),
        sa.Column("raw_payload_json", _jsonb(), nullable=False),
        sa.Column("repair_action", sa.Text(), nullable=True),
        sa.Column("repair_attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("repair_result", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_trade_reconciliation_object_observed",
        "trade_reconciliation_observations",
        ["object_type", "object_id", "observed_at"],
    )
    op.create_index(
        "idx_trade_reconciliation_state_observed",
        "trade_reconciliation_observations",
        ["reconciliation_state", "observed_at"],
    )

    op.create_table(
        "trade_lifecycle_events",
        sa.Column("lifecycle_event_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("object_type", sa.Text(), nullable=False),
        sa.Column("object_id", sa.Text(), nullable=False),
        sa.Column("from_state", sa.Text(), nullable=True),
        sa.Column("to_state", sa.Text(), nullable=True),
        sa.Column("correlation_id", sa.Text(), nullable=True),
        sa.Column("causation_id", sa.Text(), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload_json", _jsonb(), nullable=False),
    )
    op.create_index(
        "idx_trade_lifecycle_events_object",
        "trade_lifecycle_events",
        ["object_type", "object_id", "occurred_at"],
    )
    op.create_index(
        "idx_trade_lifecycle_events_correlation",
        "trade_lifecycle_events",
        ["correlation_id", "occurred_at"],
    )
    op.create_index(
        "idx_trade_lifecycle_events_type",
        "trade_lifecycle_events",
        ["event_type", "occurred_at"],
    )


def downgrade() -> None:
    op.drop_table("trade_lifecycle_events")
    op.drop_table("trade_reconciliation_observations")
    op.drop_table("trade_position_closes")
    op.drop_table("trade_close_decisions")
    op.drop_table("trade_positions")
    op.drop_table("trade_broker_fills")
    op.drop_table("trade_broker_orders")
    op.drop_table("trade_execution_attempts")
    op.drop_table("trade_admissions")
    op.drop_table("trade_execution_intents")
    op.drop_table("trade_decisions")
    op.drop_table("trade_signals")
