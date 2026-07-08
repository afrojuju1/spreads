"""add engine events and outbox

Revision ID: 20260708_0064
Revises: 20260622_0063
Create Date: 2026-07-08 12:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260708_0064"
down_revision = "20260622_0063"
branch_labels = None
depends_on = None


def _jsonb() -> postgresql.JSONB:
    return postgresql.JSONB(astext_type=sa.Text())


def upgrade() -> None:
    op.create_table(
        "engine_events",
        sa.Column("engine_event_id", sa.Text(), primary_key=True),
        sa.Column("sequence", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("run_id", sa.Text(), nullable=True),
        sa.Column("workflow_id", sa.Text(), nullable=True),
        sa.Column("workflow_run_id", sa.Text(), nullable=True),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("event_version", sa.Integer(), nullable=False),
        sa.Column("aggregate_type", sa.Text(), nullable=False),
        sa.Column("aggregate_id", sa.Text(), nullable=False),
        sa.Column("aggregate_version", sa.Integer(), nullable=True),
        sa.Column("lifecycle_object", sa.Text(), nullable=True),
        sa.Column("from_state", sa.Text(), nullable=True),
        sa.Column("to_state", sa.Text(), nullable=True),
        sa.Column("trading_strategy_id", sa.Text(), nullable=True),
        sa.Column("trade_signal_id", sa.Text(), nullable=True),
        sa.Column("trade_decision_id", sa.Text(), nullable=True),
        sa.Column("execution_intent_id", sa.Text(), nullable=True),
        sa.Column("execution_attempt_id", sa.Text(), nullable=True),
        sa.Column("broker_order_id", sa.Text(), nullable=True),
        sa.Column("position_id", sa.Text(), nullable=True),
        sa.Column("session_date", sa.Date(), nullable=True),
        sa.Column("market_session", sa.Text(), nullable=True),
        sa.Column("correlation_id", sa.Text(), nullable=True),
        sa.Column("causation_id", sa.Text(), nullable=True),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("payload_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("metadata_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ux_engine_events_sequence", "engine_events", ["sequence"], unique=True)
    op.create_index("ux_engine_events_idempotency_key", "engine_events", ["idempotency_key"], unique=True)
    op.create_index("idx_engine_events_aggregate", "engine_events", ["aggregate_type", "aggregate_id", "sequence"], unique=False)
    op.create_index("idx_engine_events_workflow", "engine_events", ["workflow_id", "sequence"], unique=False)
    op.create_index("idx_engine_events_type_occurred", "engine_events", ["event_type", "occurred_at"], unique=False)
    op.create_index("idx_engine_events_correlation", "engine_events", ["correlation_id", "sequence"], unique=False)
    op.create_index("idx_engine_events_intent", "engine_events", ["execution_intent_id", "sequence"], unique=False)
    op.create_index("idx_engine_events_attempt", "engine_events", ["execution_attempt_id", "sequence"], unique=False)
    op.create_index("idx_engine_events_position", "engine_events", ["position_id", "sequence"], unique=False)
    op.create_index("idx_engine_events_session", "engine_events", ["session_date", "sequence"], unique=False)

    op.create_table(
        "engine_outbox",
        sa.Column("engine_outbox_id", sa.Text(), primary_key=True),
        sa.Column(
            "engine_event_id",
            sa.Text(),
            sa.ForeignKey("engine_events.engine_event_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("stream", sa.Text(), nullable=False),
        sa.Column("subject", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("aggregate_type", sa.Text(), nullable=False),
        sa.Column("aggregate_id", sa.Text(), nullable=False),
        sa.Column("payload_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("headers_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("publish_state", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("idx_engine_outbox_pending", "engine_outbox", ["publish_state", "next_attempt_at", "created_at"], unique=False)
    op.create_index("idx_engine_outbox_event", "engine_outbox", ["engine_event_id"], unique=False)
    op.create_index("idx_engine_outbox_subject", "engine_outbox", ["subject", "created_at"], unique=False)
    op.create_index("idx_engine_outbox_aggregate", "engine_outbox", ["aggregate_type", "aggregate_id", "created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("idx_engine_outbox_aggregate", table_name="engine_outbox")
    op.drop_index("idx_engine_outbox_subject", table_name="engine_outbox")
    op.drop_index("idx_engine_outbox_event", table_name="engine_outbox")
    op.drop_index("idx_engine_outbox_pending", table_name="engine_outbox")
    op.drop_table("engine_outbox")
    op.drop_index("idx_engine_events_session", table_name="engine_events")
    op.drop_index("idx_engine_events_position", table_name="engine_events")
    op.drop_index("idx_engine_events_attempt", table_name="engine_events")
    op.drop_index("idx_engine_events_intent", table_name="engine_events")
    op.drop_index("idx_engine_events_correlation", table_name="engine_events")
    op.drop_index("idx_engine_events_type_occurred", table_name="engine_events")
    op.drop_index("idx_engine_events_workflow", table_name="engine_events")
    op.drop_index("idx_engine_events_aggregate", table_name="engine_events")
    op.drop_index("ux_engine_events_idempotency_key", table_name="engine_events")
    op.drop_index("ux_engine_events_sequence", table_name="engine_events")
    op.drop_table("engine_events")
