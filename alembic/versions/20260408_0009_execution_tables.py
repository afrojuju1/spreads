"""add execution tracking tables

Revision ID: 20260408_0009
Revises: 20260408_0008
Create Date: 2026-04-08 14:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260408_0009"
down_revision = "20260408_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "execution_attempts",
        sa.Column("execution_attempt_id", sa.Text(), primary_key=True),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("cycle_id", sa.Text(), nullable=True),
        sa.Column("candidate_id", sa.BigInteger(), nullable=True),
        sa.Column("bucket", sa.Text(), nullable=True),
        sa.Column("candidate_generated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("run_id", sa.Text(), nullable=True),
        sa.Column("job_run_id", sa.Text(), nullable=True),
        sa.Column("underlying_symbol", sa.Text(), nullable=False),
        sa.Column("strategy", sa.Text(), nullable=False),
        sa.Column("expiration_date", sa.Date(), nullable=False),
        sa.Column("short_symbol", sa.Text(), nullable=False),
        sa.Column("long_symbol", sa.Text(), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("limit_price", sa.Float(), nullable=False),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("broker", sa.Text(), nullable=False, server_default="alpaca"),
        sa.Column("broker_order_id", sa.Text(), nullable=True),
        sa.Column("client_order_id", sa.Text(), nullable=True),
        sa.Column("request_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("candidate_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["candidate_id"], ["collector_cycle_candidates.candidate_id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["cycle_id"], ["collector_cycles.cycle_id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["job_run_id"], ["job_runs.job_run_id"], ondelete="SET NULL"),
    )
    op.create_index(
        "idx_execution_attempts_session_requested",
        "execution_attempts",
        ["session_id", "requested_at"],
        unique=False,
    )
    op.create_index(
        "idx_execution_attempts_status_requested",
        "execution_attempts",
        ["status", "requested_at"],
        unique=False,
    )
    op.create_index(
        "idx_execution_attempts_candidate_requested",
        "execution_attempts",
        ["candidate_id", "requested_at"],
        unique=False,
    )

    op.create_table(
        "execution_orders",
        sa.Column("execution_order_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("execution_attempt_id", sa.Text(), nullable=False),
        sa.Column("broker", sa.Text(), nullable=False, server_default="alpaca"),
        sa.Column("broker_order_id", sa.Text(), nullable=False),
        sa.Column("parent_broker_order_id", sa.Text(), nullable=True),
        sa.Column("client_order_id", sa.Text(), nullable=True),
        sa.Column("order_status", sa.Text(), nullable=False),
        sa.Column("order_type", sa.Text(), nullable=True),
        sa.Column("time_in_force", sa.Text(), nullable=True),
        sa.Column("order_class", sa.Text(), nullable=True),
        sa.Column("side", sa.Text(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("leg_symbol", sa.Text(), nullable=True),
        sa.Column("leg_side", sa.Text(), nullable=True),
        sa.Column("position_intent", sa.Text(), nullable=True),
        sa.Column("quantity", sa.Float(), nullable=True),
        sa.Column("limit_price", sa.Float(), nullable=True),
        sa.Column("filled_qty", sa.Float(), nullable=True),
        sa.Column("filled_avg_price", sa.Float(), nullable=True),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("order_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(
            ["execution_attempt_id"],
            ["execution_attempts.execution_attempt_id"],
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        "idx_execution_orders_attempt_updated",
        "execution_orders",
        ["execution_attempt_id", "updated_at"],
        unique=False,
    )
    op.create_index(
        "idx_execution_orders_parent",
        "execution_orders",
        ["parent_broker_order_id"],
        unique=False,
    )
    op.create_index(
        "ux_execution_orders_broker_order_id",
        "execution_orders",
        ["broker_order_id"],
        unique=True,
    )

    op.create_table(
        "execution_fills",
        sa.Column("execution_fill_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("execution_attempt_id", sa.Text(), nullable=False),
        sa.Column("execution_order_id", sa.BigInteger(), nullable=True),
        sa.Column("broker", sa.Text(), nullable=False, server_default="alpaca"),
        sa.Column("broker_fill_id", sa.Text(), nullable=False),
        sa.Column("broker_order_id", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=True),
        sa.Column("fill_type", sa.Text(), nullable=True),
        sa.Column("quantity", sa.Float(), nullable=False),
        sa.Column("cumulative_quantity", sa.Float(), nullable=True),
        sa.Column("remaining_quantity", sa.Float(), nullable=True),
        sa.Column("price", sa.Float(), nullable=True),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("fill_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(
            ["execution_attempt_id"],
            ["execution_attempts.execution_attempt_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["execution_order_id"],
            ["execution_orders.execution_order_id"],
            ondelete="SET NULL",
        ),
    )
    op.create_index(
        "idx_execution_fills_attempt_filled",
        "execution_fills",
        ["execution_attempt_id", "filled_at"],
        unique=False,
    )
    op.create_index(
        "idx_execution_fills_order",
        "execution_fills",
        ["broker_order_id"],
        unique=False,
    )
    op.create_index(
        "ux_execution_fills_broker_fill_id",
        "execution_fills",
        ["broker_fill_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ux_execution_fills_broker_fill_id", table_name="execution_fills")
    op.drop_index("idx_execution_fills_order", table_name="execution_fills")
    op.drop_index("idx_execution_fills_attempt_filled", table_name="execution_fills")
    op.drop_table("execution_fills")

    op.drop_index("ux_execution_orders_broker_order_id", table_name="execution_orders")
    op.drop_index("idx_execution_orders_parent", table_name="execution_orders")
    op.drop_index("idx_execution_orders_attempt_updated", table_name="execution_orders")
    op.drop_table("execution_orders")

    op.drop_index("idx_execution_attempts_candidate_requested", table_name="execution_attempts")
    op.drop_index("idx_execution_attempts_status_requested", table_name="execution_attempts")
    op.drop_index("idx_execution_attempts_session_requested", table_name="execution_attempts")
    op.drop_table("execution_attempts")
