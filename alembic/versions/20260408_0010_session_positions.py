"""add session position tables and execution intent

Revision ID: 20260408_0010
Revises: 20260408_0009
Create Date: 2026-04-08 18:10:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260408_0010"
down_revision = "20260408_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "session_positions",
        sa.Column("session_position_id", sa.Text(), primary_key=True),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("candidate_id", sa.BigInteger(), nullable=True),
        sa.Column("open_execution_attempt_id", sa.Text(), nullable=False),
        sa.Column("underlying_symbol", sa.Text(), nullable=False),
        sa.Column("strategy", sa.Text(), nullable=False),
        sa.Column("expiration_date", sa.Date(), nullable=False),
        sa.Column("short_symbol", sa.Text(), nullable=False),
        sa.Column("long_symbol", sa.Text(), nullable=False),
        sa.Column("requested_quantity", sa.Integer(), nullable=False),
        sa.Column("opened_quantity", sa.Float(), nullable=False),
        sa.Column("remaining_quantity", sa.Float(), nullable=False),
        sa.Column("entry_credit", sa.Float(), nullable=True),
        sa.Column("entry_notional", sa.Float(), nullable=True),
        sa.Column("width", sa.Float(), nullable=True),
        sa.Column("max_profit", sa.Float(), nullable=True),
        sa.Column("max_loss", sa.Float(), nullable=True),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("realized_pnl", sa.Float(), nullable=False, server_default="0"),
        sa.Column("unrealized_pnl", sa.Float(), nullable=True),
        sa.Column("close_mark", sa.Float(), nullable=True),
        sa.Column("close_mark_source", sa.Text(), nullable=True),
        sa.Column("close_marked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_broker_status", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["candidate_id"], ["collector_cycle_candidates.candidate_id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["open_execution_attempt_id"],
            ["execution_attempts.execution_attempt_id"],
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        "idx_session_positions_session_updated",
        "session_positions",
        ["session_id", "updated_at"],
        unique=False,
    )
    op.create_index(
        "idx_session_positions_session_status",
        "session_positions",
        ["session_id", "status"],
        unique=False,
    )
    op.create_index(
        "ux_session_positions_open_attempt",
        "session_positions",
        ["open_execution_attempt_id"],
        unique=True,
    )

    op.create_table(
        "session_position_closes",
        sa.Column("session_position_close_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("session_position_id", sa.Text(), nullable=False),
        sa.Column("execution_attempt_id", sa.Text(), nullable=False),
        sa.Column("closed_quantity", sa.Float(), nullable=False),
        sa.Column("exit_debit", sa.Float(), nullable=True),
        sa.Column("realized_pnl", sa.Float(), nullable=False, server_default="0"),
        sa.Column("broker_order_id", sa.Text(), nullable=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["session_position_id"],
            ["session_positions.session_position_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["execution_attempt_id"],
            ["execution_attempts.execution_attempt_id"],
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        "idx_session_position_closes_position_closed",
        "session_position_closes",
        ["session_position_id", "closed_at"],
        unique=False,
    )
    op.create_index(
        "ux_session_position_closes_execution_attempt",
        "session_position_closes",
        ["execution_attempt_id"],
        unique=True,
    )

    op.add_column(
        "execution_attempts",
        sa.Column("trade_intent", sa.Text(), nullable=False, server_default="open"),
    )
    op.add_column(
        "execution_attempts",
        sa.Column("session_position_id", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_execution_attempts_position_requested",
        "execution_attempts",
        ["session_position_id", "requested_at"],
        unique=False,
    )
    op.create_foreign_key(
        "fk_execution_attempts_session_position_id",
        "execution_attempts",
        "session_positions",
        ["session_position_id"],
        ["session_position_id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint("fk_execution_attempts_session_position_id", "execution_attempts", type_="foreignkey")
    op.drop_index("idx_execution_attempts_position_requested", table_name="execution_attempts")
    op.drop_column("execution_attempts", "session_position_id")
    op.drop_column("execution_attempts", "trade_intent")

    op.drop_index(
        "ux_session_position_closes_execution_attempt",
        table_name="session_position_closes",
    )
    op.drop_index(
        "idx_session_position_closes_position_closed",
        table_name="session_position_closes",
    )
    op.drop_table("session_position_closes")

    op.drop_index("ux_session_positions_open_attempt", table_name="session_positions")
    op.drop_index("idx_session_positions_session_status", table_name="session_positions")
    op.drop_index("idx_session_positions_session_updated", table_name="session_positions")
    op.drop_table("session_positions")
