"""add broker sync tables and session position policy fields

Revision ID: 20260409_0011
Revises: 20260408_0010
Create Date: 2026-04-09 09:10:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260409_0011"
down_revision = "20260408_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "session_positions",
        sa.Column(
            "exit_policy_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.add_column(
        "session_positions",
        sa.Column(
            "risk_policy_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.add_column("session_positions", sa.Column("source_job_type", sa.Text(), nullable=True))
    op.add_column("session_positions", sa.Column("source_job_key", sa.Text(), nullable=True))
    op.add_column("session_positions", sa.Column("source_job_run_id", sa.Text(), nullable=True))
    op.add_column("session_positions", sa.Column("last_exit_evaluated_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("session_positions", sa.Column("last_exit_reason", sa.Text(), nullable=True))
    op.add_column("session_positions", sa.Column("last_reconciled_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("session_positions", sa.Column("reconciliation_status", sa.Text(), nullable=True))
    op.add_column("session_positions", sa.Column("reconciliation_note", sa.Text(), nullable=True))

    op.create_table(
        "account_snapshots",
        sa.Column("snapshot_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("broker", sa.Text(), nullable=False, server_default="alpaca"),
        sa.Column("environment", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False, server_default="broker_sync"),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("account_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("pnl_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("positions_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("history_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    )
    op.create_index(
        "idx_account_snapshots_broker_captured",
        "account_snapshots",
        ["broker", "captured_at"],
        unique=False,
    )

    op.create_table(
        "broker_sync_state",
        sa.Column("sync_key", sa.Text(), primary_key=True),
        sa.Column("broker", sa.Text(), nullable=False, server_default="alpaca"),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("cursor_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("error_text", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_broker_sync_state_broker_updated",
        "broker_sync_state",
        ["broker", "updated_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_broker_sync_state_broker_updated", table_name="broker_sync_state")
    op.drop_table("broker_sync_state")

    op.drop_index("idx_account_snapshots_broker_captured", table_name="account_snapshots")
    op.drop_table("account_snapshots")

    op.drop_column("session_positions", "reconciliation_note")
    op.drop_column("session_positions", "reconciliation_status")
    op.drop_column("session_positions", "last_reconciled_at")
    op.drop_column("session_positions", "last_exit_reason")
    op.drop_column("session_positions", "last_exit_evaluated_at")
    op.drop_column("session_positions", "source_job_run_id")
    op.drop_column("session_positions", "source_job_key")
    op.drop_column("session_positions", "source_job_type")
    op.drop_column("session_positions", "risk_policy_json")
    op.drop_column("session_positions", "exit_policy_json")
