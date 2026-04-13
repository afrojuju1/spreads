"""add truthful live recovery tables

Revision ID: 20260413_0022
Revises: 20260412_0021
Create Date: 2026-04-13 12:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260413_0022"
down_revision = "20260412_0021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "live_session_slots",
        sa.Column("session_slot_id", sa.Text(), primary_key=True),
        sa.Column("job_key", sa.Text(), nullable=False),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("slot_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column(
            "job_run_id",
            sa.Text(),
            sa.ForeignKey("job_runs.job_run_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("capture_status", sa.Text(), nullable=True),
        sa.Column("recovery_note", sa.Text(), nullable=True),
        sa.Column("slot_details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("queued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_live_session_slots_session_slot",
        "live_session_slots",
        ["session_id", "slot_at"],
        unique=True,
    )
    op.create_index(
        "idx_live_session_slots_status_updated",
        "live_session_slots",
        ["status", "updated_at"],
        unique=False,
    )
    op.create_index(
        "idx_live_session_slots_job_key_session_date",
        "live_session_slots",
        ["job_key", "session_date"],
        unique=False,
    )

    op.create_table(
        "market_recorder_targets",
        sa.Column("capture_target_id", sa.Text(), primary_key=True),
        sa.Column("owner_kind", sa.Text(), nullable=False),
        sa.Column("owner_key", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("session_id", sa.Text(), nullable=True),
        sa.Column("session_date", sa.Date(), nullable=True),
        sa.Column("label", sa.Text(), nullable=True),
        sa.Column("profile", sa.Text(), nullable=True),
        sa.Column("underlying_symbol", sa.Text(), nullable=True),
        sa.Column("strategy", sa.Text(), nullable=True),
        sa.Column("leg_role", sa.Text(), nullable=True),
        sa.Column("option_symbol", sa.Text(), nullable=False),
        sa.Column("quote_enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("trade_enabled", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("feed", sa.Text(), nullable=False, server_default="opra"),
        sa.Column("data_base_url", sa.Text(), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ux_market_recorder_targets_owner_reason_symbol",
        "market_recorder_targets",
        ["owner_kind", "owner_key", "reason", "option_symbol"],
        unique=True,
    )
    op.create_index(
        "idx_market_recorder_targets_expires_at",
        "market_recorder_targets",
        ["expires_at"],
        unique=False,
    )
    op.create_index(
        "idx_market_recorder_targets_session_reason",
        "market_recorder_targets",
        ["session_id", "reason"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_market_recorder_targets_session_reason", table_name="market_recorder_targets")
    op.drop_index("idx_market_recorder_targets_expires_at", table_name="market_recorder_targets")
    op.drop_index("ux_market_recorder_targets_owner_reason_symbol", table_name="market_recorder_targets")
    op.drop_table("market_recorder_targets")

    op.drop_index("idx_live_session_slots_job_key_session_date", table_name="live_session_slots")
    op.drop_index("idx_live_session_slots_status_updated", table_name="live_session_slots")
    op.drop_index("idx_live_session_slots_session_slot", table_name="live_session_slots")
    op.drop_table("live_session_slots")
