"""rename capture targets and add recorder summaries

Revision ID: 20260604_0048
Revises: 20260604_0047
Create Date: 2026-06-04 02:34:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260604_0048"
down_revision = "20260604_0047"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.rename_table("market_recorder_targets", "capture_targets")
    op.drop_index("ux_market_recorder_targets_owner_reason_symbol", table_name="capture_targets")
    op.drop_index("idx_market_recorder_targets_expires_at", table_name="capture_targets")
    op.drop_index("idx_market_recorder_targets_session_reason", table_name="capture_targets")
    op.add_column(
        "capture_targets",
        sa.Column("priority", sa.Integer(), nullable=False, server_default="100"),
    )
    op.create_index(
        "ux_capture_targets_owner_reason_symbol",
        "capture_targets",
        ["owner_kind", "owner_key", "reason", "option_symbol"],
        unique=True,
    )
    op.create_index(
        "idx_capture_targets_active_priority",
        "capture_targets",
        ["expires_at", "priority", "updated_at"],
        unique=False,
    )
    op.create_index(
        "idx_capture_targets_session_reason",
        "capture_targets",
        ["session_id", "reason"],
        unique=False,
    )

    op.create_table(
        "capture_summaries",
        sa.Column("capture_summary_id", sa.Text(), primary_key=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("active_target_count", sa.Integer(), nullable=False),
        sa.Column("selected_target_count", sa.Integer(), nullable=False),
        sa.Column("capture_group_count", sa.Integer(), nullable=False),
        sa.Column("quote_rows_saved", sa.Integer(), nullable=False),
        sa.Column("trade_rows_saved", sa.Integer(), nullable=False),
        sa.Column("target_limit", sa.Integer(), nullable=True),
        sa.Column("target_counts_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("group_summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("error_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_capture_summaries_source_captured",
        "capture_summaries",
        ["source", "captured_at"],
        unique=False,
    )
    op.create_index(
        "idx_capture_summaries_status_captured",
        "capture_summaries",
        ["status", "captured_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_capture_summaries_status_captured", table_name="capture_summaries")
    op.drop_index("idx_capture_summaries_source_captured", table_name="capture_summaries")
    op.drop_table("capture_summaries")

    op.drop_index("idx_capture_targets_session_reason", table_name="capture_targets")
    op.drop_index("idx_capture_targets_active_priority", table_name="capture_targets")
    op.drop_index("ux_capture_targets_owner_reason_symbol", table_name="capture_targets")
    op.drop_column("capture_targets", "priority")
    op.rename_table("capture_targets", "market_recorder_targets")
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
