"""initial history schema

Revision ID: 20260407_0001
Revises: None
Create Date: 2026-04-07 16:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260407_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "scan_runs",
        sa.Column("run_id", sa.Text(), primary_key=True),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("strategy", sa.Text(), nullable=False, server_default="call_credit"),
        sa.Column("session_label", sa.Text(), nullable=True),
        sa.Column("profile", sa.Text(), nullable=False),
        sa.Column("spot_price", sa.Float(), nullable=False),
        sa.Column("candidate_count", sa.Integer(), nullable=False),
        sa.Column("output_path", sa.Text(), nullable=True),
        sa.Column("filters_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("setup_status", sa.Text(), nullable=True),
        sa.Column("setup_score", sa.Float(), nullable=True),
        sa.Column("setup_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_index(
        "idx_scan_runs_symbol_generated_at",
        "scan_runs",
        ["symbol", "generated_at"],
        unique=False,
    )

    op.create_table(
        "scan_candidates",
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("strategy", sa.Text(), nullable=False, server_default="call_credit"),
        sa.Column("expiration_date", sa.Date(), nullable=False),
        sa.Column("short_symbol", sa.Text(), nullable=False),
        sa.Column("long_symbol", sa.Text(), nullable=False),
        sa.Column("short_strike", sa.Float(), nullable=False),
        sa.Column("long_strike", sa.Float(), nullable=False),
        sa.Column("width", sa.Float(), nullable=False, server_default="0"),
        sa.Column("midpoint_credit", sa.Float(), nullable=False, server_default="0"),
        sa.Column("natural_credit", sa.Float(), nullable=False, server_default="0"),
        sa.Column("breakeven", sa.Float(), nullable=False),
        sa.Column("max_profit", sa.Float(), nullable=False, server_default="0"),
        sa.Column("max_loss", sa.Float(), nullable=False, server_default="0"),
        sa.Column("quality_score", sa.Float(), nullable=False),
        sa.Column("return_on_risk", sa.Float(), nullable=False),
        sa.Column("short_otm_pct", sa.Float(), nullable=False),
        sa.Column("calendar_status", sa.Text(), nullable=True),
        sa.Column("setup_status", sa.Text(), nullable=True),
        sa.Column("expected_move", sa.Float(), nullable=True),
        sa.Column("short_vs_expected_move", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(["run_id"], ["scan_runs.run_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("run_id", "rank"),
    )
    op.create_index("idx_scan_candidates_run_id", "scan_candidates", ["run_id"], unique=False)

    op.create_table(
        "option_quote_events",
        sa.Column("quote_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("cycle_id", sa.Text(), nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("underlying_symbol", sa.Text(), nullable=True),
        sa.Column("strategy", sa.Text(), nullable=True),
        sa.Column("profile", sa.Text(), nullable=True),
        sa.Column("option_symbol", sa.Text(), nullable=False),
        sa.Column("leg_role", sa.Text(), nullable=False),
        sa.Column("bid", sa.Float(), nullable=False),
        sa.Column("ask", sa.Float(), nullable=False),
        sa.Column("midpoint", sa.Float(), nullable=False),
        sa.Column("bid_size", sa.Integer(), nullable=False),
        sa.Column("ask_size", sa.Integer(), nullable=False),
        sa.Column("quote_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source", sa.Text(), nullable=False, server_default="alpaca_websocket"),
    )
    op.create_index("idx_option_quote_events_cycle_id", "option_quote_events", ["cycle_id"], unique=False)
    op.create_index(
        "idx_option_quote_events_symbol_captured_at",
        "option_quote_events",
        ["option_symbol", "captured_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_option_quote_events_symbol_captured_at", table_name="option_quote_events")
    op.drop_index("idx_option_quote_events_cycle_id", table_name="option_quote_events")
    op.drop_table("option_quote_events")
    op.drop_index("idx_scan_candidates_run_id", table_name="scan_candidates")
    op.drop_table("scan_candidates")
    op.drop_index("idx_scan_runs_symbol_generated_at", table_name="scan_runs")
    op.drop_table("scan_runs")
