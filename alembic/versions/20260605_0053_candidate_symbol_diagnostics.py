"""add candidate symbol diagnostics

Revision ID: 20260605_0053
Revises: 20260605_0052
Create Date: 2026-06-05 11:05:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260605_0053"
down_revision = "20260605_0052"
branch_labels = None
depends_on = None


def _jsonb() -> postgresql.JSONB:
    return postgresql.JSONB(astext_type=sa.Text())


def upgrade() -> None:
    op.create_table(
        "candidate_symbol_diagnostics",
        sa.Column(
            "candidate_run_id",
            sa.Text(),
            sa.ForeignKey("candidate_runs.candidate_run_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("underlying_symbol", sa.Text(), nullable=False),
        sa.Column("trading_strategy_id", sa.Text(), nullable=False),
        sa.Column("trade_structure", sa.Text(), nullable=False),
        sa.Column("routine", sa.Text(), nullable=False),
        sa.Column(
            "ticker_source_run_id",
            sa.Text(),
            sa.ForeignKey("ticker_source_runs.ticker_source_run_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("ticker_source_kind", sa.Text(), nullable=False),
        sa.Column("ticker_source_id", sa.Text(), nullable=False),
        sa.Column("diagnostic_status", sa.Text(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("spot_price", sa.Float(), nullable=True),
        sa.Column("expiration_count", sa.Integer(), nullable=False),
        sa.Column("contract_count", sa.Integer(), nullable=False),
        sa.Column("snapshot_count", sa.Integer(), nullable=False),
        sa.Column("raw_candidate_count", sa.Integer(), nullable=False),
        sa.Column("postprocess_candidate_count", sa.Integer(), nullable=False),
        sa.Column("runtime_candidate_count", sa.Integer(), nullable=False),
        sa.Column("returned_candidate_count", sa.Integer(), nullable=False),
        sa.Column("setup_json", _jsonb(), nullable=False),
        sa.Column("market_data_json", _jsonb(), nullable=False),
        sa.Column("rejection_counts_json", _jsonb(), nullable=False),
        sa.Column("ranking_gate_json", _jsonb(), nullable=False),
        sa.Column("examples_json", _jsonb(), nullable=False),
        sa.Column("evidence_json", _jsonb(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("candidate_run_id", "underlying_symbol"),
    )
    op.create_index(
        "idx_candidate_symbol_diagnostics_strategy_status",
        "candidate_symbol_diagnostics",
        ["trading_strategy_id", "routine", "diagnostic_status", "observed_at"],
    )
    op.create_index(
        "idx_candidate_symbol_diagnostics_symbol_observed",
        "candidate_symbol_diagnostics",
        ["underlying_symbol", "observed_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "idx_candidate_symbol_diagnostics_symbol_observed",
        table_name="candidate_symbol_diagnostics",
    )
    op.drop_index(
        "idx_candidate_symbol_diagnostics_strategy_status",
        table_name="candidate_symbol_diagnostics",
    )
    op.drop_table("candidate_symbol_diagnostics")
