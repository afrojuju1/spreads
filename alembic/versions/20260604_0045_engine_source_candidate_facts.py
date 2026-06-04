"""add engine source and candidate facts

Revision ID: 20260604_0045
Revises: 20260603_0044
Create Date: 2026-06-04 02:15:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260604_0045"
down_revision = "20260603_0044"
branch_labels = None
depends_on = None


def _jsonb() -> postgresql.JSONB:
    return postgresql.JSONB(astext_type=sa.Text())


def upgrade() -> None:
    op.create_table(
        "source_runs",
        sa.Column("source_run_id", sa.Text(), primary_key=True),
        sa.Column("source_type", sa.Text(), nullable=False),
        sa.Column("source_ref", sa.Text(), nullable=False),
        sa.Column("source_job_run_id", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("config_hash", sa.Text(), nullable=True),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("symbol_count", sa.Integer(), nullable=False),
        sa.Column("summary_json", _jsonb(), nullable=False),
        sa.Column("evidence_json", _jsonb(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_source_runs_source_generated",
        "source_runs",
        ["source_ref", "generated_at"],
    )
    op.create_index(
        "idx_source_runs_status_generated",
        "source_runs",
        ["status", "generated_at"],
    )

    op.create_table(
        "source_tickers",
        sa.Column("source_ticker_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "source_run_id",
            sa.Text(),
            sa.ForeignKey("source_runs.source_run_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_ref", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=True),
        sa.Column("score", sa.Float(), nullable=True),
        sa.Column("reason_codes_json", _jsonb(), nullable=False),
        sa.Column("evidence_json", _jsonb(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ux_source_tickers_run_symbol",
        "source_tickers",
        ["source_run_id", "symbol"],
        unique=True,
    )
    op.create_index(
        "idx_source_tickers_symbol_created",
        "source_tickers",
        ["symbol", "created_at"],
    )

    op.create_table(
        "candidate_runs",
        sa.Column("candidate_run_id", sa.Text(), primary_key=True),
        sa.Column("run_key", sa.Text(), nullable=False),
        sa.Column("trading_strategy_id", sa.Text(), nullable=False),
        sa.Column("trade_structure", sa.Text(), nullable=False),
        sa.Column("routine", sa.Text(), nullable=False),
        sa.Column(
            "source_run_id",
            sa.Text(),
            sa.ForeignKey("source_runs.source_run_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("source_type", sa.Text(), nullable=False),
        sa.Column("source_ref", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("config_hash", sa.Text(), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("symbol_count", sa.Integer(), nullable=False),
        sa.Column("candidate_count", sa.Integer(), nullable=False),
        sa.Column("summary_json", _jsonb(), nullable=False),
        sa.Column("evidence_json", _jsonb(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_candidate_runs_strategy_generated",
        "candidate_runs",
        ["trading_strategy_id", "routine", "generated_at"],
    )
    op.create_index(
        "idx_candidate_runs_source_generated",
        "candidate_runs",
        ["source_ref", "generated_at"],
    )

    op.create_table(
        "trade_candidates",
        sa.Column("trade_candidate_id", sa.Text(), primary_key=True),
        sa.Column(
            "candidate_run_id",
            sa.Text(),
            sa.ForeignKey("candidate_runs.candidate_run_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("trading_strategy_id", sa.Text(), nullable=False),
        sa.Column("trade_structure", sa.Text(), nullable=False),
        sa.Column("routine", sa.Text(), nullable=False),
        sa.Column("config_hash", sa.Text(), nullable=False),
        sa.Column("underlying_symbol", sa.Text(), nullable=False),
        sa.Column("root_symbol", sa.Text(), nullable=True),
        sa.Column("candidate_identity", sa.Text(), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=True),
        sa.Column("score", sa.Float(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("expiration_date", sa.Date(), nullable=True),
        sa.Column("selection_state", sa.Text(), nullable=True),
        sa.Column("candidate_state", sa.Text(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("legs_json", _jsonb(), nullable=False),
        sa.Column("execution_shape_json", _jsonb(), nullable=False),
        sa.Column("economics_json", _jsonb(), nullable=False),
        sa.Column("risk_hints_json", _jsonb(), nullable=False),
        sa.Column("reason_codes_json", _jsonb(), nullable=False),
        sa.Column("blockers_json", _jsonb(), nullable=False),
        sa.Column("candidate_json", _jsonb(), nullable=False),
        sa.Column("evidence_json", _jsonb(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ux_trade_candidates_run_identity",
        "trade_candidates",
        ["candidate_run_id", "underlying_symbol", "candidate_identity"],
        unique=True,
    )
    op.create_index(
        "idx_trade_candidates_strategy_state",
        "trade_candidates",
        ["trading_strategy_id", "routine", "candidate_state"],
    )
    op.create_index(
        "idx_trade_candidates_underlying_updated",
        "trade_candidates",
        ["underlying_symbol", "updated_at"],
    )

    op.add_column(
        "trade_signals",
        sa.Column(
            "trade_candidate_id",
            sa.Text(),
            sa.ForeignKey("trade_candidates.trade_candidate_id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.add_column("trade_signals", sa.Column("trading_strategy_id", sa.Text(), nullable=True))
    op.add_column("trade_signals", sa.Column("routine", sa.Text(), nullable=True))
    op.add_column("trade_signals", sa.Column("config_hash", sa.Text(), nullable=True))
    op.create_index(
        "idx_trade_signals_strategy_session",
        "trade_signals",
        ["trading_strategy_id", "routine", "session_date", "signal_state"],
    )
    op.create_index(
        "idx_trade_signals_candidate",
        "trade_signals",
        ["trade_candidate_id"],
    )


def downgrade() -> None:
    op.drop_index("idx_trade_signals_candidate", table_name="trade_signals")
    op.drop_index("idx_trade_signals_strategy_session", table_name="trade_signals")
    op.drop_column("trade_signals", "config_hash")
    op.drop_column("trade_signals", "routine")
    op.drop_column("trade_signals", "trading_strategy_id")
    op.drop_column("trade_signals", "trade_candidate_id")
    op.drop_table("trade_candidates")
    op.drop_table("candidate_runs")
    op.drop_table("source_tickers")
    op.drop_table("source_runs")
