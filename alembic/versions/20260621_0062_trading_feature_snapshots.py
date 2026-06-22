"""add trading feature snapshots

Revision ID: 20260621_0062
Revises: 20260617_0061
Create Date: 2026-06-21 00:40:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260621_0062"
down_revision = "20260617_0061"
branch_labels = None
depends_on = None


def _jsonb() -> postgresql.JSONB:
    return postgresql.JSONB(astext_type=sa.Text())


def upgrade() -> None:
    op.create_table(
        "trading_feature_snapshots",
        sa.Column("trading_feature_snapshot_id", sa.Text(), primary_key=True),
        sa.Column("feature_version", sa.Text(), nullable=False),
        sa.Column("candidate_run_id", sa.Text(), sa.ForeignKey("candidate_runs.candidate_run_id", ondelete="CASCADE"), nullable=False),
        sa.Column("trade_candidate_id", sa.Text(), sa.ForeignKey("trade_candidates.trade_candidate_id", ondelete="SET NULL"), nullable=True),
        sa.Column("ticker_source_run_id", sa.Text(), sa.ForeignKey("ticker_source_runs.ticker_source_run_id", ondelete="SET NULL"), nullable=True),
        sa.Column("ticker_source_kind", sa.Text(), nullable=False),
        sa.Column("ticker_source_id", sa.Text(), nullable=False),
        sa.Column("trading_strategy_id", sa.Text(), nullable=False),
        sa.Column("trade_structure", sa.Text(), nullable=False),
        sa.Column("routine", sa.Text(), nullable=False),
        sa.Column("config_hash", sa.Text(), nullable=False),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("underlying_symbol", sa.Text(), nullable=False),
        sa.Column("candidate_identity", sa.Text(), nullable=True),
        sa.Column("feature_scope", sa.Text(), nullable=False),
        sa.Column("quality_profile_id", sa.Text(), nullable=False),
        sa.Column("quality_status", sa.Text(), nullable=False),
        sa.Column("market_data_quality_state", sa.Text(), nullable=False),
        sa.Column("market_data_quality_reason", sa.Text(), nullable=False),
        sa.Column("source_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("underlying_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("chain_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("premium_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("candidate_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("metadata_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("quality_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("market_data_quality_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ux_trading_feature_snapshots_identity",
        "trading_feature_snapshots",
        ["candidate_run_id", "underlying_symbol", "candidate_identity"],
        unique=True,
    )
    op.create_index(
        "idx_trading_feature_snapshots_strategy_observed",
        "trading_feature_snapshots",
        ["trading_strategy_id", "routine", "observed_at"],
        unique=False,
    )
    op.create_index(
        "idx_trading_feature_snapshots_strategy_session",
        "trading_feature_snapshots",
        ["trading_strategy_id", "session_date", "market_data_quality_state"],
        unique=False,
    )
    op.create_index(
        "idx_trading_feature_snapshots_run",
        "trading_feature_snapshots",
        ["candidate_run_id"],
        unique=False,
    )
    op.create_index(
        "idx_trading_feature_snapshots_symbol_observed",
        "trading_feature_snapshots",
        ["underlying_symbol", "observed_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_trading_feature_snapshots_symbol_observed", table_name="trading_feature_snapshots")
    op.drop_index("idx_trading_feature_snapshots_run", table_name="trading_feature_snapshots")
    op.drop_index("idx_trading_feature_snapshots_strategy_session", table_name="trading_feature_snapshots")
    op.drop_index("idx_trading_feature_snapshots_strategy_observed", table_name="trading_feature_snapshots")
    op.drop_index("ux_trading_feature_snapshots_identity", table_name="trading_feature_snapshots")
    op.drop_table("trading_feature_snapshots")
