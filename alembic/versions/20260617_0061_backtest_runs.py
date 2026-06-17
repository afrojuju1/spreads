"""add backtest run persistence

Revision ID: 20260617_0061
Revises: 20260615_0060
Create Date: 2026-06-17 02:45:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260617_0061"
down_revision = "20260615_0060"
branch_labels = None
depends_on = None


def _jsonb() -> postgresql.JSONB:
    return postgresql.JSONB(astext_type=sa.Text())


def upgrade() -> None:
    op.create_table(
        "backtest_runs",
        sa.Column("backtest_run_id", sa.Text(), primary_key=True),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("requested_by", sa.Text(), nullable=True),
        sa.Column("strategy_ids_json", _jsonb(), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("config_snapshot_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("request_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("summary_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("fidelity_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("artifact_root", sa.Text(), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "idx_backtest_runs_mode_state_created",
        "backtest_runs",
        ["mode", "state", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_backtest_runs_window",
        "backtest_runs",
        ["start_date", "end_date"],
        unique=False,
    )

    op.create_table(
        "backtest_artifacts",
        sa.Column("backtest_artifact_id", sa.Text(), primary_key=True),
        sa.Column("backtest_run_id", sa.Text(), sa.ForeignKey("backtest_runs.backtest_run_id", ondelete="CASCADE"), nullable=False),
        sa.Column("artifact_kind", sa.Text(), nullable=False),
        sa.Column("storage_kind", sa.Text(), nullable=False),
        sa.Column("uri", sa.Text(), nullable=False),
        sa.Column("content_type", sa.Text(), nullable=True),
        sa.Column("row_count", sa.BigInteger(), nullable=True),
        sa.Column("byte_count", sa.BigInteger(), nullable=True),
        sa.Column("schema_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("metadata_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_backtest_artifacts_run_kind",
        "backtest_artifacts",
        ["backtest_run_id", "artifact_kind"],
        unique=False,
    )
    op.create_index(
        "idx_backtest_artifacts_created",
        "backtest_artifacts",
        ["created_at"],
        unique=False,
    )

    op.create_table(
        "backtest_variant_results",
        sa.Column("backtest_variant_id", sa.Text(), primary_key=True),
        sa.Column("backtest_run_id", sa.Text(), sa.ForeignKey("backtest_runs.backtest_run_id", ondelete="CASCADE"), nullable=False),
        sa.Column("trading_strategy_id", sa.Text(), nullable=False),
        sa.Column("config_hash", sa.Text(), nullable=False),
        sa.Column("variant_hash", sa.Text(), nullable=False),
        sa.Column("parameters_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("summary_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("metrics_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("fidelity_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("rank", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_backtest_variant_results_run_rank",
        "backtest_variant_results",
        ["backtest_run_id", "rank"],
        unique=False,
    )
    op.create_index(
        "idx_backtest_variant_results_strategy",
        "backtest_variant_results",
        ["trading_strategy_id", "variant_hash"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_backtest_variant_results_strategy", table_name="backtest_variant_results")
    op.drop_index("idx_backtest_variant_results_run_rank", table_name="backtest_variant_results")
    op.drop_table("backtest_variant_results")
    op.drop_index("idx_backtest_artifacts_created", table_name="backtest_artifacts")
    op.drop_index("idx_backtest_artifacts_run_kind", table_name="backtest_artifacts")
    op.drop_table("backtest_artifacts")
    op.drop_index("idx_backtest_runs_window", table_name="backtest_runs")
    op.drop_index("idx_backtest_runs_mode_state_created", table_name="backtest_runs")
    op.drop_table("backtest_runs")
