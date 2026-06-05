"""rename ticker source fact storage

Revision ID: 20260605_0052
Revises: 20260604_0051
Create Date: 2026-06-05 00:45:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260605_0052"
down_revision = "20260604_0051"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            DROP INDEX IF EXISTS idx_source_runs_source_generated;
            DROP INDEX IF EXISTS idx_source_runs_status_generated;
            DROP INDEX IF EXISTS ux_source_tickers_run_symbol;
            DROP INDEX IF EXISTS idx_source_tickers_symbol_created;
            DROP INDEX IF EXISTS idx_candidate_runs_source_generated;

            ALTER TABLE source_tickers DROP CONSTRAINT IF EXISTS source_tickers_source_run_id_fkey;
            ALTER TABLE candidate_runs DROP CONSTRAINT IF EXISTS candidate_runs_source_run_id_fkey;

            ALTER TABLE source_runs RENAME TO ticker_source_runs;
            ALTER TABLE ticker_source_runs RENAME COLUMN source_run_id TO ticker_source_run_id;
            ALTER TABLE ticker_source_runs RENAME COLUMN source_type TO ticker_source_type;
            ALTER TABLE ticker_source_runs RENAME COLUMN source_ref TO ticker_source_id;
            ALTER TABLE ticker_source_runs RENAME COLUMN source_job_run_id TO job_run_id;
            ALTER TABLE ticker_source_runs RENAME COLUMN symbol_count TO selected_count;
            ALTER TABLE ticker_source_runs ADD COLUMN observed_count INTEGER NOT NULL DEFAULT 0;
            ALTER TABLE ticker_source_runs ADD COLUMN excluded_count INTEGER NOT NULL DEFAULT 0;
            UPDATE ticker_source_runs
            SET observed_count = selected_count
            WHERE observed_count = 0;

            ALTER TABLE source_tickers RENAME TO ticker_source_observations;
            ALTER TABLE ticker_source_observations RENAME COLUMN source_ticker_id TO ticker_source_observation_id;
            ALTER TABLE ticker_source_observations RENAME COLUMN source_run_id TO ticker_source_run_id;
            ALTER TABLE ticker_source_observations RENAME COLUMN source_ref TO ticker_source_id;
            ALTER TABLE ticker_source_observations ADD COLUMN observation_state TEXT NOT NULL DEFAULT 'selected';
            ALTER TABLE ticker_source_observations ADD COLUMN company TEXT;
            ALTER TABLE ticker_source_observations ADD COLUMN sector TEXT;
            ALTER TABLE ticker_source_observations ADD COLUMN industry TEXT;
            ALTER TABLE ticker_source_observations ADD COLUMN country TEXT;
            ALTER TABLE ticker_source_observations ADD COLUMN price DOUBLE PRECISION;
            ALTER TABLE ticker_source_observations ADD COLUMN market_cap BIGINT;
            ALTER TABLE ticker_source_observations ADD COLUMN daily_volume BIGINT;
            ALTER TABLE ticker_source_observations ADD COLUMN move_percent DOUBLE PRECISION;
            ALTER TABLE ticker_source_observations ADD COLUMN relative_volume DOUBLE PRECISION;

            ALTER TABLE candidate_runs RENAME COLUMN source_run_id TO ticker_source_run_id;
            ALTER TABLE candidate_runs RENAME COLUMN source_type TO ticker_source_kind;
            ALTER TABLE candidate_runs RENAME COLUMN source_ref TO ticker_source_id;
            """
        )
    )
    op.create_foreign_key(
        "fk_ticker_source_observations_run",
        "ticker_source_observations",
        "ticker_source_runs",
        ["ticker_source_run_id"],
        ["ticker_source_run_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_candidate_runs_ticker_source_run",
        "candidate_runs",
        "ticker_source_runs",
        ["ticker_source_run_id"],
        ["ticker_source_run_id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "idx_ticker_source_runs_source_generated",
        "ticker_source_runs",
        ["ticker_source_id", "generated_at"],
    )
    op.create_index(
        "idx_ticker_source_runs_status_generated",
        "ticker_source_runs",
        ["status", "generated_at"],
    )
    op.create_index(
        "ux_ticker_source_observations_run_symbol",
        "ticker_source_observations",
        ["ticker_source_run_id", "symbol"],
        unique=True,
    )
    op.create_index(
        "idx_ticker_source_observations_source_state",
        "ticker_source_observations",
        ["ticker_source_id", "observation_state", "created_at"],
    )
    op.create_index(
        "idx_ticker_source_observations_symbol_created",
        "ticker_source_observations",
        ["symbol", "created_at"],
    )
    op.create_index(
        "idx_candidate_runs_ticker_source_generated",
        "candidate_runs",
        ["ticker_source_id", "generated_at"],
    )

    op.create_table(
        "ticker_source_state",
        sa.Column("ticker_source_id", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("first_selected_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_selected_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("seen_count", sa.Integer(), nullable=False),
        sa.Column("selected_count", sa.Integer(), nullable=False),
        sa.Column("consecutive_seen_count", sa.Integer(), nullable=False),
        sa.Column("consecutive_missing_count", sa.Integer(), nullable=False),
        sa.Column("last_rank", sa.Integer(), nullable=True),
        sa.Column("best_rank", sa.Integer(), nullable=True),
        sa.Column("last_score", sa.Float(), nullable=True),
        sa.Column("best_score", sa.Float(), nullable=True),
        sa.Column("last_state", sa.Text(), nullable=False),
        sa.Column(
            "last_ticker_source_run_id",
            sa.Text(),
            sa.ForeignKey("ticker_source_runs.ticker_source_run_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "last_observation_id",
            sa.BigInteger(),
            sa.ForeignKey("ticker_source_observations.ticker_source_observation_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("last_metrics_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("ticker_source_id", "symbol"),
    )
    op.create_index(
        "idx_ticker_source_state_active_rank",
        "ticker_source_state",
        ["ticker_source_id", "active", "last_rank"],
    )
    op.create_index(
        "idx_ticker_source_state_symbol_updated",
        "ticker_source_state",
        ["symbol", "updated_at"],
    )

    op.execute(
        sa.text(
            """
            INSERT INTO ticker_source_state (
                ticker_source_id,
                symbol,
                active,
                first_seen_at,
                last_seen_at,
                first_selected_at,
                last_selected_at,
                seen_count,
                selected_count,
                consecutive_seen_count,
                consecutive_missing_count,
                last_rank,
                best_rank,
                last_score,
                best_score,
                last_state,
                last_ticker_source_run_id,
                last_observation_id,
                last_metrics_json,
                created_at,
                updated_at
            )
            SELECT DISTINCT ON (observation.ticker_source_id, observation.symbol)
                observation.ticker_source_id,
                observation.symbol,
                true,
                observation.created_at,
                observation.created_at,
                observation.created_at,
                observation.created_at,
                1,
                1,
                1,
                0,
                observation.rank,
                observation.rank,
                observation.score,
                observation.score,
                observation.observation_state,
                observation.ticker_source_run_id,
                observation.ticker_source_observation_id,
                '{}'::jsonb,
                observation.created_at,
                observation.created_at
            FROM ticker_source_observations AS observation
            ORDER BY observation.ticker_source_id, observation.symbol, observation.created_at DESC;
            """
        )
    )


def downgrade() -> None:
    pass
