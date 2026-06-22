"""add market context snapshots

Revision ID: 20260622_0063
Revises: 20260621_0062
Create Date: 2026-06-22 12:05:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260622_0063"
down_revision = "20260621_0062"
branch_labels = None
depends_on = None


def _jsonb() -> postgresql.JSONB:
    return postgresql.JSONB(astext_type=sa.Text())


def upgrade() -> None:
    op.create_table(
        "market_context_snapshots",
        sa.Column("market_context_snapshot_id", sa.Text(), primary_key=True),
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("context_version", sa.Integer(), nullable=False),
        sa.Column("config_hash", sa.Text(), nullable=True),
        sa.Column("regime_label", sa.Text(), nullable=False),
        sa.Column("risk_posture", sa.Text(), nullable=False),
        sa.Column("trend_strength", sa.Text(), nullable=False),
        sa.Column("volatility_state", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("data_quality_state", sa.Text(), nullable=False),
        sa.Column("freshness_state", sa.Text(), nullable=False),
        sa.Column("fidelity_json", _jsonb(), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("payload_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("regime_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("benchmark_evidence_json", _jsonb(), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("source_evidence_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_market_context_snapshots_scope_observed",
        "market_context_snapshots",
        ["scope", "observed_at"],
        unique=False,
    )
    op.create_index(
        "idx_market_context_snapshots_scope_expires",
        "market_context_snapshots",
        ["scope", "expires_at"],
        unique=False,
    )
    op.create_index(
        "idx_market_context_snapshots_regime_observed",
        "market_context_snapshots",
        ["regime_label", "observed_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_market_context_snapshots_regime_observed", table_name="market_context_snapshots")
    op.drop_index("idx_market_context_snapshots_scope_expires", table_name="market_context_snapshots")
    op.drop_index("idx_market_context_snapshots_scope_observed", table_name="market_context_snapshots")
    op.drop_table("market_context_snapshots")
