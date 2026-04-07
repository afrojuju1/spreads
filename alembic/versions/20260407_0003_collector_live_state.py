"""add collector live state tables

Revision ID: 20260407_0003
Revises: 20260407_0002
Create Date: 2026-04-07 22:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260407_0003"
down_revision = "20260407_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "collector_cycles",
        sa.Column("cycle_id", sa.Text(), primary_key=True),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("universe_label", sa.Text(), nullable=False),
        sa.Column("strategy", sa.Text(), nullable=False),
        sa.Column("profile", sa.Text(), nullable=False),
        sa.Column("greeks_source", sa.Text(), nullable=False),
        sa.Column("symbols_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("failures_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("selection_state_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    )
    op.create_index(
        "idx_collector_cycles_label_session_generated_at",
        "collector_cycles",
        ["label", "session_date", "generated_at"],
        unique=False,
    )

    op.create_table(
        "collector_cycle_candidates",
        sa.Column("candidate_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("cycle_id", sa.Text(), nullable=False),
        sa.Column("bucket", sa.Text(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("underlying_symbol", sa.Text(), nullable=False),
        sa.Column("strategy", sa.Text(), nullable=False),
        sa.Column("expiration_date", sa.Date(), nullable=False),
        sa.Column("short_symbol", sa.Text(), nullable=False),
        sa.Column("long_symbol", sa.Text(), nullable=False),
        sa.Column("quality_score", sa.Float(), nullable=False),
        sa.Column("midpoint_credit", sa.Float(), nullable=False),
        sa.Column("candidate_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(["cycle_id"], ["collector_cycles.cycle_id"], ondelete="CASCADE"),
    )
    op.create_index(
        "idx_collector_cycle_candidates_cycle_bucket_position",
        "collector_cycle_candidates",
        ["cycle_id", "bucket", "position"],
        unique=False,
    )
    op.create_index(
        "idx_collector_cycle_candidates_run_id",
        "collector_cycle_candidates",
        ["run_id"],
        unique=False,
    )
    op.create_index(
        "idx_collector_cycle_candidates_identity",
        "collector_cycle_candidates",
        ["underlying_symbol", "strategy", "expiration_date", "short_symbol", "long_symbol"],
        unique=False,
    )

    op.create_table(
        "collector_cycle_events",
        sa.Column("event_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("cycle_id", sa.Text(), nullable=False),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("previous_candidate_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("current_candidate_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(["cycle_id"], ["collector_cycles.cycle_id"], ondelete="CASCADE"),
    )
    op.create_index(
        "idx_collector_cycle_events_label_session_generated_at",
        "collector_cycle_events",
        ["label", "session_date", "generated_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_collector_cycle_events_label_session_generated_at", table_name="collector_cycle_events")
    op.drop_table("collector_cycle_events")
    op.drop_index("idx_collector_cycle_candidates_identity", table_name="collector_cycle_candidates")
    op.drop_index("idx_collector_cycle_candidates_run_id", table_name="collector_cycle_candidates")
    op.drop_index("idx_collector_cycle_candidates_cycle_bucket_position", table_name="collector_cycle_candidates")
    op.drop_table("collector_cycle_candidates")
    op.drop_index("idx_collector_cycles_label_session_generated_at", table_name="collector_cycles")
    op.drop_table("collector_cycles")
