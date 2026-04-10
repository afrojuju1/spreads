"""add signal state and opportunity tables

Revision ID: 20260410_0015
Revises: 20260410_0014
Create Date: 2026-04-10 02:15:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260410_0015"
down_revision = "20260410_0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "signal_states",
        sa.Column("signal_state_id", sa.Text(), primary_key=True),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("strategy_family", sa.Text(), nullable=False),
        sa.Column("profile", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("entity_key", sa.Text(), nullable=False),
        sa.Column("underlying_symbol", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("blockers_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("evidence_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("active_cycle_id", sa.Text(), nullable=True),
        sa.Column("active_candidate_id", sa.BigInteger(), nullable=True),
        sa.Column("active_bucket", sa.Text(), nullable=True),
        sa.Column("opportunity_id", sa.Text(), nullable=True),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("market_session", sa.Text(), nullable=False),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_signal_states_label_state_updated",
        "signal_states",
        ["label", "state", "updated_at"],
        unique=False,
    )
    op.create_index(
        "idx_signal_states_entity_updated",
        "signal_states",
        ["entity_type", "entity_key", "updated_at"],
        unique=False,
    )
    op.create_index(
        "idx_signal_states_session_updated",
        "signal_states",
        ["session_date", "updated_at"],
        unique=False,
    )

    op.create_table(
        "signal_state_transitions",
        sa.Column("transition_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "signal_state_id",
            sa.Text(),
            sa.ForeignKey("signal_states.signal_state_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("strategy_family", sa.Text(), nullable=False),
        sa.Column("profile", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("entity_key", sa.Text(), nullable=False),
        sa.Column("underlying_symbol", sa.Text(), nullable=False),
        sa.Column("from_state", sa.Text(), nullable=True),
        sa.Column("to_state", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("blockers_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("evidence_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("active_cycle_id", sa.Text(), nullable=True),
        sa.Column("active_candidate_id", sa.BigInteger(), nullable=True),
        sa.Column("active_bucket", sa.Text(), nullable=True),
        sa.Column("opportunity_id", sa.Text(), nullable=True),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("market_session", sa.Text(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_signal_state_transitions_state_occurred",
        "signal_state_transitions",
        ["signal_state_id", "occurred_at"],
        unique=False,
    )
    op.create_index(
        "idx_signal_state_transitions_label_session_occurred",
        "signal_state_transitions",
        ["label", "session_date", "occurred_at"],
        unique=False,
    )
    op.create_index(
        "idx_signal_state_transitions_entity_occurred",
        "signal_state_transitions",
        ["entity_type", "entity_key", "occurred_at"],
        unique=False,
    )

    op.create_table(
        "opportunities",
        sa.Column("opportunity_id", sa.Text(), primary_key=True),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("strategy_family", sa.Text(), nullable=False),
        sa.Column("profile", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("entity_key", sa.Text(), nullable=False),
        sa.Column("underlying_symbol", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=True),
        sa.Column("classification", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("signal_state_ref", sa.Text(), nullable=True),
        sa.Column("lifecycle_state", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("blockers_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("execution_shape_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("risk_hints_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source_cycle_id", sa.Text(), nullable=True),
        sa.Column("source_candidate_id", sa.BigInteger(), nullable=True),
        sa.Column("source_bucket", sa.Text(), nullable=True),
        sa.Column("candidate_identity", sa.Text(), nullable=True),
        sa.Column("candidate_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("consumed_by_execution_attempt_id", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_opportunities_label_session_lifecycle",
        "opportunities",
        ["label", "session_date", "lifecycle_state"],
        unique=False,
    )
    op.create_index(
        "idx_opportunities_entity_updated",
        "opportunities",
        ["entity_type", "entity_key", "updated_at"],
        unique=False,
    )
    op.create_index(
        "idx_opportunities_source_candidate",
        "opportunities",
        ["source_candidate_id"],
        unique=False,
    )
    op.create_index(
        "idx_opportunities_updated_at",
        "opportunities",
        ["updated_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_opportunities_updated_at", table_name="opportunities")
    op.drop_index("idx_opportunities_source_candidate", table_name="opportunities")
    op.drop_index("idx_opportunities_entity_updated", table_name="opportunities")
    op.drop_index("idx_opportunities_label_session_lifecycle", table_name="opportunities")
    op.drop_table("opportunities")

    op.drop_index("idx_signal_state_transitions_entity_occurred", table_name="signal_state_transitions")
    op.drop_index("idx_signal_state_transitions_label_session_occurred", table_name="signal_state_transitions")
    op.drop_index("idx_signal_state_transitions_state_occurred", table_name="signal_state_transitions")
    op.drop_table("signal_state_transitions")

    op.drop_index("idx_signal_states_session_updated", table_name="signal_states")
    op.drop_index("idx_signal_states_entity_updated", table_name="signal_states")
    op.drop_index("idx_signal_states_label_state_updated", table_name="signal_states")
    op.drop_table("signal_states")
