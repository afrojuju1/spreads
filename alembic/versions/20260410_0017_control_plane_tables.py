"""add control plane tables

Revision ID: 20260410_0017
Revises: 20260410_0016
Create Date: 2026-04-10 15:20:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260410_0017"
down_revision = "20260410_0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "operator_actions",
        sa.Column("operator_action_id", sa.Text(), primary_key=True),
        sa.Column("action_kind", sa.Text(), nullable=False),
        sa.Column("source_kind", sa.Text(), nullable=False),
        sa.Column("actor_id", sa.Text(), nullable=True),
        sa.Column("target_scope", sa.Text(), nullable=False),
        sa.Column("requested_payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("resulting_state_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("correlation_id", sa.Text(), nullable=True),
        sa.Column("causation_id", sa.Text(), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_operator_actions_kind_occurred",
        "operator_actions",
        ["action_kind", "occurred_at"],
        unique=False,
    )
    op.create_index(
        "idx_operator_actions_source_occurred",
        "operator_actions",
        ["source_kind", "occurred_at"],
        unique=False,
    )

    op.create_table(
        "policy_rollouts",
        sa.Column("policy_rollout_id", sa.Text(), primary_key=True),
        sa.Column("family", sa.Text(), nullable=False),
        sa.Column("scope_kind", sa.Text(), nullable=False),
        sa.Column("scope_key", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("version_token", sa.Text(), nullable=False),
        sa.Column("policy_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("source_kind", sa.Text(), nullable=False),
        sa.Column(
            "operator_action_id",
            sa.Text(),
            sa.ForeignKey("operator_actions.operator_action_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("effective_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    )
    op.create_index(
        "idx_policy_rollouts_family_status_effective",
        "policy_rollouts",
        ["family", "status", "effective_at"],
        unique=False,
    )

    op.create_table(
        "control_state",
        sa.Column("control_state_id", sa.Text(), primary_key=True),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("reason_code", sa.Text(), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("source_kind", sa.Text(), nullable=False),
        sa.Column(
            "triggered_by_action_id",
            sa.Text(),
            sa.ForeignKey("operator_actions.operator_action_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("effective_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    )
    op.create_index(
        "idx_control_state_mode_updated",
        "control_state",
        ["mode", "updated_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_control_state_mode_updated", table_name="control_state")
    op.drop_table("control_state")

    op.drop_index("idx_policy_rollouts_family_status_effective", table_name="policy_rollouts")
    op.drop_table("policy_rollouts")

    op.drop_index("idx_operator_actions_source_occurred", table_name="operator_actions")
    op.drop_index("idx_operator_actions_kind_occurred", table_name="operator_actions")
    op.drop_table("operator_actions")
