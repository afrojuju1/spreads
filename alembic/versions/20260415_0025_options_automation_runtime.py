"""add options automation decisions and intents

Revision ID: 20260415_0025
Revises: 20260414_0024
Create Date: 2026-04-15 17:45:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260415_0025"
down_revision = "20260414_0024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "opportunity_decisions",
        sa.Column("opportunity_decision_id", sa.Text(), primary_key=True),
        sa.Column(
            "opportunity_id",
            sa.Text(),
            sa.ForeignKey("opportunities.opportunity_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("bot_id", sa.Text(), nullable=False),
        sa.Column("automation_id", sa.Text(), nullable=False),
        sa.Column("run_key", sa.Text(), nullable=False),
        sa.Column("scope_key", sa.Text(), nullable=False),
        sa.Column(
            "policy_ref_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("config_hash", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("score", sa.Float(), nullable=True),
        sa.Column("rank", sa.Integer(), nullable=True),
        sa.Column(
            "reason_codes_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("superseded_by_id", sa.Text(), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
    )
    op.create_index(
        "ux_opportunity_decisions_run_opportunity",
        "opportunity_decisions",
        ["run_key", "opportunity_id"],
        unique=True,
    )
    op.create_index(
        "idx_opportunity_decisions_bot_automation_decided",
        "opportunity_decisions",
        ["bot_id", "automation_id", "decided_at"],
        unique=False,
    )
    op.create_index(
        "idx_opportunity_decisions_opportunity_decided",
        "opportunity_decisions",
        ["opportunity_id", "decided_at"],
        unique=False,
    )
    op.create_index(
        "idx_opportunity_decisions_state_decided",
        "opportunity_decisions",
        ["state", "decided_at"],
        unique=False,
    )

    op.create_table(
        "execution_intents",
        sa.Column("execution_intent_id", sa.Text(), primary_key=True),
        sa.Column("bot_id", sa.Text(), nullable=False),
        sa.Column("automation_id", sa.Text(), nullable=False),
        sa.Column(
            "opportunity_decision_id",
            sa.Text(),
            sa.ForeignKey(
                "opportunity_decisions.opportunity_decision_id", ondelete="SET NULL"
            ),
            nullable=True,
        ),
        sa.Column(
            "strategy_position_id",
            sa.Text(),
            sa.ForeignKey("portfolio_positions.position_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "execution_attempt_id",
            sa.Text(),
            sa.ForeignKey(
                "execution_attempts.execution_attempt_id", ondelete="SET NULL"
            ),
            nullable=True,
        ),
        sa.Column("action_type", sa.Text(), nullable=False),
        sa.Column("slot_key", sa.Text(), nullable=False),
        sa.Column("claim_token", sa.Text(), nullable=True),
        sa.Column(
            "policy_ref_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("config_hash", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("superseded_by_id", sa.Text(), nullable=True),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_execution_intents_bot_created",
        "execution_intents",
        ["bot_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_execution_intents_slot_state",
        "execution_intents",
        ["slot_key", "state"],
        unique=False,
    )
    op.create_index(
        "idx_execution_intents_opportunity_decision",
        "execution_intents",
        ["opportunity_decision_id"],
        unique=False,
    )
    op.create_index(
        "idx_execution_intents_strategy_position",
        "execution_intents",
        ["strategy_position_id"],
        unique=False,
    )
    op.create_index(
        "idx_execution_intents_execution_attempt",
        "execution_intents",
        ["execution_attempt_id"],
        unique=False,
    )

    op.create_table(
        "execution_intent_events",
        sa.Column(
            "execution_intent_event_id",
            sa.BigInteger(),
            primary_key=True,
            autoincrement=True,
        ),
        sa.Column(
            "execution_intent_id",
            sa.Text(),
            sa.ForeignKey("execution_intents.execution_intent_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("event_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
    )
    op.create_index(
        "idx_execution_intent_events_intent_event_at",
        "execution_intent_events",
        ["execution_intent_id", "event_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "idx_execution_intent_events_intent_event_at",
        table_name="execution_intent_events",
    )
    op.drop_table("execution_intent_events")

    op.drop_index(
        "idx_execution_intents_execution_attempt", table_name="execution_intents"
    )
    op.drop_index(
        "idx_execution_intents_strategy_position", table_name="execution_intents"
    )
    op.drop_index(
        "idx_execution_intents_opportunity_decision",
        table_name="execution_intents",
    )
    op.drop_index("idx_execution_intents_slot_state", table_name="execution_intents")
    op.drop_index("idx_execution_intents_bot_created", table_name="execution_intents")
    op.drop_table("execution_intents")

    op.drop_index(
        "idx_opportunity_decisions_state_decided",
        table_name="opportunity_decisions",
    )
    op.drop_index(
        "idx_opportunity_decisions_opportunity_decided",
        table_name="opportunity_decisions",
    )
    op.drop_index(
        "idx_opportunity_decisions_bot_automation_decided",
        table_name="opportunity_decisions",
    )
    op.drop_index(
        "ux_opportunity_decisions_run_opportunity",
        table_name="opportunity_decisions",
    )
    op.drop_table("opportunity_decisions")
