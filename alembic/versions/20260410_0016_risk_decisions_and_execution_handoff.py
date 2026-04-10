"""add risk decisions and explicit execution handoff refs

Revision ID: 20260410_0016
Revises: 20260410_0015
Create Date: 2026-04-10 03:05:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260410_0016"
down_revision = "20260410_0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "risk_decisions",
        sa.Column("risk_decision_id", sa.Text(), primary_key=True),
        sa.Column("decision_kind", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("note", sa.Text(), nullable=False),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("cycle_id", sa.Text(), sa.ForeignKey("collector_cycles.cycle_id", ondelete="SET NULL"), nullable=True),
        sa.Column(
            "candidate_id",
            sa.BigInteger(),
            sa.ForeignKey("collector_cycle_candidates.candidate_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "opportunity_id",
            sa.Text(),
            sa.ForeignKey("opportunities.opportunity_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "execution_attempt_id",
            sa.Text(),
            sa.ForeignKey("execution_attempts.execution_attempt_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("trade_intent", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("entity_key", sa.Text(), nullable=False),
        sa.Column("underlying_symbol", sa.Text(), nullable=False),
        sa.Column("strategy", sa.Text(), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("limit_price", sa.Float(), nullable=True),
        sa.Column("reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("blockers_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("metrics_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("evidence_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("policy_refs_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("resolved_risk_policy_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_risk_decisions_session_decided",
        "risk_decisions",
        ["session_id", "decided_at"],
        unique=False,
    )
    op.create_index(
        "idx_risk_decisions_status_decided",
        "risk_decisions",
        ["status", "decided_at"],
        unique=False,
    )
    op.create_index(
        "idx_risk_decisions_opportunity_decided",
        "risk_decisions",
        ["opportunity_id", "decided_at"],
        unique=False,
    )
    op.create_index(
        "idx_risk_decisions_execution_attempt",
        "risk_decisions",
        ["execution_attempt_id"],
        unique=False,
    )

    op.add_column("execution_attempts", sa.Column("opportunity_id", sa.Text(), nullable=True))
    op.add_column("execution_attempts", sa.Column("risk_decision_id", sa.Text(), nullable=True))
    op.create_foreign_key(
        "fk_execution_attempts_opportunity_id",
        "execution_attempts",
        "opportunities",
        ["opportunity_id"],
        ["opportunity_id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_execution_attempts_risk_decision_id",
        "execution_attempts",
        "risk_decisions",
        ["risk_decision_id"],
        ["risk_decision_id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "idx_execution_attempts_opportunity_requested",
        "execution_attempts",
        ["opportunity_id", "requested_at"],
        unique=False,
    )
    op.create_index(
        "idx_execution_attempts_risk_decision_requested",
        "execution_attempts",
        ["risk_decision_id", "requested_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_execution_attempts_risk_decision_requested", table_name="execution_attempts")
    op.drop_index("idx_execution_attempts_opportunity_requested", table_name="execution_attempts")
    op.drop_constraint("fk_execution_attempts_risk_decision_id", "execution_attempts", type_="foreignkey")
    op.drop_constraint("fk_execution_attempts_opportunity_id", "execution_attempts", type_="foreignkey")
    op.drop_column("execution_attempts", "risk_decision_id")
    op.drop_column("execution_attempts", "opportunity_id")

    op.drop_index("idx_risk_decisions_execution_attempt", table_name="risk_decisions")
    op.drop_index("idx_risk_decisions_opportunity_decided", table_name="risk_decisions")
    op.drop_index("idx_risk_decisions_status_decided", table_name="risk_decisions")
    op.drop_index("idx_risk_decisions_session_decided", table_name="risk_decisions")
    op.drop_table("risk_decisions")
