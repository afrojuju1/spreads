"""delete legacy opportunity runtime storage

Revision ID: 20260604_0050
Revises: 20260604_0049
Create Date: 2026-06-04 04:20:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260604_0050"
down_revision = "20260604_0049"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            ALTER TABLE execution_attempts DROP CONSTRAINT IF EXISTS execution_attempts_opportunity_id_fkey;
            ALTER TABLE execution_intents DROP CONSTRAINT IF EXISTS execution_intents_opportunity_decision_id_fkey;
            ALTER TABLE risk_decisions DROP CONSTRAINT IF EXISTS risk_decisions_opportunity_id_fkey;
            ALTER TABLE portfolio_positions DROP CONSTRAINT IF EXISTS portfolio_positions_source_opportunity_id_fkey;
            ALTER TABLE opportunity_decisions DROP CONSTRAINT IF EXISTS opportunity_decisions_opportunity_id_fkey;

            ALTER TABLE execution_intents DROP COLUMN IF EXISTS opportunity_decision_id;

            DROP TABLE IF EXISTS opportunity_decisions CASCADE;
            DROP TABLE IF EXISTS opportunities CASCADE;
            DROP TABLE IF EXISTS signal_state_transitions CASCADE;
            DROP TABLE IF EXISTS signal_states CASCADE;
            """
        )
    )


def downgrade() -> None:
    pass
