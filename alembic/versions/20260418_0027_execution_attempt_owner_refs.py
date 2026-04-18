"""add execution attempt owner references

Revision ID: 20260418_0027
Revises: 20260416_0026
Create Date: 2026-04-18 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260418_0027"
down_revision = "20260416_0026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("execution_attempts", sa.Column("bot_id", sa.Text(), nullable=True))
    op.add_column(
        "execution_attempts", sa.Column("automation_id", sa.Text(), nullable=True)
    )
    op.add_column(
        "execution_attempts",
        sa.Column("strategy_config_id", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_execution_attempts_bot_automation_requested",
        "execution_attempts",
        ["bot_id", "automation_id", "requested_at"],
    )

    op.execute(
        """
        UPDATE execution_attempts AS attempt
        SET bot_id = opp.bot_id,
            automation_id = opp.automation_id,
            strategy_config_id = opp.strategy_config_id
        FROM opportunities AS opp
        WHERE attempt.opportunity_id IS NOT NULL
          AND opp.opportunity_id = attempt.opportunity_id
          AND (
            attempt.bot_id IS DISTINCT FROM opp.bot_id
            OR attempt.automation_id IS DISTINCT FROM opp.automation_id
            OR attempt.strategy_config_id IS DISTINCT FROM opp.strategy_config_id
          )
        """
    )
    op.execute(
        """
        UPDATE execution_attempts AS attempt
        SET bot_id = position.bot_id,
            automation_id = position.automation_id,
            strategy_config_id = position.strategy_config_id
        FROM portfolio_positions AS position
        WHERE attempt.position_id IS NOT NULL
          AND position.position_id = attempt.position_id
          AND (
            attempt.bot_id IS NULL
            OR attempt.automation_id IS NULL
            OR attempt.strategy_config_id IS NULL
          )
        """
    )
    op.execute(
        """
        UPDATE execution_attempts
        SET bot_id = NULLIF(request_json ->> 'bot_id', ''),
            automation_id = NULLIF(request_json ->> 'automation_id', ''),
            strategy_config_id = NULLIF(request_json ->> 'strategy_config_id', '')
        WHERE (bot_id IS NULL OR automation_id IS NULL OR strategy_config_id IS NULL)
          AND request_json IS NOT NULL
        """
    )


def downgrade() -> None:
    op.drop_index(
        "idx_execution_attempts_bot_automation_requested",
        table_name="execution_attempts",
    )
    op.drop_column("execution_attempts", "strategy_config_id")
    op.drop_column("execution_attempts", "automation_id")
    op.drop_column("execution_attempts", "bot_id")
