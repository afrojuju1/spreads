"""add config-driven runtime lineage

Revision ID: 20260416_0026
Revises: 20260415_0025
Create Date: 2026-04-16 13:15:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260416_0026"
down_revision = "20260415_0025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "automation_runs",
        sa.Column("automation_run_id", sa.Text(), primary_key=True),
        sa.Column("bot_id", sa.Text(), nullable=False),
        sa.Column("automation_id", sa.Text(), nullable=False),
        sa.Column("strategy_config_id", sa.Text(), nullable=False),
        sa.Column("trigger_type", sa.Text(), nullable=False),
        sa.Column("job_run_id", sa.Text(), nullable=True),
        sa.Column("cycle_id", sa.Text(), nullable=True),
        sa.Column("label", sa.Text(), nullable=True),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column(
            "result_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("config_hash", sa.Text(), nullable=False),
    )
    op.create_index(
        "idx_automation_runs_bot_automation_started",
        "automation_runs",
        ["bot_id", "automation_id", "started_at"],
        unique=False,
    )
    op.create_index(
        "idx_automation_runs_session_started",
        "automation_runs",
        ["session_date", "started_at"],
        unique=False,
    )
    op.create_index(
        "idx_automation_runs_cycle_automation",
        "automation_runs",
        ["cycle_id", "automation_id"],
        unique=False,
    )

    op.add_column("opportunities", sa.Column("bot_id", sa.Text(), nullable=True))
    op.add_column("opportunities", sa.Column("automation_id", sa.Text(), nullable=True))
    op.add_column(
        "opportunities", sa.Column("automation_run_id", sa.Text(), nullable=True)
    )
    op.add_column(
        "opportunities", sa.Column("strategy_config_id", sa.Text(), nullable=True)
    )
    op.add_column("opportunities", sa.Column("strategy_id", sa.Text(), nullable=True))
    op.add_column("opportunities", sa.Column("config_hash", sa.Text(), nullable=True))
    op.add_column(
        "opportunities",
        sa.Column(
            "policy_ref_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.create_foreign_key(
        "fk_opportunities_automation_run",
        "opportunities",
        "automation_runs",
        ["automation_run_id"],
        ["automation_run_id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "idx_opportunities_bot_automation_session",
        "opportunities",
        ["bot_id", "automation_id", "session_date", "updated_at"],
        unique=False,
    )
    op.create_index(
        "idx_opportunities_automation_run",
        "opportunities",
        ["automation_run_id"],
        unique=False,
    )
    op.alter_column("opportunities", "policy_ref_json", server_default=None)

    op.add_column("portfolio_positions", sa.Column("bot_id", sa.Text(), nullable=True))
    op.add_column(
        "portfolio_positions", sa.Column("automation_id", sa.Text(), nullable=True)
    )
    op.add_column(
        "portfolio_positions", sa.Column("strategy_config_id", sa.Text(), nullable=True)
    )
    op.add_column(
        "portfolio_positions", sa.Column("strategy_id", sa.Text(), nullable=True)
    )
    op.add_column(
        "portfolio_positions",
        sa.Column("opening_execution_intent_id", sa.Text(), nullable=True),
    )
    op.add_column(
        "portfolio_positions", sa.Column("config_hash", sa.Text(), nullable=True)
    )
    op.create_foreign_key(
        "fk_portfolio_positions_opening_execution_intent",
        "portfolio_positions",
        "execution_intents",
        ["opening_execution_intent_id"],
        ["execution_intent_id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "idx_portfolio_positions_bot_status",
        "portfolio_positions",
        ["bot_id", "status"],
        unique=False,
    )
    op.create_index(
        "idx_portfolio_positions_strategy_config_status",
        "portfolio_positions",
        ["strategy_config_id", "status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "idx_portfolio_positions_strategy_config_status",
        table_name="portfolio_positions",
    )
    op.drop_index(
        "idx_portfolio_positions_bot_status", table_name="portfolio_positions"
    )
    op.drop_constraint(
        "fk_portfolio_positions_opening_execution_intent",
        "portfolio_positions",
        type_="foreignkey",
    )
    op.drop_column("portfolio_positions", "config_hash")
    op.drop_column("portfolio_positions", "opening_execution_intent_id")
    op.drop_column("portfolio_positions", "strategy_id")
    op.drop_column("portfolio_positions", "strategy_config_id")
    op.drop_column("portfolio_positions", "automation_id")
    op.drop_column("portfolio_positions", "bot_id")

    op.drop_index("idx_opportunities_automation_run", table_name="opportunities")
    op.drop_index(
        "idx_opportunities_bot_automation_session", table_name="opportunities"
    )
    op.drop_constraint(
        "fk_opportunities_automation_run",
        "opportunities",
        type_="foreignkey",
    )
    op.drop_column("opportunities", "policy_ref_json")
    op.drop_column("opportunities", "config_hash")
    op.drop_column("opportunities", "strategy_id")
    op.drop_column("opportunities", "strategy_config_id")
    op.drop_column("opportunities", "automation_run_id")
    op.drop_column("opportunities", "automation_id")
    op.drop_column("opportunities", "bot_id")

    op.drop_index("idx_automation_runs_cycle_automation", table_name="automation_runs")
    op.drop_index("idx_automation_runs_session_started", table_name="automation_runs")
    op.drop_index(
        "idx_automation_runs_bot_automation_started", table_name="automation_runs"
    )
    op.drop_table("automation_runs")
