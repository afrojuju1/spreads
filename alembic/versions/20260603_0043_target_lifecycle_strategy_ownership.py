"""replace target lifecycle strategy ownership columns

Revision ID: 20260603_0043
Revises: 20260603_0042
Create Date: 2026-06-03 11:35:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260603_0043"
down_revision = "20260603_0042"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "trade_signals",
        "strategy_family",
        new_column_name="trade_structure",
        existing_type=sa.Text(),
        existing_nullable=True,
    )

    op.drop_index("idx_trade_decisions_bot_decided", table_name="trade_decisions")
    op.add_column(
        "trade_decisions",
        sa.Column("trading_strategy_id", sa.Text(), nullable=False),
    )
    op.add_column(
        "trade_decisions",
        sa.Column("trade_structure", sa.Text(), nullable=False),
    )
    op.add_column("trade_decisions", sa.Column("routine", sa.Text(), nullable=False))
    op.alter_column(
        "trade_decisions",
        "config_hash",
        existing_type=sa.Text(),
        nullable=False,
    )
    op.drop_column("trade_decisions", "strategy_config_id")
    op.drop_column("trade_decisions", "automation_id")
    op.drop_column("trade_decisions", "bot_id")
    op.create_index(
        "idx_trade_decisions_strategy_decided",
        "trade_decisions",
        ["trading_strategy_id", "routine", "decided_at"],
    )

    op.add_column(
        "trade_execution_intents",
        sa.Column("trading_strategy_id", sa.Text(), nullable=True),
    )
    op.add_column(
        "trade_execution_intents",
        sa.Column("trade_structure", sa.Text(), nullable=True),
    )
    op.add_column(
        "trade_execution_intents",
        sa.Column("routine", sa.Text(), nullable=True),
    )
    op.drop_column("trade_execution_intents", "automation_id")
    op.drop_column("trade_execution_intents", "bot_id")

    op.alter_column(
        "trade_positions",
        "strategy_family",
        new_column_name="trade_structure",
        existing_type=sa.Text(),
        existing_nullable=True,
    )
    op.add_column(
        "trade_positions",
        sa.Column("trading_strategy_id", sa.Text(), nullable=True),
    )
    op.add_column("trade_positions", sa.Column("routine", sa.Text(), nullable=True))
    op.add_column(
        "trade_positions",
        sa.Column("config_hash", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("trade_positions", "config_hash")
    op.drop_column("trade_positions", "routine")
    op.drop_column("trade_positions", "trading_strategy_id")
    op.alter_column(
        "trade_positions",
        "trade_structure",
        new_column_name="strategy_family",
        existing_type=sa.Text(),
        existing_nullable=True,
    )

    op.add_column(
        "trade_execution_intents",
        sa.Column("bot_id", sa.Text(), nullable=True),
    )
    op.add_column(
        "trade_execution_intents",
        sa.Column("automation_id", sa.Text(), nullable=True),
    )
    op.drop_column("trade_execution_intents", "routine")
    op.drop_column("trade_execution_intents", "trade_structure")
    op.drop_column("trade_execution_intents", "trading_strategy_id")

    op.drop_index("idx_trade_decisions_strategy_decided", table_name="trade_decisions")
    op.add_column("trade_decisions", sa.Column("bot_id", sa.Text(), nullable=False))
    op.add_column(
        "trade_decisions",
        sa.Column("automation_id", sa.Text(), nullable=False),
    )
    op.add_column(
        "trade_decisions",
        sa.Column("strategy_config_id", sa.Text(), nullable=True),
    )
    op.alter_column(
        "trade_decisions",
        "config_hash",
        existing_type=sa.Text(),
        nullable=True,
    )
    op.drop_column("trade_decisions", "routine")
    op.drop_column("trade_decisions", "trade_structure")
    op.drop_column("trade_decisions", "trading_strategy_id")
    op.create_index(
        "idx_trade_decisions_bot_decided",
        "trade_decisions",
        ["bot_id", "automation_id", "decided_at"],
    )

    op.alter_column(
        "trade_signals",
        "trade_structure",
        new_column_name="strategy_family",
        existing_type=sa.Text(),
        existing_nullable=True,
    )
