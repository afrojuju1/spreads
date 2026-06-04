"""add trade refs to active execution intents

Revision ID: 20260604_0046
Revises: 20260604_0045
Create Date: 2026-06-04 02:45:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260604_0046"
down_revision = "20260604_0045"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "execution_intents",
        sa.Column(
            "trade_signal_id",
            sa.Text(),
            sa.ForeignKey("trade_signals.trade_signal_id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.add_column(
        "execution_intents",
        sa.Column(
            "trade_decision_id",
            sa.Text(),
            sa.ForeignKey("trade_decisions.trade_decision_id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.create_index(
        "idx_execution_intents_trade_signal",
        "execution_intents",
        ["trade_signal_id"],
    )
    op.create_index(
        "idx_execution_intents_trade_decision",
        "execution_intents",
        ["trade_decision_id"],
    )


def downgrade() -> None:
    op.drop_index("idx_execution_intents_trade_decision", table_name="execution_intents")
    op.drop_index("idx_execution_intents_trade_signal", table_name="execution_intents")
    op.drop_column("execution_intents", "trade_decision_id")
    op.drop_column("execution_intents", "trade_signal_id")
