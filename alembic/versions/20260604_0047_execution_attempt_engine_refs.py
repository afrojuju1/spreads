"""add engine refs to active execution attempts

Revision ID: 20260604_0047
Revises: 20260604_0046
Create Date: 2026-06-04 03:15:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260604_0047"
down_revision = "20260604_0046"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("execution_attempts", sa.Column("source_object_type", sa.Text(), nullable=True))
    op.add_column("execution_attempts", sa.Column("source_object_id", sa.Text(), nullable=True))
    op.add_column(
        "execution_attempts",
        sa.Column(
            "trade_signal_id",
            sa.Text(),
            sa.ForeignKey("trade_signals.trade_signal_id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.add_column(
        "execution_attempts",
        sa.Column(
            "trade_decision_id",
            sa.Text(),
            sa.ForeignKey("trade_decisions.trade_decision_id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.add_column(
        "execution_attempts",
        sa.Column(
            "admission_decision_id",
            sa.Text(),
            sa.ForeignKey("trade_admissions.admission_decision_id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.create_index(
        "idx_execution_attempts_source_object",
        "execution_attempts",
        ["source_object_type", "source_object_id"],
    )
    op.create_index(
        "idx_execution_attempts_trade_signal",
        "execution_attempts",
        ["trade_signal_id"],
    )
    op.create_index(
        "idx_execution_attempts_trade_decision",
        "execution_attempts",
        ["trade_decision_id"],
    )
    op.create_index(
        "idx_execution_attempts_admission_decision",
        "execution_attempts",
        ["admission_decision_id"],
    )


def downgrade() -> None:
    op.drop_index("idx_execution_attempts_admission_decision", table_name="execution_attempts")
    op.drop_index("idx_execution_attempts_trade_decision", table_name="execution_attempts")
    op.drop_index("idx_execution_attempts_trade_signal", table_name="execution_attempts")
    op.drop_index("idx_execution_attempts_source_object", table_name="execution_attempts")
    op.drop_column("execution_attempts", "admission_decision_id")
    op.drop_column("execution_attempts", "trade_decision_id")
    op.drop_column("execution_attempts", "trade_signal_id")
    op.drop_column("execution_attempts", "source_object_id")
    op.drop_column("execution_attempts", "source_object_type")
