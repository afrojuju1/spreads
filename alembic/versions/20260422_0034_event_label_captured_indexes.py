"""add label/captured_at indexes for option event tables

Revision ID: 20260422_0034
Revises: 20260421_0033
Create Date: 2026-04-22 15:40:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260422_0034"
down_revision = "20260421_0033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            sa.text(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS
                idx_option_quote_events_label_captured_at
                ON option_quote_events (label, captured_at)
                """
            )
        )
        op.execute(
            sa.text(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS
                idx_option_trade_events_label_captured_at
                ON option_trade_events (label, captured_at)
                """
            )
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            sa.text(
                """
                DROP INDEX CONCURRENTLY IF EXISTS
                idx_option_trade_events_label_captured_at
                """
            )
        )
        op.execute(
            sa.text(
                """
                DROP INDEX CONCURRENTLY IF EXISTS
                idx_option_quote_events_label_captured_at
                """
            )
        )
