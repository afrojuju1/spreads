"""add captured_at indexes for option event retention

Revision ID: 20260602_0041
Revises: 20260430_0040
Create Date: 2026-06-02 07:40:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260602_0041"
down_revision = "20260430_0040"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            sa.text(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS
                idx_option_quote_events_captured_at
                ON option_quote_events (captured_at)
                """
            )
        )
        op.execute(
            sa.text(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS
                idx_option_trade_events_captured_at
                ON option_trade_events (captured_at)
                """
            )
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            sa.text(
                """
                DROP INDEX CONCURRENTLY IF EXISTS
                idx_option_trade_events_captured_at
                """
            )
        )
        op.execute(
            sa.text(
                """
                DROP INDEX CONCURRENTLY IF EXISTS
                idx_option_quote_events_captured_at
                """
            )
        )
