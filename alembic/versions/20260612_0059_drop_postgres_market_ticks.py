"""drop postgres raw market tick tables

Revision ID: 20260612_0059
Revises: 20260611_0058
Create Date: 2026-06-12 10:20:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260612_0059"
down_revision = "20260611_0058"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("DROP TABLE IF EXISTS option_trade_ticks CASCADE"))
    op.execute(sa.text("DROP TABLE IF EXISTS option_quote_ticks CASCADE"))


def downgrade() -> None:
    raise NotImplementedError("Raw market ticks now live in ClickHouse; recreating the removed Postgres tick tables is not supported.")
