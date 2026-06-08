"""drop legacy scan history storage

Revision ID: 20260608_0055
Revises: 20260605_0054
Create Date: 2026-06-08 00:55:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260608_0055"
down_revision = "20260605_0054"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("DROP TABLE IF EXISTS scan_candidates CASCADE"))
    op.execute(sa.text("DROP TABLE IF EXISTS scan_runs CASCADE"))


def downgrade() -> None:
    pass
