"""rename execution attempt bucket to attempt context

Revision ID: 20260412_0021
Revises: 20260412_0020
Create Date: 2026-04-12 18:05:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260412_0021"
down_revision = "20260412_0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "execution_attempts",
        "bucket",
        new_column_name="attempt_context",
        existing_type=sa.Text(),
    )

    # Preserve any unknown historical values verbatim so operators can inspect them later.
    op.execute(
        """
        UPDATE execution_attempts
        SET attempt_context = CASE
            WHEN attempt_context = 'promotable' THEN 'open_promotable'
            WHEN attempt_context = 'monitor' THEN 'open_monitor'
            WHEN attempt_context = 'position_close' THEN 'position_close'
            ELSE attempt_context
        END
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE execution_attempts
        SET attempt_context = CASE
            WHEN attempt_context = 'open_promotable' THEN 'promotable'
            WHEN attempt_context = 'open_monitor' THEN 'monitor'
            WHEN attempt_context = 'position_close' THEN 'position_close'
            ELSE attempt_context
        END
        """
    )

    op.alter_column(
        "execution_attempts",
        "attempt_context",
        new_column_name="bucket",
        existing_type=sa.Text(),
    )
