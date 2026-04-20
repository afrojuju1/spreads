"""remove execution attempt pair columns

Revision ID: 20260420_0030
Revises: 20260420_0029
Create Date: 2026-04-20 02:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260420_0030"
down_revision = "20260420_0029"
branch_labels = None
depends_on = None


def _first_leg_symbol_sql(*, role: str) -> str:
    return f"""
    (
        SELECT elem ->> 'symbol'
        FROM jsonb_array_elements(COALESCE(legs_json, '[]'::jsonb)) AS elem
        WHERE COALESCE(elem ->> 'role', '') = '{role}'
        ORDER BY elem ->> 'symbol'
        LIMIT 1
    )
    """


def upgrade() -> None:
    op.drop_column("execution_attempts", "long_symbol")
    op.drop_column("execution_attempts", "short_symbol")


def downgrade() -> None:
    op.add_column(
        "execution_attempts",
        sa.Column("short_symbol", sa.Text(), nullable=True),
    )
    op.add_column(
        "execution_attempts",
        sa.Column("long_symbol", sa.Text(), nullable=True),
    )
    op.execute(
        f"""
        UPDATE execution_attempts
        SET short_symbol = {_first_leg_symbol_sql(role='short')},
            long_symbol = {_first_leg_symbol_sql(role='long')}
        """
    )
