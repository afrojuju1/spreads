"""remove scan candidate pair columns

Revision ID: 20260420_0031
Revises: 20260420_0030
Create Date: 2026-04-20 03:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260420_0031"
down_revision = "20260420_0030"
branch_labels = None
depends_on = None


def _leg_strike_value_sql(*, element: str) -> str:
    return f"""
    COALESCE(
        NULLIF({element} ->> 'strike', '')::double precision,
        CASE
            WHEN NULLIF(substring({element} ->> 'symbol' FROM '[CP](\\d+(?:\\.\\d+)?)$'), '') IS NULL
            THEN NULL
            WHEN position('.' IN substring({element} ->> 'symbol' FROM '[CP](\\d+(?:\\.\\d+)?)$')) > 0
            THEN substring({element} ->> 'symbol' FROM '[CP](\\d+(?:\\.\\d+)?)$')::double precision
            WHEN length(substring({element} ->> 'symbol' FROM '[CP](\\d+(?:\\.\\d+)?)$')) >= 8
            THEN (substring({element} ->> 'symbol' FROM '[CP](\\d+(?:\\.\\d+)?)$')::numeric / 1000.0)::double precision
            ELSE substring({element} ->> 'symbol' FROM '[CP](\\d+(?:\\.\\d+)?)$')::double precision
        END
    )
    """


def _has_leg_role_sql(*, role: str) -> str:
    return f"""
    EXISTS (
        SELECT 1
        FROM jsonb_array_elements(COALESCE(legs_json, '[]'::jsonb)) AS elem
        WHERE COALESCE(elem ->> 'role', '') = '{role}'
    )
    """


def _min_leg_strike_sql(*, role: str | None = None) -> str:
    role_filter = (
        ""
        if role is None
        else f"WHERE COALESCE(elem ->> 'role', '') = '{role}'"
    )
    return f"""
    (
        SELECT min({_leg_strike_value_sql(element='elem')})
        FROM jsonb_array_elements(COALESCE(legs_json, '[]'::jsonb)) AS elem
        {role_filter}
    )
    """


def _max_leg_strike_sql() -> str:
    return f"""
    (
        SELECT max({_leg_strike_value_sql(element='elem')})
        FROM jsonb_array_elements(COALESCE(legs_json, '[]'::jsonb)) AS elem
    )
    """


def upgrade() -> None:
    op.drop_column("scan_candidates", "long_strike")
    op.drop_column("scan_candidates", "short_strike")


def downgrade() -> None:
    op.add_column(
        "scan_candidates",
        sa.Column("short_strike", sa.Float(), nullable=True),
    )
    op.add_column(
        "scan_candidates",
        sa.Column("long_strike", sa.Float(), nullable=True),
    )
    op.execute(
        f"""
        UPDATE scan_candidates
        SET short_strike = CASE
                WHEN {_has_leg_role_sql(role='short')}
                THEN COALESCE(
                    {_min_leg_strike_sql(role='short')},
                    {_min_leg_strike_sql()},
                    {_max_leg_strike_sql()}
                )
                ELSE COALESCE(
                    {_min_leg_strike_sql()},
                    {_max_leg_strike_sql()}
                )
            END,
            long_strike = CASE
                WHEN {_has_leg_role_sql(role='short')}
                THEN COALESCE(
                    {_min_leg_strike_sql(role='long')},
                    {_max_leg_strike_sql()},
                    {_min_leg_strike_sql()}
                )
                ELSE COALESCE(
                    {_max_leg_strike_sql()},
                    {_min_leg_strike_sql()}
                )
            END
        """
    )
