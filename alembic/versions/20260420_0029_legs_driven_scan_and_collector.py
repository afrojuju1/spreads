"""make scan and collector candidates legs-first

Revision ID: 20260420_0029
Revises: 20260420_0028
Create Date: 2026-04-20 00:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260420_0029"
down_revision = "20260420_0028"
branch_labels = None
depends_on = None


_NORMALIZED_STRATEGY_SQL = """
CASE {column}
    WHEN 'call_credit' THEN 'call_credit_spread'
    WHEN 'put_credit' THEN 'put_credit_spread'
    WHEN 'call_debit' THEN 'call_debit_spread'
    WHEN 'put_debit' THEN 'put_debit_spread'
    ELSE {column}
END
"""


def _scan_candidate_legs_sql() -> str:
    return """
    CASE
        WHEN NULLIF(short_symbol, '') IS NOT NULL OR NULLIF(long_symbol, '') IS NOT NULL
        THEN (
            CASE
                WHEN NULLIF(short_symbol, '') IS NOT NULL
                 AND NULLIF(long_symbol, '') IS NOT NULL
                THEN jsonb_build_array(
                    jsonb_strip_nulls(
                        jsonb_build_object(
                            'symbol', short_symbol,
                            'role', 'short',
                            'ratio_qty', '1',
                            'expiration_date', expiration_date
                        )
                    ),
                    jsonb_strip_nulls(
                        jsonb_build_object(
                            'symbol', long_symbol,
                            'role', 'long',
                            'ratio_qty', '1',
                            'expiration_date', expiration_date
                        )
                    )
                )
                WHEN NULLIF(short_symbol, '') IS NOT NULL
                THEN jsonb_build_array(
                    jsonb_strip_nulls(
                        jsonb_build_object(
                            'symbol', short_symbol,
                            'role', 'short',
                            'ratio_qty', '1',
                            'expiration_date', expiration_date
                        )
                    )
                )
                WHEN NULLIF(long_symbol, '') IS NOT NULL
                THEN jsonb_build_array(
                    jsonb_strip_nulls(
                        jsonb_build_object(
                            'symbol', long_symbol,
                            'role', 'long',
                            'ratio_qty', '1',
                            'expiration_date', expiration_date
                        )
                    )
                )
                ELSE '[]'::jsonb
            END
        )
        ELSE '[]'::jsonb
    END
    """


def _structure_identity_sql(*, strategy_column: str, short_column: str, long_column: str) -> str:
    normalized_strategy = _NORMALIZED_STRATEGY_SQL.format(column=strategy_column)
    return f"""
    concat_ws(
        '|',
        {normalized_strategy},
        CASE
            WHEN {short_column} IS NULL OR {short_column} = '' THEN NULL
            ELSE concat_ws(
                ':',
                'short',
                {short_column},
                '',
                '1',
                COALESCE(expiration_date::text, '')
            )
        END,
        CASE
            WHEN {long_column} IS NULL OR {long_column} = '' THEN NULL
            ELSE concat_ws(
                ':',
                'long',
                {long_column},
                '',
                '1',
                COALESCE(expiration_date::text, '')
            )
        END
    )
    """


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
    op.add_column(
        "scan_candidates",
        sa.Column("structure_identity", sa.Text(), nullable=True),
    )
    op.add_column(
        "scan_candidates",
        sa.Column(
            "legs_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.execute(
        f"""
        UPDATE scan_candidates
        SET legs_json = {_scan_candidate_legs_sql()},
            structure_identity = {_structure_identity_sql(
                strategy_column='strategy',
                short_column='short_symbol',
                long_column='long_symbol',
            )}
        """
    )
    op.alter_column("scan_candidates", "structure_identity", nullable=False)
    op.create_index(
        "idx_scan_candidates_structure",
        "scan_candidates",
        ["run_id", "strategy", "expiration_date", "structure_identity"],
    )
    op.alter_column("scan_candidates", "legs_json", server_default=None)
    op.drop_column("scan_candidates", "long_symbol")
    op.drop_column("scan_candidates", "short_symbol")

    op.drop_index(
        "idx_collector_cycle_candidates_identity",
        table_name="collector_cycle_candidates",
    )
    op.add_column(
        "collector_cycle_candidates",
        sa.Column("structure_identity", sa.Text(), nullable=True),
    )
    op.add_column(
        "collector_cycle_candidates",
        sa.Column(
            "legs_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.execute(
        f"""
        UPDATE collector_cycle_candidates
        SET legs_json = CASE
            WHEN candidate_json IS NOT NULL
             AND jsonb_typeof(candidate_json -> 'legs') = 'array'
             AND jsonb_array_length(candidate_json -> 'legs') > 0
            THEN candidate_json -> 'legs'
            WHEN candidate_json IS NOT NULL
             AND jsonb_typeof(candidate_json -> 'order_payload') = 'object'
             AND jsonb_typeof(candidate_json -> 'order_payload' -> 'legs') = 'array'
             AND jsonb_array_length(candidate_json -> 'order_payload' -> 'legs') > 0
            THEN candidate_json -> 'order_payload' -> 'legs'
            ELSE {_scan_candidate_legs_sql()}
        END,
        structure_identity = COALESCE(
            NULLIF(candidate_json ->> 'structure_identity', ''),
            NULLIF(candidate_json ->> 'candidate_identity', ''),
            {_structure_identity_sql(
                strategy_column='strategy',
                short_column='short_symbol',
                long_column='long_symbol',
            )}
        )
        """
    )
    op.alter_column(
        "collector_cycle_candidates",
        "structure_identity",
        nullable=False,
    )
    op.create_index(
        "idx_collector_cycle_candidates_identity",
        "collector_cycle_candidates",
        ["underlying_symbol", "strategy", "expiration_date", "structure_identity"],
    )
    op.alter_column("collector_cycle_candidates", "legs_json", server_default=None)
    op.drop_column("collector_cycle_candidates", "long_symbol")
    op.drop_column("collector_cycle_candidates", "short_symbol")


def downgrade() -> None:
    op.add_column(
        "scan_candidates",
        sa.Column("short_symbol", sa.Text(), nullable=False, server_default=""),
    )
    op.add_column(
        "scan_candidates",
        sa.Column("long_symbol", sa.Text(), nullable=False, server_default=""),
    )
    op.execute(
        f"""
        UPDATE scan_candidates
        SET short_symbol = COALESCE({_first_leg_symbol_sql(role='short')}, ''),
            long_symbol = COALESCE({_first_leg_symbol_sql(role='long')}, '')
        """
    )
    op.drop_index("idx_scan_candidates_structure", table_name="scan_candidates")
    op.alter_column("scan_candidates", "long_symbol", server_default=None)
    op.alter_column("scan_candidates", "short_symbol", server_default=None)
    op.drop_column("scan_candidates", "legs_json")
    op.drop_column("scan_candidates", "structure_identity")

    op.drop_index(
        "idx_collector_cycle_candidates_identity",
        table_name="collector_cycle_candidates",
    )
    op.add_column(
        "collector_cycle_candidates",
        sa.Column("short_symbol", sa.Text(), nullable=False, server_default=""),
    )
    op.add_column(
        "collector_cycle_candidates",
        sa.Column("long_symbol", sa.Text(), nullable=False, server_default=""),
    )
    op.execute(
        f"""
        UPDATE collector_cycle_candidates
        SET short_symbol = COALESCE({_first_leg_symbol_sql(role='short')}, ''),
            long_symbol = COALESCE({_first_leg_symbol_sql(role='long')}, '')
        """
    )
    op.create_index(
        "idx_collector_cycle_candidates_identity",
        "collector_cycle_candidates",
        ["underlying_symbol", "strategy", "expiration_date", "short_symbol", "long_symbol"],
    )
    op.alter_column(
        "collector_cycle_candidates",
        "long_symbol",
        server_default=None,
    )
    op.alter_column(
        "collector_cycle_candidates",
        "short_symbol",
        server_default=None,
    )
    op.drop_column("collector_cycle_candidates", "legs_json")
    op.drop_column("collector_cycle_candidates", "structure_identity")
