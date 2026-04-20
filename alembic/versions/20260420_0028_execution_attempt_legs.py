"""make execution attempts legs-first

Revision ID: 20260420_0028
Revises: 20260418_0027
Create Date: 2026-04-20 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260420_0028"
down_revision = "20260418_0027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "execution_attempts",
        sa.Column("structure_identity", sa.Text(), nullable=True),
    )
    op.add_column(
        "execution_attempts",
        sa.Column(
            "legs_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.add_column(
        "execution_attempts",
        sa.Column(
            "order_payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.add_column(
        "execution_attempts",
        sa.Column(
            "economics_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.alter_column("execution_attempts", "expiration_date", nullable=True)
    op.alter_column("execution_attempts", "short_symbol", nullable=True)
    op.alter_column("execution_attempts", "long_symbol", nullable=True)
    op.create_index(
        "idx_execution_attempts_session_structure_requested",
        "execution_attempts",
        ["session_id", "structure_identity", "requested_at"],
    )

    op.execute(
        """
        UPDATE execution_attempts
        SET order_payload_json = CASE
            WHEN request_json IS NOT NULL
             AND jsonb_typeof(request_json -> 'order') = 'object'
            THEN request_json -> 'order'
            WHEN candidate_json IS NOT NULL
             AND jsonb_typeof(candidate_json -> 'order_payload') = 'object'
            THEN candidate_json -> 'order_payload'
            ELSE '{}'::jsonb
        END
        """
    )
    op.execute(
        """
        UPDATE execution_attempts
        SET economics_json = jsonb_strip_nulls(
            jsonb_build_object(
                'midpoint_credit', candidate_json -> 'midpoint_credit',
                'natural_credit', candidate_json -> 'natural_credit',
                'max_profit', candidate_json -> 'max_profit',
                'max_loss', candidate_json -> 'max_loss',
                'return_on_risk', candidate_json -> 'return_on_risk',
                'fill_ratio', candidate_json -> 'fill_ratio'
            )
        )
        """
    )
    op.execute(
        """
        UPDATE execution_attempts
        SET legs_json = CASE
            WHEN jsonb_typeof(order_payload_json -> 'legs') = 'array'
             AND jsonb_array_length(order_payload_json -> 'legs') > 0
            THEN order_payload_json -> 'legs'
            WHEN short_symbol IS NOT NULL OR long_symbol IS NOT NULL
            THEN (
                CASE
                    WHEN short_symbol IS NOT NULL AND long_symbol IS NOT NULL
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
                    WHEN short_symbol IS NOT NULL
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
                    WHEN long_symbol IS NOT NULL
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
    )
    op.execute(
        """
        UPDATE execution_attempts
        SET structure_identity = concat_ws(
            '|',
            CASE COALESCE(strategy_family, strategy)
                WHEN 'call_credit' THEN 'call_credit_spread'
                WHEN 'put_credit' THEN 'put_credit_spread'
                WHEN 'call_debit' THEN 'call_debit_spread'
                WHEN 'put_debit' THEN 'put_debit_spread'
                ELSE COALESCE(strategy_family, strategy)
            END,
            concat_ws(
                ':',
                'short',
                COALESCE(short_symbol, ''),
                '',
                '1',
                COALESCE(expiration_date::text, '')
            ),
            concat_ws(
                ':',
                'long',
                COALESCE(long_symbol, ''),
                '',
                '1',
                COALESCE(expiration_date::text, '')
            )
        )
        WHERE structure_identity IS NULL
          AND (short_symbol IS NOT NULL OR long_symbol IS NOT NULL)
        """
    )

    op.alter_column("execution_attempts", "legs_json", server_default=None)
    op.alter_column("execution_attempts", "order_payload_json", server_default=None)
    op.alter_column("execution_attempts", "economics_json", server_default=None)


def downgrade() -> None:
    op.execute(
        """
        UPDATE execution_attempts
        SET expiration_date = COALESCE(expiration_date, session_date),
            short_symbol = COALESCE(short_symbol, ''),
            long_symbol = COALESCE(long_symbol, '')
        """
    )
    op.drop_index(
        "idx_execution_attempts_session_structure_requested",
        table_name="execution_attempts",
    )
    op.alter_column("execution_attempts", "long_symbol", nullable=False)
    op.alter_column("execution_attempts", "short_symbol", nullable=False)
    op.alter_column("execution_attempts", "expiration_date", nullable=False)
    op.drop_column("execution_attempts", "economics_json")
    op.drop_column("execution_attempts", "order_payload_json")
    op.drop_column("execution_attempts", "legs_json")
    op.drop_column("execution_attempts", "structure_identity")
