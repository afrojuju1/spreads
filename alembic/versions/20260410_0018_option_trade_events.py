"""add option trade events

Revision ID: 20260410_0018
Revises: 20260410_0017
Create Date: 2026-04-10 11:05:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260410_0018"
down_revision = "20260410_0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "option_trade_events",
        sa.Column("trade_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("cycle_id", sa.Text(), nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("underlying_symbol", sa.Text(), nullable=True),
        sa.Column("strategy", sa.Text(), nullable=True),
        sa.Column("profile", sa.Text(), nullable=True),
        sa.Column("option_symbol", sa.Text(), nullable=False),
        sa.Column("leg_role", sa.Text(), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("size", sa.Integer(), nullable=False),
        sa.Column("premium", sa.Float(), nullable=False),
        sa.Column("exchange_code", sa.Text(), nullable=True),
        sa.Column("conditions_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("trade_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("included_in_score", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("exclusion_reason", sa.Text(), nullable=True),
        sa.Column("raw_payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source", sa.Text(), nullable=False, server_default="alpaca_websocket"),
    )
    op.create_index("idx_option_trade_events_cycle_id", "option_trade_events", ["cycle_id"], unique=False)
    op.create_index(
        "idx_option_trade_events_symbol_captured_at",
        "option_trade_events",
        ["option_symbol", "captured_at"],
        unique=False,
    )
    op.create_index(
        "idx_option_trade_events_underlying_captured_at",
        "option_trade_events",
        ["underlying_symbol", "captured_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "idx_option_trade_events_underlying_captured_at",
        table_name="option_trade_events",
    )
    op.drop_index("idx_option_trade_events_symbol_captured_at", table_name="option_trade_events")
    op.drop_index("idx_option_trade_events_cycle_id", table_name="option_trade_events")
    op.drop_table("option_trade_events")
