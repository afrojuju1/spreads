"""cut over raw market storage to option ticks

Revision ID: 20260605_0054
Revises: 20260605_0053
Create Date: 2026-06-05 11:40:00
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260605_0054"
down_revision = "20260605_0053"
branch_labels = None
depends_on = None

TOMBSTONE_SUFFIX = "old_20260605"
QUOTE_RETENTION_DAYS = 7
TRADE_RETENTION_DAYS = 30
FUTURE_PARTITION_DAYS = 14


def _jsonb() -> postgresql.JSONB:
    return postgresql.JSONB(astext_type=sa.Text())


def _inspector() -> sa.Inspector:
    return sa.inspect(op.get_bind())


def _table_exists(table_name: str) -> bool:
    return _inspector().has_table(table_name)


def _rename_to_tombstone(table_name: str) -> None:
    tombstone_name = f"{table_name}_{TOMBSTONE_SUFFIX}"
    if not _table_exists(table_name):
        return
    if _table_exists(tombstone_name):
        raise RuntimeError(f"Cannot tombstone {table_name}; {tombstone_name} already exists.")
    op.rename_table(table_name, tombstone_name)


def _restore_tombstone(table_name: str) -> None:
    tombstone_name = f"{table_name}_{TOMBSTONE_SUFFIX}"
    if not _table_exists(tombstone_name):
        return
    if _table_exists(table_name):
        raise RuntimeError(f"Cannot restore {tombstone_name}; {table_name} already exists.")
    op.rename_table(tombstone_name, table_name)


def _utc_partition_bounds(day: date) -> tuple[str, str]:
    next_day = day + timedelta(days=1)
    return f"{day.isoformat()} 00:00:00+00", f"{next_day.isoformat()} 00:00:00+00"


def _create_daily_partitions(
    *,
    parent_table: str,
    partition_prefix: str,
    retention_days: int,
) -> None:
    today = datetime.now(timezone.utc).date()
    start_day = today - timedelta(days=retention_days)
    end_day = today + timedelta(days=FUTURE_PARTITION_DAYS)
    current_day = start_day
    while current_day <= end_day:
        partition_name = f"{partition_prefix}_{current_day:%Y_%m_%d}"
        lower_bound, upper_bound = _utc_partition_bounds(current_day)
        op.execute(sa.text(f"""
                CREATE TABLE IF NOT EXISTS {partition_name}
                PARTITION OF {parent_table}
                FOR VALUES FROM ('{lower_bound}') TO ('{upper_bound}')
                """))
        current_day += timedelta(days=1)


def _create_option_quote_ticks() -> None:
    op.create_table(
        "option_quote_ticks",
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("quote_tick_id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("cycle_id", sa.Text(), nullable=False),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("underlying_symbol", sa.Text(), nullable=True),
        sa.Column("strategy", sa.Text(), nullable=True),
        sa.Column("profile", sa.Text(), nullable=True),
        sa.Column("option_symbol", sa.Text(), nullable=False),
        sa.Column("leg_role", sa.Text(), nullable=False),
        sa.Column("bid", sa.Float(), nullable=False),
        sa.Column("ask", sa.Float(), nullable=False),
        sa.Column("midpoint", sa.Float(), nullable=False),
        sa.Column("bid_size", sa.Integer(), nullable=False),
        sa.Column("ask_size", sa.Integer(), nullable=False),
        sa.Column("source_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source", sa.Text(), nullable=False, server_default="alpaca_websocket"),
        sa.PrimaryKeyConstraint("captured_at", "quote_tick_id"),
        postgresql_partition_by="RANGE (captured_at)",
    )
    op.execute(sa.text("CREATE INDEX idx_option_quote_ticks_symbol_captured ON option_quote_ticks (option_symbol, captured_at DESC)"))
    op.execute(sa.text("CREATE INDEX idx_option_quote_ticks_label_captured ON option_quote_ticks (label, captured_at DESC)"))
    op.execute(sa.text("CREATE INDEX idx_option_quote_ticks_cycle ON option_quote_ticks (cycle_id)"))
    op.execute(sa.text("CREATE INDEX idx_option_quote_ticks_captured_brin ON option_quote_ticks USING BRIN (captured_at)"))


def _create_option_trade_ticks() -> None:
    op.create_table(
        "option_trade_ticks",
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("trade_tick_id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("cycle_id", sa.Text(), nullable=False),
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
        sa.Column("conditions_json", _jsonb(), nullable=False),
        sa.Column("source_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("included_in_score", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("exclusion_reason", sa.Text(), nullable=True),
        sa.Column("raw_payload_json", _jsonb(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False, server_default="alpaca_websocket"),
        sa.PrimaryKeyConstraint("captured_at", "trade_tick_id"),
        postgresql_partition_by="RANGE (captured_at)",
    )
    op.execute(sa.text("CREATE INDEX idx_option_trade_ticks_symbol_captured ON option_trade_ticks (option_symbol, captured_at DESC)"))
    op.execute(sa.text("CREATE INDEX idx_option_trade_ticks_underlying_captured ON option_trade_ticks (underlying_symbol, captured_at DESC)"))
    op.execute(sa.text("CREATE INDEX idx_option_trade_ticks_label_captured ON option_trade_ticks (label, captured_at DESC)"))
    op.execute(sa.text("CREATE INDEX idx_option_trade_ticks_cycle ON option_trade_ticks (cycle_id)"))
    op.execute(sa.text("CREATE INDEX idx_option_trade_ticks_captured_brin ON option_trade_ticks USING BRIN (captured_at)"))


def upgrade() -> None:
    _rename_to_tombstone("option_quote_events")
    _rename_to_tombstone("option_trade_events")
    _rename_to_tombstone("event_log")

    _create_option_quote_ticks()
    _create_option_trade_ticks()
    _create_daily_partitions(
        parent_table="option_quote_ticks",
        partition_prefix="option_quote_ticks",
        retention_days=QUOTE_RETENTION_DAYS,
    )
    _create_daily_partitions(
        parent_table="option_trade_ticks",
        partition_prefix="option_trade_ticks",
        retention_days=TRADE_RETENTION_DAYS,
    )


def downgrade() -> None:
    op.execute(sa.text("DROP TABLE IF EXISTS option_trade_ticks CASCADE"))
    op.execute(sa.text("DROP TABLE IF EXISTS option_quote_ticks CASCADE"))
    _restore_tombstone("event_log")
    _restore_tombstone("option_trade_events")
    _restore_tombstone("option_quote_events")
