"""cut over raw market storage to option ticks

Revision ID: 20260605_0054
Revises: 20260605_0053
Create Date: 2026-06-05 11:40:00
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, time, timedelta

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260605_0054"
down_revision = "20260605_0053"
branch_labels = None
depends_on = None

TOMBSTONE_SUFFIX = "old_20260605"
DEFAULT_OPTION_QUOTE_TICK_RETENTION_DAYS = 7
DEFAULT_OPTION_TRADE_TICK_RETENTION_DAYS = 30
DEFAULT_FUTURE_PARTITION_DAYS = 14

_IDENTIFIER_RE = re.compile(r"^[a-z][a-z0-9_]*$")


class MarketTickPartitionFamily:
    def __init__(self, *, name: str, parent_table: str, partition_prefix: str, retention_days: int) -> None:
        self.name = name
        self.parent_table = parent_table
        self.partition_prefix = partition_prefix
        self.retention_days = retention_days


OPTION_QUOTE_TICK_PARTITIONS = MarketTickPartitionFamily(
    name="option_quote_ticks",
    parent_table="option_quote_ticks",
    partition_prefix="option_quote_ticks",
    retention_days=DEFAULT_OPTION_QUOTE_TICK_RETENTION_DAYS,
)
OPTION_TRADE_TICK_PARTITIONS = MarketTickPartitionFamily(
    name="option_trade_ticks",
    parent_table="option_trade_ticks",
    partition_prefix="option_trade_ticks",
    retention_days=DEFAULT_OPTION_TRADE_TICK_RETENTION_DAYS,
)


def market_tick_partition_families(
    *,
    option_quote_tick_days: int | None = None,
    option_trade_tick_days: int | None = None,
) -> tuple[MarketTickPartitionFamily, ...]:
    quote_family = (
        OPTION_QUOTE_TICK_PARTITIONS
        if option_quote_tick_days is None
        else MarketTickPartitionFamily(
            name=OPTION_QUOTE_TICK_PARTITIONS.name,
            parent_table=OPTION_QUOTE_TICK_PARTITIONS.parent_table,
            partition_prefix=OPTION_QUOTE_TICK_PARTITIONS.partition_prefix,
            retention_days=option_quote_tick_days,
        )
    )
    trade_family = (
        OPTION_TRADE_TICK_PARTITIONS
        if option_trade_tick_days is None
        else MarketTickPartitionFamily(
            name=OPTION_TRADE_TICK_PARTITIONS.name,
            parent_table=OPTION_TRADE_TICK_PARTITIONS.parent_table,
            partition_prefix=OPTION_TRADE_TICK_PARTITIONS.partition_prefix,
            retention_days=option_trade_tick_days,
        )
    )
    return (quote_family, trade_family)


def _validate_identifier(value: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def _utc_day(value: date | datetime | None = None) -> date:
    if value is None:
        return datetime.now(UTC).date()
    if isinstance(value, datetime):
        normalized = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
        return normalized.date()
    return value


def partition_day_bounds(day: date) -> tuple[datetime, datetime]:
    start_at = datetime.combine(day, time.min, tzinfo=UTC)
    return start_at, start_at + timedelta(days=1)


def render_partition_bound(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(sep=" ", timespec="seconds").replace("+00:00", "+00")


def partition_name_for_day(family: MarketTickPartitionFamily, day: date) -> str:
    return f"{_validate_identifier(family.partition_prefix)}_{day:%Y_%m_%d}"


def _inclusive_days(start_day: date, end_day: date) -> list[date]:
    days: list[date] = []
    current_day = start_day
    while current_day <= end_day:
        days.append(current_day)
        current_day += timedelta(days=1)
    return days


def initial_partition_days(
    family: MarketTickPartitionFamily,
    *,
    as_of: date | datetime | None = None,
    future_days: int = DEFAULT_FUTURE_PARTITION_DAYS,
) -> list[date]:
    today = _utc_day(as_of)
    start_day = today - timedelta(days=family.retention_days)
    end_day = today + timedelta(days=max(int(future_days), 0))
    return _inclusive_days(start_day, end_day)


def create_partition_sql(family: MarketTickPartitionFamily, day: date) -> str:
    partition_name = partition_name_for_day(family, day)
    start_at, end_at = partition_day_bounds(day)
    return (
        f"CREATE TABLE IF NOT EXISTS {_validate_identifier(partition_name)} "
        f"PARTITION OF {_validate_identifier(family.parent_table)} "
        f"FOR VALUES FROM ('{render_partition_bound(start_at)}') TO ('{render_partition_bound(end_at)}')"
    )


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
    for family in market_tick_partition_families():
        for day in initial_partition_days(family, future_days=DEFAULT_FUTURE_PARTITION_DAYS):
            op.execute(sa.text(create_partition_sql(family, day)))


def downgrade() -> None:
    op.execute(sa.text("DROP TABLE IF EXISTS option_trade_ticks CASCADE"))
    op.execute(sa.text("DROP TABLE IF EXISTS option_quote_ticks CASCADE"))
    _restore_tombstone("event_log")
    _restore_tombstone("option_trade_events")
    _restore_tombstone("option_quote_events")
