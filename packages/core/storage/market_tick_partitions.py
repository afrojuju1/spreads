from __future__ import annotations

import re
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from sqlalchemy import text

from core.storage.serializers import render_value

DEFAULT_OPTION_QUOTE_TICK_RETENTION_DAYS = 7
DEFAULT_OPTION_TRADE_TICK_RETENTION_DAYS = 30
DEFAULT_FUTURE_PARTITION_DAYS = 14

_IDENTIFIER_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_PARTITION_BOUND_RE = re.compile(r"FROM \('([^']+)'\) TO \('([^']+)'\)")


@dataclass(frozen=True, slots=True)
class MarketTickPartitionFamily:
    name: str
    parent_table: str
    partition_prefix: str
    retention_days: int


@dataclass(frozen=True, slots=True)
class MarketTickPartition:
    family: str
    parent_table: str
    partition_name: str
    bound_expression: str | None
    start_at: datetime | None
    end_at: datetime | None
    day: date | None


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
MARKET_TICK_PARTITION_FAMILIES = (
    OPTION_QUOTE_TICK_PARTITIONS,
    OPTION_TRADE_TICK_PARTITIONS,
)


def market_tick_partition_families(
    *,
    option_quote_tick_days: int | None = None,
    option_trade_tick_days: int | None = None,
) -> tuple[MarketTickPartitionFamily, ...]:
    quote_family = (
        OPTION_QUOTE_TICK_PARTITIONS
        if option_quote_tick_days is None
        else replace(OPTION_QUOTE_TICK_PARTITIONS, retention_days=option_quote_tick_days)
    )
    trade_family = (
        OPTION_TRADE_TICK_PARTITIONS
        if option_trade_tick_days is None
        else replace(OPTION_TRADE_TICK_PARTITIONS, retention_days=option_trade_tick_days)
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


def maintenance_partition_days(
    *,
    as_of: date | datetime | None = None,
    future_days: int = DEFAULT_FUTURE_PARTITION_DAYS,
) -> list[date]:
    today = _utc_day(as_of)
    end_day = today + timedelta(days=max(int(future_days), 0))
    return _inclusive_days(today, end_day)


def create_partition_sql(family: MarketTickPartitionFamily, day: date) -> str:
    partition_name = partition_name_for_day(family, day)
    start_at, end_at = partition_day_bounds(day)
    return (
        f"CREATE TABLE IF NOT EXISTS {_validate_identifier(partition_name)} "
        f"PARTITION OF {_validate_identifier(family.parent_table)} "
        f"FOR VALUES FROM ('{render_partition_bound(start_at)}') TO ('{render_partition_bound(end_at)}')"
    )


def drop_partition_sql(partition_name: str) -> str:
    return f"DROP TABLE IF EXISTS {_validate_identifier(partition_name)}"


def _inclusive_days(start_day: date, end_day: date) -> list[date]:
    days: list[date] = []
    current_day = start_day
    while current_day <= end_day:
        days.append(current_day)
        current_day += timedelta(days=1)
    return days


def _parse_bound_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace(" ", "T"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def parse_partition_bound_expression(expression: str | None) -> tuple[datetime | None, datetime | None]:
    if not expression:
        return None, None
    match = _PARTITION_BOUND_RE.search(expression)
    if match is None:
        return None, None
    return _parse_bound_timestamp(match.group(1)), _parse_bound_timestamp(match.group(2))


def list_market_tick_partitions(
    session: Any,
    family: MarketTickPartitionFamily,
) -> list[MarketTickPartition]:
    statement = text("""
        SELECT
          child.relname AS partition_name,
          pg_get_expr(child.relpartbound, child.oid, true) AS bound_expression
        FROM pg_inherits
        JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
        JOIN pg_namespace parent_namespace ON parent_namespace.oid = parent.relnamespace
        JOIN pg_class child ON child.oid = pg_inherits.inhrelid
        JOIN pg_namespace child_namespace ON child_namespace.oid = child.relnamespace
        WHERE parent_namespace.nspname = 'public'
          AND child_namespace.nspname = 'public'
          AND parent.relname = :parent_table
        ORDER BY child.relname
        """)
    rows = session.execute(statement, {"parent_table": family.parent_table}).mappings().all()
    partitions: list[MarketTickPartition] = []
    for row in rows:
        bound_expression = None if row["bound_expression"] is None else str(row["bound_expression"])
        start_at, end_at = parse_partition_bound_expression(bound_expression)
        partitions.append(
            MarketTickPartition(
                family=family.name,
                parent_table=family.parent_table,
                partition_name=str(row["partition_name"]),
                bound_expression=bound_expression,
                start_at=start_at,
                end_at=end_at,
                day=None if start_at is None else start_at.date(),
            )
        )
    return partitions


def partition_to_record(partition: MarketTickPartition) -> dict[str, Any]:
    return {
        "family": partition.family,
        "parent_table": partition.parent_table,
        "partition_name": partition.partition_name,
        "bound_expression": partition.bound_expression,
        "start_at": render_value(partition.start_at),
        "end_at": render_value(partition.end_at),
        "day": None if partition.day is None else partition.day.isoformat(),
    }


def partition_status(
    session: Any,
    family: MarketTickPartitionFamily,
    *,
    as_of: date | datetime | None = None,
    future_days: int = DEFAULT_FUTURE_PARTITION_DAYS,
) -> dict[str, Any]:
    today = _utc_day(as_of)
    partitions = list_market_tick_partitions(session, family)
    partitions_by_day = {partition.day: partition for partition in partitions if partition.day is not None}
    current_partition_ready = today in partitions_by_day
    required_future_days = max(int(future_days), 0)
    future_partition_days = 0
    missing_future_days: list[str] = []
    contiguous = True
    for offset in range(1, required_future_days + 1):
        day = today + timedelta(days=offset)
        if day in partitions_by_day:
            if contiguous:
                future_partition_days += 1
            continue
        contiguous = False
        missing_future_days.append(day.isoformat())
    cutoff_start, _ = partition_day_bounds(today - timedelta(days=family.retention_days))
    expired = [partition for partition in partitions if partition.end_at is not None and partition.end_at <= cutoff_start]
    starts = [partition.start_at for partition in partitions if partition.start_at is not None]
    ends = [partition.end_at for partition in partitions if partition.end_at is not None]
    return {
        "name": family.name,
        "physical_table": family.parent_table,
        "retention_days": family.retention_days,
        "partition_count": len(partitions),
        "oldest_partition_start": render_value(min(starts) if starts else None),
        "newest_partition_end": render_value(max(ends) if ends else None),
        "current_partition_ready": current_partition_ready,
        "current_partition_day": today.isoformat(),
        "future_partition_days": future_partition_days,
        "required_future_partition_days": required_future_days,
        "missing_current_partition": None if current_partition_ready else today.isoformat(),
        "missing_future_partition_days": missing_future_days,
        "expired_partition_count": len(expired),
        "expired_partitions": [partition_to_record(partition) for partition in expired],
        "partitions": [partition_to_record(partition) for partition in partitions],
    }


def maintain_partitions(
    session: Any,
    family: MarketTickPartitionFamily,
    *,
    as_of: date | datetime | None = None,
    future_days: int = DEFAULT_FUTURE_PARTITION_DAYS,
    dry_run: bool = True,
) -> dict[str, Any]:
    today = _utc_day(as_of)
    before_partitions = list_market_tick_partitions(session, family)
    existing_days = {partition.day for partition in before_partitions if partition.day is not None}
    create_days = [day for day in maintenance_partition_days(as_of=today, future_days=future_days) if day not in existing_days]
    created_partitions = [
        {
            "partition_name": partition_name_for_day(family, day),
            "day": day.isoformat(),
            "start_at": render_value(partition_day_bounds(day)[0]),
            "end_at": render_value(partition_day_bounds(day)[1]),
        }
        for day in create_days
    ]
    if not dry_run:
        for day in create_days:
            session.execute(text(create_partition_sql(family, day)))
        session.flush()

    current_partitions = before_partitions if dry_run else list_market_tick_partitions(session, family)
    cutoff_start, _ = partition_day_bounds(today - timedelta(days=family.retention_days))
    expired_partitions = [partition for partition in current_partitions if partition.end_at is not None and partition.end_at <= cutoff_start]
    if not dry_run:
        for partition in expired_partitions:
            session.execute(text(drop_partition_sql(partition.partition_name)))
        session.flush()

    status = partition_status(session, family, as_of=today, future_days=future_days)
    return {
        **{key: value for key, value in status.items() if key != "partitions"},
        "created_partition_count": len(created_partitions),
        "created_partitions": created_partitions,
        "expired_partition_count": len(expired_partitions),
        "expired_partitions": [partition_to_record(partition) for partition in expired_partitions],
        "dropped_partition_count": 0 if dry_run else len(expired_partitions),
        "dropped_partitions": [] if dry_run else [partition_to_record(partition) for partition in expired_partitions],
    }


__all__ = [
    "DEFAULT_FUTURE_PARTITION_DAYS",
    "DEFAULT_OPTION_QUOTE_TICK_RETENTION_DAYS",
    "DEFAULT_OPTION_TRADE_TICK_RETENTION_DAYS",
    "MARKET_TICK_PARTITION_FAMILIES",
    "MarketTickPartition",
    "MarketTickPartitionFamily",
    "create_partition_sql",
    "drop_partition_sql",
    "initial_partition_days",
    "list_market_tick_partitions",
    "maintain_partitions",
    "market_tick_partition_families",
    "partition_day_bounds",
    "partition_name_for_day",
    "partition_status",
    "render_partition_bound",
]
