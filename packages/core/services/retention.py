from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text

from core.storage.factory import build_storage_context
from core.storage.market_tick_partitions import (
    DEFAULT_FUTURE_PARTITION_DAYS,
    DEFAULT_OPTION_QUOTE_TICK_RETENTION_DAYS,
    DEFAULT_OPTION_TRADE_TICK_RETENTION_DAYS,
    maintain_partitions,
    market_tick_partition_families,
    partition_status,
)
from core.storage.serializers import render_value

DEFAULT_RETENTION_LOG_NAME = "retention.log"

RETENTION_TABLES = (
    "option_quote_ticks",
    "option_trade_ticks",
)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def retention_defaults() -> dict[str, int]:
    return {
        "option_quote_tick_days": _env_int(
            "SPREADS_OPTION_QUOTE_TICK_RETENTION_DAYS",
            DEFAULT_OPTION_QUOTE_TICK_RETENTION_DAYS,
        ),
        "option_trade_tick_days": _env_int(
            "SPREADS_OPTION_TRADE_TICK_RETENTION_DAYS",
            DEFAULT_OPTION_TRADE_TICK_RETENTION_DAYS,
        ),
        "future_partition_days": _env_int(
            "SPREADS_TICK_FUTURE_PARTITION_DAYS",
            DEFAULT_FUTURE_PARTITION_DAYS,
        ),
    }


def _validated_days(value: int, *, field_name: str) -> int:
    if value < 1:
        raise ValueError(f"{field_name} must be at least 1 day.")
    return value


def _validated_future_days(value: int, *, field_name: str = "future_partition_days") -> int:
    if value < 0:
        raise ValueError(f"{field_name} must be zero or greater.")
    return value


def _retention_log_path() -> Path | None:
    explicit = os.environ.get("SPREADS_RETENTION_LOG")
    if explicit:
        return Path(explicit)
    log_dir = os.environ.get("SPREADS_OPS_LOG_DIR")
    if log_dir:
        return Path(log_dir) / DEFAULT_RETENTION_LOG_NAME
    return None


def _read_latest_json_payload(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        payload = path.read_text(errors="replace")
    except OSError:
        return None
    decoder = json.JSONDecoder()
    latest: dict[str, Any] | None = None
    for index in range(len(payload) - 1, -1, -1):
        if payload[index] != "{":
            continue
        try:
            value, _ = decoder.raw_decode(payload[index:])
        except ValueError:
            continue
        if isinstance(value, dict) and "tables" in value and "total_dropped_partition_count" in value:
            latest = value
            break
    return latest


def _table_storage_stats(session: Any) -> dict[str, dict[str, Any]]:
    quoted_names = ",".join(f"'{name}'" for name in RETENTION_TABLES)
    statement = text(f"""
        WITH RECURSIVE rel_tree(parent_name, relid) AS (
          SELECT parent.relname AS parent_name, parent.oid AS relid
          FROM pg_class parent
          JOIN pg_namespace namespace ON namespace.oid = parent.relnamespace
          WHERE namespace.nspname = 'public'
            AND parent.relname IN ({quoted_names})
          UNION ALL
          SELECT rel_tree.parent_name, child.oid AS relid
          FROM rel_tree
          JOIN pg_inherits inherited ON inherited.inhparent = rel_tree.relid
          JOIN pg_class child ON child.oid = inherited.inhrelid
          JOIN pg_namespace child_namespace ON child_namespace.oid = child.relnamespace
          WHERE child_namespace.nspname = 'public'
        )
        SELECT
          rel_tree.parent_name AS relname,
          COALESCE(SUM(stats.n_live_tup), 0) AS n_live_tup,
          COALESCE(SUM(stats.n_dead_tup), 0) AS n_dead_tup,
          MAX(stats.last_vacuum) AS last_vacuum,
          MAX(stats.last_autovacuum) AS last_autovacuum,
          MAX(stats.last_analyze) AS last_analyze,
          MAX(stats.last_autoanalyze) AS last_autoanalyze,
          COALESCE(SUM(pg_total_relation_size(rel_tree.relid)), 0) AS total_bytes,
          COALESCE(SUM(pg_relation_size(rel_tree.relid)), 0) AS heap_bytes,
          COALESCE(SUM(pg_indexes_size(rel_tree.relid)), 0) AS index_bytes,
          COUNT(*) AS relation_count
        FROM rel_tree
        LEFT JOIN pg_stat_user_tables stats ON stats.relid = rel_tree.relid
        GROUP BY rel_tree.parent_name
        """)
    rows = session.execute(statement).mappings().all()
    return {
        str(row["relname"]): {
            "estimated_live_rows": int(row["n_live_tup"] or 0),
            "estimated_dead_rows": int(row["n_dead_tup"] or 0),
            "last_vacuum": render_value(row["last_vacuum"]),
            "last_autovacuum": render_value(row["last_autovacuum"]),
            "last_analyze": render_value(row["last_analyze"]),
            "last_autoanalyze": render_value(row["last_autoanalyze"]),
            "total_size_bytes": int(row["total_bytes"] or 0),
            "heap_size_bytes": int(row["heap_bytes"] or 0),
            "index_size_bytes": int(row["index_bytes"] or 0),
            "relation_count": int(row["relation_count"] or 0),
        }
        for row in rows
    }


def _latest_maintenance_by_table(latest_run: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(latest_run, dict):
        return {}
    return {str(row.get("name")): dict(row) for row in list(latest_run.get("tables") or []) if isinstance(row, dict) and row.get("name")}


def _families(
    *,
    option_quote_tick_days: int,
    option_trade_tick_days: int,
) -> tuple[Any, ...]:
    return market_tick_partition_families(
        option_quote_tick_days=_validated_days(
            option_quote_tick_days,
            field_name="option_quote_tick_days",
        ),
        option_trade_tick_days=_validated_days(
            option_trade_tick_days,
            field_name="option_trade_tick_days",
        ),
    )


def prune_retained_data(
    *,
    db_target: str | None = None,
    dry_run: bool = True,
    option_quote_tick_days: int | None = None,
    option_trade_tick_days: int | None = None,
    future_partition_days: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    defaults = retention_defaults()
    resolved_quote_days = option_quote_tick_days or defaults["option_quote_tick_days"]
    resolved_trade_days = option_trade_tick_days or defaults["option_trade_tick_days"]
    resolved_future_days = _validated_future_days(
        int(future_partition_days if future_partition_days is not None else defaults["future_partition_days"])
    )
    resolved_now = now or datetime.now(UTC)
    families = _families(
        option_quote_tick_days=resolved_quote_days,
        option_trade_tick_days=resolved_trade_days,
    )

    with build_storage_context(db_target) as storage:
        missing_tables = [family.parent_table for family in families if not storage.history.schema_has_tables(family.parent_table)]
        if missing_tables:
            raise RuntimeError(f"Tick storage tables are missing: {', '.join(missing_tables)}.")

        table_results: list[dict[str, Any]] = []
        with storage.history.session_scope() as session:
            for family in families:
                table_results.append(
                    maintain_partitions(
                        session,
                        family,
                        as_of=resolved_now,
                        future_days=resolved_future_days,
                        dry_run=dry_run,
                    )
                )

    return {
        "status": "dry_run" if dry_run else "maintained",
        "dry_run": dry_run,
        "generated_at": render_value(resolved_now),
        "future_partition_days": resolved_future_days,
        "tables": table_results,
        "total_created_partition_count": sum(int(row["created_partition_count"]) for row in table_results),
        "total_expired_partition_count": sum(int(row["expired_partition_count"]) for row in table_results),
        "total_dropped_partition_count": sum(int(row["dropped_partition_count"]) for row in table_results),
    }


def build_retention_status(
    *,
    db_target: str | None = None,
    include_pending_counts: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    defaults = retention_defaults()
    resolved_now = now or datetime.now(UTC)
    resolved_future_days = _validated_future_days(defaults["future_partition_days"])
    log_path = _retention_log_path()
    latest_run = _read_latest_json_payload(log_path)
    latest_by_table = _latest_maintenance_by_table(latest_run)
    families = _families(
        option_quote_tick_days=defaults["option_quote_tick_days"],
        option_trade_tick_days=defaults["option_trade_tick_days"],
    )

    with build_storage_context(db_target) as storage:
        missing_tables = [family.parent_table for family in families if not storage.history.schema_has_tables(family.parent_table)]
        if missing_tables:
            raise RuntimeError(f"Tick storage tables are missing: {', '.join(missing_tables)}.")

        table_results: list[dict[str, Any]] = []
        with storage.history.session_scope() as session:
            storage_stats = _table_storage_stats(session)
            for family in families:
                status = partition_status(
                    session,
                    family,
                    as_of=resolved_now,
                    future_days=resolved_future_days,
                )
                result = {
                    **status,
                    "data_class": "option_quotes" if family.name == "option_quote_ticks" else "option_trades",
                    "latest_maintenance": latest_by_table.get(family.name),
                    **dict(storage_stats.get(family.parent_table) or {}),
                }
                if not include_pending_counts:
                    result.pop("partitions", None)
                table_results.append(result)

    latest_run_status = "missing" if latest_run is None else str(latest_run.get("status") or "unknown").strip().lower()
    missing_current = [str(row["name"]) for row in table_results if not bool(row.get("current_partition_ready"))]
    future_short = [
        str(row["name"]) for row in table_results if int(row.get("future_partition_days") or 0) < int(row.get("required_future_partition_days") or 0)
    ]
    if missing_current:
        status = "blocked"
    elif latest_run_status == "failed" or future_short:
        status = "degraded"
    else:
        status = "healthy"

    latest_created = 0 if latest_run is None else int(latest_run.get("total_created_partition_count") or 0)
    latest_expired = 0 if latest_run is None else int(latest_run.get("total_expired_partition_count") or 0)
    latest_dropped = 0 if latest_run is None else int(latest_run.get("total_dropped_partition_count") or 0)
    return {
        "status": status,
        "generated_at": render_value(resolved_now),
        "summary": {
            "latest_run_status": latest_run_status,
            "latest_run_at": None if latest_run is None else latest_run.get("generated_at"),
            "latest_created_partition_count": latest_created,
            "latest_expired_partition_count": latest_expired,
            "latest_dropped_partition_count": latest_dropped,
            "partition_ready": not missing_current and not future_short,
            "missing_current_partitions": missing_current,
            "future_partition_short_tables": future_short,
            "future_partition_days": min((int(row.get("future_partition_days") or 0) for row in table_results), default=0),
            "required_future_partition_days": resolved_future_days,
            "retention_log_path": None if log_path is None else str(log_path),
            "schedule": "30 22 * * 1-5",
            "market_hours_safe": True,
            "table_count": len(table_results),
            "total_size_bytes": sum(int(row.get("total_size_bytes") or 0) for row in table_results),
            "estimated_live_rows": sum(int(row.get("estimated_live_rows") or 0) for row in table_results),
            "estimated_dead_rows": sum(int(row.get("estimated_dead_rows") or 0) for row in table_results),
        },
        "details": {
            "defaults": defaults,
            "latest_run": latest_run,
            "tables": table_results,
            "maintenance": {
                "lock_profile": "retention creates future tick partitions and drops expired child partitions off-hours; it does not row-delete tick parents",
                "default_state_uses_partition_catalog": True,
            },
        },
    }


__all__ = ["build_retention_status", "prune_retained_data", "retention_defaults"]
