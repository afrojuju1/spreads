from __future__ import annotations

import os
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import delete, func, not_, or_, select, text

from core.storage.event_models import EventLogModel
from core.storage.factory import build_storage_context
from core.storage.models import OptionQuoteEventModel, OptionTradeEventModel
from core.storage.serializers import render_value

RAW_MARKET_EVENT_TOPICS = frozenset(
    {
        "market.quote.captured",
        "market.trade.captured",
    }
)

DEFAULT_OPTION_QUOTE_RETENTION_DAYS = 7
DEFAULT_OPTION_TRADE_RETENTION_DAYS = 30
DEFAULT_EVENT_LOG_MARKET_RETENTION_DAYS = 14
DEFAULT_EVENT_LOG_CONTROL_RETENTION_DAYS = 180
DEFAULT_RETENTION_BATCH_SIZE = 50_000
DEFAULT_RETENTION_MAX_BATCHES = 20
DEFAULT_RETENTION_LOG_NAME = "retention.log"
VACUUM_FULL_SIZE_THRESHOLD_BYTES = 1_000_000_000
VACUUM_FULL_DEAD_TUPLE_THRESHOLD = 1_000_000
VACUUM_FULL_DEAD_TUPLE_RATIO = 0.2

RETENTION_TABLES = (
    "option_quote_events",
    "option_trade_events",
    "event_log",
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
        "option_quote_days": _env_int(
            "SPREADS_OPTION_QUOTE_RETENTION_DAYS",
            DEFAULT_OPTION_QUOTE_RETENTION_DAYS,
        ),
        "option_trade_days": _env_int(
            "SPREADS_OPTION_TRADE_RETENTION_DAYS",
            DEFAULT_OPTION_TRADE_RETENTION_DAYS,
        ),
        "event_log_market_days": _env_int(
            "SPREADS_EVENT_LOG_MARKET_RETENTION_DAYS",
            DEFAULT_EVENT_LOG_MARKET_RETENTION_DAYS,
        ),
        "event_log_control_days": _env_int(
            "SPREADS_EVENT_LOG_CONTROL_RETENTION_DAYS",
            DEFAULT_EVENT_LOG_CONTROL_RETENTION_DAYS,
        ),
        "batch_size": _env_int(
            "SPREADS_RETENTION_BATCH_SIZE",
            DEFAULT_RETENTION_BATCH_SIZE,
        ),
        "max_batches": _env_int(
            "SPREADS_RETENTION_MAX_BATCHES",
            DEFAULT_RETENTION_MAX_BATCHES,
        ),
    }


def _validated_days(value: int, *, field_name: str) -> int:
    if value < 1:
        raise ValueError(f"{field_name} must be at least 1 day.")
    return value


def _cutoff(days: int, *, now: datetime) -> datetime:
    return now - timedelta(days=_validated_days(days, field_name="retention days"))


def _count(session: Any, model: Any, condition: Any) -> int:
    return int(
        session.scalar(select(func.count()).select_from(model).where(condition)) or 0
    )


def _delete_batches(
    session: Any,
    *,
    model: Any,
    id_column: Any,
    condition: Any,
    batch_size: int,
    max_batches: int,
) -> int:
    deleted = 0
    for _ in range(max_batches):
        subquery = select(id_column).where(condition).limit(batch_size)
        result = session.execute(delete(model).where(id_column.in_(subquery)))
        rowcount = int(result.rowcount or 0)
        deleted += rowcount
        if rowcount < batch_size:
            break
    return deleted


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
        if (
            isinstance(value, dict)
            and "tables" in value
            and "total_deleted_count" in value
        ):
            latest = value
            break
    return latest


def _table_storage_stats(session: Any) -> dict[str, dict[str, Any]]:
    quoted_names = ",".join(f"'{name}'" for name in RETENTION_TABLES)
    statement = text(
        f"""
        SELECT
          relname,
          n_live_tup,
          n_dead_tup,
          last_vacuum,
          last_autovacuum,
          last_analyze,
          last_autoanalyze,
          pg_total_relation_size(relid) AS total_bytes,
          pg_relation_size(relid) AS heap_bytes,
          pg_indexes_size(relid) AS index_bytes
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
          AND relname IN ({quoted_names})
        """
    )
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
        }
        for row in rows
    }


def _retained_range(
    session: Any,
    *,
    model: Any,
    timestamp_column: Any,
    condition: Any | None = None,
) -> tuple[Any, Any]:
    statement = select(func.min(timestamp_column), func.max(timestamp_column)).select_from(
        model
    )
    if condition is not None:
        statement = statement.where(condition)
    row = session.execute(statement).one()
    return row[0], row[1]


def _latest_prune_by_table(latest_run: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(latest_run, dict):
        return {}
    return {
        str(row.get("name")): dict(row)
        for row in list(latest_run.get("tables") or [])
        if isinstance(row, dict) and row.get("name")
    }


def _vacuum_full_signal(
    *,
    table: dict[str, Any],
    latest_prune: dict[str, Any] | None,
) -> dict[str, Any]:
    live_rows = int(table.get("estimated_live_rows") or 0)
    dead_rows = int(table.get("estimated_dead_rows") or 0)
    total_size_bytes = int(table.get("total_size_bytes") or 0)
    latest_deleted = int((latest_prune or {}).get("deleted_count") or 0)
    dead_ratio = 0.0 if live_rows <= 0 else dead_rows / max(live_rows, 1)
    pending = (
        total_size_bytes >= VACUUM_FULL_SIZE_THRESHOLD_BYTES
        and (
            latest_deleted > 0
            or dead_rows >= VACUUM_FULL_DEAD_TUPLE_THRESHOLD
            or dead_ratio >= VACUUM_FULL_DEAD_TUPLE_RATIO
        )
    )
    reasons: list[str] = []
    if latest_deleted > 0:
        reasons.append("latest_retention_deleted_rows")
    if dead_rows >= VACUUM_FULL_DEAD_TUPLE_THRESHOLD:
        reasons.append("dead_tuple_pressure")
    if dead_ratio >= VACUUM_FULL_DEAD_TUPLE_RATIO:
        reasons.append("dead_tuple_ratio")
    if total_size_bytes >= VACUUM_FULL_SIZE_THRESHOLD_BYTES:
        reasons.append("large_relation")
    return {
        "pending": pending,
        "dead_tuple_ratio": round(dead_ratio, 4),
        "latest_deleted_count": latest_deleted,
        "reasons": reasons,
    }


def _market_event_condition() -> Any:
    return or_(
        EventLogModel.event_class == "market_event",
        EventLogModel.topic.in_(tuple(RAW_MARKET_EVENT_TOPICS)),
    )


def prune_retained_data(
    *,
    db_target: str | None = None,
    dry_run: bool = True,
    option_quote_days: int | None = None,
    option_trade_days: int | None = None,
    event_log_market_days: int | None = None,
    event_log_control_days: int | None = None,
    batch_size: int | None = None,
    max_batches: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    defaults = retention_defaults()
    resolved_quote_days = _validated_days(
        option_quote_days or defaults["option_quote_days"],
        field_name="option_quote_days",
    )
    resolved_trade_days = _validated_days(
        option_trade_days or defaults["option_trade_days"],
        field_name="option_trade_days",
    )
    resolved_market_days = _validated_days(
        event_log_market_days or defaults["event_log_market_days"],
        field_name="event_log_market_days",
    )
    resolved_control_days = _validated_days(
        event_log_control_days or defaults["event_log_control_days"],
        field_name="event_log_control_days",
    )
    resolved_batch_size = max(int(batch_size or defaults["batch_size"]), 1)
    resolved_max_batches = max(int(max_batches or defaults["max_batches"]), 1)
    resolved_now = now or datetime.now(UTC)

    quote_cutoff = _cutoff(resolved_quote_days, now=resolved_now)
    trade_cutoff = _cutoff(resolved_trade_days, now=resolved_now)
    market_event_cutoff = _cutoff(resolved_market_days, now=resolved_now)
    control_event_cutoff = _cutoff(resolved_control_days, now=resolved_now)

    with build_storage_context(db_target) as storage:
        if not storage.history.schema_has_tables("option_quote_events"):
            raise RuntimeError("option_quote_events table is missing.")
        if not storage.history.schema_has_tables("option_trade_events"):
            raise RuntimeError("option_trade_events table is missing.")
        if not storage.events.schema_ready():
            raise RuntimeError("event_log table is missing.")

        market_condition = _market_event_condition()
        rows = [
            {
                "name": "option_quote_events",
                "retention_days": resolved_quote_days,
                "cutoff": quote_cutoff,
                "model": OptionQuoteEventModel,
                "id_column": OptionQuoteEventModel.quote_id,
                "condition": OptionQuoteEventModel.captured_at < quote_cutoff,
            },
            {
                "name": "option_trade_events",
                "retention_days": resolved_trade_days,
                "cutoff": trade_cutoff,
                "model": OptionTradeEventModel,
                "id_column": OptionTradeEventModel.trade_id,
                "condition": OptionTradeEventModel.captured_at < trade_cutoff,
            },
            {
                "name": "event_log_market_events",
                "retention_days": resolved_market_days,
                "cutoff": market_event_cutoff,
                "model": EventLogModel,
                "id_column": EventLogModel.event_id,
                "condition": market_condition
                & (EventLogModel.occurred_at < market_event_cutoff),
            },
            {
                "name": "event_log_control_events",
                "retention_days": resolved_control_days,
                "cutoff": control_event_cutoff,
                "model": EventLogModel,
                "id_column": EventLogModel.event_id,
                "condition": not_(market_condition)
                & (EventLogModel.occurred_at < control_event_cutoff),
            },
        ]

        table_results: list[dict[str, Any]] = []
        with storage.history.session_scope() as session:
            for row in rows:
                matching = _count(session, row["model"], row["condition"])
                deleted = (
                    0
                    if dry_run
                    else _delete_batches(
                        session,
                        model=row["model"],
                        id_column=row["id_column"],
                        condition=row["condition"],
                        batch_size=resolved_batch_size,
                        max_batches=resolved_max_batches,
                    )
                )
                table_results.append(
                    {
                        "name": row["name"],
                        "retention_days": row["retention_days"],
                        "cutoff": render_value(row["cutoff"]),
                        "matching_count": matching,
                        "deleted_count": deleted,
                    }
                )

    return {
        "status": "dry_run" if dry_run else "pruned",
        "dry_run": dry_run,
        "generated_at": render_value(resolved_now),
        "batch_size": resolved_batch_size,
        "max_batches": resolved_max_batches,
        "tables": table_results,
        "total_matching_count": sum(int(row["matching_count"]) for row in table_results),
        "total_deleted_count": sum(int(row["deleted_count"]) for row in table_results),
    }


def build_retention_status(
    *,
    db_target: str | None = None,
    include_pending_counts: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    defaults = retention_defaults()
    resolved_now = now or datetime.now(UTC)
    log_path = _retention_log_path()
    latest_run = _read_latest_json_payload(log_path)
    latest_by_table = _latest_prune_by_table(latest_run)

    with build_storage_context(db_target) as storage:
        if not storage.history.schema_has_tables("option_quote_events"):
            raise RuntimeError("option_quote_events table is missing.")
        if not storage.history.schema_has_tables("option_trade_events"):
            raise RuntimeError("option_trade_events table is missing.")
        if not storage.events.schema_ready():
            raise RuntimeError("event_log table is missing.")

        market_condition = _market_event_condition()
        rows = [
            {
                "name": "option_quote_events",
                "physical_table": "option_quote_events",
                "retention_days": defaults["option_quote_days"],
                "cutoff": _cutoff(defaults["option_quote_days"], now=resolved_now),
                "model": OptionQuoteEventModel,
                "timestamp_column": OptionQuoteEventModel.captured_at,
                "condition": None,
                "pending_condition": OptionQuoteEventModel.captured_at
                < _cutoff(defaults["option_quote_days"], now=resolved_now),
            },
            {
                "name": "option_trade_events",
                "physical_table": "option_trade_events",
                "retention_days": defaults["option_trade_days"],
                "cutoff": _cutoff(defaults["option_trade_days"], now=resolved_now),
                "model": OptionTradeEventModel,
                "timestamp_column": OptionTradeEventModel.captured_at,
                "condition": None,
                "pending_condition": OptionTradeEventModel.captured_at
                < _cutoff(defaults["option_trade_days"], now=resolved_now),
            },
            {
                "name": "event_log_market_events",
                "physical_table": "event_log",
                "retention_days": defaults["event_log_market_days"],
                "cutoff": _cutoff(defaults["event_log_market_days"], now=resolved_now),
                "model": EventLogModel,
                "timestamp_column": EventLogModel.occurred_at,
                "condition": market_condition,
                "pending_condition": market_condition
                & (
                    EventLogModel.occurred_at
                    < _cutoff(defaults["event_log_market_days"], now=resolved_now)
                ),
            },
            {
                "name": "event_log_control_events",
                "physical_table": "event_log",
                "retention_days": defaults["event_log_control_days"],
                "cutoff": _cutoff(defaults["event_log_control_days"], now=resolved_now),
                "model": EventLogModel,
                "timestamp_column": EventLogModel.occurred_at,
                "condition": not_(market_condition),
                "pending_condition": not_(market_condition)
                & (
                    EventLogModel.occurred_at
                    < _cutoff(defaults["event_log_control_days"], now=resolved_now)
                ),
            },
        ]

        table_results: list[dict[str, Any]] = []
        physical_vacuum: dict[str, dict[str, Any]] = {}
        with storage.history.session_scope() as session:
            storage_stats = _table_storage_stats(session)
            for row in rows:
                first_seen, last_seen = _retained_range(
                    session,
                    model=row["model"],
                    timestamp_column=row["timestamp_column"],
                    condition=row["condition"],
                )
                stats = dict(storage_stats.get(str(row["physical_table"])) or {})
                latest_prune = latest_by_table.get(str(row["name"]))
                vacuum_signal = _vacuum_full_signal(
                    table=stats,
                    latest_prune=latest_prune,
                )
                physical_name = str(row["physical_table"])
                existing_signal = physical_vacuum.get(physical_name)
                if existing_signal is None or (
                    vacuum_signal["pending"] and not existing_signal["pending"]
                ):
                    physical_vacuum[physical_name] = vacuum_signal

                result = {
                    "name": row["name"],
                    "physical_table": physical_name,
                    "retention_days": row["retention_days"],
                    "cutoff": render_value(row["cutoff"]),
                    "retained_from": render_value(first_seen),
                    "retained_to": render_value(last_seen),
                    "latest_prune": latest_prune,
                    "vacuum_full": vacuum_signal,
                    **stats,
                }
                if include_pending_counts:
                    result["pending_prune_count"] = _count(
                        session,
                        row["model"],
                        row["pending_condition"],
                    )
                table_results.append(result)

    latest_run_status = (
        "missing"
        if latest_run is None
        else str(latest_run.get("status") or "unknown").strip().lower()
    )
    vacuum_full_pending_tables = [
        name for name, signal in physical_vacuum.items() if bool(signal.get("pending"))
    ]
    status = "degraded" if latest_run_status == "failed" or vacuum_full_pending_tables else "healthy"
    return {
        "status": status,
        "generated_at": render_value(resolved_now),
        "summary": {
            "latest_run_status": latest_run_status,
            "latest_run_at": None if latest_run is None else latest_run.get("generated_at"),
            "latest_deleted_count": 0
            if latest_run is None
            else int(latest_run.get("total_deleted_count") or 0),
            "latest_matching_count": 0
            if latest_run is None
            else int(latest_run.get("total_matching_count") or 0),
            "vacuum_full_pending": bool(vacuum_full_pending_tables),
            "vacuum_full_pending_tables": vacuum_full_pending_tables,
            "retention_log_path": None if log_path is None else str(log_path),
            "schedule": "30 22 * * 1-5",
            "market_hours_safe": True,
        },
        "details": {
            "defaults": defaults,
            "latest_run": latest_run,
            "tables": table_results,
            "maintenance": {
                "vacuum_full_pending": bool(vacuum_full_pending_tables),
                "vacuum_full_pending_tables": vacuum_full_pending_tables,
                "vacuum_full_runbook": "spr-f0m",
                "lock_profile": (
                    "retention uses batched DELETEs; VACUUM FULL remains a separate "
                    "off-hours strong-lock maintenance task"
                ),
            },
        },
    }


__all__ = ["build_retention_status", "prune_retained_data", "retention_defaults"]
