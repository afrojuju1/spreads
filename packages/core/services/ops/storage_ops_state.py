from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from core.db.decorators import with_storage
from core.services.retention import (
    _latest_prune_by_table,
    _read_latest_json_payload,
    _retention_log_path,
    _table_storage_stats,
    _vacuum_full_signal,
    retention_defaults,
)
from core.storage.serializers import render_value

from .shared import _attention, _combine_statuses

STORAGE_POLICIES = (
    {
        "name": "option_quote_ticks",
        "physical_table": "option_quote_ticks",
        "retention_days_key": "option_quote_tick_days",
        "data_class": "option_quotes",
    },
    {
        "name": "option_trade_ticks",
        "physical_table": "option_trade_ticks",
        "retention_days_key": "option_trade_tick_days",
        "data_class": "option_trades",
    },
)


@with_storage()
def build_storage_ops_state(
    *,
    db_target: str | None = None,
    storage: Any | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    resolved_now = now or datetime.now(UTC)
    generated_at = render_value(resolved_now)
    defaults = retention_defaults()
    log_path = _retention_log_path()
    latest_run = _read_latest_json_payload(log_path)
    latest_by_table = _latest_prune_by_table(latest_run)
    attention: list[dict[str, str]] = []
    statuses: list[str] = []

    missing_tables: list[str] = []
    if not storage.history.schema_has_tables("option_quote_ticks"):
        missing_tables.append("option_quote_ticks")
    if not storage.history.schema_has_tables("option_trade_ticks"):
        missing_tables.append("option_trade_ticks")
    if missing_tables:
        attention.append(
            _attention(
                severity="high",
                code="storage_schema_unavailable",
                message=f"Storage tables are missing: {', '.join(missing_tables)}.",
            )
        )
        return {
            "status": "blocked",
            "generated_at": generated_at,
            "summary": {
                "latest_run_status": None if latest_run is None else latest_run.get("status"),
                "latest_run_at": None if latest_run is None else latest_run.get("generated_at"),
                "missing_tables": missing_tables,
                "retention_log_path": None if log_path is None else str(log_path),
                "vacuum_full_pending": False,
                "vacuum_full_pending_tables": [],
                "schedule": "30 22 * * 1-5",
                "market_hours_safe": True,
            },
            "attention": attention,
            "details": {
                "defaults": defaults,
                "latest_run": latest_run,
                "tables": [],
                "maintenance": {},
            },
        }

    table_results: list[dict[str, Any]] = []
    physical_vacuum: dict[str, dict[str, Any]] = {}
    with storage.history.session_scope() as session:
        storage_stats = _table_storage_stats(session)
        for policy in STORAGE_POLICIES:
            physical_name = str(policy["physical_table"])
            logical_name = str(policy["name"])
            stats = dict(storage_stats.get(physical_name) or {})
            latest_prune = latest_by_table.get(logical_name)
            vacuum_signal = _vacuum_full_signal(
                table=stats,
                latest_prune=latest_prune,
            )
            existing_signal = physical_vacuum.get(physical_name)
            if existing_signal is None or (vacuum_signal["pending"] and not existing_signal["pending"]):
                physical_vacuum[physical_name] = vacuum_signal
            table_results.append(
                {
                    "name": logical_name,
                    "physical_table": physical_name,
                    "data_class": policy["data_class"],
                    "retention_days": defaults[str(policy["retention_days_key"])],
                    "latest_prune": latest_prune,
                    "vacuum_full": vacuum_signal,
                    **stats,
                }
            )

    latest_run_status = "missing" if latest_run is None else str(latest_run.get("status") or "unknown").strip().lower()
    vacuum_full_pending_tables = [name for name, signal in physical_vacuum.items() if bool(signal.get("pending"))]
    if latest_run_status == "failed":
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="retention_latest_run_failed",
                message="The latest retention prune run failed.",
            )
        )
    if vacuum_full_pending_tables:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="vacuum_full_pending",
                message=f"Off-hours VACUUM FULL is pending for: {', '.join(vacuum_full_pending_tables)}.",
            )
        )

    summary = {
        "latest_run_status": latest_run_status,
        "latest_run_at": None if latest_run is None else latest_run.get("generated_at"),
        "latest_deleted_count": 0 if latest_run is None else int(latest_run.get("total_deleted_count") or 0),
        "latest_matching_count": 0 if latest_run is None else int(latest_run.get("total_matching_count") or 0),
        "vacuum_full_pending": bool(vacuum_full_pending_tables),
        "vacuum_full_pending_tables": vacuum_full_pending_tables,
        "retention_log_path": None if log_path is None else str(log_path),
        "schedule": "30 22 * * 1-5",
        "market_hours_safe": True,
        "table_count": len(table_results),
        "total_size_bytes": sum(int(row.get("total_size_bytes") or 0) for row in table_results),
        "estimated_live_rows": sum(int(row.get("estimated_live_rows") or 0) for row in table_results),
        "estimated_dead_rows": sum(int(row.get("estimated_dead_rows") or 0) for row in table_results),
    }
    return {
        "status": _combine_statuses(*(statuses or ["healthy"])),
        "generated_at": generated_at,
        "summary": summary,
        "attention": attention,
        "details": {
            "defaults": defaults,
            "latest_run": latest_run,
            "tables": table_results,
            "maintenance": {
                "vacuum_full_pending": bool(vacuum_full_pending_tables),
                "vacuum_full_pending_tables": vacuum_full_pending_tables,
                "vacuum_full_runbook": "spr-f0m",
                "lock_profile": "retention uses batched DELETEs; VACUUM FULL remains a separate off-hours strong-lock maintenance task",
                "default_state_uses_pending_counts": False,
            },
        },
    }


__all__ = ["build_storage_ops_state"]
