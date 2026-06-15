from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, text

from core.storage.capture_models import CaptureSummaryModel
from core.storage.factory import build_market_data_store, build_storage_context
from core.storage.serializers import render_value
from core.value_coercion import utc_iso

from .shared import _attention, _combine_statuses


def _table_storage_stats(session: Any, table_name: str) -> dict[str, Any]:
    statement = text("""
        SELECT
          COALESCE(stats.n_live_tup, 0) AS n_live_tup,
          COALESCE(stats.n_dead_tup, 0) AS n_dead_tup,
          stats.last_vacuum,
          stats.last_autovacuum,
          stats.last_analyze,
          stats.last_autoanalyze,
          pg_total_relation_size(table_class.oid) AS total_bytes,
          pg_relation_size(table_class.oid) AS heap_bytes,
          pg_indexes_size(table_class.oid) AS index_bytes
        FROM pg_class table_class
        JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace
        LEFT JOIN pg_stat_user_tables stats ON stats.relid = table_class.oid
        WHERE namespace.nspname = 'public'
          AND table_class.relname = :table_name
        """)
    row = session.execute(statement, {"table_name": table_name}).mappings().first()
    if row is None:
        return {}
    return {
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


def _capture_summaries_storage_row(*, db_target: str | None, storage: Any | None) -> tuple[str, dict[str, Any], list[dict[str, str]]]:
    owns_context = storage is None
    resolved_storage = build_storage_context(db_target) if storage is None else storage
    try:
        if not resolved_storage.capture.schema_has_tables("capture_summaries"):
            return (
                "blocked",
                {
                    "name": "capture_summaries",
                    "physical_table": "capture_summaries",
                    "data_class": "capture_summaries",
                    "schema_ready": False,
                },
                [
                    _attention(
                        severity="high",
                        code="capture_summaries_missing",
                        message="capture_summaries table is missing.",
                    )
                ],
            )

        with resolved_storage.capture.session_factory() as session:
            stats = _table_storage_stats(session, "capture_summaries")
            latest = session.scalars(select(CaptureSummaryModel).order_by(CaptureSummaryModel.captured_at.desc()).limit(1)).first()
        latest_summary = None if latest is None else resolved_storage.capture.row(latest)
        latest_status = None if latest_summary is None else latest_summary.get("status")
        row = {
            "name": "capture_summaries",
            "physical_table": "capture_summaries",
            "database": "postgres",
            "engine": "PostgreSQL",
            "data_class": "capture_summaries",
            "schema_ready": True,
            "retention_owner": "postgres_ops_state",
            "latest_capture_summary_id": None if latest_summary is None else latest_summary.get("capture_summary_id"),
            "latest_capture_status": latest_status,
            "latest_captured_at": None if latest_summary is None else latest_summary.get("captured_at"),
            "latest_quote_rows_saved": 0 if latest_summary is None else int(latest_summary.get("quote_rows_saved") or 0),
            "latest_trade_rows_saved": 0 if latest_summary is None else int(latest_summary.get("trade_rows_saved") or 0),
            **stats,
        }
        status = "idle" if latest_summary is None else ("healthy" if str(latest_status or "").lower() in {"ok", "idle"} else "degraded")
        return status, row, []
    finally:
        if owns_context:
            resolved_storage.close()


def _market_data_storage_payload() -> tuple[str, dict[str, Any], dict[str, Any], list[dict[str, str]]]:
    store = build_market_data_store()
    try:
        payload = store.storage_status()
    except Exception as exc:
        return (
            "blocked",
            {
                "market_data_database": None,
                "market_data_url": None,
                "retention_owner": "clickhouse_ttl",
                "market_data_table_count": 0,
                "market_data_tables_ready": False,
                "missing_market_data_tables": [],
                "total_size_bytes": 0,
                "estimated_live_rows": 0,
                "estimated_dead_rows": 0,
                "inactive_part_count": 0,
            },
            {
                "tables": [],
                "maintenance": {
                    "retention_owner": "clickhouse_ttl",
                    "lock_profile": "ClickHouse market-data health could not be read.",
                    "manual_prune_command": None,
                    "default_state_uses_partition_catalog": False,
                },
                "market_data_error": str(exc),
            },
            [
                _attention(
                    severity="high",
                    code="clickhouse_market_data_unavailable",
                    message=f"ClickHouse market-data storage is unavailable: {exc}",
                )
            ],
        )
    finally:
        store.close()

    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})
    attention: list[dict[str, str]] = []
    missing_tables = [str(value) for value in list(summary.get("missing_market_data_tables") or [])]
    if missing_tables:
        attention.append(
            _attention(
                severity="high",
                code="clickhouse_market_data_tables_missing",
                message=f"ClickHouse market-data tables are missing: {', '.join(missing_tables)}.",
            )
        )
    return str(payload.get("status") or "unknown"), summary, details, attention


def build_storage_ops_state(
    *,
    db_target: str | None = None,
    storage: Any | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    resolved_now = now or datetime.now(UTC)
    market_data_status, summary, details, attention = _market_data_storage_payload()

    capture_status, capture_row, capture_attention = _capture_summaries_storage_row(db_target=db_target, storage=storage)
    attention.extend(capture_attention)
    tables = [dict(row) for row in list(details.get("tables") or [])]
    tables.append(capture_row)
    details["tables"] = tables
    market_data_total_size_bytes = int(summary.pop("total_size_bytes", 0) or 0)
    market_data_estimated_live_rows = int(summary.pop("estimated_live_rows", 0) or 0)
    market_data_estimated_dead_rows = int(summary.pop("estimated_dead_rows", 0) or 0)
    market_data_inactive_part_count = int(summary.pop("inactive_part_count", 0) or 0)

    latest_capture_status = capture_row.get("latest_capture_status")
    if latest_capture_status is not None and str(latest_capture_status).lower() not in {"ok", "idle"}:
        attention.append(
            _attention(
                severity="medium",
                code="latest_capture_summary_degraded",
                message=f"The latest market-recorder capture summary status is {latest_capture_status}.",
            )
        )

    summary["latest_capture_summary_id"] = capture_row.get("latest_capture_summary_id")
    summary["latest_capture_status"] = latest_capture_status
    summary["latest_captured_at"] = capture_row.get("latest_captured_at")
    summary["latest_quote_rows_saved"] = capture_row.get("latest_quote_rows_saved")
    summary["latest_trade_rows_saved"] = capture_row.get("latest_trade_rows_saved")
    summary["market_data_total_size_bytes"] = market_data_total_size_bytes
    summary["market_data_estimated_live_rows"] = market_data_estimated_live_rows
    summary["market_data_estimated_dead_rows"] = market_data_estimated_dead_rows
    summary["market_data_inactive_part_count"] = market_data_inactive_part_count
    summary["storage_table_count"] = len(tables)
    summary["storage_total_size_bytes"] = sum(int(row.get("total_size_bytes") or 0) for row in tables)
    summary["storage_estimated_live_rows"] = sum(int(row.get("estimated_live_rows") or 0) for row in tables)
    summary["storage_estimated_dead_rows"] = sum(int(row.get("estimated_dead_rows") or 0) for row in tables)
    summary["schedule"] = "clickhouse_ttl_background"
    summary["market_hours_safe"] = True

    return {
        "status": _combine_statuses(market_data_status, capture_status),
        "summary": summary,
        "details": details,
        "generated_at": utc_iso(resolved_now),
        "attention": attention,
    }


__all__ = ["build_storage_ops_state"]
