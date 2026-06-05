from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, text

from core.services.retention import build_retention_status
from core.storage.capture_models import CaptureSummaryModel
from core.storage.factory import build_storage_context
from core.storage.serializers import render_value

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
            "data_class": "capture_summaries",
            "schema_ready": True,
            "retention_days": None,
            "partition_count": None,
            "current_partition_ready": None,
            "future_partition_days": None,
            "required_future_partition_days": None,
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


def build_storage_ops_state(
    *,
    db_target: str | None = None,
    storage: Any | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    resolved_now = now or datetime.now(UTC)
    payload = build_retention_status(db_target=db_target, now=resolved_now)
    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})
    attention: list[dict[str, str]] = []

    missing_current = [str(value) for value in list(summary.get("missing_current_partitions") or [])]
    if missing_current:
        attention.append(
            _attention(
                severity="high",
                code="tick_partition_current_missing",
                message=f"Current-day tick partitions are missing for: {', '.join(missing_current)}.",
            )
        )

    future_short = [str(value) for value in list(summary.get("future_partition_short_tables") or [])]
    if future_short:
        attention.append(
            _attention(
                severity="medium",
                code="tick_partition_future_short",
                message=f"Future tick partition coverage is short for: {', '.join(future_short)}.",
            )
        )

    if summary.get("latest_run_status") == "failed":
        attention.append(
            _attention(
                severity="medium",
                code="retention_latest_run_failed",
                message="The latest tick partition maintenance run failed.",
            )
        )

    capture_status, capture_row, capture_attention = _capture_summaries_storage_row(db_target=db_target, storage=storage)
    attention.extend(capture_attention)
    tables = [dict(row) for row in list(details.get("tables") or [])]
    tables.append(capture_row)
    details["tables"] = tables
    summary["table_count"] = len(tables)
    summary["total_size_bytes"] = sum(int(row.get("total_size_bytes") or 0) for row in tables)
    summary["estimated_live_rows"] = sum(int(row.get("estimated_live_rows") or 0) for row in tables)
    summary["estimated_dead_rows"] = sum(int(row.get("estimated_dead_rows") or 0) for row in tables)

    return {
        **payload,
        "status": _combine_statuses(str(payload.get("status") or "unknown"), capture_status),
        "summary": summary,
        "details": details,
        "generated_at": payload.get("generated_at") or resolved_now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "attention": attention,
    }


__all__ = ["build_storage_ops_state"]
