from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from spreads.db.decorators import with_storage
from spreads.services.live_collector_health import enrich_live_collector_job_run_payload
from spreads.services.live_recovery import list_session_slot_health_by_session_id
from spreads.services.pipelines import (
    DEFAULT_ANALYSIS_PROFIT_TARGET,
    DEFAULT_ANALYSIS_STOP_MULTIPLE,
    _serialize_pipeline_summary,
    get_pipeline_detail,
)
from spreads.services.runtime_identity import (
    build_live_session_id,
    build_pipeline_id,
    parse_live_session_id,
)


def _resolve_session_identity(
    session_id: str,
    *,
    collector_store: Any,
    job_store: Any,
) -> dict[str, str] | None:
    resolved = parse_live_session_id(session_id)
    if resolved is not None:
        return {
            **resolved,
            "pipeline_id": build_pipeline_id(resolved["label"]),
        }

    latest_cycle = collector_store.get_latest_session_cycle(session_id)
    if latest_cycle is not None:
        label = str(latest_cycle["label"])
        session_date = str(latest_cycle["session_date"])
        return {
            "session_id": session_id,
            "label": label,
            "session_date": session_date,
            "pipeline_id": build_pipeline_id(label),
        }

    session_runs = job_store.list_job_runs(
        job_type="live_collector",
        session_id=session_id,
        limit=1,
    )
    if not session_runs:
        return None

    payload = (
        session_runs[0].get("payload")
        if isinstance(session_runs[0].get("payload"), Mapping)
        else {}
    )
    label = payload.get("label")
    session_date = payload.get("session_date")
    if not isinstance(label, str) or not isinstance(session_date, str):
        return None
    return {
        "session_id": session_id,
        "label": label,
        "session_date": session_date,
        "pipeline_id": build_pipeline_id(label),
    }


def _coerce_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _quote_capture_counts(latest_run: Mapping[str, Any] | None) -> dict[str, int]:
    quote_capture = (
        latest_run.get("quote_capture")
        if isinstance(latest_run, Mapping)
        and isinstance(latest_run.get("quote_capture"), Mapping)
        else {}
    )
    stream_quote_events_saved = _coerce_int(
        quote_capture.get("stream_quote_events_saved")
        or quote_capture.get("websocket_quote_events_saved")
    )
    return {
        "stream_quote_events_saved": stream_quote_events_saved,
        "websocket_quote_events_saved": stream_quote_events_saved,
        "baseline_quote_events_saved": _coerce_int(
            quote_capture.get("baseline_quote_events_saved")
        ),
        "recovery_quote_events_saved": _coerce_int(
            quote_capture.get("recovery_quote_events_saved")
        ),
    }


def _latest_pipeline_cycles(
    *,
    collector_store: Any,
    pipeline_rows: list[dict[str, Any]],
    market_date: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    if market_date is not None:
        resolved_cycles: list[dict[str, Any]] = []
        for pipeline in pipeline_rows:
            latest_cycle = collector_store.get_latest_pipeline_cycle(
                str(pipeline["pipeline_id"]),
                market_date=market_date,
            )
            if latest_cycle is not None:
                resolved_cycles.append(dict(latest_cycle))
        return resolved_cycles

    latest_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    per_pipeline_limit = max(limit * 10, 200)
    for pipeline in pipeline_rows:
        for cycle in collector_store.list_pipeline_cycles(
            pipeline_id=str(pipeline["pipeline_id"]),
            limit=per_pipeline_limit,
        ):
            cycle_payload = dict(cycle)
            key = (
                str(cycle_payload["pipeline_id"]),
                str(cycle_payload["market_date"]),
            )
            existing = latest_by_key.get(key)
            if existing is None:
                latest_by_key[key] = cycle_payload
                continue
            candidate_key = (
                str(cycle_payload.get("generated_at") or ""),
                str(cycle_payload.get("cycle_id") or ""),
            )
            existing_key = (
                str(existing.get("generated_at") or ""),
                str(existing.get("cycle_id") or ""),
            )
            if candidate_key > existing_key:
                latest_by_key[key] = cycle_payload
    return list(latest_by_key.values())


@with_storage()
def list_existing_sessions(
    *,
    db_target: str,
    limit: int = 100,
    session_date: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    collector_store = storage.collector
    if not collector_store.pipeline_schema_ready():
        return {"sessions": []}

    pipeline_rows = [
        dict(row) for row in collector_store.list_pipelines(limit=max(limit * 5, limit))
    ]
    latest_cycles = _latest_pipeline_cycles(
        collector_store=collector_store,
        pipeline_rows=pipeline_rows,
        market_date=session_date,
        limit=limit,
    )
    pipeline_by_id = {str(row["pipeline_id"]): row for row in pipeline_rows}
    session_ids = [
        build_live_session_id(str(row["label"]), str(row["market_date"]))
        for row in latest_cycles
    ]
    latest_runs_by_session_id = {
        str(row["session_id"]): enrich_live_collector_job_run_payload(row)
        for row in storage.jobs.list_latest_runs_by_session_ids(
            session_ids=session_ids,
            job_type="live_collector",
        )
        if row.get("session_id")
    }
    candidate_counts_by_cycle_id = collector_store.count_cycle_candidates_by_cycle_ids(
        [str(row["cycle_id"]) for row in latest_cycles]
    )
    alert_counts = storage.alerts.count_alert_events_by_session_keys(
        [(str(row["market_date"]), str(row["label"])) for row in latest_cycles]
    )

    slot_health_by_session_id = list_session_slot_health_by_session_id(
        recovery_store=storage.recovery,
        session_ids=session_ids,
        session_date=session_date,
    )

    pipeline_summaries: list[dict[str, Any]] = []
    for row in latest_cycles:
        pipeline = pipeline_by_id.get(str(row["pipeline_id"]))
        if pipeline is None:
            continue
        legacy_session_id = build_live_session_id(
            str(row["label"]),
            str(row["market_date"]),
        )
        pipeline_summaries.append(
            _serialize_pipeline_summary(
                pipeline=pipeline,
                latest_cycle=row,
                latest_run=latest_runs_by_session_id.get(legacy_session_id),
                slot_health=dict(
                    slot_health_by_session_id.get(legacy_session_id) or {}
                ),
                candidate_counts=candidate_counts_by_cycle_id.get(
                    str(row["cycle_id"]),
                    {},
                ),
                alert_count=int(
                    alert_counts.get((str(row["market_date"]), str(row["label"]))) or 0
                ),
            )
        )
    pipeline_summaries.sort(
        key=lambda row: (
            str(row.get("updated_at") or ""),
            str(row.get("legacy_session_id") or ""),
        ),
        reverse=True,
    )

    sessions: list[dict[str, Any]] = []
    for row in pipeline_summaries[:limit]:
        legacy_session_id = row.get("legacy_session_id")
        if legacy_session_id in (None, ""):
            continue
        latest_run = latest_runs_by_session_id.get(str(legacy_session_id))
        sessions.append(
            {
                "session_id": str(legacy_session_id),
                "label": str(row["label"]),
                "session_date": str(row["latest_market_date"]),
                "status": str(row.get("status") or "unknown"),
                "latest_slot_at": row.get("latest_slot_at"),
                "latest_slot_status": row.get("latest_slot_status"),
                "latest_capture_status": row.get("latest_capture_status"),
                **_quote_capture_counts(latest_run),
                "promotable_count": _coerce_int(row.get("promotable_count")),
                "monitor_count": _coerce_int(row.get("monitor_count")),
                "alert_count": _coerce_int(row.get("alert_count")),
                "live_action_gate": row.get("live_action_gate"),
                "tradeability": row.get("tradeability"),
                "tradeability_state": row.get("tradeability_state"),
                "tradeability_reason": row.get("tradeability_reason"),
                "tradeability_message": row.get("tradeability_message"),
                "gap_active": bool(row.get("gap_active")),
                "recovery_state": row.get("recovery_state"),
                "missed_slot_count": _coerce_int(row.get("missed_slot_count")),
                "unrecoverable_slot_count": _coerce_int(
                    row.get("unrecoverable_slot_count")
                ),
                "latest_fresh_slot_at": row.get("latest_fresh_slot_at"),
                "latest_resume_slot_at": row.get("latest_resume_slot_at"),
                "updated_at": row.get("updated_at"),
                "pipeline_id": row.get("pipeline_id"),
            }
        )
    return {"sessions": sessions}


@with_storage()
def get_session_detail(
    *,
    db_target: str,
    session_id: str,
    profit_target: float,
    stop_multiple: float,
    storage: Any | None = None,
) -> dict[str, Any]:
    identity = _resolve_session_identity(
        session_id,
        collector_store=storage.collector,
        job_store=storage.jobs,
    )
    if identity is None:
        raise ValueError(f"Unknown session_id: {session_id}")

    detail = get_pipeline_detail(
        db_target=db_target,
        pipeline_id=identity["pipeline_id"],
        market_date=identity["session_date"],
        profit_target=profit_target,
        stop_multiple=stop_multiple,
        storage=storage,
    )
    return {
        **detail,
        "session_id": identity["session_id"],
        "label": identity["label"],
        "session_date": identity["session_date"],
    }
