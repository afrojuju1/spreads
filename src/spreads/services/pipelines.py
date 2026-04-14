from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from spreads.db.decorators import with_storage
from spreads.services.runtime_identity import (
    build_live_session_id,
    build_pipeline_id,
    parse_pipeline_id,
)
from spreads.services.sessions import get_session_detail, list_existing_sessions


def _pick_latest_by_pipeline(
    sessions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    latest_by_pipeline: dict[str, dict[str, Any]] = {}
    for row in sessions:
        pipeline_id = build_pipeline_id(str(row["label"]))
        existing = latest_by_pipeline.get(pipeline_id)
        if existing is None:
            latest_by_pipeline[pipeline_id] = dict(row)
            continue
        existing_updated = str(existing.get("updated_at") or "")
        row_updated = str(row.get("updated_at") or "")
        if row_updated >= existing_updated:
            latest_by_pipeline[pipeline_id] = dict(row)
    return [latest_by_pipeline[key] for key in sorted(latest_by_pipeline)]


@with_storage()
def list_pipelines(
    *,
    db_target: str,
    limit: int = 100,
    market_date: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    session_payload = list_existing_sessions(
        db_target=db_target,
        limit=max(limit * 10, limit),
        session_date=market_date,
        storage=storage,
    )
    latest_sessions = _pick_latest_by_pipeline(
        [dict(row) for row in session_payload.get("sessions") or []]
    )
    collector_store = storage.collector

    pipelines: list[dict[str, Any]] = []
    for row in latest_sessions[:limit]:
        pipeline_id = build_pipeline_id(str(row["label"]))
        pipeline = collector_store.get_pipeline(pipeline_id) if collector_store.pipeline_schema_ready() else None
        pipelines.append(
            {
                "pipeline_id": pipeline_id,
                "label": str(row["label"]),
                "name": None if pipeline is None else pipeline.get("name"),
                "status": str(row["status"]),
                "latest_market_date": str(row["session_date"]),
                "legacy_session_id": str(row["session_id"]),
                "latest_slot_at": row.get("latest_slot_at"),
                "latest_slot_status": row.get("latest_slot_status"),
                "latest_capture_status": row.get("latest_capture_status"),
                "promotable_count": int(row.get("promotable_count") or 0),
                "monitor_count": int(row.get("monitor_count") or 0),
                "alert_count": int(row.get("alert_count") or 0),
                "updated_at": row.get("updated_at"),
                "style_profile": None if pipeline is None else pipeline.get("style_profile"),
                "horizon_intent": None if pipeline is None else pipeline.get("default_horizon_intent"),
                "product_scope": None if pipeline is None else pipeline.get("product_scope_json"),
                "policy": None if pipeline is None else pipeline.get("policy_json"),
            }
        )
    return {"pipelines": pipelines}


@with_storage()
def get_pipeline_detail(
    *,
    db_target: str,
    pipeline_id: str,
    market_date: str | None,
    profit_target: float,
    stop_multiple: float,
    storage: Any | None = None,
) -> dict[str, Any]:
    parsed = parse_pipeline_id(pipeline_id)
    if parsed is None:
        raise ValueError(f"Unknown pipeline_id: {pipeline_id}")
    label = str(parsed["label"])
    collector_store = storage.collector
    pipeline = collector_store.get_pipeline(pipeline_id) if collector_store.pipeline_schema_ready() else None

    resolved_market_date = market_date
    if resolved_market_date is None and collector_store.pipeline_schema_ready():
        latest_cycle = collector_store.get_latest_pipeline_cycle(pipeline_id)
        if latest_cycle is not None:
            resolved_market_date = str(latest_cycle["market_date"])
    if resolved_market_date is None:
        sessions = list_existing_sessions(
            db_target=db_target,
            limit=200,
            storage=storage,
        ).get("sessions") or []
        for row in sessions:
            if str(row.get("label")) == label:
                resolved_market_date = str(row["session_date"])
                break
    if resolved_market_date is None:
        raise ValueError(f"Unknown pipeline_id: {pipeline_id}")

    legacy_session_id = build_live_session_id(label, resolved_market_date)
    detail = get_session_detail(
        db_target=db_target,
        session_id=legacy_session_id,
        profit_target=profit_target,
        stop_multiple=stop_multiple,
        storage=storage,
    )
    cycle_rows = (
        []
        if not collector_store.pipeline_schema_ready()
        else [
            {
                **dict(row),
                "legacy_session_id": build_live_session_id(
                    str(row["label"]), str(row["market_date"])
                ),
            }
            for row in collector_store.list_pipeline_cycles(
                pipeline_id=pipeline_id,
                limit=50,
            )
        ]
    )
    return {
        **detail,
        "pipeline_id": pipeline_id,
        "market_date": resolved_market_date,
        "legacy_session_id": legacy_session_id,
        "pipeline": pipeline,
        "cycles": cycle_rows,
    }


@with_storage()
def list_pipeline_cycles(
    *,
    db_target: str,
    pipeline_id: str,
    market_date: str | None = None,
    limit: int = 100,
    storage: Any | None = None,
) -> dict[str, Any]:
    collector_store = storage.collector
    if not collector_store.pipeline_schema_ready():
        return {"cycles": []}
    rows = [
        {
            **dict(row),
            "legacy_session_id": build_live_session_id(
                str(row["label"]), str(row["market_date"])
            ),
        }
        for row in collector_store.list_pipeline_cycles(
            pipeline_id=pipeline_id,
            market_date=market_date,
            limit=limit,
        )
    ]
    return {"cycles": rows}
