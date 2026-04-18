from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.db.decorators import with_storage
from core.services.runtime_identity import build_live_run_scope_id


def serialize_opportunity_row(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    market_date = str(row.get("market_date") or row.get("session_date") or "")
    pipeline_id = row.get("pipeline_id")
    label = str(row.get("label") or "")
    if not label and pipeline_id:
        label = str(pipeline_id).partition(":")[2]
    candidate = payload.get("candidate")
    candidate_payload = (
        dict(candidate)
        if isinstance(candidate, Mapping)
        else dict(payload.get("candidate_json") or {})
    )
    execution_shape = payload.get("execution_shape_json")
    if not isinstance(execution_shape, Mapping):
        execution_shape = payload.get("execution_shape")
    order_payload = payload.get("order_payload_json") or payload.get("order_payload")
    if order_payload is None and isinstance(execution_shape, Mapping):
        order_payload = execution_shape.get("order_payload")
    return {
        **payload,
        "market_date": market_date,
        "pipeline_id": pipeline_id,
        "cycle_id": row.get("cycle_id") or row.get("source_cycle_id"),
        "root_symbol": row.get("root_symbol") or row.get("underlying_symbol"),
        "style_profile": row.get("style_profile"),
        "horizon_intent": row.get("horizon_intent"),
        "product_class": row.get("product_class"),
        "legacy_session_id": build_live_run_scope_id(label, market_date)
        if label and market_date
        else None,
        "owner": {
            "owner_kind": (
                "automation"
                if row.get("bot_id") or row.get("automation_id")
                else "discovery"
            ),
            "bot_id": row.get("bot_id"),
            "automation_id": row.get("automation_id"),
            "strategy_config_id": row.get("strategy_config_id"),
            "strategy_id": row.get("strategy_id"),
            "config_hash": row.get("config_hash"),
            "automation_run_id": row.get("automation_run_id"),
        },
        "discovery": {
            "label": label or None,
            "pipeline_id": pipeline_id,
            "cycle_id": row.get("cycle_id") or row.get("source_cycle_id"),
            "session_id": (
                build_live_run_scope_id(label, market_date)
                if label and market_date
                else None
            ),
            "candidate_id": row.get("source_candidate_id"),
        },
        "candidate": candidate_payload,
        "candidate_id": row.get("source_candidate_id"),
        "eligibility": row.get("eligibility_state") or row.get("eligibility"),
        "order_payload": order_payload,
        "legs": row.get("legs_json") or row.get("legs") or [],
        "economics": row.get("economics_json") or row.get("economics") or {},
        "strategy_metrics": row.get("strategy_metrics_json")
        or row.get("strategy_metrics")
        or {},
        "evidence": row.get("evidence_json") or row.get("evidence") or {},
    }


def list_active_cycle_opportunity_rows(
    signal_store: Any,
    *,
    cycle_id: str,
    pipeline_id: str | None = None,
    market_date: str | None = None,
    runtime_owned: bool = False,
    limit: int = 500,
) -> list[dict[str, Any]]:
    if signal_store is None or not signal_store.schema_ready():
        return []
    rows = [
        serialize_opportunity_row(dict(row))
        for row in signal_store.list_active_cycle_opportunities(
            cycle_id,
            runtime_owned=runtime_owned,
            limit=limit,
        )
    ]
    filtered = [
        row
        for row in rows
        if (pipeline_id is None or str(row.get("pipeline_id") or "") == pipeline_id)
        and (
            market_date is None
            or str(row.get("market_date") or row.get("session_date") or "")
            == market_date
        )
    ]
    filtered.sort(
        key=lambda row: (
            0 if str(row.get("selection_state") or "") == "promotable" else 1,
            int(row.get("selection_rank") or 999_999),
            str(row.get("opportunity_id") or ""),
        )
    )
    return filtered


def list_session_opportunity_rows(
    signal_store: Any,
    *,
    label: str,
    session_date: str,
    runtime_owned: bool = False,
    limit: int = 200,
) -> list[dict[str, Any]]:
    if signal_store is None or not signal_store.schema_ready():
        return []
    return [
        serialize_opportunity_row(dict(row))
        for row in signal_store.list_opportunities(
            label=label,
            session_date=session_date,
            runtime_owned=runtime_owned,
            limit=limit,
        )
    ]


@with_storage()
def list_opportunities(
    *,
    db_target: str,
    pipeline_id: str | None = None,
    label: str | None = None,
    market_date: str | None = None,
    lifecycle_state: str | None = None,
    bot_id: str | None = None,
    automation_id: str | None = None,
    strategy_config_id: str | None = None,
    include_analysis_only: bool = False,
    limit: int = 200,
    storage: Any | None = None,
) -> dict[str, Any]:
    signal_store = storage.signals
    rows = [
        serialize_opportunity_row(dict(row))
        for row in signal_store.list_opportunities(
            pipeline_id=pipeline_id,
            label=label,
            market_date=market_date,
            lifecycle_state=lifecycle_state,
            bot_id=bot_id,
            automation_id=automation_id,
            strategy_config_id=strategy_config_id,
            eligibility_state=None if include_analysis_only else "live",
            limit=limit,
        )
    ]
    return {"opportunities": rows}


@with_storage()
def get_opportunity_detail(
    *,
    db_target: str,
    opportunity_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    signal_store = storage.signals
    row = signal_store.get_opportunity(opportunity_id)
    if row is None:
        raise ValueError(f"Unknown opportunity_id: {opportunity_id}")
    return serialize_opportunity_row(dict(row))
