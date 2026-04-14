from __future__ import annotations

from typing import Any

from spreads.db.decorators import with_storage
from spreads.services.runtime_identity import build_live_session_id


def _serialize_opportunity(row: dict[str, Any]) -> dict[str, Any]:
    market_date = str(row.get("market_date") or row.get("session_date") or "")
    label = str(row.get("label") or "")
    return {
        **row,
        "market_date": market_date,
        "pipeline_id": row.get("pipeline_id"),
        "cycle_id": row.get("cycle_id") or row.get("source_cycle_id"),
        "root_symbol": row.get("root_symbol") or row.get("underlying_symbol"),
        "style_profile": row.get("style_profile"),
        "horizon_intent": row.get("horizon_intent"),
        "product_class": row.get("product_class"),
        "legacy_session_id": build_live_session_id(label, market_date)
        if label and market_date
        else None,
        "order_payload": row.get("order_payload_json") or row.get("execution_shape_json", {}).get("order_payload"),
        "legs": row.get("legs_json") or [],
        "economics": row.get("economics_json") or {},
        "strategy_metrics": row.get("strategy_metrics_json") or {},
        "evidence": row.get("evidence_json") or {},
    }


@with_storage()
def list_opportunities(
    *,
    db_target: str,
    pipeline_id: str | None = None,
    market_date: str | None = None,
    lifecycle_state: str | None = None,
    limit: int = 200,
    storage: Any | None = None,
) -> dict[str, Any]:
    signal_store = storage.signals
    rows = [
        _serialize_opportunity(dict(row))
        for row in signal_store.list_opportunities(
            pipeline_id=pipeline_id,
            market_date=market_date,
            lifecycle_state=lifecycle_state,
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
    return _serialize_opportunity(dict(row))
