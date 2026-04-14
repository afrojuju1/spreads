from __future__ import annotations

from typing import Any

from spreads.db.decorators import with_storage

OPEN_POSITION_STATUSES = {"open", "partial_open", "partial_close", "pending_open"}


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _round_money(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 2)


def _serialize_position(
    row: dict[str, Any],
    *,
    execution_store: Any,
) -> dict[str, Any]:
    closes = execution_store.list_position_closes(position_id=str(row["position_id"]))
    total_closed_quantity = sum(_coerce_float(close.get("closed_quantity")) or 0.0 for close in closes)
    realized_pnl = _coerce_float(row.get("realized_pnl")) or 0.0
    unrealized_pnl = _coerce_float(row.get("unrealized_pnl"))
    return {
        **row,
        "market_date": str(row.get("market_date_opened")),
        "position_status": row.get("status"),
        "closed_quantity": _round_money(total_closed_quantity),
        "net_pnl": _round_money(realized_pnl + (unrealized_pnl or 0.0)),
        "legacy_session_position_id": row.get("legacy_session_position_id"),
        "open_execution_attempt": execution_store.get_attempt(str(row["open_execution_attempt_id"])),
        "closes": closes,
    }


@with_storage()
def list_positions(
    *,
    db_target: str,
    pipeline_id: str | None = None,
    market_date: str | None = None,
    limit: int = 200,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    if not execution_store.portfolio_schema_ready():
        return {"summary": {"position_count": 0, "open_position_count": 0, "closed_position_count": 0}, "positions": []}

    rows = [
        _serialize_position(dict(row), execution_store=execution_store)
        for row in execution_store.list_positions(
            pipeline_id=pipeline_id,
            market_date=market_date,
            limit=limit,
        )
    ]
    open_count = sum(1 for row in rows if str(row.get("position_status")) in OPEN_POSITION_STATUSES)
    closed_count = sum(1 for row in rows if str(row.get("position_status")) == "closed")
    return {
        "summary": {
            "position_count": len(rows),
            "open_position_count": open_count,
            "closed_position_count": closed_count,
            "pipeline_id": pipeline_id,
            "market_date": market_date,
        },
        "positions": rows,
    }


@with_storage()
def get_position_detail(
    *,
    db_target: str,
    position_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    if not execution_store.portfolio_schema_ready():
        raise ValueError(f"Unknown position_id: {position_id}")
    row = execution_store.get_position(position_id)
    if row is None:
        raise ValueError(f"Unknown position_id: {position_id}")
    return _serialize_position(dict(row), execution_store=execution_store)
