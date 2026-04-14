from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from spreads.db.decorators import with_storage
from spreads.services.option_structures import position_legs, primary_short_long_symbols
from spreads.services.runtime_identity import build_live_run_scope_id

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


def _derive_position_legs(
    row: Mapping[str, Any],
) -> tuple[str | None, str | None, str | None]:
    expiration_date = _as_text(row.get("expiration_date"))
    legs = position_legs(row)
    short_symbol, long_symbol = primary_short_long_symbols(legs)
    for leg in legs:
        if expiration_date is None:
            expiration_date = _as_text(leg.get("expiration_date"))
    return short_symbol, long_symbol, expiration_date


def enrich_position_row(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    economics = (
        payload.get("economics")
        if isinstance(payload.get("economics"), Mapping)
        else {}
    )
    strategy_metrics = (
        payload.get("strategy_metrics")
        if isinstance(payload.get("strategy_metrics"), Mapping)
        else {}
    )
    short_symbol, long_symbol, expiration_date = _derive_position_legs(payload)
    pipeline_id = _as_text(payload.get("pipeline_id"))
    label = None if pipeline_id is None else pipeline_id.partition(":")[2]
    market_date = _as_text(payload.get("market_date_opened"))
    payload.update(
        {
            "market_date": market_date,
            "session_date": market_date,
            "label": label,
            "session_id": None
            if label is None or market_date is None
            else build_live_run_scope_id(label, market_date),
            "position_status": payload.get("status"),
            "underlying_symbol": payload.get("root_symbol"),
            "strategy": payload.get("strategy_family"),
            "short_symbol": short_symbol,
            "long_symbol": long_symbol,
            "expiration_date": expiration_date,
            "entry_credit": _coerce_float(economics.get("entry_credit")),
            "entry_notional": _coerce_float(economics.get("entry_notional")),
            "max_profit": _coerce_float(economics.get("max_profit")),
            "max_loss": _coerce_float(economics.get("max_loss")),
            "width": _coerce_float(strategy_metrics.get("width")),
        }
    )
    return payload


def _serialize_position(
    row: dict[str, Any],
    *,
    execution_store: Any,
) -> dict[str, Any]:
    row = enrich_position_row(row)
    public_row = {
        key: value
        for key, value in row.items()
        if key
        not in {
            "label",
        }
    }
    closes = execution_store.list_position_closes(position_id=str(row["position_id"]))
    total_closed_quantity = sum(
        _coerce_float(close.get("closed_quantity")) or 0.0 for close in closes
    )
    realized_pnl = _coerce_float(row.get("realized_pnl")) or 0.0
    unrealized_pnl = _coerce_float(row.get("unrealized_pnl"))
    return {
        **public_row,
        "market_date": str(row.get("market_date_opened")),
        "position_status": row.get("status"),
        "closed_quantity": _round_money(total_closed_quantity),
        "net_pnl": _round_money(realized_pnl + (unrealized_pnl or 0.0)),
        "open_execution_attempt": execution_store.get_attempt(
            str(row["open_execution_attempt_id"])
        ),
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
        return {
            "summary": {
                "position_count": 0,
                "open_position_count": 0,
                "closed_position_count": 0,
            },
            "positions": [],
        }

    rows = [
        _serialize_position(dict(row), execution_store=execution_store)
        for row in execution_store.list_positions(
            pipeline_id=pipeline_id,
            market_date=market_date,
            limit=limit,
        )
    ]
    open_count = sum(
        1 for row in rows if str(row.get("position_status")) in OPEN_POSITION_STATUSES
    )
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
