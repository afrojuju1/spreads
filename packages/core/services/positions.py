from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from core.db.decorators import with_storage
from core.jobs.orchestration import NEW_YORK
from core.services.close_lifecycle import build_close_lifecycle_summary
from core.services.option_structures import position_legs, primary_short_long_symbols
from core.services.runtime_identity import build_live_run_scope_id, build_pipeline_id

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
    label = _as_text(payload.get("label"))
    if label is None and pipeline_id is not None:
        label = pipeline_id.partition(":")[2]
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
            "owner": {
                "owner_kind": (
                    "automation"
                    if payload.get("bot_id") or payload.get("automation_id")
                    else "discovery"
                ),
                "bot_id": payload.get("bot_id"),
                "automation_id": payload.get("automation_id"),
                "strategy_config_id": payload.get("strategy_config_id"),
                "strategy_id": payload.get("strategy_id"),
                "config_hash": payload.get("config_hash"),
            },
            "discovery": {
                "label": label,
                "pipeline_id": pipeline_id,
                "session_id": None
                if label is None or market_date is None
                else build_live_run_scope_id(label, market_date),
                "source_opportunity_id": payload.get("source_opportunity_id"),
            },
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


def _matches_optional(value: Any, expected: str | None) -> bool:
    return expected is None or _as_text(value) == expected


def _attempt_matches_scope(
    row: Mapping[str, Any],
    *,
    bot_id: str | None,
    automation_id: str | None,
    strategy_config_id: str | None,
    position_ids: set[str],
) -> bool:
    if not _matches_optional(row.get("bot_id"), bot_id):
        return False
    if not _matches_optional(row.get("automation_id"), automation_id):
        return False
    if not _matches_optional(row.get("strategy_config_id"), strategy_config_id):
        return False
    position_id = _as_text(row.get("position_id"))
    return not position_ids or position_id is None or position_id in position_ids


def _intent_matches_scope(
    row: Mapping[str, Any],
    *,
    bot_id: str | None,
    automation_id: str | None,
    strategy_config_id: str | None,
    position_ids: set[str],
) -> bool:
    if not _matches_optional(row.get("bot_id"), bot_id):
        return False
    if not _matches_optional(row.get("automation_id"), automation_id):
        return False
    policy_ref = row.get("policy_ref") if isinstance(row.get("policy_ref"), Mapping) else {}
    if not _matches_optional(policy_ref.get("strategy_config_id"), strategy_config_id):
        return False
    position_id = _as_text(row.get("strategy_position_id"))
    return not position_ids or position_id is None or position_id in position_ids


def _position_in_lifecycle_scope(row: Mapping[str, Any], *, market_date: str) -> bool:
    status = str(row.get("position_status") or row.get("status") or "").strip().lower()
    if status in OPEN_POSITION_STATUSES:
        return True
    return str(row.get("market_date_opened") or row.get("market_date") or "") == market_date


@with_storage()
def list_positions(
    *,
    db_target: str,
    pipeline_id: str | None = None,
    label: str | None = None,
    market_date: str | None = None,
    bot_id: str | None = None,
    automation_id: str | None = None,
    strategy_config_id: str | None = None,
    limit: int = 200,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    resolved_pipeline_id = pipeline_id or (
        None if label is None else build_pipeline_id(label)
    )
    if not execution_store.portfolio_schema_ready():
        return {
            "summary": {
                "position_count": 0,
                "open_position_count": 0,
                "closed_position_count": 0,
                "pipeline_id": resolved_pipeline_id,
                "label": label,
                "market_date": market_date,
                "bot_id": bot_id,
                "automation_id": automation_id,
                "strategy_config_id": strategy_config_id,
            },
            "positions": [],
        }

    rows = [
        _serialize_position(dict(row), execution_store=execution_store)
        for row in execution_store.list_positions(
            pipeline_id=resolved_pipeline_id,
            market_date=market_date,
            bot_id=bot_id,
            automation_id=automation_id,
            strategy_config_id=strategy_config_id,
            limit=limit,
        )
    ]
    position_ids = {
        str(row["position_id"])
        for row in rows
        if _as_text(row.get("position_id")) is not None
    }
    lifecycle_market_date = market_date or datetime.now(NEW_YORK).date().isoformat()
    close_attempts = [
        dict(row)
        for row in execution_store.list_attempts_for_market_date(
            market_date=lifecycle_market_date,
            limit=500,
        )
        if _attempt_matches_scope(
            row,
            bot_id=bot_id,
            automation_id=automation_id,
            strategy_config_id=strategy_config_id,
            position_ids=position_ids,
        )
    ]
    close_intents: list[dict[str, Any]] = []
    if execution_store.intent_schema_ready():
        close_intents = [
            dict(row)
            for row in execution_store.list_execution_intents(
                bot_id=bot_id,
                automation_id=automation_id,
                limit=200,
            )
            if _intent_matches_scope(
                row,
                bot_id=bot_id,
                automation_id=automation_id,
                strategy_config_id=strategy_config_id,
                position_ids=position_ids,
            )
        ]
    close_lifecycle = build_close_lifecycle_summary(
        attempts=close_attempts,
        intents=close_intents,
        positions=[
            row
            for row in rows
            if _position_in_lifecycle_scope(row, market_date=lifecycle_market_date)
        ],
        limit=8,
    )
    open_count = sum(
        1 for row in rows if str(row.get("position_status")) in OPEN_POSITION_STATUSES
    )
    closed_count = sum(1 for row in rows if str(row.get("position_status")) == "closed")
    return {
        "summary": {
            "position_count": len(rows),
            "open_position_count": open_count,
            "closed_position_count": closed_count,
            "pipeline_id": resolved_pipeline_id,
            "label": label,
            "market_date": market_date,
            "bot_id": bot_id,
            "automation_id": automation_id,
            "strategy_config_id": strategy_config_id,
            "close_lifecycle_market_date": lifecycle_market_date,
            "close_lifecycle": close_lifecycle,
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
