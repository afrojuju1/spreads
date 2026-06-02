from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from core.db.decorators import with_storage
from core.jobs.orchestration import NEW_YORK
from core.services.close_lifecycle import build_close_lifecycle_summary
from core.services.positions import OPEN_POSITION_STATUSES, enrich_position_row
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_float as _coerce_float,
    utc_now_iso as _utc_now,
)

from .shared import _attention, _combine_statuses


DEFAULT_FEED_ID = "finviz_momentum"
DEFAULT_BOT_ID = "finviz"
DEFAULT_AUTOMATION_ID = "finviz_direct"


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _round_money(value: Any) -> float | None:
    parsed = _coerce_float(value)
    return None if parsed is None else round(parsed, 2)


def _run_status(row: Mapping[str, Any] | None) -> str:
    if row is None:
        return "blocked"
    status = str(row.get("status") or "unknown").strip().lower()
    if status in {"succeeded", "completed"}:
        return "healthy"
    if status in {"queued", "running", "leased"}:
        return "idle"
    return "degraded"


def _reason_counts(decisions: list[Any]) -> dict[str, int]:
    counts = Counter(
        str(item.get("reason") or "unknown")
        for item in decisions
        if isinstance(item, Mapping)
    )
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _decision_preview(decisions: list[Any], *, limit: int = 8) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in decisions:
        if not isinstance(item, Mapping):
            continue
        rows.append(
            {
                "kind": item.get("kind"),
                "symbol": item.get("symbol"),
                "instrument": item.get("instrument"),
                "option_symbol": item.get("option_symbol"),
                "passed": item.get("passed"),
                "triggered": item.get("triggered"),
                "created": item.get("created"),
                "reason": item.get("reason"),
                "limit_price": item.get("limit_price"),
                "quantity": item.get("quantity"),
            }
        )
        if len(rows) >= limit:
            break
    return rows


def _summarize_direct_run(row: Mapping[str, Any]) -> dict[str, Any]:
    result = _mapping(row.get("result"))
    decisions = _list(result.get("decisions"))
    return {
        "job_run_id": row.get("job_run_id"),
        "job_status": row.get("status"),
        "scheduled_for": row.get("scheduled_for"),
        "worker_name": row.get("worker_name"),
        "result_status": result.get("status"),
        "feed_status": result.get("feed_status"),
        "feed_job_run_id": result.get("feed_job_run_id"),
        "entry_candidates": result.get("entry_candidates"),
        "managed_positions": result.get("managed_positions"),
        "active_entry_intents": result.get("active_entry_intents"),
        "max_daily_entries": result.get("max_daily_entries"),
        "daily_entry_budget": result.get("daily_entry_budget"),
        "armed": result.get("armed"),
        "entry_armed": result.get("entry_armed"),
        "dispatch_result": result.get("dispatch_result"),
        "decision_count": len(decisions),
        "created_count": sum(
            1 for item in decisions if isinstance(item, Mapping) and item.get("created")
        ),
        "triggered_count": sum(
            1
            for item in decisions
            if isinstance(item, Mapping) and item.get("triggered")
        ),
        "reason_counts": _reason_counts(decisions),
        "decisions": _decision_preview(decisions),
    }


def _summarize_feed_run(row: Mapping[str, Any]) -> dict[str, Any]:
    result = _mapping(row.get("result"))
    summary = _mapping(result.get("summary"))
    return {
        "job_run_id": row.get("job_run_id"),
        "job_status": row.get("status"),
        "scheduled_for": row.get("scheduled_for"),
        "worker_name": row.get("worker_name"),
        "result_status": result.get("status"),
        "symbol_count": summary.get("symbol_count"),
        "candidate_count": summary.get("candidate_count"),
        "retained_count": summary.get("retained_count"),
        "min_price": summary.get("min_price"),
        "min_market_cap": summary.get("min_market_cap"),
        "below_min_market_cap_count": summary.get("below_min_market_cap_count"),
        "excluded_instrument_count": summary.get("excluded_instrument_count"),
        "excluded_instrument_reason_counts": summary.get(
            "excluded_instrument_reason_counts"
        ),
        "symbols": list(result.get("symbols") or [])[:10],
    }


def _is_finviz_attempt(row: Mapping[str, Any], *, feed_id: str) -> bool:
    if _as_text(row.get("bot_id")) != DEFAULT_BOT_ID:
        return False
    if _as_text(row.get("automation_id")) != DEFAULT_AUTOMATION_ID:
        return False
    strategy_config_id = _as_text(row.get("strategy_config_id"))
    if strategy_config_id is not None:
        return strategy_config_id == feed_id
    request = _mapping(row.get("request"))
    source = _mapping(request.get("source"))
    return source.get("flow") == "finviz_direct" and source.get("feed_id") == feed_id


def _summarize_attempt(
    row: Mapping[str, Any],
    *,
    orders: list[dict[str, Any]],
    fills: list[dict[str, Any]],
) -> dict[str, Any]:
    filled_qty = sum(_coerce_float(fill.get("quantity")) or 0.0 for fill in fills)
    fill_notional = sum(
        (_coerce_float(fill.get("quantity")) or 0.0)
        * (_coerce_float(fill.get("price")) or 0.0)
        for fill in fills
    )
    avg_fill_price = None if filled_qty <= 0 else round(fill_notional / filled_qty, 4)
    return {
        "execution_attempt_id": row.get("execution_attempt_id"),
        "position_id": row.get("position_id"),
        "trade_intent": row.get("trade_intent"),
        "status": row.get("status"),
        "root_symbol": row.get("root_symbol") or row.get("underlying_symbol"),
        "strategy_family": row.get("strategy_family") or row.get("strategy"),
        "symbol_path": row.get("symbol_path"),
        "long_symbol": row.get("long_symbol"),
        "requested_quantity": row.get("requested_quantity") or row.get("quantity"),
        "limit_price": row.get("requested_limit_price") or row.get("limit_price"),
        "broker_order_id": row.get("broker_order_id"),
        "client_order_id": row.get("client_order_id"),
        "requested_at": row.get("requested_at"),
        "submitted_at": row.get("submitted_at"),
        "completed_at": row.get("completed_at"),
        "error_text": row.get("error_text"),
        "order_count": len(orders),
        "order_statuses": dict(
            sorted(Counter(str(order.get("order_status") or "unknown") for order in orders).items())
        ),
        "fill_count": len(fills),
        "filled_qty": round(filled_qty, 4),
        "avg_fill_price": avg_fill_price,
        "orders": [
            {
                "broker_order_id": order.get("broker_order_id"),
                "symbol": order.get("symbol") or order.get("leg_symbol"),
                "side": order.get("side") or order.get("leg_side"),
                "position_intent": order.get("position_intent"),
                "order_status": order.get("order_status"),
                "limit_price": order.get("limit_price"),
                "filled_qty": order.get("filled_qty"),
                "filled_avg_price": order.get("filled_avg_price"),
                "updated_at": order.get("updated_at"),
            }
            for order in orders[:5]
        ],
        "fills": [
            {
                "symbol": fill.get("symbol"),
                "side": fill.get("side"),
                "quantity": fill.get("quantity"),
                "price": fill.get("price"),
                "filled_at": fill.get("filled_at"),
            }
            for fill in fills[:5]
        ],
    }


def _summarize_position(
    row: Mapping[str, Any],
    *,
    closes: list[dict[str, Any]],
) -> dict[str, Any]:
    position = enrich_position_row(row)
    realized = _coerce_float(position.get("realized_pnl")) or 0.0
    unrealized = _coerce_float(position.get("unrealized_pnl")) or 0.0
    return {
        "position_id": position.get("position_id"),
        "status": position.get("status"),
        "root_symbol": position.get("root_symbol"),
        "strategy_family": position.get("strategy_family"),
        "long_symbol": position.get("long_symbol"),
        "market_date_opened": position.get("market_date_opened"),
        "market_date_closed": position.get("market_date_closed"),
        "opened_at": position.get("opened_at"),
        "closed_at": position.get("closed_at"),
        "remaining_quantity": position.get("remaining_quantity"),
        "entry_value": _round_money(position.get("entry_value")),
        "close_mark": _round_money(position.get("close_mark")),
        "close_mark_source": position.get("close_mark_source"),
        "realized_pnl": _round_money(realized),
        "unrealized_pnl": _round_money(unrealized),
        "net_pnl": _round_money(realized + unrealized),
        "last_exit_reason": position.get("last_exit_reason"),
        "reconciliation_status": position.get("reconciliation_status"),
        "close_count": len(closes),
        "closes": [
            {
                "execution_attempt_id": close.get("execution_attempt_id"),
                "closed_quantity": close.get("closed_quantity"),
                "exit_value": _round_money(close.get("exit_value")),
                "realized_pnl": _round_money(close.get("realized_pnl")),
                "closed_at": close.get("closed_at"),
                "broker_order_id": close.get("broker_order_id"),
            }
            for close in closes[:5]
        ],
    }


def _position_in_lifecycle_scope(row: Mapping[str, Any], *, market_date: str) -> bool:
    status = str(row.get("status") or "").strip().lower()
    if status in OPEN_POSITION_STATUSES:
        return True
    return str(row.get("market_date_opened") or "") == market_date


def _intent_matches(row: Mapping[str, Any], *, feed_id: str) -> bool:
    if _as_text(row.get("bot_id")) != DEFAULT_BOT_ID:
        return False
    if _as_text(row.get("automation_id")) != DEFAULT_AUTOMATION_ID:
        return False
    payload = _mapping(row.get("payload"))
    source = _mapping(payload.get("source"))
    policy_ref = _mapping(row.get("policy_ref"))
    return (
        source.get("flow") == "finviz_direct"
        and source.get("feed_id") == feed_id
    ) or policy_ref.get("strategy_config_id") == feed_id


def _summarize_intent(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = _mapping(row.get("payload"))
    source = _mapping(payload.get("source"))
    return {
        "execution_intent_id": row.get("execution_intent_id"),
        "execution_attempt_id": row.get("execution_attempt_id"),
        "slot_key": row.get("slot_key"),
        "state": row.get("state"),
        "action_type": row.get("action_type"),
        "trade_intent": payload.get("trade_intent"),
        "position_intent": payload.get("position_intent"),
        "symbol": payload.get("symbol"),
        "underlying_symbol": payload.get("underlying_symbol"),
        "position_id": payload.get("position_id"),
        "quantity": payload.get("quantity"),
        "limit_price": payload.get("limit_price"),
        "feed_job_run_id": source.get("feed_job_run_id"),
        "trading_job_run_id": source.get("trading_job_run_id"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "expires_at": row.get("expires_at"),
    }


@with_storage()
def build_finviz_direct_ledger(
    *,
    db_target: str | None = None,
    feed_id: str = DEFAULT_FEED_ID,
    market_date: str | None = None,
    limit: int = 10,
    storage: Any | None = None,
) -> dict[str, Any]:
    generated_at = _utc_now()
    resolved_feed_id = _as_text(feed_id) or DEFAULT_FEED_ID
    resolved_market_date = (
        _as_text(market_date) or datetime.now(NEW_YORK).date().isoformat()
    )
    job_store = storage.jobs
    execution_store = storage.execution

    direct_job_key = f"finviz_direct_trading:{resolved_feed_id}"
    feed_job_key = f"symbol_feed:{resolved_feed_id}"
    attention: list[dict[str, str]] = []

    direct_runs = job_store.list_job_runs(job_key=direct_job_key, limit=limit)
    feed_runs = job_store.list_job_runs(job_key=feed_job_key, limit=limit)
    latest_direct_run = direct_runs[0] if direct_runs else None
    latest_feed_run = feed_runs[0] if feed_runs else None
    if latest_feed_run is None:
        attention.append(
            _attention(
                severity="critical",
                code="finviz_feed_missing",
                message=f"No feed runs found for {feed_job_key}.",
            )
        )
    elif str(latest_feed_run.get("status") or "").lower() != "succeeded":
        attention.append(
            _attention(
                severity="warning",
                code="latest_finviz_feed_not_succeeded",
                message=f"Latest feed run status is {latest_feed_run.get('status')}.",
            )
        )
    if latest_direct_run is None:
        attention.append(
            _attention(
                severity="critical",
                code="finviz_direct_missing",
                message=f"No direct trading runs found for {direct_job_key}.",
            )
        )
    elif str(latest_direct_run.get("status") or "").lower() != "succeeded":
        attention.append(
            _attention(
                severity="warning",
                code="latest_finviz_direct_not_succeeded",
                message=f"Latest direct run status is {latest_direct_run.get('status')}.",
            )
        )

    attempts: list[dict[str, Any]] = []
    orders_by_attempt: dict[str, list[dict[str, Any]]] = defaultdict(list)
    fills_by_attempt: dict[str, list[dict[str, Any]]] = defaultdict(list)
    positions: list[dict[str, Any]] = []
    intents: list[dict[str, Any]] = []

    if execution_store.schema_ready():
        raw_attempts = [
            dict(row)
            for row in execution_store.list_attempts_for_market_date(
                market_date=resolved_market_date,
                limit=500,
            )
            if _is_finviz_attempt(row, feed_id=resolved_feed_id)
        ]
        attempt_ids = [str(row["execution_attempt_id"]) for row in raw_attempts]
        if attempt_ids:
            for order in execution_store.list_orders(execution_attempt_ids=attempt_ids):
                orders_by_attempt[str(order.get("execution_attempt_id"))].append(
                    dict(order)
                )
            for fill in execution_store.list_fills(execution_attempt_ids=attempt_ids):
                fills_by_attempt[str(fill.get("execution_attempt_id"))].append(
                    dict(fill)
                )
        attempts = [
            _summarize_attempt(
                row,
                orders=orders_by_attempt[str(row.get("execution_attempt_id"))],
                fills=fills_by_attempt[str(row.get("execution_attempt_id"))],
            )
            for row in raw_attempts
        ]

    if execution_store.portfolio_schema_ready():
        raw_positions = [
            dict(row)
            for row in execution_store.list_positions(
                bot_id=DEFAULT_BOT_ID,
                automation_id=DEFAULT_AUTOMATION_ID,
                strategy_config_id=resolved_feed_id,
                limit=100,
            )
        ]
        position_ids = [str(row["position_id"]) for row in raw_positions]
        closes_by_position: dict[str, list[dict[str, Any]]] = defaultdict(list)
        if position_ids:
            for close in execution_store.list_position_closes(position_ids=position_ids):
                closes_by_position[str(close.get("position_id"))].append(dict(close))
        positions = [
            _summarize_position(
                row,
                closes=closes_by_position[str(row.get("position_id"))],
            )
            for row in raw_positions
        ]

    if execution_store.intent_schema_ready():
        intents = [
            _summarize_intent(row)
            for row in execution_store.list_execution_intents(
                bot_id=DEFAULT_BOT_ID,
                automation_id=DEFAULT_AUTOMATION_ID,
                limit=200,
            )
            if _intent_matches(row, feed_id=resolved_feed_id)
        ]

    open_positions = [
        row
        for row in positions
        if str(row.get("status") or "").lower() in OPEN_POSITION_STATUSES
    ]
    realized_pnl = sum(_coerce_float(row.get("realized_pnl")) or 0.0 for row in positions)
    unrealized_pnl = sum(
        _coerce_float(row.get("unrealized_pnl")) or 0.0 for row in open_positions
    )
    filled_entry_count = sum(
        1
        for row in attempts
        if str(row.get("trade_intent") or "open").lower() == "open"
        and str(row.get("status") or "").lower() == "filled"
    )
    active_entry_intent_count = sum(
        1
        for row in intents
        if str(row.get("trade_intent") or "open").lower() == "open"
        and str(row.get("state") or "").lower() in {"pending", "claimed", "dispatching"}
    )
    position_entry_count = sum(
        1
        for row in positions
        if str(row.get("market_date_opened") or "") == resolved_market_date
    )
    session_entry_count = max(filled_entry_count, position_entry_count)
    recent_feed_runs = [_summarize_feed_run(row) for row in feed_runs]
    recent_direct_runs = [_summarize_direct_run(row) for row in direct_runs]
    lifecycle_positions = [
        row
        for row in positions
        if _position_in_lifecycle_scope(row, market_date=resolved_market_date)
    ]
    close_lifecycle = build_close_lifecycle_summary(
        attempts=attempts,
        intents=intents,
        positions=lifecycle_positions,
        recent_direct_runs=recent_direct_runs,
        limit=limit,
    )
    status = _combine_statuses(_run_status(latest_feed_run), _run_status(latest_direct_run))
    if attention:
        severities = {item.get("severity") for item in attention}
        status = _combine_statuses(
            status,
            "blocked" if "critical" in severities else "degraded",
        )

    return {
        "status": status,
        "generated_at": generated_at,
        "summary": {
            "feed_id": resolved_feed_id,
            "market_date": resolved_market_date,
            "latest_feed_job_run_id": None
            if latest_feed_run is None
            else latest_feed_run.get("job_run_id"),
            "latest_direct_job_run_id": None
            if latest_direct_run is None
            else latest_direct_run.get("job_run_id"),
            "feed_run_count": len(feed_runs),
            "direct_run_count": len(direct_runs),
            "attempt_count": len(attempts),
            "filled_attempt_count": sum(
                1 for row in attempts if str(row.get("status") or "") == "filled"
            ),
            "filled_entry_count": filled_entry_count,
            "position_entry_count": position_entry_count,
            "session_entry_count": session_entry_count,
            "intent_count": len(intents),
            "active_intent_count": sum(
                1
                for row in intents
                if str(row.get("state") or "").lower()
                in {"pending", "claimed", "dispatching"}
            ),
            "active_entry_intent_count": active_entry_intent_count,
            "position_count": len(positions),
            "open_position_count": len(open_positions),
            "closed_position_count": sum(
                1 for row in positions if str(row.get("status") or "").lower() == "closed"
            ),
            "close_lifecycle_status": close_lifecycle.get("status"),
            "active_close_attempt_count": close_lifecycle.get(
                "active_close_attempt_count"
            ),
            "pending_close_intent_count": close_lifecycle.get(
                "pending_close_intent_count"
            ),
            "failed_close_attempt_count": close_lifecycle.get(
                "failed_close_attempt_count"
            ),
            "stale_reconciliation_skip_count": close_lifecycle.get(
                "stale_reconciliation_skip_count"
            ),
            "intent_mismatch_reject_count": close_lifecycle.get(
                "intent_mismatch_reject_count"
            ),
            "realized_pnl": round(realized_pnl, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "net_pnl": round(realized_pnl + unrealized_pnl, 2),
        },
        "attention": attention,
        "details": {
            "recent_feed_runs": recent_feed_runs,
            "recent_direct_runs": recent_direct_runs,
            "positions": positions,
            "attempts": attempts,
            "intents": intents[:limit],
            "close_lifecycle": close_lifecycle,
        },
    }


__all__ = ["build_finviz_direct_ledger"]
