from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.db.decorators import with_storage
from core.jobs.orchestration import NEW_YORK
from core.services.close_lifecycle import build_close_lifecycle_summary
from core.services.execution_lifecycle import project_execution_attempt_lifecycle
from core.services.finviz_lifecycle import summarize_lifecycle_decision_states
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


def _round_metric(value: Any, digits: int = 4) -> float | None:
    parsed = _coerce_float(value)
    if parsed is None:
        return None
    rounded = round(parsed, digits)
    return 0.0 if rounded == 0 else rounded


def _round_count(value: Any) -> int | None:
    parsed = _coerce_float(value)
    return None if parsed is None else int(round(parsed))


def _first_metric(*values: Any) -> float | None:
    for value in values:
        parsed = _coerce_float(value)
        if parsed is not None:
            return parsed
    return None


def _avg_metric(values: list[Any], digits: int = 4) -> float | None:
    parsed = [float(value) for value in (_coerce_float(item) for item in values) if value is not None]
    if not parsed:
        return None
    return round(sum(parsed) / len(parsed), digits)


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
    counts = Counter(str(item.get("reason") or "unknown") for item in decisions if isinstance(item, Mapping))
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
                "lifecycle_state": item.get("lifecycle_state")
                or _mapping(_mapping(item.get("lifecycle")).get("trade_decision")).get("decision_state"),
                "trade_signal_id": item.get("trade_signal_id"),
                "trade_decision_id": item.get("trade_decision_id"),
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
        "created_count": sum(1 for item in decisions if isinstance(item, Mapping) and item.get("created")),
        "triggered_count": sum(1 for item in decisions if isinstance(item, Mapping) and item.get("triggered")),
        "reason_counts": _reason_counts(decisions),
        "lifecycle_decision_state_counts": summarize_lifecycle_decision_states(decisions),
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
        "excluded_instrument_reason_counts": summary.get("excluded_instrument_reason_counts"),
        "symbols": list(result.get("symbols") or [])[:10],
    }


def _summarize_option_selection(value: Any) -> dict[str, Any] | None:
    payload = _mapping(value)
    if not payload:
        return None
    quote_metrics = _mapping(payload.get("quote_metrics"))
    bid = _first_metric(quote_metrics.get("bid"), payload.get("snapshot_bid"))
    ask = _first_metric(quote_metrics.get("ask"), payload.get("snapshot_ask"))
    midpoint = _first_metric(
        quote_metrics.get("midpoint"),
        payload.get("snapshot_midpoint"),
    )
    spread = None if bid is None or ask is None else max(ask - bid, 0.0)
    quote_summary = {
        "bid": _round_metric(bid),
        "ask": _round_metric(ask),
        "midpoint": _round_metric(midpoint),
        "spread": _round_metric(spread),
        "spread_pct": _round_metric(_first_metric(quote_metrics.get("spread_pct"), payload.get("snapshot_spread_pct"))),
        "age_seconds": _round_metric(quote_metrics.get("age_seconds"), digits=2),
        "timestamp": quote_metrics.get("timestamp"),
    }
    return {
        "selected": payload.get("selected"),
        "reason": payload.get("reason"),
        "symbol": payload.get("symbol"),
        "expiration_date": payload.get("expiration_date"),
        "days_to_expiration": _round_count(payload.get("days_to_expiration")),
        "strike": _round_metric(payload.get("strike")),
        "underlying_price": _round_metric(payload.get("underlying_price")),
        "strike_distance": _round_metric(payload.get("strike_distance")),
        "delta": _round_metric(payload.get("delta")),
        "delta_distance": _round_metric(payload.get("delta_distance")),
        "preferred_delta_miss": _round_metric(payload.get("preferred_delta_miss")),
        "premium": _round_metric(payload.get("premium"), digits=2),
        "daily_volume": _round_count(payload.get("daily_volume")),
        "open_interest": _round_count(payload.get("open_interest")),
        "implied_volatility": _round_metric(payload.get("implied_volatility")),
        "quote_source": payload.get("quote_source"),
        "quote_feeds": list(payload.get("quote_feeds") or []) if isinstance(payload.get("quote_feeds"), list) else [],
        "quote": quote_summary,
    }


def _attempt_side(
    *,
    row: Mapping[str, Any],
    orders: list[dict[str, Any]],
    fills: list[dict[str, Any]],
) -> str | None:
    for fill in fills:
        side = _as_text(fill.get("side"))
        if side is not None:
            return side.lower()
    for order in orders:
        side = _as_text(order.get("side") or order.get("leg_side"))
        if side is not None:
            return side.lower()
    return "sell" if _as_text(row.get("trade_intent")) == "close" else "buy"


def _summarize_fill_quality(
    *,
    row: Mapping[str, Any],
    orders: list[dict[str, Any]],
    fills: list[dict[str, Any]],
    filled_qty: float,
    avg_fill_price: float | None,
    option_selection: Mapping[str, Any] | None,
) -> dict[str, Any]:
    side = _attempt_side(row=row, orders=orders, fills=fills)
    limit_price = _first_metric(row.get("requested_limit_price"), row.get("limit_price"))
    quote = _mapping((option_selection or {}).get("quote"))
    bid = _coerce_float(quote.get("bid"))
    ask = _coerce_float(quote.get("ask"))
    midpoint = _coerce_float(quote.get("midpoint"))
    spread = _coerce_float(quote.get("spread"))
    price_improvement_vs_limit: float | None = None
    slippage_vs_midpoint: float | None = None
    spread_capture: float | None = None

    if avg_fill_price is not None and limit_price is not None:
        if side == "buy":
            price_improvement_vs_limit = limit_price - avg_fill_price
        elif side == "sell":
            price_improvement_vs_limit = avg_fill_price - limit_price

    if avg_fill_price is not None and midpoint is not None:
        if side == "buy":
            slippage_vs_midpoint = avg_fill_price - midpoint
        elif side == "sell":
            slippage_vs_midpoint = midpoint - avg_fill_price

    if avg_fill_price is not None and bid is not None and ask is not None and spread is not None and spread > 0:
        if side == "buy":
            spread_capture = (ask - avg_fill_price) / spread
        elif side == "sell":
            spread_capture = (avg_fill_price - bid) / spread

    return {
        "basis": "selection_quote",
        "side": side,
        "status": "filled" if filled_qty > 0 else "unfilled",
        "requested_limit_price": _round_metric(limit_price),
        "filled_qty": round(filled_qty, 4),
        "avg_fill_price": _round_metric(avg_fill_price),
        "price_improvement_vs_limit": _round_metric(price_improvement_vs_limit),
        "slippage_vs_limit": _round_metric(None if price_improvement_vs_limit is None else -price_improvement_vs_limit),
        "slippage_vs_midpoint": _round_metric(slippage_vs_midpoint),
        "spread_capture": _round_metric(spread_capture),
        "filled_notional_estimate": _round_money(None if avg_fill_price is None else avg_fill_price * filled_qty * 100.0),
        "quote_bid": _round_metric(bid),
        "quote_ask": _round_metric(ask),
        "quote_midpoint": _round_metric(midpoint),
        "quote_spread_pct": _round_metric(quote.get("spread_pct")),
        "quote_age_seconds": _round_metric(quote.get("age_seconds"), digits=2),
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
    intent: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    attempt_lifecycle = project_execution_attempt_lifecycle(
        {
            **dict(row),
            "orders": orders,
            "fills": fills,
        },
        now=datetime.now(UTC),
    )
    filled_qty = sum(_coerce_float(fill.get("quantity")) or 0.0 for fill in fills)
    fill_notional = sum((_coerce_float(fill.get("quantity")) or 0.0) * (_coerce_float(fill.get("price")) or 0.0) for fill in fills)
    avg_fill_price = None if filled_qty <= 0 else round(fill_notional / filled_qty, 4)
    intent_payload = _mapping((intent or {}).get("payload"))
    option_selection = _summarize_option_selection(intent_payload.get("option_selection") or _mapping(row.get("request")).get("option_selection"))
    fill_quality = _summarize_fill_quality(
        row=row,
        orders=orders,
        fills=fills,
        filled_qty=filled_qty,
        avg_fill_price=avg_fill_price,
        option_selection=option_selection,
    )
    return {
        "execution_attempt_id": row.get("execution_attempt_id"),
        "execution_intent_id": None if intent is None else intent.get("execution_intent_id"),
        "position_id": row.get("position_id"),
        "trade_intent": row.get("trade_intent"),
        "status": row.get("status"),
        "lifecycle_state": attempt_lifecycle.get("lifecycle_state"),
        "lifecycle_phase": attempt_lifecycle.get("phase"),
        "broker_order_state": attempt_lifecycle.get("broker_order_state"),
        "next_action": attempt_lifecycle.get("next_action"),
        "stale": bool(attempt_lifecycle.get("stale")),
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
        "order_statuses": dict(sorted(Counter(str(order.get("order_status") or "unknown") for order in orders).items())),
        "fill_count": len(fills),
        "filled_qty": round(filled_qty, 4),
        "avg_fill_price": avg_fill_price,
        "option_selection": option_selection,
        "fill_quality": fill_quality,
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
    attempts_by_id: Mapping[str, dict[str, Any]],
) -> dict[str, Any]:
    position = enrich_position_row(row)
    realized = _coerce_float(position.get("realized_pnl")) or 0.0
    unrealized = _coerce_float(position.get("unrealized_pnl")) or 0.0
    open_attempt_id = _as_text(position.get("open_execution_attempt_id"))
    opening_intent_id = _as_text(position.get("opening_execution_intent_id"))
    entry_attempt = attempts_by_id.get(open_attempt_id) if open_attempt_id is not None else None
    close_attempts = [
        attempts_by_id[str(close.get("execution_attempt_id"))] for close in closes if str(close.get("execution_attempt_id")) in attempts_by_id
    ]
    return {
        "position_id": position.get("position_id"),
        "open_execution_attempt_id": open_attempt_id,
        "opening_execution_intent_id": opening_intent_id,
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
        "entry_quality": _compact_attempt_quality(entry_attempt),
        "latest_close_quality": _compact_attempt_quality(close_attempts[0]) if close_attempts else None,
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


def _compact_attempt_quality(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    option_selection = _mapping(row.get("option_selection"))
    fill_quality = _mapping(row.get("fill_quality"))
    quote = _mapping(option_selection.get("quote"))
    return {
        "execution_attempt_id": row.get("execution_attempt_id"),
        "execution_intent_id": row.get("execution_intent_id"),
        "trade_intent": row.get("trade_intent"),
        "status": row.get("status"),
        "option_symbol": row.get("long_symbol") or row.get("symbol_path") or option_selection.get("symbol"),
        "requested_at": row.get("requested_at"),
        "completed_at": row.get("completed_at"),
        "limit_price": row.get("limit_price"),
        "avg_fill_price": row.get("avg_fill_price"),
        "selection": {
            "expiration_date": option_selection.get("expiration_date"),
            "days_to_expiration": option_selection.get("days_to_expiration"),
            "strike": option_selection.get("strike"),
            "delta": option_selection.get("delta"),
            "premium": option_selection.get("premium"),
            "daily_volume": option_selection.get("daily_volume"),
            "open_interest": option_selection.get("open_interest"),
            "implied_volatility": option_selection.get("implied_volatility"),
            "quote_source": option_selection.get("quote_source"),
            "quote_spread_pct": quote.get("spread_pct"),
            "quote_age_seconds": quote.get("age_seconds"),
        },
        "fill": fill_quality,
    }


def _append_metric(bucket: dict[str, Any], key: str, value: Any) -> None:
    parsed = _coerce_float(value)
    if parsed is not None:
        bucket.setdefault(key, []).append(parsed)


def _build_quality_by_symbol(
    *,
    attempts: list[dict[str, Any]],
    positions: list[dict[str, Any]],
    market_date: str,
) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "contracts": set(),
            "exit_reasons": Counter(),
            "entry_attempt_count": 0,
            "exit_attempt_count": 0,
            "position_count": 0,
            "closed_position_count": 0,
            "open_position_count": 0,
            "realized_pnl": 0.0,
            "net_pnl": 0.0,
        }
    )
    for attempt in attempts:
        symbol = _as_text(attempt.get("root_symbol"))
        if symbol is None:
            continue
        bucket = buckets[symbol]
        option_symbol = _as_text(attempt.get("long_symbol") or attempt.get("symbol_path"))
        if option_symbol is not None:
            bucket["contracts"].add(option_symbol)
        option_selection = _mapping(attempt.get("option_selection"))
        quote = _mapping(option_selection.get("quote"))
        fill_quality = _mapping(attempt.get("fill_quality"))
        trade_intent = str(attempt.get("trade_intent") or "open").lower()
        if trade_intent == "close":
            bucket["exit_attempt_count"] += 1
            _append_metric(
                bucket,
                "exit_slippage_vs_midpoint",
                fill_quality.get("slippage_vs_midpoint"),
            )
            _append_metric(
                bucket,
                "exit_price_improvement_vs_limit",
                fill_quality.get("price_improvement_vs_limit"),
            )
        else:
            bucket["entry_attempt_count"] += 1
            _append_metric(bucket, "entry_spread_pct", quote.get("spread_pct"))
            _append_metric(bucket, "entry_quote_age_seconds", quote.get("age_seconds"))
            _append_metric(
                bucket,
                "entry_slippage_vs_midpoint",
                fill_quality.get("slippage_vs_midpoint"),
            )
            _append_metric(
                bucket,
                "entry_price_improvement_vs_limit",
                fill_quality.get("price_improvement_vs_limit"),
            )
            _append_metric(bucket, "entry_delta", option_selection.get("delta"))
            _append_metric(
                bucket,
                "entry_days_to_expiration",
                option_selection.get("days_to_expiration"),
            )
            _append_metric(bucket, "entry_daily_volume", option_selection.get("daily_volume"))
            _append_metric(bucket, "entry_open_interest", option_selection.get("open_interest"))

    for position in positions:
        if str(position.get("market_date_opened") or "") != market_date:
            continue
        symbol = _as_text(position.get("root_symbol"))
        if symbol is None:
            continue
        bucket = buckets[symbol]
        option_symbol = _as_text(position.get("long_symbol"))
        if option_symbol is not None:
            bucket["contracts"].add(option_symbol)
        bucket["position_count"] += 1
        if str(position.get("status") or "").lower() in OPEN_POSITION_STATUSES:
            bucket["open_position_count"] += 1
        else:
            bucket["closed_position_count"] += 1
        realized = _coerce_float(position.get("realized_pnl")) or 0.0
        net = _coerce_float(position.get("net_pnl")) or 0.0
        bucket["realized_pnl"] += realized
        bucket["net_pnl"] += net
        reason = _as_text(position.get("last_exit_reason"))
        if reason is not None:
            bucket["exit_reasons"][reason] += 1

    rows: list[dict[str, Any]] = []
    for symbol, bucket in sorted(buckets.items()):
        rows.append(
            {
                "symbol": symbol,
                "selected_contracts": sorted(bucket["contracts"]),
                "entry_attempt_count": bucket["entry_attempt_count"],
                "exit_attempt_count": bucket["exit_attempt_count"],
                "position_count": bucket["position_count"],
                "closed_position_count": bucket["closed_position_count"],
                "open_position_count": bucket["open_position_count"],
                "avg_entry_spread_pct": _avg_metric(bucket.get("entry_spread_pct", [])),
                "avg_entry_quote_age_seconds": _avg_metric(
                    bucket.get("entry_quote_age_seconds", []),
                    digits=2,
                ),
                "avg_entry_slippage_vs_midpoint": _avg_metric(bucket.get("entry_slippage_vs_midpoint", [])),
                "avg_entry_price_improvement_vs_limit": _avg_metric(bucket.get("entry_price_improvement_vs_limit", [])),
                "avg_exit_slippage_vs_midpoint": _avg_metric(bucket.get("exit_slippage_vs_midpoint", [])),
                "avg_exit_price_improvement_vs_limit": _avg_metric(bucket.get("exit_price_improvement_vs_limit", [])),
                "avg_entry_delta": _avg_metric(bucket.get("entry_delta", [])),
                "avg_entry_days_to_expiration": _avg_metric(
                    bucket.get("entry_days_to_expiration", []),
                    digits=1,
                ),
                "avg_entry_daily_volume": _avg_metric(
                    bucket.get("entry_daily_volume", []),
                    digits=0,
                ),
                "avg_entry_open_interest": _avg_metric(
                    bucket.get("entry_open_interest", []),
                    digits=0,
                ),
                "exit_reasons": dict(bucket["exit_reasons"]),
                "realized_pnl": _round_money(bucket["realized_pnl"]),
                "net_pnl": _round_money(bucket["net_pnl"]),
            }
        )
    return rows


def _build_quality_summary(
    *,
    attempts: list[dict[str, Any]],
    quality_by_symbol: list[dict[str, Any]],
) -> dict[str, Any]:
    entry_attempts = [row for row in attempts if str(row.get("trade_intent") or "open").lower() != "close"]
    exit_attempts = [row for row in attempts if str(row.get("trade_intent") or "open").lower() == "close"]
    return {
        "symbol_count": len(quality_by_symbol),
        "entry_attempt_count": len(entry_attempts),
        "exit_attempt_count": len(exit_attempts),
        "avg_entry_spread_pct": _avg_metric(
            [_mapping(_mapping(row.get("option_selection")).get("quote")).get("spread_pct") for row in entry_attempts]
        ),
        "avg_entry_quote_age_seconds": _avg_metric(
            [_mapping(_mapping(row.get("option_selection")).get("quote")).get("age_seconds") for row in entry_attempts],
            digits=2,
        ),
        "avg_entry_slippage_vs_midpoint": _avg_metric([_mapping(row.get("fill_quality")).get("slippage_vs_midpoint") for row in entry_attempts]),
        "avg_exit_slippage_vs_midpoint": _avg_metric([_mapping(row.get("fill_quality")).get("slippage_vs_midpoint") for row in exit_attempts]),
        "winning_symbol_count": sum(1 for row in quality_by_symbol if (_coerce_float(row.get("net_pnl")) or 0) > 0),
        "losing_symbol_count": sum(1 for row in quality_by_symbol if (_coerce_float(row.get("net_pnl")) or 0) < 0),
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
    return (source.get("flow") == "finviz_direct" and source.get("feed_id") == feed_id) or policy_ref.get("strategy_config_id") == feed_id


def _summarize_intent(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = _mapping(row.get("payload"))
    source = _mapping(payload.get("source"))
    option_selection = _summarize_option_selection(payload.get("option_selection"))
    execution_admission = _mapping(payload.get("execution_admission"))
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
        "option_selection": option_selection,
        "execution_admission": execution_admission,
        "admission_state": execution_admission.get("admission_state") or execution_admission.get("status"),
        "admission_reason": execution_admission.get("reason")
        or next(iter(execution_admission.get("reason_codes") or []), None),
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
    resolved_market_date = _as_text(market_date) or datetime.now(NEW_YORK).date().isoformat()
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

    raw_attempts: list[dict[str, Any]] = []
    raw_positions: list[dict[str, Any]] = []
    raw_intents: list[dict[str, Any]] = []
    orders_by_attempt: dict[str, list[dict[str, Any]]] = defaultdict(list)
    fills_by_attempt: dict[str, list[dict[str, Any]]] = defaultdict(list)
    closes_by_position: dict[str, list[dict[str, Any]]] = defaultdict(list)
    attempts: list[dict[str, Any]] = []
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
                orders_by_attempt[str(order.get("execution_attempt_id"))].append(dict(order))
            for fill in execution_store.list_fills(execution_attempt_ids=attempt_ids):
                fills_by_attempt[str(fill.get("execution_attempt_id"))].append(dict(fill))

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
        if position_ids:
            for close in execution_store.list_position_closes(position_ids=position_ids):
                closes_by_position[str(close.get("position_id"))].append(dict(close))

    if execution_store.intent_schema_ready():
        raw_intents = [
            dict(row)
            for row in execution_store.list_execution_intents(
                bot_id=DEFAULT_BOT_ID,
                automation_id=DEFAULT_AUTOMATION_ID,
                limit=200,
            )
            if _intent_matches(row, feed_id=resolved_feed_id)
        ]

    intents_by_attempt = {str(row.get("execution_attempt_id")): row for row in raw_intents if row.get("execution_attempt_id")}
    attempts = [
        _summarize_attempt(
            row,
            orders=orders_by_attempt[str(row.get("execution_attempt_id"))],
            fills=fills_by_attempt[str(row.get("execution_attempt_id"))],
            intent=intents_by_attempt.get(str(row.get("execution_attempt_id"))),
        )
        for row in raw_attempts
    ]
    attempts_by_id = {str(row.get("execution_attempt_id")): row for row in attempts if row.get("execution_attempt_id")}
    positions = [
        _summarize_position(
            row,
            closes=closes_by_position[str(row.get("position_id"))],
            attempts_by_id=attempts_by_id,
        )
        for row in raw_positions
    ]
    intents = [_summarize_intent(row) for row in raw_intents]
    quality_by_symbol = _build_quality_by_symbol(
        attempts=attempts,
        positions=positions,
        market_date=resolved_market_date,
    )
    quality_summary = _build_quality_summary(
        attempts=attempts,
        quality_by_symbol=quality_by_symbol,
    )

    open_positions = [row for row in positions if str(row.get("status") or "").lower() in OPEN_POSITION_STATUSES]
    realized_pnl = sum(_coerce_float(row.get("realized_pnl")) or 0.0 for row in positions)
    unrealized_pnl = sum(_coerce_float(row.get("unrealized_pnl")) or 0.0 for row in open_positions)
    filled_entry_count = sum(
        1 for row in attempts if str(row.get("trade_intent") or "open").lower() == "open" and str(row.get("status") or "").lower() == "filled"
    )
    active_entry_intent_count = sum(
        1
        for row in intents
        if str(row.get("trade_intent") or "open").lower() == "open" and str(row.get("state") or "").lower() in {"pending", "claimed", "dispatching"}
    )
    position_entry_count = sum(1 for row in positions if str(row.get("market_date_opened") or "") == resolved_market_date)
    session_entry_count = max(filled_entry_count, position_entry_count)
    recent_feed_runs = [_summarize_feed_run(row) for row in feed_runs]
    recent_direct_runs = [_summarize_direct_run(row) for row in direct_runs]
    latest_lifecycle_counts = (
        {} if latest_direct_run is None else summarize_lifecycle_decision_states(_list(_mapping(latest_direct_run.get("result")).get("decisions")))
    )
    lifecycle_positions = [row for row in positions if _position_in_lifecycle_scope(row, market_date=resolved_market_date)]
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
            "latest_feed_job_run_id": None if latest_feed_run is None else latest_feed_run.get("job_run_id"),
            "latest_direct_job_run_id": None if latest_direct_run is None else latest_direct_run.get("job_run_id"),
            "feed_run_count": len(feed_runs),
            "direct_run_count": len(direct_runs),
            "attempt_count": len(attempts),
            "filled_attempt_count": sum(1 for row in attempts if str(row.get("status") or "") == "filled"),
            "filled_entry_count": filled_entry_count,
            "position_entry_count": position_entry_count,
            "session_entry_count": session_entry_count,
            "intent_count": len(intents),
            "active_intent_count": sum(1 for row in intents if str(row.get("state") or "").lower() in {"pending", "claimed", "dispatching"}),
            "active_entry_intent_count": active_entry_intent_count,
            "position_count": len(positions),
            "open_position_count": len(open_positions),
            "closed_position_count": sum(1 for row in positions if str(row.get("status") or "").lower() == "closed"),
            "close_lifecycle_status": close_lifecycle.get("status"),
            "active_close_attempt_count": close_lifecycle.get("active_close_attempt_count"),
            "pending_close_intent_count": close_lifecycle.get("pending_close_intent_count"),
            "failed_close_attempt_count": close_lifecycle.get("failed_close_attempt_count"),
            "stale_reconciliation_skip_count": close_lifecycle.get("stale_reconciliation_skip_count"),
            "intent_mismatch_reject_count": close_lifecycle.get("intent_mismatch_reject_count"),
            "realized_pnl": round(realized_pnl, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "net_pnl": round(realized_pnl + unrealized_pnl, 2),
            "quality": quality_summary,
            "latest_lifecycle_decision_state_counts": latest_lifecycle_counts,
        },
        "attention": attention,
        "details": {
            "recent_feed_runs": recent_feed_runs,
            "recent_direct_runs": recent_direct_runs,
            "quality_by_symbol": quality_by_symbol,
            "positions": positions,
            "attempts": attempts,
            "intents": intents[:limit],
            "close_lifecycle": close_lifecycle,
        },
    }


__all__ = ["build_finviz_direct_ledger"]
