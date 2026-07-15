from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
import hashlib
from typing import Any

from core.money import option_contract_notional, option_limit_price, repriced_limit_price
from core.services.backtest.market_context import (
    merge_market_context_from_strategy_results,
    summarize_market_context_day_results,
)
from core.services.backtest.market_slices import load_latest_historical_option_quotes
from core.services.backtest.strategy_rerun import build_strategy_rerun_backtest
from core.services.backtest.strategy_scope import load_backtest_strategy_scope
from core.services.execution.order_requests import (
    normalize_submit_order_request,
    resolve_open_limit_price,
    validate_option_structure_submission,
)
from core.services.market_dates import NEW_YORK
from core.services.option_structures import (
    build_order_payload,
    candidate_legs,
    net_premium_kind,
    normalize_legs,
    signed_net_limit_price,
    structure_quote_snapshot,
    unique_leg_symbols,
)
from core.services.session_positions import OPEN_TRADE_INTENT
from core.services.trading_engine.close_policy import resolve_exit_policy_snapshot
from core.services.trading_engine.entry_signals import candidate_payload
from core.services.trading_strategy_runtime import build_entry_runtime
from core.storage.serializers import parse_datetime
from core.value_coercion import as_mapping, coerce_float, coerce_int, utc_now_iso

SIMULATED_BROKER = "simulated_alpaca"
DEFAULT_FILL_MODEL = "quote_touch_with_executor_repricing"


@dataclass(frozen=True)
class _QuoteContext:
    legs: list[dict[str, Any]]
    latest_quotes: dict[str, dict[str, Any]]
    snapshot_rows: list[dict[str, Any]]
    trade_rows: list[dict[str, Any]]
    quote_scope: str
    quote_timestamp: datetime | None
    start_at: datetime
    expires_at: datetime
    missing_symbols: tuple[str, ...]


def _session_bounds(market_date: str | date) -> tuple[datetime, datetime]:
    parsed = date.fromisoformat(str(market_date)) if not isinstance(market_date, date) else market_date
    start = datetime.combine(parsed, time.min, tzinfo=NEW_YORK)
    end = start + timedelta(days=1)
    return start.astimezone(UTC), end.astimezone(UTC)


def _utc_datetime(value: Any) -> datetime | None:
    parsed = parse_datetime(value)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _utc_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _stable_id(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha1("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}:{digest}"


def _admission_allows_simulation(admission: Mapping[str, Any] | None) -> bool:
    if admission is None:
        return False
    state = str(admission.get("admission_state") or "").strip().lower()
    if state in {"blocked", "failed", "rejected", "unknown"}:
        return False
    blockers = [
        str(item) for item in admission.get("blockers") or [] if str(item or "").strip() and str(item) != "backtest_execution_capacity_deferred"
    ]
    return not blockers


def _quantity_from(signal: Mapping[str, Any], admission: Mapping[str, Any] | None) -> int:
    execution_shape = as_mapping(signal.get("execution_shape"))
    order_payload = as_mapping(signal.get("order_payload")) or as_mapping(execution_shape.get("order_payload"))
    quantity = coerce_int(
        (None if admission is None else admission.get("requested_quantity"))
        or execution_shape.get("quantity")
        or order_payload.get("qty")
        or order_payload.get("quantity")
        or signal.get("quantity")
    )
    return max(quantity or 1, 1)


def _candidate_for_execution(
    *,
    signal: Mapping[str, Any],
    quote_timestamp: datetime | None,
    max_quote_age_seconds: Any,
) -> dict[str, Any]:
    payload = candidate_payload(dict(signal))
    payload.setdefault("strategy", signal.get("strategy"))
    payload.setdefault("strategy_family", signal.get("strategy") or signal.get("trade_structure"))
    payload.setdefault("underlying_symbol", signal.get("underlying_symbol"))
    order_payload = as_mapping(signal.get("order_payload"))
    if order_payload:
        payload.setdefault("order_payload", dict(order_payload))
    if quote_timestamp is not None:
        payload["quote_freshness"] = {
            "timestamp": _utc_iso(quote_timestamp),
            "max_quote_age_seconds": coerce_int(max_quote_age_seconds),
            "source": "historical_quote_snapshot",
        }
    return payload


def _quote_context(
    *,
    storage: Any,
    runtime: Any,
    day_result: Mapping[str, Any],
    signal: Mapping[str, Any],
    ttl_minutes: int,
) -> _QuoteContext:
    market_date = str(day_result["market_date"])
    session_start, session_end = _session_bounds(market_date)
    day_as_of = _utc_datetime(day_result.get("as_of")) or session_end
    underlying = str(signal.get("underlying_symbol") or as_mapping(signal.get("candidate")).get("underlying_symbol") or "").upper()
    raw_legs = list(signal.get("legs") or as_mapping(signal.get("execution_shape")).get("legs") or [])
    legs = normalize_legs(raw_legs) or candidate_legs(candidate_payload(dict(signal)))
    symbols = tuple(unique_leg_symbols(legs))
    latest_quotes = load_latest_historical_option_quotes(
        storage=storage,
        underlying_symbol=underlying,
        captured_from=session_start,
        captured_to=min(day_as_of, session_end),
        label=runtime.trading_strategy_id,
        profile=runtime.build_settings.build_profile,
        limit=1000,
    )
    latest_by_symbol = {
        str(row.get("option_symbol") or "").upper(): dict(row)
        for row in latest_quotes.rows
        if str(row.get("option_symbol") or "").strip().upper() in symbols
    }
    quote_timestamps = [
        value
        for value in (_utc_datetime(row.get("captured_at") or row.get("source_timestamp")) for row in latest_by_symbol.values())
        if value is not None
    ]
    quote_timestamp = min(quote_timestamps) if quote_timestamps else None
    start_at = max(quote_timestamps) if quote_timestamps else day_as_of
    expires_at = start_at + timedelta(minutes=max(ttl_minutes, 1))
    snapshot_rows = storage.market_data.list_option_quote_snapshots_window(
        option_symbols=list(symbols),
        captured_from=start_at - timedelta(seconds=1),
        captured_to=expires_at,
        label=runtime.trading_strategy_id if latest_quotes.scope.startswith("strategy_label") else None,
        profile=runtime.build_settings.build_profile if latest_quotes.scope.endswith("profile") else None,
        resolution="1m",
    )
    if not snapshot_rows and latest_by_symbol:
        snapshot_rows = list(latest_by_symbol.values())
    trade_rows = storage.market_data.list_option_trade_ticks_window(
        option_symbols=list(symbols),
        captured_from=start_at,
        captured_to=expires_at,
        label=runtime.trading_strategy_id if latest_quotes.scope.startswith("strategy_label") else None,
        profile=runtime.build_settings.build_profile if latest_quotes.scope.endswith("profile") else None,
    )
    missing_symbols = tuple(symbol for symbol in symbols if symbol not in latest_by_symbol)
    return _QuoteContext(
        legs=legs,
        latest_quotes=latest_by_symbol,
        snapshot_rows=[dict(row) for row in snapshot_rows],
        trade_rows=[dict(row) for row in trade_rows],
        quote_scope=latest_quotes.scope,
        quote_timestamp=quote_timestamp,
        start_at=start_at,
        expires_at=expires_at,
        missing_symbols=missing_symbols,
    )


def _initial_order_request(
    *,
    runtime: Any,
    signal: Mapping[str, Any],
    candidate: Mapping[str, Any],
    execution_policy: Mapping[str, Any],
    quantity: int,
    client_order_id: str,
) -> tuple[dict[str, Any], float]:
    signal_order_payload = as_mapping(signal.get("order_payload")) or as_mapping(as_mapping(signal.get("execution_shape")).get("order_payload"))
    explicit_limit = coerce_float(signal_order_payload.get("limit_price") or signal.get("limit_price"))
    limit_price = resolve_open_limit_price(
        candidate_payload=dict(candidate),
        explicit_limit_price=explicit_limit,
        execution_policy=dict(execution_policy),
    )
    order_request = dict(signal_order_payload)
    if not order_request:
        order_request = build_order_payload(
            legs=candidate_legs(candidate),
            limit_price=limit_price,
            strategy_family=runtime.trade_structure,
            trade_intent=OPEN_TRADE_INTENT,
            quantity=quantity,
        )
    order_request["qty"] = str(quantity)
    order_request["limit_price"] = str(
        signed_net_limit_price(
            limit_price=limit_price,
            strategy_family=runtime.trade_structure,
            trade_intent=OPEN_TRADE_INTENT,
        )
        if str(order_request.get("order_class") or "").lower() == "mleg"
        else option_limit_price(limit_price)
    )
    order_request["client_order_id"] = client_order_id
    return order_request, limit_price


def _quote_rows_by_time(
    snapshot_rows: list[dict[str, Any]], latest_quotes: Mapping[str, Mapping[str, Any]]
) -> list[tuple[datetime, dict[str, dict[str, Any]]]]:
    grouped: dict[datetime, dict[str, dict[str, Any]]] = {}
    for row in snapshot_rows:
        symbol = str(row.get("option_symbol") or "").upper()
        captured_at = _utc_datetime(row.get("captured_at") or row.get("bucket_at"))
        if not symbol or captured_at is None:
            continue
        grouped.setdefault(captured_at, {})[symbol] = dict(row)
    if not grouped and latest_quotes:
        timestamps = [
            value
            for value in (_utc_datetime(row.get("captured_at") or row.get("source_timestamp")) for row in latest_quotes.values())
            if value is not None
        ]
        if timestamps:
            grouped[max(timestamps)] = {symbol: dict(row) for symbol, row in latest_quotes.items()}
    return sorted(grouped.items(), key=lambda item: item[0])


def _with_latest_quotes(
    current: dict[str, dict[str, Any]],
    rows: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    updated = {symbol: dict(row) for symbol, row in current.items()}
    for symbol, row in rows.items():
        updated[symbol] = dict(row)
    return updated


def _limit_fills_quote(*, premium_kind: str | None, natural_value: float | None, limit_price: float) -> bool:
    if natural_value is None or natural_value <= 0:
        return False
    if premium_kind == "debit":
        return natural_value <= limit_price
    return natural_value >= limit_price


def _simulate_fill(
    *,
    runtime: Any,
    quantity: int,
    order_request: Mapping[str, Any],
    quote_context: _QuoteContext,
    execution_policy: Mapping[str, Any],
) -> dict[str, Any]:
    premium_kind = net_premium_kind(runtime.trade_structure)
    original_limit = abs(coerce_float(order_request.get("limit_price")) or 0.0)
    current_limit = original_limit
    reprice_policy = as_mapping(execution_policy.get("repricing_policy"))
    reprice_enabled = bool(reprice_policy.get("enabled", True))
    stale_after_seconds = max(coerce_int(reprice_policy.get("stale_after_seconds")) or 75, 1)
    max_reprices = max(coerce_int(reprice_policy.get("max_reprices")) or 0, 0)
    price_step = coerce_float(reprice_policy.get("price_step")) or 0.01
    max_concession = coerce_float(reprice_policy.get("max_concession")) or coerce_float(execution_policy.get("max_credit_concession")) or 0.02
    next_reprice_at = quote_context.start_at + timedelta(seconds=stale_after_seconds)
    reprice_count = 0
    quote_sets = _quote_rows_by_time(quote_context.snapshot_rows, quote_context.latest_quotes)
    latest_quotes: dict[str, dict[str, Any]] = {}
    evaluation_rows: list[dict[str, Any]] = []
    last_snapshot: dict[str, Any] | None = None

    for captured_at, rows in quote_sets:
        if captured_at > quote_context.expires_at:
            break
        latest_quotes = _with_latest_quotes(latest_quotes, rows)
        while reprice_enabled and reprice_count < max_reprices and captured_at >= next_reprice_at:
            repriced = repriced_limit_price(
                current_limit=current_limit,
                original_limit=original_limit,
                step=price_step,
                max_concession=max_concession,
                premium_kind=premium_kind,
            )
            if repriced is None or repriced == current_limit:
                break
            current_limit = repriced
            reprice_count += 1
            next_reprice_at += timedelta(seconds=stale_after_seconds)
        snapshot = structure_quote_snapshot(
            legs=quote_context.legs,
            strategy_family=runtime.trade_structure,
            quotes_by_symbol=latest_quotes,
            normalized_legs=True,
        )
        if snapshot is None:
            evaluation_rows.append(
                {
                    "captured_at": _utc_iso(captured_at),
                    "limit_price": current_limit,
                    "status": "missing_structure_quote",
                }
            )
            continue
        last_snapshot = dict(snapshot)
        natural_value = coerce_float(snapshot.get("natural_value"))
        midpoint_value = coerce_float(snapshot.get("midpoint_value"))
        fills = _limit_fills_quote(
            premium_kind=premium_kind,
            natural_value=natural_value,
            limit_price=current_limit,
        )
        evaluation_rows.append(
            {
                "captured_at": _utc_iso(captured_at),
                "limit_price": current_limit,
                "natural_value": natural_value,
                "midpoint_value": midpoint_value,
                "premium_kind": premium_kind,
                "reprice_count": reprice_count,
                "status": "filled" if fills else "working",
            }
        )
        if fills:
            return {
                "status": "filled",
                "reason": "quote_touch_fill",
                "filled_at": _utc_iso(captured_at),
                "fill_price": current_limit,
                "fill_notional": option_contract_notional(current_limit, quantity),
                "reprice_count": reprice_count,
                "final_limit_price": current_limit,
                "quote_snapshot": snapshot,
                "quote_evaluations": evaluation_rows,
                "fill_fidelity": "simulated_quote_touch_fill",
            }

    while reprice_enabled and latest_quotes and reprice_count < max_reprices and next_reprice_at <= quote_context.expires_at:
        repriced = repriced_limit_price(
            current_limit=current_limit,
            original_limit=original_limit,
            step=price_step,
            max_concession=max_concession,
            premium_kind=premium_kind,
        )
        if repriced is None or repriced == current_limit:
            break
        current_limit = repriced
        reprice_count += 1
        snapshot = structure_quote_snapshot(
            legs=quote_context.legs,
            strategy_family=runtime.trade_structure,
            quotes_by_symbol=latest_quotes,
            normalized_legs=True,
        )
        if snapshot is None:
            evaluation_rows.append(
                {
                    "captured_at": _utc_iso(next_reprice_at),
                    "limit_price": current_limit,
                    "reprice_count": reprice_count,
                    "status": "missing_structure_quote_after_reprice",
                }
            )
            next_reprice_at += timedelta(seconds=stale_after_seconds)
            continue
        last_snapshot = dict(snapshot)
        natural_value = coerce_float(snapshot.get("natural_value"))
        midpoint_value = coerce_float(snapshot.get("midpoint_value"))
        fills = _limit_fills_quote(
            premium_kind=premium_kind,
            natural_value=natural_value,
            limit_price=current_limit,
        )
        evaluation_rows.append(
            {
                "captured_at": _utc_iso(next_reprice_at),
                "limit_price": current_limit,
                "natural_value": natural_value,
                "midpoint_value": midpoint_value,
                "premium_kind": premium_kind,
                "reprice_count": reprice_count,
                "status": "filled_after_reprice" if fills else "working_after_reprice",
            }
        )
        if fills:
            return {
                "status": "filled",
                "reason": "quote_touch_after_reprice",
                "filled_at": _utc_iso(next_reprice_at),
                "fill_price": current_limit,
                "fill_notional": option_contract_notional(current_limit, quantity),
                "reprice_count": reprice_count,
                "final_limit_price": current_limit,
                "quote_snapshot": snapshot,
                "quote_evaluations": evaluation_rows,
                "fill_fidelity": "simulated_quote_touch_fill",
            }
        next_reprice_at += timedelta(seconds=stale_after_seconds)

    return {
        "status": "expired",
        "reason": "no_quote_touch_before_ttl",
        "filled_at": None,
        "fill_price": None,
        "fill_notional": None,
        "reprice_count": reprice_count,
        "final_limit_price": current_limit,
        "quote_snapshot": last_snapshot,
        "quote_evaluations": evaluation_rows,
        "fill_fidelity": "simulated_no_fill",
    }


def _simulate_selected_entry(
    *,
    storage: Any,
    runtime: Any,
    day_result: Mapping[str, Any],
    signal: Mapping[str, Any],
    decision: Mapping[str, Any],
    admission: Mapping[str, Any] | None,
) -> dict[str, Any]:
    quantity = _quantity_from(signal, admission)
    execution_policy = runtime.strategy.execution.execution_policy_for_intent_kind("open", quantity=quantity)
    ttl_minutes = coerce_int(execution_policy.get("submit_ttl_minutes")) or 5
    quote_context = _quote_context(
        storage=storage,
        runtime=runtime,
        day_result=day_result,
        signal=signal,
        ttl_minutes=ttl_minutes,
    )
    exit_policy = resolve_exit_policy_snapshot(
        session_date=str(day_result["market_date"]),
        payload=runtime.strategy.management_policy,
    )
    risk_policy = dict(runtime.strategy.risk_defaults)
    candidate = _candidate_for_execution(
        signal=signal,
        quote_timestamp=quote_context.quote_timestamp,
        max_quote_age_seconds=execution_policy.get("max_quote_age_seconds"),
    )
    execution_intent_id = _stable_id("backtest_intent", decision.get("trade_decision_id"), day_result.get("market_date"))
    execution_attempt_id = _stable_id("backtest_attempt", execution_intent_id, DEFAULT_FILL_MODEL)
    broker_order_id = _stable_id("backtest_order", execution_attempt_id, "open")
    client_order_id = f"bt-{hashlib.sha1(execution_attempt_id.encode('utf-8')).hexdigest()[:20]}"
    order_request, initial_limit_price = _initial_order_request(
        runtime=runtime,
        signal=signal,
        candidate=candidate,
        execution_policy=execution_policy,
        quantity=quantity,
        client_order_id=client_order_id,
    )
    attempt_payload = {
        "execution_attempt_id": execution_attempt_id,
        "trade_intent": OPEN_TRADE_INTENT,
        "strategy": runtime.trade_structure,
        "strategy_family": runtime.trade_structure,
        "underlying_symbol": signal.get("underlying_symbol"),
        "expiration_date": candidate.get("expiration_date"),
        "quantity": quantity,
        "limit_price": initial_limit_price,
        "candidate": candidate,
        "legs": quote_context.legs,
        "request": {
            "order": order_request,
            "execution_policy": dict(execution_policy),
            "executor_profile": runtime.strategy.execution.executor_profile_snapshot("open"),
            "exit_policy": exit_policy,
            "risk_policy": risk_policy,
            "trading_strategy_id": runtime.trading_strategy_id,
            "config_hash": runtime.config_hash,
            "trade_intent": OPEN_TRADE_INTENT,
        },
        "candidate_generated_at": None if quote_context.quote_timestamp is None else _utc_iso(quote_context.quote_timestamp),
        "requested_at": _utc_iso(quote_context.start_at),
    }
    normalized_order = normalize_submit_order_request(
        payload=attempt_payload,
        order_request=order_request,
    )
    structure_guard = validate_option_structure_submission(
        payload={**attempt_payload, "request": {**attempt_payload["request"], "order": normalized_order}},
        order_request=normalized_order,
        now=quote_context.start_at,
    )
    if quote_context.missing_symbols:
        structure_guard = {
            "ok": False,
            "reason": "historical_quote_snapshot_missing",
            "message": "Execution simulation could not resolve historical quote snapshots for every option leg.",
            "reason_codes": ["historical_quote_snapshot_missing"],
            "blockers": ["historical_quote_snapshot_missing"],
            "evidence": {"missing_symbols": list(quote_context.missing_symbols)},
        }

    if not structure_guard["ok"]:
        fill = {
            "status": "failed",
            "reason": str(structure_guard["reason"]),
            "filled_at": None,
            "fill_price": None,
            "fill_notional": None,
            "reprice_count": 0,
            "final_limit_price": initial_limit_price,
            "quote_snapshot": None,
            "quote_evaluations": [],
            "fill_fidelity": "simulated_blocked_before_submit",
        }
    else:
        fill = _simulate_fill(
            runtime=runtime,
            quantity=quantity,
            order_request=normalized_order,
            quote_context=quote_context,
            execution_policy=execution_policy,
        )

    attempt_status = "filled" if fill["status"] == "filled" else ("failed" if fill["status"] == "failed" else "expired")
    submitted_at = _utc_iso(quote_context.start_at) if structure_guard["ok"] else None
    completed_at = fill.get("filled_at") or _utc_iso(quote_context.expires_at)
    intent_state = "filled" if attempt_status == "filled" else ("failed" if attempt_status == "failed" else "expired")
    final_limit_price = coerce_float(fill.get("final_limit_price"))
    order = {
        "execution_attempt_id": execution_attempt_id,
        "broker": SIMULATED_BROKER,
        "broker_order_id": broker_order_id,
        "parent_broker_order_id": None,
        "client_order_id": client_order_id,
        "order_status": attempt_status,
        "order_type": normalized_order.get("type"),
        "time_in_force": normalized_order.get("time_in_force"),
        "order_class": normalized_order.get("order_class") or ("mleg" if len(quote_context.legs) > 1 else "single"),
        "side": normalized_order.get("side"),
        "symbol": normalized_order.get("symbol"),
        "quantity": quantity,
        "limit_price": final_limit_price,
        "filled_qty": quantity if attempt_status == "filled" else 0,
        "filled_avg_price": fill.get("fill_price"),
        "submitted_at": submitted_at,
        "updated_at": completed_at,
        "order": {
            **normalized_order,
            "final_limit_price": final_limit_price,
            "reprice_count": fill.get("reprice_count"),
        },
    }
    fill_row = None
    if attempt_status == "filled":
        fill_row = {
            "execution_attempt_id": execution_attempt_id,
            "broker": SIMULATED_BROKER,
            "broker_fill_id": _stable_id("backtest_fill", execution_attempt_id, broker_order_id),
            "broker_order_id": broker_order_id,
            "symbol": str(signal.get("underlying_symbol") or ""),
            "side": normalized_order.get("side"),
            "fill_type": "simulated_quote_touch",
            "quantity": quantity,
            "cumulative_quantity": quantity,
            "remaining_quantity": 0,
            "price": fill.get("fill_price"),
            "filled_at": fill.get("filled_at"),
            "fill": {
                "fill_model": DEFAULT_FILL_MODEL,
                "fidelity": fill.get("fill_fidelity"),
                "quote_snapshot": fill.get("quote_snapshot"),
            },
        }
    return {
        "intent": {
            "execution_intent_id": execution_intent_id,
            "trading_strategy_id": runtime.trading_strategy_id,
            "trade_signal_id": signal.get("trade_signal_id"),
            "trade_decision_id": decision.get("trade_decision_id"),
            "execution_attempt_id": execution_attempt_id,
            "intent_kind": "open",
            "state": intent_state,
            "expires_at": _utc_iso(quote_context.expires_at),
            "payload": {
                "validation_provenance": "backtest_execution_simulation",
                "fill_model": DEFAULT_FILL_MODEL,
                "execution_policy": dict(execution_policy),
                "executor_profile": runtime.strategy.execution.executor_profile_snapshot("open"),
                "exit_policy": exit_policy,
                "risk_policy": risk_policy,
            },
        },
        "attempt": {
            "execution_attempt_id": execution_attempt_id,
            "execution_intent_id": execution_intent_id,
            "trading_strategy_id": runtime.trading_strategy_id,
            "trade_signal_id": signal.get("trade_signal_id"),
            "trade_decision_id": decision.get("trade_decision_id"),
            "admission_decision_id": None if admission is None else admission.get("admission_decision_id"),
            "underlying_symbol": signal.get("underlying_symbol"),
            "strategy": runtime.trade_structure,
            "strategy_family": runtime.trade_structure,
            "trade_intent": OPEN_TRADE_INTENT,
            "quantity": quantity,
            "requested_quantity": quantity,
            "requested_limit_price": initial_limit_price,
            "limit_price": fill.get("final_limit_price"),
            "requested_at": _utc_iso(quote_context.start_at),
            "submitted_at": submitted_at,
            "completed_at": completed_at,
            "status": attempt_status,
            "broker": SIMULATED_BROKER,
            "broker_order_id": broker_order_id if structure_guard["ok"] else None,
            "client_order_id": client_order_id,
            "request": {**attempt_payload["request"], "order": normalized_order},
            "candidate": candidate,
            "legs": quote_context.legs,
            "order_payload": normalized_order,
            "economics": as_mapping(signal.get("economics")),
            "error_text": None if structure_guard["ok"] else str(structure_guard["message"]),
        },
        "order": order if structure_guard["ok"] else None,
        "fill": fill_row,
        "diagnostics": {
            "fill_model": DEFAULT_FILL_MODEL,
            "structure_guard": structure_guard,
            "quote_scope": quote_context.quote_scope,
            "quote_timestamp": None if quote_context.quote_timestamp is None else _utc_iso(quote_context.quote_timestamp),
            "quote_snapshot_count": len(quote_context.snapshot_rows),
            "trade_tick_count": len(quote_context.trade_rows),
            "quote_evaluations": fill.get("quote_evaluations"),
            "reprice_count": fill.get("reprice_count"),
            "final_limit_price": fill.get("final_limit_price"),
            "fill_fidelity": fill.get("fill_fidelity"),
        },
    }


def _simulate_day(
    *,
    storage: Any,
    runtime: Any,
    day_result: dict[str, Any],
) -> dict[str, Any]:
    signals_by_id = {str(row.get("trade_signal_id") or ""): row for row in day_result.get("signals") or [] if isinstance(row, Mapping)}
    admissions_by_decision_id = {
        str(row.get("trade_decision_id") or ""): row for row in day_result.get("admissions") or [] if isinstance(row, Mapping)
    }
    intents: list[dict[str, Any]] = []
    attempts: list[dict[str, Any]] = []
    orders: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for decision in day_result.get("decisions") or []:
        if not isinstance(decision, Mapping) or str(decision.get("decision_state") or "") != "selected":
            continue
        signal = signals_by_id.get(str(decision.get("trade_signal_id") or ""))
        admission = admissions_by_decision_id.get(str(decision.get("trade_decision_id") or ""))
        if signal is None:
            skipped.append({"trade_decision_id": decision.get("trade_decision_id"), "reason": "signal_missing"})
            continue
        if not _admission_allows_simulation(admission):
            skipped.append({"trade_decision_id": decision.get("trade_decision_id"), "reason": "admission_blocked_or_missing"})
            continue
        simulation = _simulate_selected_entry(
            storage=storage,
            runtime=runtime,
            day_result=day_result,
            signal=signal,
            decision=decision,
            admission=admission,
        )
        intents.append(simulation["intent"])
        attempts.append(simulation["attempt"])
        if simulation["order"] is not None:
            orders.append(simulation["order"])
        if simulation["fill"] is not None:
            fills.append(simulation["fill"])
        diagnostics.append(
            {
                "trade_decision_id": decision.get("trade_decision_id"),
                "execution_attempt_id": simulation["attempt"]["execution_attempt_id"],
                **simulation["diagnostics"],
            }
        )
    attempt_states = Counter(str(row.get("status") or "unknown") for row in attempts)
    return {
        "summary": {
            "fill_model": DEFAULT_FILL_MODEL,
            "intent_count": len(intents),
            "attempt_count": len(attempts),
            "order_count": len(orders),
            "fill_count": len(fills),
            "skipped_count": len(skipped),
            "attempt_status_counts": dict(sorted(attempt_states.items())),
            "fill_rate": None if not attempts else round(len(fills) / len(attempts), 4),
            "fidelity": "simulated_quote_touch_or_no_fill",
        },
        "simulated_intents": intents,
        "simulated_attempts": attempts,
        "simulated_orders": orders,
        "simulated_fills": fills,
        "execution_diagnostics": diagnostics,
        "skipped_decisions": skipped,
    }


def _update_strategy_execution(strategy_result: dict[str, Any]) -> None:
    day_results = [row for row in strategy_result.get("day_results") or [] if isinstance(row, dict)]
    attempts = [row for day in day_results for row in day.get("simulated_attempts") or [] if isinstance(row, Mapping)]
    fills = [row for day in day_results for row in day.get("simulated_fills") or [] if isinstance(row, Mapping)]
    orders = [row for day in day_results for row in day.get("simulated_orders") or [] if isinstance(row, Mapping)]
    state_counts = Counter(str(row.get("status") or "unknown") for row in attempts)
    strategy_result["execution"] = {
        "intent_count": sum(len(day.get("simulated_intents") or []) for day in day_results),
        "attempt_count": len(attempts),
        "order_count": len(orders),
        "fill_count": len(fills),
        "attempt_status_counts": dict(sorted(state_counts.items())),
        "fill_rate": None if not attempts else round(len(fills) / len(attempts), 4),
        "fill_model": DEFAULT_FILL_MODEL,
    }
    strategy_result["market_context"] = summarize_market_context_day_results(day_results)
    fidelity = as_mapping(strategy_result.get("fidelity_labels"))
    strategy_result["fidelity_labels"] = {
        **dict(fidelity),
        "execution": "simulated_execution_lifecycle",
        "fill": "simulated_quote_touch_or_no_fill",
    }
    strategy_result["outcome_label"] = (
        "simulated_fills_created" if fills else ("simulated_attempts_no_fill" if attempts else strategy_result.get("outcome_label"))
    )


def build_execution_simulation_backtest(
    *,
    start_date: str | date,
    end_date: str | date | None = None,
    strategy_ids: tuple[str, ...] | None = None,
    symbols: tuple[str, ...] | None = None,
    max_days: int = 31,
    market_data_symbol_limit: int = 250,
    candidate_limit: int = 10,
    per_symbol_top: int = 1,
    storage: Any,
    db_target: str,
    config_root: str | None = None,
    strategy_scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result = build_strategy_rerun_backtest(
        start_date=start_date,
        end_date=end_date,
        strategy_ids=strategy_ids,
        symbols=symbols,
        max_days=max_days,
        market_data_symbol_limit=market_data_symbol_limit,
        candidate_limit=candidate_limit,
        per_symbol_top=per_symbol_top,
        storage=storage,
        db_target=db_target,
        config_root=config_root,
        strategy_scope=strategy_scope,
    )
    strategies = load_backtest_strategy_scope(strategy_ids) if strategy_scope is None else dict(strategy_scope)
    for strategy_result in result.get("strategies") or []:
        if not isinstance(strategy_result, dict):
            continue
        strategy = strategies.get(str(strategy_result.get("trading_strategy_id") or ""))
        if strategy is None:
            continue
        runtime = build_entry_runtime(strategy)
        for day_result in strategy_result.get("day_results") or []:
            if not isinstance(day_result, dict):
                continue
            simulation = _simulate_day(storage=storage, runtime=runtime, day_result=day_result)
            day_result.update(simulation)
            day_result["fidelity_labels"] = {
                **dict(as_mapping(day_result.get("fidelity_labels"))),
                "execution": "simulated_execution_lifecycle",
                "fill": "simulated_quote_touch_or_no_fill",
            }
        _update_strategy_execution(strategy_result)

    total_attempts = sum(coerce_int(as_mapping(row.get("execution")).get("attempt_count")) or 0 for row in result.get("strategies") or [])
    total_fills = sum(coerce_int(as_mapping(row.get("execution")).get("fill_count")) or 0 for row in result.get("strategies") or [])
    market_context_summary = merge_market_context_from_strategy_results([row for row in result.get("strategies") or [] if isinstance(row, Mapping)])
    result["evaluation_mode"] = "execution_simulation_current_model"
    result["generated_at"] = utc_now_iso()
    result["summary"] = {
        **dict(as_mapping(result.get("summary"))),
        "attempt_count": total_attempts,
        "fill_count": total_fills,
        "fill_rate": None if total_attempts <= 0 else round(total_fills / total_attempts, 4),
        "market_context_status_counts": dict(market_context_summary.get("status_counts") or {}),
        "market_context_regime_buckets": list(market_context_summary.get("regime_buckets") or []),
    }
    result["market_context"] = market_context_summary
    result["fidelity_labels"] = {
        **dict(as_mapping(result.get("fidelity_labels"))),
        "mode": "execution_simulation_current_model",
        "execution": "simulated_execution_lifecycle",
        "fill": "simulated_quote_touch_or_no_fill",
        "live_writes": "none",
    }
    return result


__all__ = ["build_execution_simulation_backtest"]
