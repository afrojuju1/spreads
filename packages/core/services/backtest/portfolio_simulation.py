from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, date, datetime, time, timedelta
import hashlib
from typing import Any

from core.money import close_pnl, money_sum_float, premium_float
from core.services.backtest.execution_simulation import build_execution_simulation_backtest
from core.services.backtest.market_slices import load_latest_historical_option_quotes
from core.services.backtest.metrics import compute_backtest_performance_metrics
from core.services.backtest.strategy_scope import load_backtest_strategy_scope
from core.services.market_dates import NEW_YORK
from core.services.option_structures import net_premium_kind, normalize_legs, structure_quote_snapshot, structure_width, unique_leg_symbols
from core.services.position_lifecycle import build_close_decision_lifecycle
from core.services.session_positions import resolve_position_exposure
from core.services.trading_engine.close_policy import evaluate_exit_policy
from core.services.trading_strategy_runtime import build_entry_runtime
from core.storage.serializers import parse_datetime
from core.value_coercion import as_mapping, as_text, coerce_float, coerce_int, utc_iso, utc_now_iso

POSITION_FIDELITY = "simulated_from_execution_fills"
EXIT_FIDELITY = "current_exit_policy_simulated_marks"
PNL_FIDELITY = "simulated_quote_mark_pnl"


def _stable_id(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha1("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}:{digest}"


def _session_bounds(market_date: str | date) -> tuple[datetime, datetime]:
    parsed = date.fromisoformat(str(market_date)) if not isinstance(market_date, date) else market_date
    start = datetime.combine(parsed, time.min, tzinfo=NEW_YORK)
    end = start + timedelta(days=1)
    return start.astimezone(UTC), end.astimezone(UTC)


def _day_as_of(day_result: Mapping[str, Any]) -> datetime:
    parsed = parse_datetime(as_text(day_result.get("as_of")))
    if parsed is not None:
        return parsed.astimezone(UTC)
    _, session_end = _session_bounds(str(day_result["market_date"]))
    return session_end


def _artifact_rows(day_result: Mapping[str, Any], *keys: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in keys:
        value = day_result.get(key)
        if isinstance(value, list):
            rows.extend(dict(row) for row in value if isinstance(row, Mapping))
    return rows


def _fills_by_attempt(day_result: Mapping[str, Any]) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    attempts = _artifact_rows(day_result, "simulated_attempts", "execution_attempts")
    fills = _artifact_rows(day_result, "simulated_fills", "execution_fills")
    fills_by_attempt: dict[str, list[dict[str, Any]]] = {}
    for fill in fills:
        attempt_id = as_text(fill.get("execution_attempt_id"))
        if attempt_id is not None:
            fills_by_attempt.setdefault(attempt_id, []).append(fill)

    for attempt in attempts:
        attempt_id = as_text(attempt.get("execution_attempt_id"))
        if attempt_id is None or attempt_id in fills_by_attempt:
            continue
        if str(attempt.get("status") or attempt.get("attempt_state") or "").strip().lower() != "filled":
            continue
        price = coerce_float(attempt.get("filled_avg_price")) or coerce_float(attempt.get("limit_price"))
        quantity = coerce_float(attempt.get("quantity")) or coerce_float(attempt.get("filled_quantity"))
        fills_by_attempt[attempt_id] = [
            {
                "execution_attempt_id": attempt_id,
                "broker_fill_id": _stable_id("backtest_synth_fill", attempt_id),
                "quantity": quantity,
                "price": price,
                "filled_at": attempt.get("completed_at") or attempt.get("submitted_at") or attempt.get("requested_at"),
                "fill": {
                    "fidelity": "stored_filled_attempt_without_fill_rows",
                },
            }
        ]
    return attempts, fills_by_attempt


def _weighted_fill(fills: list[Mapping[str, Any]]) -> tuple[float, float | None, str | None]:
    total_quantity = 0.0
    notional = 0.0
    filled_at_values: list[str] = []
    for fill in fills:
        quantity = coerce_float(fill.get("quantity")) or 0.0
        price = coerce_float(fill.get("price"))
        if quantity <= 0:
            continue
        total_quantity += quantity
        if price is not None:
            notional += price * quantity
        filled_at = as_text(fill.get("filled_at"))
        if filled_at is not None:
            filled_at_values.append(filled_at)
    if total_quantity <= 0:
        return 0.0, None, None
    return total_quantity, premium_float(notional / total_quantity), min(filled_at_values, default=None)


def _position_from_fill(*, runtime: Any, day_result: Mapping[str, Any], attempt: Mapping[str, Any], fills: list[Mapping[str, Any]]) -> dict[str, Any] | None:
    quantity, entry_value, filled_at = _weighted_fill(fills)
    if quantity <= 0:
        return None
    if entry_value is None:
        entry_value = coerce_float(attempt.get("limit_price")) or coerce_float(attempt.get("requested_limit_price"))
    if entry_value is None:
        return None

    request = as_mapping(attempt.get("request"))
    candidate = as_mapping(attempt.get("candidate"))
    strategy_family = as_text(attempt.get("strategy_family")) or as_text(attempt.get("strategy")) or runtime.trade_structure
    legs = normalize_legs(attempt.get("legs"), expiration_date=as_text(attempt.get("expiration_date")))
    width = coerce_float(candidate.get("width")) or structure_width(legs, strategy=strategy_family)
    exposure = resolve_position_exposure(
        candidate=candidate,
        existing=None,
        entry_value=entry_value,
        width=width,
        quantity=quantity,
        strategy_family=strategy_family,
    )
    opened_at = filled_at or as_text(attempt.get("completed_at")) or as_text(attempt.get("submitted_at")) or as_text(attempt.get("requested_at"))
    position_id = _stable_id("backtest_position", attempt.get("execution_attempt_id"), opened_at)
    premium_kind = net_premium_kind(strategy_family)
    return {
        "position_id": position_id,
        "trading_strategy_id": runtime.trading_strategy_id,
        "trade_signal_id": attempt.get("trade_signal_id"),
        "trade_decision_id": attempt.get("trade_decision_id"),
        "admission_decision_id": attempt.get("admission_decision_id"),
        "opening_execution_intent_id": attempt.get("execution_intent_id") or request.get("execution_intent_id"),
        "open_execution_attempt_id": attempt.get("execution_attempt_id"),
        "root_symbol": attempt.get("underlying_symbol"),
        "underlying_symbol": attempt.get("underlying_symbol"),
        "strategy_family": strategy_family,
        "strategy": strategy_family,
        "market_date_opened": day_result.get("market_date"),
        "market_date_closed": None,
        "status": "open",
        "legs": legs,
        "economics": {
            "entry_credit": premium_float(entry_value),
            "entry_value": premium_float(entry_value),
            "entry_value_kind": premium_kind,
            "entry_notional": exposure["entry_notional"],
            "max_profit": exposure["max_profit"],
            "max_loss": exposure["max_loss"],
        },
        "strategy_metrics": {
            "width": width,
            "strategy": strategy_family,
        },
        "requested_quantity": coerce_int(attempt.get("requested_quantity")) or int(quantity),
        "opened_quantity": quantity,
        "remaining_quantity": quantity,
        "entry_value": premium_float(entry_value),
        "entry_value_kind": premium_kind,
        "realized_pnl": 0.0,
        "unrealized_pnl": None,
        "close_mark": None,
        "close_mark_source": None,
        "close_marked_at": None,
        "exit_policy": dict(as_mapping(request.get("exit_policy"))),
        "risk_policy": dict(as_mapping(request.get("risk_policy"))),
        "config_hash": as_text(request.get("config_hash")) or runtime.config_hash,
        "opened_at": opened_at,
        "closed_at": None,
        "updated_at": utc_iso(_day_as_of(day_result)),
        "fidelity": POSITION_FIDELITY,
    }


def _mark_position(*, storage: Any, runtime: Any, day_result: Mapping[str, Any], position: Mapping[str, Any]) -> dict[str, Any]:
    market_date = str(day_result["market_date"])
    session_start, session_end = _session_bounds(market_date)
    marked_at = min(_day_as_of(day_result), session_end)
    legs = normalize_legs(position.get("legs"), expiration_date=as_text(position.get("expiration_date")))
    symbols = tuple(unique_leg_symbols(legs))
    quotes = load_latest_historical_option_quotes(
        storage=storage,
        underlying_symbol=str(position.get("underlying_symbol") or position.get("root_symbol") or "").upper(),
        captured_from=session_start,
        captured_to=marked_at,
        label=runtime.trading_strategy_id,
        profile=runtime.build_settings.build_profile,
        limit=1000,
    )
    quotes_by_symbol = {
        str(row.get("option_symbol") or "").upper(): dict(row)
        for row in quotes.rows
        if str(row.get("option_symbol") or "").strip().upper() in {symbol.upper() for symbol in symbols}
    }
    snapshot = structure_quote_snapshot(
        legs=legs,
        strategy_family=position.get("strategy_family") or position.get("strategy"),
        quotes_by_symbol=quotes_by_symbol,
        normalized_legs=True,
    )
    captured_at = as_text(as_mapping(snapshot).get("captured_at")) or utc_iso(marked_at)
    return {
        "position_id": position.get("position_id"),
        "mark": coerce_float(as_mapping(snapshot).get("close_mark")),
        "midpoint_value": coerce_float(as_mapping(snapshot).get("midpoint_value")),
        "natural_value": coerce_float(as_mapping(snapshot).get("natural_value")),
        "marked_at": captured_at,
        "quote_scope": quotes.scope,
        "requested_symbols": list(symbols),
        "covered_symbols": sorted(quotes_by_symbol),
        "quote_covered": snapshot is not None,
        "quote_snapshot": snapshot,
        "fidelity": "historical_quote_snapshot_mark" if snapshot is not None else "missing_historical_quote_snapshot_mark",
    }


def _decision_details(decision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: decision.get(key)
        for key in (
            "policy",
            "mark",
            "effective_mark",
            "mark_state",
            "entry_value",
            "premium_kind",
            "profit_target_mark",
            "stop_mark",
            "force_close_at",
            "max_quote_age_seconds",
            "quote_spread_pct",
            "quote_spread_state",
            "underlying_invalidation_state",
            "underlying_invalidation_reason",
        )
        if key in decision
    }


def _evaluate_position(*, storage: Any, runtime: Any, day_result: Mapping[str, Any], position: Mapping[str, Any]) -> dict[str, Any]:
    mark = _mark_position(storage=storage, runtime=runtime, day_result=day_result, position=position)
    marked_at = parse_datetime(mark.get("marked_at")) or _day_as_of(day_result)
    mark_value = coerce_float(mark.get("mark"))
    position_with_mark = {
        **dict(position),
        "close_mark": mark_value,
        "close_mark_source": "historical_quote_snapshot",
        "close_marked_at": utc_iso(marked_at),
        "updated_at": utc_iso(marked_at),
    }
    decision = evaluate_exit_policy(position=position_with_mark, mark=mark_value, now=marked_at)
    close_decision = build_close_decision_lifecycle(
        position=position_with_mark,
        decision={
            **dict(decision),
            "decision_source": "backtest_exit_policy",
            "decision_details": _decision_details(decision),
        },
        decision_source="backtest_exit_policy",
        decided_at=utc_iso(marked_at),
    )
    if not decision.get("should_close"):
        unrealized_pnl = (
            close_pnl(
                entry_value=position.get("entry_value"),
                exit_value=mark_value,
                quantity=position.get("remaining_quantity"),
                premium_kind=as_text(position.get("entry_value_kind")) or net_premium_kind(position.get("strategy_family")),
                equity=str(position.get("strategy_family") or "").strip().lower().startswith("equity_"),
            )
            if mark_value is not None
            else None
        )
        return {
            "position": {
                **position_with_mark,
                "unrealized_pnl": unrealized_pnl,
                "last_exit_evaluated_at": utc_iso(marked_at),
                "last_exit_reason": decision.get("reason"),
            },
            "mark": mark,
            "close_decision": close_decision,
            "position_close": None,
        }

    exit_value = coerce_float(decision.get("effective_mark")) or mark_value or coerce_float(decision.get("limit_price"))
    realized_pnl = close_pnl(
        entry_value=position.get("entry_value"),
        exit_value=exit_value,
        quantity=position.get("remaining_quantity"),
        premium_kind=as_text(position.get("entry_value_kind")) or net_premium_kind(position.get("strategy_family")),
        equity=str(position.get("strategy_family") or "").strip().lower().startswith("equity_"),
    )
    position_close = {
        "position_close_id": _stable_id("backtest_position_close", close_decision.get("close_decision_id")),
        "position_id": position.get("position_id"),
        "close_decision_id": close_decision.get("close_decision_id"),
        "execution_intent_id": None,
        "execution_attempt_id": None,
        "closed_quantity": position.get("remaining_quantity"),
        "exit_value": premium_float(exit_value),
        "realized_pnl": realized_pnl,
        "broker_order_id": None,
        "close_state": "complete",
        "closed_at": utc_iso(marked_at),
        "created_at": utc_iso(marked_at),
        "updated_at": utc_iso(marked_at),
        "fidelity": EXIT_FIDELITY,
    }
    return {
        "position": {
            **position_with_mark,
            "status": "closed",
            "market_date_closed": day_result.get("market_date"),
            "remaining_quantity": 0.0,
            "realized_pnl": realized_pnl,
            "unrealized_pnl": 0.0,
            "closed_at": utc_iso(marked_at),
            "last_exit_evaluated_at": utc_iso(marked_at),
            "last_exit_reason": decision.get("reason"),
        },
        "mark": mark,
        "close_decision": close_decision,
        "position_close": position_close,
    }


def _simulate_day_portfolio(*, storage: Any, runtime: Any, day_result: Mapping[str, Any]) -> dict[str, Any]:
    attempts, fills_by_attempt = _fills_by_attempt(day_result)
    attempts_by_id = {str(attempt.get("execution_attempt_id")): attempt for attempt in attempts if attempt.get("execution_attempt_id")}
    positions: list[dict[str, Any]] = []
    marks: list[dict[str, Any]] = []
    close_decisions: list[dict[str, Any]] = []
    closes: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for attempt_id, fills in sorted(fills_by_attempt.items()):
        attempt = attempts_by_id.get(attempt_id)
        if attempt is None:
            skipped.append({"execution_attempt_id": attempt_id, "reason": "attempt_missing"})
            continue
        position = _position_from_fill(runtime=runtime, day_result=day_result, attempt=attempt, fills=fills)
        if position is None:
            skipped.append({"execution_attempt_id": attempt_id, "reason": "position_projection_unavailable"})
            continue
        evaluated = _evaluate_position(storage=storage, runtime=runtime, day_result=day_result, position=position)
        positions.append(evaluated["position"])
        marks.append(evaluated["mark"])
        close_decisions.append(evaluated["close_decision"])
        if evaluated["position_close"] is not None:
            closes.append(evaluated["position_close"])

    pnl = {
        "realized_pnl": money_sum_float(close.get("realized_pnl") for close in closes),
        "unrealized_pnl": money_sum_float(position.get("unrealized_pnl") for position in positions if str(position.get("status") or "") != "closed"),
    }
    pnl["net_pnl"] = money_sum_float([pnl["realized_pnl"], pnl["unrealized_pnl"]])
    close_decision_states = Counter(str(row.get("decision_state") or "unknown") for row in close_decisions)
    position_states = Counter(str(row.get("status") or "unknown") for row in positions)
    return {
        "portfolio_summary": {
            "position_count": len(positions),
            "open_position_count": sum(1 for row in positions if str(row.get("status") or "") != "closed"),
            "closed_position_count": sum(1 for row in positions if str(row.get("status") or "") == "closed"),
            "close_decision_count": len(close_decisions),
            "close_count": len(closes),
            "mark_count": sum(1 for row in marks if bool(row.get("quote_covered"))),
            "missing_mark_count": sum(1 for row in marks if not bool(row.get("quote_covered"))),
            "skipped_count": len(skipped),
            "position_status_counts": dict(sorted(position_states.items())),
            "close_decision_state_counts": dict(sorted(close_decision_states.items())),
            "fidelity": POSITION_FIDELITY,
        },
        "pnl": pnl,
        "simulated_positions": positions,
        "simulated_position_marks": marks,
        "simulated_close_decisions": close_decisions,
        "simulated_position_closes": closes,
        "portfolio_skipped_fills": skipped,
    }


def _update_strategy_portfolio(strategy_result: dict[str, Any]) -> None:
    day_results = [row for row in strategy_result.get("day_results") or [] if isinstance(row, dict)]
    metrics = compute_backtest_performance_metrics(day_results)
    positions = _rows_for_strategy(day_results, "simulated_positions")
    close_decisions = _rows_for_strategy(day_results, "simulated_close_decisions")
    closes = _rows_for_strategy(day_results, "simulated_position_closes")
    marks = _rows_for_strategy(day_results, "simulated_position_marks")
    position_states = Counter(str(row.get("status") or "unknown") for row in positions)
    close_decision_states = Counter(str(row.get("decision_state") or "unknown") for row in close_decisions)
    close_reasons = Counter(str(row.get("reason") or "unknown") for row in close_decisions)
    strategy_result["positions"] = {
        "position_count": len(positions),
        "open_position_count": sum(1 for row in positions if str(row.get("status") or "") != "closed"),
        "closed_position_count": sum(1 for row in positions if str(row.get("status") or "") == "closed"),
        "position_status_counts": dict(sorted(position_states.items())),
        "mark_count": sum(1 for row in marks if bool(row.get("quote_covered"))),
        "missing_mark_count": sum(1 for row in marks if not bool(row.get("quote_covered"))),
    }
    strategy_result["exits"] = {
        **dict(metrics["exits"]),
        "close_decision_state_counts": dict(sorted(close_decision_states.items())),
        "top_close_decision_reasons": dict(close_reasons.most_common(12)),
    }
    strategy_result["pnl"] = dict(metrics["pnl"])
    strategy_result["performance_metrics"] = {
        **dict(metrics["performance"]),
        "fill_rate": metrics["execution"]["fill_rate"],
        "selection_rate": metrics["selection"]["selection_rate"],
        "admission_approval_rate": metrics["admission"]["admission_approval_rate"],
        "quote_coverage": metrics["market_data"]["quote_coverage"],
    }
    strategy_result["metrics_fidelity"] = dict(metrics["fidelity"])
    fidelity = as_mapping(strategy_result.get("fidelity_labels"))
    strategy_result["fidelity_labels"] = {
        **dict(fidelity),
        "position": POSITION_FIDELITY if positions else "no_simulated_fills",
        "exit": EXIT_FIDELITY if close_decisions else "no_positions_to_evaluate",
        "pnl": PNL_FIDELITY if positions else "no_simulated_position_pnl",
        "metrics": str(metrics["fidelity"]["metrics_engine"]),
    }
    strategy_result["outcome_label"] = (
        "simulated_closed_positions"
        if closes
        else ("simulated_open_positions_marked" if positions else strategy_result.get("outcome_label"))
    )


def _rows_for_strategy(day_results: list[Mapping[str, Any]], key: str) -> list[dict[str, Any]]:
    return [dict(row) for day in day_results for row in day.get(key) or [] if isinstance(row, Mapping)]


def build_portfolio_simulation_backtest(
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
) -> dict[str, Any]:
    result = build_execution_simulation_backtest(
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
    )
    strategies = load_backtest_strategy_scope(strategy_ids)
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
            simulation = _simulate_day_portfolio(storage=storage, runtime=runtime, day_result=day_result)
            day_result.update(simulation)
            day_result["fidelity_labels"] = {
                **dict(as_mapping(day_result.get("fidelity_labels"))),
                "position": POSITION_FIDELITY if simulation["simulated_positions"] else "no_simulated_fills",
                "exit": EXIT_FIDELITY if simulation["simulated_close_decisions"] else "no_positions_to_evaluate",
                "pnl": PNL_FIDELITY if simulation["simulated_positions"] else "no_simulated_position_pnl",
            }
        _update_strategy_portfolio(strategy_result)

    all_day_results = [
        day
        for strategy_result in result.get("strategies") or []
        if isinstance(strategy_result, Mapping)
        for day in strategy_result.get("day_results") or []
        if isinstance(day, Mapping)
    ]
    metrics = compute_backtest_performance_metrics(all_day_results)
    result["evaluation_mode"] = "portfolio_simulation_current_model"
    result["generated_at"] = utc_now_iso()
    result["summary"] = {
        **dict(as_mapping(result.get("summary"))),
        "position_count": sum(coerce_int(as_mapping(row.get("positions")).get("position_count")) or 0 for row in result.get("strategies") or []),
        "close_decision_count": sum(coerce_int(as_mapping(row.get("exits")).get("close_decision_count")) or 0 for row in result.get("strategies") or []),
        "close_count": sum(coerce_int(as_mapping(row.get("exits")).get("close_count")) or 0 for row in result.get("strategies") or []),
        "realized_pnl": metrics["pnl"]["realized_pnl"],
        "unrealized_pnl": metrics["pnl"]["unrealized_pnl"],
        "net_pnl": metrics["pnl"]["net_pnl"],
        "performance_metrics": {
            **dict(metrics["performance"]),
            "fill_rate": metrics["execution"]["fill_rate"],
            "selection_rate": metrics["selection"]["selection_rate"],
            "admission_approval_rate": metrics["admission"]["admission_approval_rate"],
            "quote_coverage": metrics["market_data"]["quote_coverage"],
        },
    }
    result["fidelity_labels"] = {
        **dict(as_mapping(result.get("fidelity_labels"))),
        "mode": "portfolio_simulation_current_model",
        "position": POSITION_FIDELITY,
        "exit": EXIT_FIDELITY,
        "pnl": PNL_FIDELITY,
        "metrics": str(metrics["fidelity"]["metrics_engine"]),
        "live_writes": "none",
    }
    return result


__all__ = ["build_portfolio_simulation_backtest"]
