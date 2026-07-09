from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import date
from typing import Any

from sqlalchemy import and_, or_, select

from core.db.decorators import with_storage
from core.money import money_float, money_sum_float
from core.services.ops.trading.strategy_ledger import build_strategy_evidence_ledger
from core.services.backtest.strategy_scope import load_backtest_strategy_scope, strategy_profile
from core.services.backtest.windows import BacktestWindow, normalize_backtest_window
from core.services.trading_strategy_runtime_models import TradingStrategyConfig
from core.storage.engine_models import TradeCandidateModel
from core.storage.execution_models import ExecutionAttemptModel, PortfolioPositionModel
from core.storage.lifecycle_models import TradeAdmissionModel, TradeDecisionModel, TradeSignalModel
from core.storage.serializers import parse_date, render_value
from core.value_coercion import as_mapping, as_text, coerce_float, coerce_int, utc_now_iso

DEFAULT_MAX_WINDOW_DAYS = 31
DEFAULT_MARKET_DATA_SYMBOL_LIMIT = 250
TOP_REASON_LIMIT = 12

_OPTION_SYMBOL_KEYS = {
    "alpaca_symbol",
    "contract_symbol",
    "leg_symbol",
    "option_symbol",
    "symbol",
}


def _merge_counts(counter: Counter[str], value: Any) -> None:
    for key, raw_count in as_mapping(value).items():
        rendered = as_text(key) or "unknown"
        count = coerce_int(raw_count) or 0
        if count > 0:
            counter[rendered] += count


def _rate(numerator: int | float, denominator: int | float) -> float | None:
    if not denominator:
        return None
    return round(float(numerator) / float(denominator), 4)


def _top_counts(counter: Counter[str], *, limit: int = TOP_REASON_LIMIT) -> dict[str, int]:
    return dict(counter.most_common(limit))


def _sum_int(rows: Iterable[Mapping[str, Any]], section: str, field: str) -> int:
    return sum(coerce_int(as_mapping(row.get(section)).get(field)) or 0 for row in rows)


def _sum_money(rows: Iterable[Mapping[str, Any]], section: str, field: str) -> float:
    return money_sum_float(coerce_float(as_mapping(row.get(section)).get(field)) for row in rows)


def _latest_text(rows: Iterable[Mapping[str, Any]], section: str, field: str) -> str | None:
    values = [as_text(as_mapping(row.get(section)).get(field)) for row in rows]
    return max((value for value in values if value), default=None)


def _max_int(rows: Iterable[Mapping[str, Any]], section: str, field: str) -> int:
    values = [coerce_int(as_mapping(row.get(section)).get(field)) for row in rows]
    return max((int(value) for value in values if value is not None), default=0)


def _classify_outcome(aggregate: Mapping[str, Any]) -> str:
    pnl = as_mapping(aggregate.get("pnl"))
    net_pnl = coerce_float(pnl.get("net_pnl")) or 0.0
    if net_pnl > 0:
        return "profitable"
    if net_pnl < 0:
        return "loss"

    positions = as_mapping(aggregate.get("positions"))
    if (coerce_int(positions.get("open_position_count")) or 0) > 0:
        if (coerce_int(positions.get("mark_count")) or 0) > 0:
            return "open_marked_flat"
        return "open_unmarked"

    execution = as_mapping(aggregate.get("execution"))
    if (coerce_int(execution.get("fill_count")) or 0) > 0:
        return "filled_flat"
    if (coerce_int(execution.get("attempt_count")) or 0) > 0:
        return "attempted_unfilled"

    admissions = as_mapping(aggregate.get("admissions"))
    admission_count = coerce_int(admissions.get("admission_count")) or 0
    approved_count = coerce_int(admissions.get("approved_count")) or 0
    if admission_count > 0 and approved_count == 0:
        return "admission_blocked"

    selection_quality = as_mapping(aggregate.get("selection_quality"))
    selected_count = coerce_int(selection_quality.get("selected_count")) or 0
    decision_count = coerce_int(selection_quality.get("decision_count")) or 0
    if selected_count > 0:
        return "selected_no_execution"
    if decision_count > 0:
        return "no_selected_decision"

    if (coerce_int(selection_quality.get("signal_count")) or 0) > 0:
        return "signals_without_decisions"

    candidates = as_mapping(aggregate.get("candidate_productivity"))
    if (coerce_int(candidates.get("candidate_run_count")) or 0) > 0:
        state_counts = as_mapping(candidates.get("candidate_productivity_state_counts"))
        top_state = max(state_counts.items(), key=lambda item: int(item[1]))[0] if state_counts else "no_candidate_output"
        return f"candidate_{top_state}"
    return "no_activity"


def _execution_fidelity(aggregate: Mapping[str, Any]) -> str:
    execution = as_mapping(aggregate.get("execution"))
    admissions = as_mapping(aggregate.get("admissions"))
    selection_quality = as_mapping(aggregate.get("selection_quality"))
    if (coerce_int(execution.get("fill_count")) or 0) > 0:
        return "broker_fill_facts"
    if (coerce_int(execution.get("order_count")) or 0) > 0:
        return "broker_order_facts"
    if (coerce_int(execution.get("attempt_count")) or 0) > 0:
        return "execution_attempt_facts"
    if (coerce_int(admissions.get("admission_count")) or 0) > 0:
        return "admission_facts_only"
    if (coerce_int(selection_quality.get("selected_count")) or 0) > 0:
        return "selected_decision_no_execution"
    return "no_execution_evidence"


def _pnl_fidelity(aggregate: Mapping[str, Any]) -> str:
    positions = as_mapping(aggregate.get("positions"))
    execution = as_mapping(aggregate.get("execution"))
    if (coerce_int(positions.get("position_count")) or 0) <= 0:
        return "fills_without_position_projection" if (coerce_int(execution.get("fill_count")) or 0) > 0 else "no_pnl_evidence"
    if (coerce_int(positions.get("missing_mark_count")) or 0) > 0:
        return "position_facts_with_missing_marks"
    if (coerce_int(positions.get("stale_mark_count")) or 0) > 0:
        return "position_facts_with_stale_marks"
    return "position_pnl_facts"


def _aggregate_strategy_days(
    *,
    strategy: TradingStrategyConfig,
    daily_rows: list[dict[str, Any]],
    market_dates: tuple[str, ...],
    market_data_fidelity: Mapping[str, Any] | None,
    risk_context: Mapping[str, Any] | None,
) -> dict[str, Any]:
    candidate_state_counts: Counter[str] = Counter()
    source_evidence_state_counts: Counter[str] = Counter()
    diagnostic_counts: Counter[str] = Counter()
    raw_rejection_counts: Counter[str] = Counter()
    data_quality_status_counts: Counter[str] = Counter()
    data_quality_reason_counts: Counter[str] = Counter()
    calendar_policy_status_counts: Counter[str] = Counter()
    calendar_policy_reason_counts: Counter[str] = Counter()
    ranking_policy_status_counts: Counter[str] = Counter()
    ranking_policy_blocker_counts: Counter[str] = Counter()
    market_data_coverage_counts: Counter[str] = Counter()
    feature_quality_status_counts: Counter[str] = Counter()
    market_data_quality_state_counts: Counter[str] = Counter()
    market_data_quality_reason_counts: Counter[str] = Counter()
    market_data_quality_component_state_counts: Counter[str] = Counter()
    signal_state_counts: Counter[str] = Counter()
    decision_state_counts: Counter[str] = Counter()
    admission_state_counts: Counter[str] = Counter()
    intent_state_counts: Counter[str] = Counter()
    attempt_status_counts: Counter[str] = Counter()
    position_status_counts: Counter[str] = Counter()
    close_decision_state_counts: Counter[str] = Counter()
    close_decision_reason_counts: Counter[str] = Counter()
    blocker_counts: Counter[str] = Counter()

    for row in daily_rows:
        source = as_mapping(row.get("source"))
        source_state = as_text(source.get("source_evidence_state")) or "unknown"
        source_evidence_state_counts[source_state] += 1
        candidates = as_mapping(row.get("candidates"))
        candidate_state = as_text(candidates.get("candidate_productivity_state")) or "unknown"
        if (coerce_int(candidates.get("candidate_run_count")) or 0) > 0:
            candidate_state_counts[candidate_state] += 1
        _merge_counts(diagnostic_counts, candidates.get("diagnostic_status_counts"))
        _merge_counts(raw_rejection_counts, candidates.get("top_raw_rejection_counts"))
        _merge_counts(data_quality_status_counts, candidates.get("data_quality_status_counts"))
        _merge_counts(data_quality_reason_counts, candidates.get("top_data_quality_reasons"))
        _merge_counts(calendar_policy_status_counts, candidates.get("calendar_policy_status_counts"))
        _merge_counts(calendar_policy_reason_counts, candidates.get("top_calendar_policy_reasons"))
        _merge_counts(ranking_policy_status_counts, candidates.get("ranking_policy_status_counts"))
        _merge_counts(ranking_policy_blocker_counts, candidates.get("top_ranking_policy_blockers"))
        _merge_counts(market_data_coverage_counts, candidates.get("market_data_coverage"))
        _merge_counts(feature_quality_status_counts, candidates.get("feature_quality_status_counts"))
        _merge_counts(market_data_quality_state_counts, candidates.get("market_data_quality_state_counts"))
        _merge_counts(market_data_quality_reason_counts, candidates.get("top_market_data_quality_reasons"))
        _merge_counts(market_data_quality_component_state_counts, candidates.get("market_data_quality_component_state_counts"))
        _merge_counts(signal_state_counts, as_mapping(row.get("signals")).get("signal_state_counts"))
        _merge_counts(decision_state_counts, as_mapping(row.get("decisions")).get("decision_state_counts"))
        _merge_counts(admission_state_counts, as_mapping(row.get("admissions")).get("admission_state_counts"))
        _merge_counts(intent_state_counts, as_mapping(row.get("intents")).get("intent_state_counts"))
        _merge_counts(attempt_status_counts, as_mapping(row.get("attempts")).get("attempt_status_counts"))
        _merge_counts(position_status_counts, as_mapping(row.get("positions")).get("position_status_counts"))
        _merge_counts(close_decision_state_counts, as_mapping(row.get("closes")).get("close_decision_state_counts"))
        _merge_counts(close_decision_reason_counts, as_mapping(row.get("closes")).get("close_decision_reason_counts"))
        _merge_counts(blocker_counts, row.get("top_blocker_reasons"))

    candidate_run_count = _sum_int(daily_rows, "candidates", "candidate_run_count")
    trade_candidate_count = _sum_int(daily_rows, "candidates", "trade_candidate_count")
    signal_count = _sum_int(daily_rows, "signals", "signal_count")
    decision_count = _sum_int(daily_rows, "decisions", "decision_count")
    selected_count = int(decision_state_counts.get("selected", 0))
    admission_count = _sum_int(daily_rows, "admissions", "admission_count")
    approved_count = int(admission_state_counts.get("approved", 0))
    intent_count = _sum_int(daily_rows, "intents", "intent_count")
    attempt_count = _sum_int(daily_rows, "attempts", "attempt_count")
    order_count = _sum_int(daily_rows, "attempts", "order_count")
    fill_count = _sum_int(daily_rows, "attempts", "fill_count")
    position_count = _sum_int(daily_rows, "positions", "position_count")
    open_position_count = _sum_int(daily_rows, "positions", "open_position_count")
    close_decision_count = _sum_int(daily_rows, "closes", "close_decision_count")
    close_count = _sum_int(daily_rows, "closes", "close_count")
    realized_pnl = _sum_money(daily_rows, "pnl", "realized_pnl")
    unrealized_pnl = _sum_money(daily_rows, "pnl", "unrealized_pnl")

    aggregate: dict[str, Any] = {
        **strategy_profile(strategy),
        "market_dates": list(market_dates),
        "observed_day_count": len(daily_rows),
        "source_evidence": {
            "source_run_count": _sum_int(daily_rows, "source", "source_run_count"),
            "configured_symbol_count": _max_int(daily_rows, "source", "configured_symbol_count"),
            "latest_symbol_count": _max_int(daily_rows, "source", "latest_symbol_count"),
            "source_evidence_state_counts": dict(sorted(source_evidence_state_counts.items())),
            "latest_ticker_source_run_id": _latest_text(daily_rows, "source", "latest_ticker_source_run_id"),
        },
        "candidate_productivity": {
            "candidate_run_count": candidate_run_count,
            "candidate_count": _sum_int(daily_rows, "candidates", "candidate_count"),
            "trade_candidate_count": trade_candidate_count,
            "diagnostic_symbol_count": _sum_int(daily_rows, "candidates", "diagnostic_symbol_count"),
            "raw_candidate_count": _sum_int(daily_rows, "candidates", "raw_candidate_count"),
            "postprocess_candidate_count": _sum_int(daily_rows, "candidates", "postprocess_candidate_count"),
            "runtime_candidate_count": _sum_int(daily_rows, "candidates", "runtime_candidate_count"),
            "returned_candidate_count": _sum_int(daily_rows, "candidates", "returned_candidate_count"),
            "feature_snapshot_count": _sum_int(daily_rows, "candidates", "feature_snapshot_count"),
            "candidate_productivity_state_counts": dict(sorted(candidate_state_counts.items())),
            "diagnostic_status_counts": dict(sorted(diagnostic_counts.items())),
            "top_raw_rejection_counts": _top_counts(raw_rejection_counts),
            "data_quality_status_counts": dict(sorted(data_quality_status_counts.items())),
            "top_data_quality_reasons": _top_counts(data_quality_reason_counts),
            "calendar_policy_status_counts": dict(sorted(calendar_policy_status_counts.items())),
            "top_calendar_policy_reasons": _top_counts(calendar_policy_reason_counts),
            "ranking_policy_status_counts": dict(sorted(ranking_policy_status_counts.items())),
            "top_ranking_policy_blockers": _top_counts(ranking_policy_blocker_counts),
            "market_data_coverage": dict(sorted(market_data_coverage_counts.items())),
            "feature_quality_status_counts": dict(sorted(feature_quality_status_counts.items())),
            "market_data_quality_state_counts": dict(sorted(market_data_quality_state_counts.items())),
            "top_market_data_quality_reasons": _top_counts(market_data_quality_reason_counts),
            "market_data_quality_component_state_counts": dict(sorted(market_data_quality_component_state_counts.items())),
            "latest_candidate_run_id": _latest_text(daily_rows, "candidates", "latest_candidate_run_id"),
        },
        "selection_quality": {
            "signal_count": signal_count,
            "signal_state_counts": dict(sorted(signal_state_counts.items())),
            "decision_count": decision_count,
            "decision_state_counts": dict(sorted(decision_state_counts.items())),
            "selected_count": selected_count,
            "non_selected_count": max(decision_count - selected_count, 0),
            "signal_to_decision_rate": _rate(decision_count, signal_count),
            "selection_rate": _rate(selected_count, decision_count),
            "selected_per_trade_candidate_rate": _rate(selected_count, trade_candidate_count),
        },
        "admissions": {
            "admission_count": admission_count,
            "admission_state_counts": dict(sorted(admission_state_counts.items())),
            "approved_count": approved_count,
            "blocked_count": int(admission_state_counts.get("blocked", 0)),
            "approval_rate": _rate(approved_count, admission_count),
            "latest_admission_decision_id": _latest_text(daily_rows, "admissions", "latest_admission_decision_id"),
        },
        "execution": {
            "intent_count": intent_count,
            "intent_state_counts": dict(sorted(intent_state_counts.items())),
            "attempt_count": attempt_count,
            "attempt_status_counts": dict(sorted(attempt_status_counts.items())),
            "order_count": order_count,
            "fill_count": fill_count,
            "fill_rate_per_attempt": _rate(fill_count, attempt_count),
            "latest_execution_intent_id": _latest_text(daily_rows, "intents", "latest_execution_intent_id"),
            "latest_execution_attempt_id": _latest_text(daily_rows, "attempts", "latest_execution_attempt_id"),
        },
        "positions": {
            "position_count": position_count,
            "open_position_count": open_position_count,
            "closed_position_count": _sum_int(daily_rows, "positions", "closed_position_count"),
            "position_status_counts": dict(sorted(position_status_counts.items())),
            "mark_count": _sum_int(daily_rows, "positions", "mark_count"),
            "missing_mark_count": _sum_int(daily_rows, "positions", "missing_mark_count"),
            "stale_mark_count": _sum_int(daily_rows, "positions", "stale_mark_count"),
            "latest_position_id": _latest_text(daily_rows, "positions", "latest_position_id"),
        },
        "exits": {
            "close_decision_count": close_decision_count,
            "close_decision_state_counts": dict(sorted(close_decision_state_counts.items())),
            "top_close_decision_reasons": _top_counts(close_decision_reason_counts),
            "close_selected_count": int(close_decision_state_counts.get("close_selected", 0)),
            "close_blocked_count": int(close_decision_state_counts.get("blocked", 0)),
            "close_unknown_count": int(close_decision_state_counts.get("unknown", 0)),
            "close_count": close_count,
            "close_decision_to_close_rate": _rate(close_count, close_decision_count),
            "latest_close_decision_id": _latest_text(daily_rows, "closes", "latest_close_decision_id"),
            "latest_position_close_id": _latest_text(daily_rows, "closes", "latest_position_close_id"),
        },
        "pnl": {
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
            "net_pnl": money_sum_float([realized_pnl, unrealized_pnl]),
        },
        "risk_context": dict(risk_context or {}),
        "reason_code_attribution": _top_counts(blocker_counts),
    }
    aggregate["outcome_label"] = _classify_outcome(aggregate)
    aggregate["fidelity_labels"] = {
        "source": "stored_ticker_source_facts",
        "candidate": "stored_candidate_facts",
        "signal_decision": "stored_signal_decision_facts",
        "admission": "stored_admission_facts" if admission_count > 0 else "no_admission_facts",
        "execution": _execution_fidelity(aggregate),
        "exit": "stored_close_decision_facts" if close_decision_count > 0 else "no_close_decision_facts",
        "pnl": _pnl_fidelity(aggregate),
        "market_data": as_text(as_mapping(market_data_fidelity).get("coverage_label")) or "not_checked",
    }
    aggregate["market_data_fidelity"] = dict(market_data_fidelity or {})
    return aggregate


def _looks_like_option_symbol(value: str) -> bool:
    symbol = value.strip().replace(" ", "").upper()
    if len(symbol) < 9 or not any(character.isdigit() for character in symbol):
        return False
    return "C" in symbol or "P" in symbol


def _extract_option_symbols(value: Any) -> set[str]:
    symbols: set[str] = set()
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key) in _OPTION_SYMBOL_KEYS and isinstance(item, str):
                rendered = item.strip().replace(" ", "").upper()
                if _looks_like_option_symbol(rendered):
                    symbols.add(rendered)
            symbols.update(_extract_option_symbols(item))
    elif isinstance(value, list):
        for item in value:
            symbols.update(_extract_option_symbols(item))
    return symbols


def _add_symbols(target: dict[str, set[str]], strategy_id: Any, *values: Any) -> None:
    rendered_strategy_id = as_text(strategy_id)
    if rendered_strategy_id is None or rendered_strategy_id not in target:
        return
    for value in values:
        target[rendered_strategy_id].update(_extract_option_symbols(value))


def _collect_option_symbols_by_strategy(
    *,
    storage: Any,
    strategy_ids: tuple[str, ...],
    window: BacktestWindow,
    per_query_limit: int,
) -> dict[str, set[str]]:
    symbols_by_strategy = {strategy_id: set() for strategy_id in strategy_ids}
    if not strategy_ids:
        return symbols_by_strategy

    engine_facts = storage.engine_facts
    if engine_facts.schema_has_tables("trade_candidates", "trade_signals", "trade_decisions"):
        with engine_facts.session_factory() as session:
            candidate_rows = session.execute(
                select(
                    TradeCandidateModel.trading_strategy_id,
                    TradeCandidateModel.legs_json,
                    TradeCandidateModel.execution_shape_json,
                )
                .where(TradeCandidateModel.trading_strategy_id.in_(strategy_ids))
                .where(TradeCandidateModel.observed_at >= window.start_at)
                .where(TradeCandidateModel.observed_at < window.end_at)
                .order_by(TradeCandidateModel.observed_at.desc())
                .limit(per_query_limit)
            )
            for strategy_id, legs, execution_shape in candidate_rows:
                _add_symbols(symbols_by_strategy, strategy_id, legs, execution_shape)

            signal_rows = session.execute(
                select(
                    TradeSignalModel.trading_strategy_id,
                    TradeSignalModel.legs_json,
                    TradeSignalModel.execution_shape_json,
                )
                .where(TradeSignalModel.trading_strategy_id.in_(strategy_ids))
                .where(TradeSignalModel.observed_at >= window.start_at)
                .where(TradeSignalModel.observed_at < window.end_at)
                .order_by(TradeSignalModel.observed_at.desc())
                .limit(per_query_limit)
            )
            for strategy_id, legs, execution_shape in signal_rows:
                _add_symbols(symbols_by_strategy, strategy_id, legs, execution_shape)

            decision_rows = session.execute(
                select(
                    TradeDecisionModel.trading_strategy_id,
                    TradeDecisionModel.selected_execution_shape_json,
                )
                .where(TradeDecisionModel.trading_strategy_id.in_(strategy_ids))
                .where(TradeDecisionModel.decided_at >= window.start_at)
                .where(TradeDecisionModel.decided_at < window.end_at)
                .order_by(TradeDecisionModel.decided_at.desc())
                .limit(per_query_limit)
            )
            for strategy_id, execution_shape in decision_rows:
                _add_symbols(symbols_by_strategy, strategy_id, execution_shape)

    execution_store = storage.execution
    if execution_store.schema_ready():
        with execution_store.session_factory() as session:
            attempt_rows = session.execute(
                select(
                    ExecutionAttemptModel.trading_strategy_id,
                    ExecutionAttemptModel.legs_json,
                    ExecutionAttemptModel.order_payload_json,
                )
                .where(ExecutionAttemptModel.trading_strategy_id.in_(strategy_ids))
                .where(
                    or_(
                        and_(ExecutionAttemptModel.requested_at >= window.start_at, ExecutionAttemptModel.requested_at < window.end_at),
                        ExecutionAttemptModel.market_date.in_([parse_date(value) for value in window.market_dates]),
                    )
                )
                .order_by(ExecutionAttemptModel.requested_at.desc())
                .limit(per_query_limit)
            )
            for strategy_id, legs, order_payload in attempt_rows:
                _add_symbols(symbols_by_strategy, strategy_id, legs, order_payload)

            position_rows = session.execute(
                select(PortfolioPositionModel.trading_strategy_id, PortfolioPositionModel.legs_json)
                .where(PortfolioPositionModel.trading_strategy_id.in_(strategy_ids))
                .where(
                    or_(
                        PortfolioPositionModel.market_date_opened.in_([parse_date(value) for value in window.market_dates]),
                        PortfolioPositionModel.market_date_closed.in_([parse_date(value) for value in window.market_dates]),
                    )
                )
                .order_by(PortfolioPositionModel.updated_at.desc())
                .limit(per_query_limit)
            )
            for strategy_id, legs in position_rows:
                _add_symbols(symbols_by_strategy, strategy_id, legs)
    return symbols_by_strategy


def _coverage_label(*, symbol_count: int, quote_covered_count: int, unavailable: bool) -> str:
    if unavailable:
        return "clickhouse_unavailable"
    if symbol_count <= 0:
        return "not_applicable_no_option_symbols"
    if quote_covered_count == symbol_count:
        return "full_quote_coverage"
    if quote_covered_count > 0:
        return "partial_quote_coverage"
    return "no_quote_coverage"


def _market_data_coverage_for_symbols(
    *,
    storage: Any,
    symbols: list[str],
    window: BacktestWindow,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    if not symbols:
        return (
            {
                "coverage_label": "not_applicable_no_option_symbols",
                "option_symbol_count": 0,
                "quote_covered_symbol_count": 0,
                "trade_covered_symbol_count": 0,
                "quote_coverage_ratio": None,
                "trade_coverage_ratio": None,
            },
            {},
        )

    try:
        quote_summary = storage.market_data.summarize_option_quote_window(
            option_symbols=symbols,
            captured_from=window.start_at,
            captured_to=window.end_at,
        )
        trade_summary = storage.market_data.summarize_option_trade_window(
            option_symbols=symbols,
            captured_from=window.start_at,
            captured_to=window.end_at,
        )
    except Exception as exc:
        return (
            {
                "coverage_label": "clickhouse_unavailable",
                "option_symbol_count": len(symbols),
                "quote_covered_symbol_count": 0,
                "trade_covered_symbol_count": 0,
                "quote_coverage_ratio": 0.0,
                "trade_coverage_ratio": 0.0,
                "error_type": type(exc).__name__,
                "error": str(exc)[:180],
            },
            {},
        )

    per_symbol: dict[str, dict[str, Any]] = {}
    quote_covered_count = 0
    trade_covered_count = 0
    for symbol in symbols:
        quote = as_mapping(quote_summary.get(symbol))
        trade = as_mapping(trade_summary.get(symbol))
        quote_ticks = coerce_int(quote.get("tick_count")) or 0
        trade_ticks = coerce_int(trade.get("tick_count")) or 0
        quote_covered_count += 1 if quote_ticks > 0 else 0
        trade_covered_count += 1 if trade_ticks > 0 else 0
        per_symbol[symbol] = {
            "quote_tick_count": quote_ticks,
            "trade_tick_count": trade_ticks,
            "last_quote_captured_at": quote.get("last_captured_at"),
            "last_trade_captured_at": trade.get("last_captured_at"),
        }
    return (
        {
            "coverage_label": _coverage_label(
                symbol_count=len(symbols),
                quote_covered_count=quote_covered_count,
                unavailable=False,
            ),
            "option_symbol_count": len(symbols),
            "quote_covered_symbol_count": quote_covered_count,
            "trade_covered_symbol_count": trade_covered_count,
            "quote_coverage_ratio": _rate(quote_covered_count, len(symbols)),
            "trade_coverage_ratio": _rate(trade_covered_count, len(symbols)),
        },
        per_symbol,
    )


def _market_data_fidelity_by_strategy(
    *,
    storage: Any,
    strategy_ids: tuple[str, ...],
    window: BacktestWindow,
    symbol_limit: int,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    per_query_limit = max(int(symbol_limit) * 10, 500)
    symbols_by_strategy = _collect_option_symbols_by_strategy(
        storage=storage,
        strategy_ids=strategy_ids,
        window=window,
        per_query_limit=per_query_limit,
    )
    all_symbols = sorted({symbol for symbols in symbols_by_strategy.values() for symbol in symbols})
    queried_symbols = all_symbols[: max(int(symbol_limit), 1)]
    coverage, per_symbol = _market_data_coverage_for_symbols(
        storage=storage,
        symbols=queried_symbols,
        window=window,
    )
    per_strategy: dict[str, dict[str, Any]] = {}
    for strategy_id in strategy_ids:
        strategy_symbols = sorted(symbols_by_strategy.get(strategy_id, set()))
        queried_strategy_symbols = [symbol for symbol in strategy_symbols if symbol in per_symbol or symbol in queried_symbols]
        strategy_quote_covered = sum(1 for symbol in queried_strategy_symbols if (coerce_int(per_symbol.get(symbol, {}).get("quote_tick_count")) or 0) > 0)
        strategy_trade_covered = sum(1 for symbol in queried_strategy_symbols if (coerce_int(per_symbol.get(symbol, {}).get("trade_tick_count")) or 0) > 0)
        per_strategy[strategy_id] = {
            "coverage_label": _coverage_label(
                symbol_count=len(queried_strategy_symbols),
                quote_covered_count=strategy_quote_covered,
                unavailable=coverage.get("coverage_label") == "clickhouse_unavailable",
            ),
            "option_symbol_count": len(strategy_symbols),
            "queried_option_symbol_count": len(queried_strategy_symbols),
            "query_truncated": len(strategy_symbols) > len(queried_strategy_symbols),
            "quote_covered_symbol_count": strategy_quote_covered,
            "trade_covered_symbol_count": strategy_trade_covered,
            "quote_coverage_ratio": _rate(strategy_quote_covered, len(queried_strategy_symbols)),
            "trade_coverage_ratio": _rate(strategy_trade_covered, len(queried_strategy_symbols)),
        }
    return per_strategy, {
        **coverage,
        "queried_option_symbol_count": len(queried_symbols),
        "total_option_symbol_count": len(all_symbols),
        "query_truncated": len(all_symbols) > len(queried_symbols),
    }


def _risk_context_by_strategy(
    *,
    storage: Any,
    strategy_ids: tuple[str, ...],
    window: BacktestWindow,
) -> dict[str, dict[str, Any]]:
    payload = {
        strategy_id: {
            "admission_requested_notional": 0.0,
            "admission_max_loss": 0.0,
            "max_loss_observation_count": 0,
        }
        for strategy_id in strategy_ids
    }
    if not strategy_ids or not storage.engine_facts.schema_has_tables("trade_signals", "trade_admissions"):
        return payload

    market_days = [parse_date(value) for value in window.market_dates]
    with storage.engine_facts.session_factory() as session:
        rows = session.execute(
            select(
                TradeSignalModel.trading_strategy_id,
                TradeAdmissionModel.requested_notional,
                TradeAdmissionModel.max_loss,
            )
            .join(TradeSignalModel, TradeAdmissionModel.trade_signal_id == TradeSignalModel.trade_signal_id)
            .where(TradeSignalModel.trading_strategy_id.in_(strategy_ids))
            .where(TradeAdmissionModel.session_date.in_(market_days))
        )
        for strategy_id, requested_notional, max_loss in rows:
            strategy_key = as_text(strategy_id)
            if strategy_key not in payload:
                continue
            requested_notional_value = coerce_float(requested_notional) or 0.0
            max_loss_value = coerce_float(max_loss)
            payload[strategy_key]["admission_requested_notional"] += requested_notional_value
            if max_loss_value is not None:
                payload[strategy_key]["admission_max_loss"] += max_loss_value
                payload[strategy_key]["max_loss_observation_count"] += 1
    for row in payload.values():
        row["admission_requested_notional"] = money_float(row["admission_requested_notional"])
        row["admission_max_loss"] = money_float(row["admission_max_loss"])
    return payload


def _daily_ledgers_by_strategy(
    *,
    storage: Any,
    strategy_ids: tuple[str, ...],
    market_dates: tuple[str, ...],
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]], dict[str, Any]]:
    daily_ledgers: list[dict[str, Any]] = []
    rows_by_strategy = {strategy_id: [] for strategy_id in strategy_ids}
    schema_counts: dict[str, Counter[str]] = {
        "engine_facts": Counter(),
        "execution": Counter(),
        "execution_intents": Counter(),
        "portfolio": Counter(),
    }
    for market_date in market_dates:
        ledger = build_strategy_evidence_ledger(storage=storage, market_date=market_date)
        daily_ledgers.append(ledger)
        for key, value in as_mapping(ledger.get("schema")).items():
            schema_counts.setdefault(str(key), Counter())[str(value or "unknown")] += 1
        for row in ledger.get("strategies") or []:
            if not isinstance(row, Mapping):
                continue
            strategy_id = as_text(row.get("trading_strategy_id"))
            if strategy_id in rows_by_strategy:
                rows_by_strategy[strategy_id].append(dict(row))
    schema = {key: dict(sorted(counter.items())) for key, counter in sorted(schema_counts.items())}
    return daily_ledgers, rows_by_strategy, schema


def _comparison_payload(strategy_results: list[dict[str, Any]]) -> dict[str, Any]:
    def ranked(metric_path: tuple[str, str]) -> list[dict[str, Any]]:
        section, field = metric_path
        rows = []
        for result in strategy_results:
            value = coerce_float(as_mapping(result.get(section)).get(field))
            rows.append(
                {
                    "variant_id": result.get("variant_id"),
                    "trading_strategy_id": result.get("trading_strategy_id"),
                    "value": value,
                }
            )
        return sorted(rows, key=lambda row: (row["value"] is None, -(row["value"] or 0.0), str(row["variant_id"] or "")))

    net_pnl_ranking = ranked(("pnl", "net_pnl"))
    selected_ranking = ranked(("selection_quality", "selected_count"))
    candidate_ranking = ranked(("candidate_productivity", "trade_candidate_count"))
    best_net_pnl = net_pnl_ranking[0]["value"] if net_pnl_ranking else None
    deltas = []
    for result in strategy_results:
        net_pnl = coerce_float(as_mapping(result.get("pnl")).get("net_pnl"))
        selected = coerce_int(as_mapping(result.get("selection_quality")).get("selected_count")) or 0
        best_selected = coerce_int(selected_ranking[0]["value"]) if selected_ranking else None
        deltas.append(
            {
                "variant_id": result.get("variant_id"),
                "trading_strategy_id": result.get("trading_strategy_id"),
                "net_pnl_delta_from_best": None if net_pnl is None or best_net_pnl is None else money_float(net_pnl - float(best_net_pnl)),
                "selected_count_delta_from_best": None if best_selected is None else int(selected - best_selected),
            }
        )
    return {
        "comparison_mode": "current_catalog_strategy_profile_source",
        "fidelity": "stored_facts_current_model_no_profile_rerun",
        "rankings": {
            "net_pnl": net_pnl_ranking,
            "selected_count": selected_ranking,
            "trade_candidate_count": candidate_ranking,
        },
        "profile_deltas": deltas,
    }


@with_storage()
def build_stored_facts_backtest(
    *,
    start_date: str | date | None,
    end_date: str | date | None = None,
    strategy_ids: Iterable[str] | None = None,
    max_days: int = DEFAULT_MAX_WINDOW_DAYS,
    market_data_symbol_limit: int = DEFAULT_MARKET_DATA_SYMBOL_LIMIT,
    db_target: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    """Evaluate a bounded backtest window using stored Spreads fact models.

    This mode is intentionally fact-backed rather than simulated. It compares
    current catalog strategy/profile/source variants through persisted source,
    candidate, signal, decision, admission, execution, and position facts, then
    labels market-data and lifecycle fidelity explicitly.
    """

    del db_target
    window = normalize_backtest_window(start_date, end_date, max_days=max_days)
    strategies = load_backtest_strategy_scope(strategy_ids)
    strategy_id_tuple = tuple(strategies)
    daily_ledgers, rows_by_strategy, schema = _daily_ledgers_by_strategy(
        storage=storage,
        strategy_ids=strategy_id_tuple,
        market_dates=window.market_dates,
    )
    market_data_by_strategy, market_data_summary = _market_data_fidelity_by_strategy(
        storage=storage,
        strategy_ids=strategy_id_tuple,
        window=window,
        symbol_limit=market_data_symbol_limit,
    )
    risk_context_by_strategy = _risk_context_by_strategy(
        storage=storage,
        strategy_ids=strategy_id_tuple,
        window=window,
    )
    strategy_results = [
        _aggregate_strategy_days(
            strategy=strategy,
            daily_rows=rows_by_strategy.get(strategy_id, []),
            market_dates=window.market_dates,
            market_data_fidelity=market_data_by_strategy.get(strategy_id),
            risk_context=risk_context_by_strategy.get(strategy_id),
        )
        for strategy_id, strategy in strategies.items()
    ]
    summary = {
        "strategy_count": len(strategy_results),
        "market_day_count": len(window.market_dates),
        "candidate_run_count": sum(coerce_int(as_mapping(row.get("candidate_productivity")).get("candidate_run_count")) or 0 for row in strategy_results),
        "trade_candidate_count": sum(coerce_int(as_mapping(row.get("candidate_productivity")).get("trade_candidate_count")) or 0 for row in strategy_results),
        "signal_count": sum(coerce_int(as_mapping(row.get("selection_quality")).get("signal_count")) or 0 for row in strategy_results),
        "decision_count": sum(coerce_int(as_mapping(row.get("selection_quality")).get("decision_count")) or 0 for row in strategy_results),
        "selected_count": sum(coerce_int(as_mapping(row.get("selection_quality")).get("selected_count")) or 0 for row in strategy_results),
        "admission_count": sum(coerce_int(as_mapping(row.get("admissions")).get("admission_count")) or 0 for row in strategy_results),
        "attempt_count": sum(coerce_int(as_mapping(row.get("execution")).get("attempt_count")) or 0 for row in strategy_results),
        "fill_count": sum(coerce_int(as_mapping(row.get("execution")).get("fill_count")) or 0 for row in strategy_results),
        "position_count": sum(coerce_int(as_mapping(row.get("positions")).get("position_count")) or 0 for row in strategy_results),
        "close_decision_count": sum(coerce_int(as_mapping(row.get("exits")).get("close_decision_count")) or 0 for row in strategy_results),
        "close_count": sum(coerce_int(as_mapping(row.get("exits")).get("close_count")) or 0 for row in strategy_results),
        "net_pnl": money_sum_float(coerce_float(as_mapping(row.get("pnl")).get("net_pnl")) for row in strategy_results),
    }
    status = "ready" if strategy_results else "empty"
    if any(as_mapping(row.get("fidelity_labels")).get("market_data") == "clickhouse_unavailable" for row in strategy_results):
        status = "degraded"
    return {
        "status": status,
        "evaluation_mode": "stored_facts_current_model",
        "generated_at": utc_now_iso(),
        "window": {
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "market_dates": list(window.market_dates),
            "start_at": render_value(window.start_at),
            "end_at": render_value(window.end_at),
        },
        "schema": schema,
        "fidelity_labels": {
            "source": "stored_ticker_source_facts",
            "candidate": "stored_candidate_facts",
            "signal_decision": "stored_signal_decision_facts",
            "admission": "stored_admission_facts" if summary["admission_count"] > 0 else "no_admission_facts",
            "execution": "stored_intent_attempt_order_fill_position_facts"
            if summary["attempt_count"] > 0 or summary["fill_count"] > 0 or summary["position_count"] > 0
            else "no_execution_evidence",
            "exit": "stored_close_decision_facts" if summary["close_decision_count"] > 0 else "no_close_decision_facts",
            "market_data": as_text(market_data_summary.get("coverage_label")) or "not_checked",
            "comparison": "stored_facts_current_model_no_profile_rerun",
        },
        "market_data": market_data_summary,
        "summary": summary,
        "comparison": _comparison_payload(strategy_results),
        "strategies": strategy_results,
        "daily_ledgers": [
            {
                "market_date": ledger.get("market_date"),
                "status": ledger.get("status"),
                "summary": ledger.get("summary"),
                "schema": ledger.get("schema"),
            }
            for ledger in daily_ledgers
        ],
    }


__all__ = ["build_stored_facts_backtest"]
