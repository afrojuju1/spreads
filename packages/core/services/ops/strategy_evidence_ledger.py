from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import and_, func, or_, select

from core.db.decorators import with_storage
from core.jobs.orchestration import NEW_YORK
from core.services.trading_strategies import load_active_trading_strategies
from core.storage.engine_models import (
    CandidateRunModel,
    CandidateSymbolDiagnosticModel,
    TickerSourceObservationModel,
    TickerSourceRunModel,
    TradeCandidateModel,
)
from core.storage.execution_models import (
    ExecutionAttemptModel,
    ExecutionFillModel,
    ExecutionIntentModel,
    ExecutionOrderModel,
    PortfolioPositionModel,
    PositionCloseModel,
)
from core.storage.lifecycle_models import TradeAdmissionModel, TradeDecisionModel, TradeSignalModel
from core.storage.serializers import parse_date
from core.value_coercion import as_list, as_mapping, as_text, coerce_float, coerce_int, utc_now_iso

LEDGER_MARK_STALE_AFTER_SECONDS = 15 * 60
SOURCE_SYMBOL_LIMIT = 25
TOP_BLOCKER_LIMIT = 10
OPEN_POSITION_STATUSES = {"open", "partial_open", "partial_close", "pending_open"}
ENGINE_LEDGER_TABLES = (
    "ticker_source_runs",
    "ticker_source_observations",
    "candidate_runs",
    "candidate_symbol_diagnostics",
    "trade_candidates",
    "trade_signals",
    "trade_decisions",
    "trade_admissions",
)


def _market_date_or_today(market_date: str | None) -> str:
    if market_date:
        return parse_date(market_date).isoformat()
    return datetime.now(NEW_YORK).date().isoformat()


def _window(market_date: str) -> tuple[date, datetime, datetime]:
    market_day = parse_date(market_date)
    start = datetime.combine(market_day, datetime.min.time(), tzinfo=UTC)
    return market_day, start, start + timedelta(days=1)


def _render_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    rendered = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return rendered.isoformat(timespec="seconds").replace("+00:00", "Z")


def _add_count_mapping(counter: Counter[str], value: Any) -> None:
    for key, raw_count in as_mapping(value).items():
        reason = as_text(key)
        count = coerce_int(raw_count)
        if reason is not None and count is not None and count > 0:
            counter[reason] += int(count)


def _add_reason_list(counter: Counter[str], value: Any) -> None:
    for raw_reason in as_list(value):
        reason = as_text(raw_reason)
        if reason is not None:
            counter[reason] += 1


def _add_quality_waterfall_reasons(counter: Counter[str], value: Any) -> None:
    waterfall = as_mapping(value)
    for result in as_list(waterfall.get("results")):
        if not isinstance(result, Mapping):
            continue
        if str(result.get("status") or "").strip().lower() != "block":
            continue
        _add_reason_list(counter, result.get("reason_codes"))


def _top_blockers(counter: Counter[str]) -> dict[str, int]:
    return dict(counter.most_common(TOP_BLOCKER_LIMIT))


def _newer_desc_asc(value_at: datetime | None, value_id: Any, current_at: datetime | None, current_id: Any) -> bool:
    if value_at is None:
        return False
    if current_at is None:
        return True
    if value_at != current_at:
        return value_at > current_at
    return str(value_id or "") < str(current_id or "")


def _newer_desc_desc(value_at: datetime | None, value_id: Any, current_at: datetime | None, current_id: Any) -> bool:
    if value_at is None:
        return False
    if current_at is None:
        return True
    if value_at != current_at:
        return value_at > current_at
    return str(value_id or "") > str(current_id or "")


def _set_latest_activity(latest_activity: dict[str, datetime], strategy_id: str, value: datetime | None) -> None:
    if value is not None and (strategy_id not in latest_activity or value > latest_activity[strategy_id]):
        latest_activity[strategy_id] = value


def _empty_engine_strategy_ledger(strategy: Any) -> dict[str, Any]:
    configured_source_symbols = list(strategy.symbols[:SOURCE_SYMBOL_LIMIT])
    return {
        "source": {
            "source_type": strategy.source.kind,
            "source_id": strategy.source.ref,
            "source_run_count": 0,
            "configured_symbol_count": len(strategy.symbols),
            "latest_symbol_count": len(strategy.symbols),
            "symbols": configured_source_symbols,
            "latest_ticker_source_run_id": None,
            "latest_generated_at": None,
        },
        "candidates": {
            "candidate_run_count": 0,
            "candidate_count": 0,
            "trade_candidate_count": 0,
            "latest_candidate_run_id": None,
            "latest_generated_at": None,
        },
        "signals": {
            "signal_count": 0,
            "signal_state_counts": {},
            "latest_trade_signal_id": None,
            "latest_observed_at": None,
        },
        "decisions": {
            "decision_count": 0,
            "decision_state_counts": {},
            "selected_count": 0,
            "latest_trade_decision_id": None,
            "latest_decided_at": None,
        },
        "admissions": {
            "admission_count": 0,
            "admission_state_counts": {},
            "latest_admission_decision_id": None,
            "latest_decided_at": None,
        },
        "top_blocker_reasons": {},
        "latest_activity_at": None,
    }


def _bump_count(mapping: dict[str, int], value: Any, count: int = 1) -> None:
    key = str(value or "unknown")
    mapping[key] = int(mapping.get(key, 0)) + int(count)


def _finalize_state_counts(payload: dict[str, Any], key: str, count_key: str) -> None:
    state_counts = dict(sorted((str(state), int(count)) for state, count in as_mapping(payload[key].get(count_key)).items()))
    payload[key][count_key] = state_counts
    payload[key][key[:-1] + "_count"] = int(sum(state_counts.values()))


def _build_engine_strategy_ledgers(
    *,
    session: Any,
    strategies: Iterable[Any],
    market_day: date,
    start: datetime,
    end: datetime,
) -> dict[str, dict[str, Any]]:
    strategy_list = list(strategies)
    payloads = {strategy.trading_strategy_id: _empty_engine_strategy_ledger(strategy) for strategy in strategy_list}
    strategy_ids = list(payloads)
    if not strategy_ids:
        return payloads

    latest_activity: dict[str, datetime] = {}
    blockers_by_strategy = {strategy_id: Counter() for strategy_id in strategy_ids}

    source_to_strategies: dict[str, list[str]] = {}
    for strategy in strategy_list:
        source_to_strategies.setdefault(str(strategy.source.ref), []).append(strategy.trading_strategy_id)

    latest_source_by_ref: dict[str, tuple[str, int, datetime]] = {}
    source_run_counts: Counter[str] = Counter()
    source_refs = sorted(source_to_strategies)
    if source_refs:
        source_rows = session.execute(
            select(
                TickerSourceRunModel.ticker_source_id,
                TickerSourceRunModel.ticker_source_run_id,
                TickerSourceRunModel.selected_count,
                TickerSourceRunModel.generated_at,
            )
            .where(TickerSourceRunModel.ticker_source_id.in_(source_refs))
            .where(TickerSourceRunModel.generated_at >= start)
            .where(TickerSourceRunModel.generated_at < end)
            .order_by(
                TickerSourceRunModel.ticker_source_id.asc(),
                TickerSourceRunModel.generated_at.desc(),
                TickerSourceRunModel.ticker_source_run_id.asc(),
            )
        )
        for source_ref, source_run_id, selected_count, generated_at in source_rows:
            source_key = str(source_ref)
            source_run_counts[source_key] += 1
            current = latest_source_by_ref.get(source_key)
            if current is None or _newer_desc_asc(generated_at, source_run_id, current[2], current[0]):
                latest_source_by_ref[source_key] = (str(source_run_id), int(selected_count or 0), generated_at)

    source_symbols_by_run: dict[str, list[str]] = {}
    latest_source_run_ids = [source_run_id for source_run_id, _, _ in latest_source_by_ref.values()]
    if latest_source_run_ids:
        symbol_rows = session.execute(
            select(TickerSourceObservationModel.ticker_source_run_id, TickerSourceObservationModel.symbol)
            .where(TickerSourceObservationModel.ticker_source_run_id.in_(latest_source_run_ids))
            .where(TickerSourceObservationModel.observation_state == "selected")
            .order_by(
                TickerSourceObservationModel.ticker_source_run_id.asc(),
                TickerSourceObservationModel.rank.asc().nulls_last(),
                TickerSourceObservationModel.symbol.asc(),
            )
        )
        for source_run_id, symbol in symbol_rows:
            symbols = source_symbols_by_run.setdefault(str(source_run_id), [])
            if len(symbols) < SOURCE_SYMBOL_LIMIT:
                symbols.append(str(symbol))

    for source_ref, source_strategy_ids in source_to_strategies.items():
        latest_source = latest_source_by_ref.get(source_ref)
        for strategy_id in source_strategy_ids:
            source_payload = payloads[strategy_id]["source"]
            source_payload["source_run_count"] = int(source_run_counts.get(source_ref, 0))
            if latest_source is None:
                continue
            source_run_id, selected_count, generated_at = latest_source
            source_payload["latest_symbol_count"] = int(selected_count)
            source_payload["symbols"] = source_symbols_by_run.get(source_run_id) or source_payload["symbols"]
            source_payload["latest_ticker_source_run_id"] = source_run_id
            source_payload["latest_generated_at"] = _render_datetime(generated_at)
            _set_latest_activity(latest_activity, strategy_id, generated_at)

    candidate_run_strategy: dict[str, str] = {}
    latest_candidate_at: dict[str, datetime] = {}
    candidate_rows = session.execute(
        select(
            CandidateRunModel.trading_strategy_id,
            CandidateRunModel.candidate_run_id,
            CandidateRunModel.candidate_count,
            CandidateRunModel.summary_json,
            CandidateRunModel.generated_at,
        )
        .where(CandidateRunModel.trading_strategy_id.in_(strategy_ids))
        .where(CandidateRunModel.routine == "entry")
        .where(CandidateRunModel.generated_at >= start)
        .where(CandidateRunModel.generated_at < end)
        .order_by(
            CandidateRunModel.trading_strategy_id.asc(),
            CandidateRunModel.generated_at.desc(),
            CandidateRunModel.candidate_run_id.asc(),
        )
    )
    for strategy_id, candidate_run_id, candidate_count, summary_json, generated_at in candidate_rows:
        strategy_key = str(strategy_id)
        run_id = str(candidate_run_id)
        candidate_run_strategy[run_id] = strategy_key
        candidate_payload = payloads[strategy_key]["candidates"]
        candidate_payload["candidate_run_count"] += 1
        candidate_payload["candidate_count"] += int(candidate_count or 0)
        if _newer_desc_asc(generated_at, candidate_run_id, latest_candidate_at.get(strategy_key), candidate_payload.get("latest_candidate_run_id")):
            latest_candidate_at[strategy_key] = generated_at
            candidate_payload["latest_candidate_run_id"] = run_id
            candidate_payload["latest_generated_at"] = _render_datetime(generated_at)
        _set_latest_activity(latest_activity, strategy_key, generated_at)
        summary = as_mapping(summary_json)
        _add_count_mapping(blockers_by_strategy[strategy_key], summary.get("top_quality_blockers"))
        _add_count_mapping(blockers_by_strategy[strategy_key], summary.get("top_rejection_counts"))

    if candidate_run_strategy:
        diagnostic_rows = session.execute(
            select(
                CandidateSymbolDiagnosticModel.candidate_run_id,
                CandidateSymbolDiagnosticModel.rejection_counts_json,
                CandidateSymbolDiagnosticModel.evidence_json,
            ).where(CandidateSymbolDiagnosticModel.candidate_run_id.in_(list(candidate_run_strategy)))
        )
        for candidate_run_id, rejection_counts_json, evidence_json in diagnostic_rows:
            strategy_key = candidate_run_strategy.get(str(candidate_run_id))
            if strategy_key is None:
                continue
            _add_count_mapping(blockers_by_strategy[strategy_key], rejection_counts_json)
            _add_quality_waterfall_reasons(blockers_by_strategy[strategy_key], as_mapping(evidence_json).get("quality_waterfall"))

    trade_candidate_rows = session.execute(
        select(
            TradeCandidateModel.trading_strategy_id,
            TradeCandidateModel.routine,
            TradeCandidateModel.reason_codes_json,
            TradeCandidateModel.blockers_json,
        )
        .where(TradeCandidateModel.trading_strategy_id.in_(strategy_ids))
        .where(TradeCandidateModel.observed_at >= start)
        .where(TradeCandidateModel.observed_at < end)
    )
    for strategy_id, routine, reason_codes_json, blockers_json in trade_candidate_rows:
        strategy_key = str(strategy_id)
        if str(routine or "") == "entry":
            payloads[strategy_key]["candidates"]["trade_candidate_count"] += 1
        _add_reason_list(blockers_by_strategy[strategy_key], reason_codes_json)
        _add_reason_list(blockers_by_strategy[strategy_key], blockers_json)

    latest_signal_at: dict[str, datetime] = {}
    signal_rows = session.execute(
        select(
            TradeSignalModel.trading_strategy_id,
            TradeSignalModel.signal_state,
            TradeSignalModel.trade_signal_id,
            TradeSignalModel.observed_at,
            TradeSignalModel.reason_codes_json,
            TradeSignalModel.blockers_json,
        )
        .where(TradeSignalModel.trading_strategy_id.in_(strategy_ids))
        .where(TradeSignalModel.session_date == market_day)
        .order_by(TradeSignalModel.trading_strategy_id.asc(), TradeSignalModel.observed_at.desc(), TradeSignalModel.trade_signal_id.asc())
    )
    for strategy_id, signal_state, trade_signal_id, observed_at, reason_codes_json, blockers_json in signal_rows:
        strategy_key = str(strategy_id)
        signal_payload = payloads[strategy_key]["signals"]
        _bump_count(signal_payload["signal_state_counts"], signal_state)
        if _newer_desc_asc(observed_at, trade_signal_id, latest_signal_at.get(strategy_key), signal_payload.get("latest_trade_signal_id")):
            latest_signal_at[strategy_key] = observed_at
            signal_payload["latest_trade_signal_id"] = str(trade_signal_id)
            signal_payload["latest_observed_at"] = _render_datetime(observed_at)
        _set_latest_activity(latest_activity, strategy_key, observed_at)
        _add_reason_list(blockers_by_strategy[strategy_key], reason_codes_json)
        _add_reason_list(blockers_by_strategy[strategy_key], blockers_json)

    latest_decision_at: dict[str, datetime] = {}
    decision_rows = session.execute(
        select(
            TradeDecisionModel.trading_strategy_id,
            TradeDecisionModel.decision_state,
            TradeDecisionModel.trade_decision_id,
            TradeDecisionModel.decided_at,
            TradeDecisionModel.reason_codes_json,
            TradeDecisionModel.blockers_json,
        )
        .where(TradeDecisionModel.trading_strategy_id.in_(strategy_ids))
        .where(TradeDecisionModel.routine == "entry")
        .where(TradeDecisionModel.decided_at >= start)
        .where(TradeDecisionModel.decided_at < end)
        .order_by(TradeDecisionModel.trading_strategy_id.asc(), TradeDecisionModel.decided_at.desc(), TradeDecisionModel.trade_decision_id.asc())
    )
    for strategy_id, decision_state, trade_decision_id, decided_at, reason_codes_json, blockers_json in decision_rows:
        strategy_key = str(strategy_id)
        decision_payload = payloads[strategy_key]["decisions"]
        _bump_count(decision_payload["decision_state_counts"], decision_state)
        if _newer_desc_asc(decided_at, trade_decision_id, latest_decision_at.get(strategy_key), decision_payload.get("latest_trade_decision_id")):
            latest_decision_at[strategy_key] = decided_at
            decision_payload["latest_trade_decision_id"] = str(trade_decision_id)
            decision_payload["latest_decided_at"] = _render_datetime(decided_at)
        _set_latest_activity(latest_activity, strategy_key, decided_at)
        _add_reason_list(blockers_by_strategy[strategy_key], reason_codes_json)
        _add_reason_list(blockers_by_strategy[strategy_key], blockers_json)

    latest_admission_at: dict[str, datetime] = {}
    admission_rows = session.execute(
        select(
            TradeSignalModel.trading_strategy_id,
            TradeAdmissionModel.admission_state,
            TradeAdmissionModel.admission_decision_id,
            TradeAdmissionModel.decided_at,
            TradeAdmissionModel.reason_codes_json,
            TradeAdmissionModel.blockers_json,
        )
        .join(TradeSignalModel, TradeAdmissionModel.trade_signal_id == TradeSignalModel.trade_signal_id)
        .where(TradeSignalModel.trading_strategy_id.in_(strategy_ids))
        .where(TradeAdmissionModel.session_date == market_day)
        .order_by(
            TradeSignalModel.trading_strategy_id.asc(),
            TradeAdmissionModel.decided_at.desc(),
            TradeAdmissionModel.admission_decision_id.asc(),
        )
    )
    for strategy_id, admission_state, admission_decision_id, decided_at, reason_codes_json, blockers_json in admission_rows:
        strategy_key = str(strategy_id)
        admission_payload = payloads[strategy_key]["admissions"]
        _bump_count(admission_payload["admission_state_counts"], admission_state)
        if _newer_desc_asc(
            decided_at,
            admission_decision_id,
            latest_admission_at.get(strategy_key),
            admission_payload.get("latest_admission_decision_id"),
        ):
            latest_admission_at[strategy_key] = decided_at
            admission_payload["latest_admission_decision_id"] = str(admission_decision_id)
            admission_payload["latest_decided_at"] = _render_datetime(decided_at)
        _set_latest_activity(latest_activity, strategy_key, decided_at)
        _add_reason_list(blockers_by_strategy[strategy_key], reason_codes_json)
        _add_reason_list(blockers_by_strategy[strategy_key], blockers_json)

    for strategy_id, payload in payloads.items():
        _finalize_state_counts(payload, "signals", "signal_state_counts")
        _finalize_state_counts(payload, "decisions", "decision_state_counts")
        _finalize_state_counts(payload, "admissions", "admission_state_counts")
        payload["decisions"]["selected_count"] = int(payload["decisions"]["decision_state_counts"].get("selected", 0))
        payload["top_blocker_reasons"] = _top_blockers(blockers_by_strategy[strategy_id])
        payload["latest_activity_at"] = _render_datetime(latest_activity.get(strategy_id))
    return payloads


def _empty_execution_strategy_ledger() -> dict[str, Any]:
    return {
        "intents": {
            "intent_count": 0,
            "intent_state_counts": {},
            "intent_action_state_counts": {},
            "latest_execution_intent_id": None,
            "latest_created_at": None,
        },
        "attempts": {
            "attempt_count": 0,
            "attempt_status_counts": {},
            "attempt_intent_status_counts": {},
            "order_count": 0,
            "fill_count": 0,
            "latest_execution_attempt_id": None,
            "latest_requested_at": None,
        },
        "positions": {
            "position_count": 0,
            "open_position_count": 0,
            "closed_position_count": 0,
            "position_status_counts": {},
            "mark_count": 0,
            "missing_mark_count": 0,
            "stale_mark_count": 0,
            "latest_position_id": None,
            "latest_marked_at": None,
            "latest_updated_at": None,
        },
        "closes": {
            "close_count": 0,
            "latest_position_close_id": None,
            "latest_closed_at": None,
        },
        "pnl": {
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "net_pnl": 0.0,
        },
        "latest_activity_at": None,
    }


def _bump_nested_count(mapping: dict[str, dict[str, int]], outer_value: Any, inner_value: Any) -> None:
    outer_key = str(outer_value or "unknown")
    inner_key = str(inner_value or "unknown")
    nested = mapping.setdefault(outer_key, {})
    nested[inner_key] = int(nested.get(inner_key, 0)) + 1


def _sort_nested_counts(mapping: Mapping[str, Any]) -> dict[str, dict[str, int]]:
    sorted_mapping: dict[str, dict[str, int]] = {}
    for outer_key, inner_mapping in sorted(as_mapping(mapping).items()):
        sorted_mapping[str(outer_key)] = dict(sorted((str(inner_key), int(count)) for inner_key, count in as_mapping(inner_mapping).items()))
    return sorted_mapping


def _build_execution_strategy_ledgers(
    *,
    session: Any,
    strategies: Iterable[Any],
    market_day: date,
    start: datetime,
    end: datetime,
    now: datetime,
    execution_schema_ready: bool,
    intent_schema_ready: bool,
    portfolio_schema_ready: bool,
) -> dict[str, dict[str, Any]]:
    strategy_list = list(strategies)
    strategy_ids = [strategy.trading_strategy_id for strategy in strategy_list]
    payloads = {strategy_id: _empty_execution_strategy_ledger() for strategy_id in strategy_ids}
    if not strategy_ids:
        return payloads

    latest_activity: dict[str, datetime] = {}

    if intent_schema_ready:
        latest_intent_at: dict[str, datetime] = {}
        intent_rows = session.execute(
            select(
                ExecutionIntentModel.trading_strategy_id,
                ExecutionIntentModel.state,
                ExecutionIntentModel.action_type,
                ExecutionIntentModel.execution_intent_id,
                ExecutionIntentModel.created_at,
            )
            .where(ExecutionIntentModel.trading_strategy_id.in_(strategy_ids))
            .where(ExecutionIntentModel.created_at >= start)
            .where(ExecutionIntentModel.created_at < end)
            .order_by(ExecutionIntentModel.trading_strategy_id.asc(), ExecutionIntentModel.created_at.desc(), ExecutionIntentModel.execution_intent_id.asc())
        )
        for strategy_id, state, action_type, execution_intent_id, created_at in intent_rows:
            strategy_key = str(strategy_id)
            intent_payload = payloads[strategy_key]["intents"]
            _bump_count(intent_payload["intent_state_counts"], state)
            _bump_nested_count(intent_payload["intent_action_state_counts"], action_type, state)
            if _newer_desc_asc(created_at, execution_intent_id, latest_intent_at.get(strategy_key), intent_payload.get("latest_execution_intent_id")):
                latest_intent_at[strategy_key] = created_at
                intent_payload["latest_execution_intent_id"] = str(execution_intent_id)
                intent_payload["latest_created_at"] = _render_datetime(created_at)
            _set_latest_activity(latest_activity, strategy_key, created_at)

    attempt_strategy: dict[str, str] = {}
    if execution_schema_ready:
        latest_attempt_at: dict[str, datetime] = {}
        attempt_rows = session.execute(
            select(
                ExecutionAttemptModel.trading_strategy_id,
                ExecutionAttemptModel.status,
                ExecutionAttemptModel.trade_intent,
                ExecutionAttemptModel.execution_attempt_id,
                ExecutionAttemptModel.requested_at,
            )
            .where(ExecutionAttemptModel.trading_strategy_id.in_(strategy_ids))
            .where(
                or_(
                    ExecutionAttemptModel.market_date == market_day,
                    and_(
                        ExecutionAttemptModel.requested_at >= start,
                        ExecutionAttemptModel.requested_at < end,
                    ),
                )
            )
            .order_by(ExecutionAttemptModel.trading_strategy_id.asc(), ExecutionAttemptModel.requested_at.desc(), ExecutionAttemptModel.execution_attempt_id.asc())
        )
        for strategy_id, status, trade_intent, execution_attempt_id, requested_at in attempt_rows:
            strategy_key = str(strategy_id)
            attempt_id = str(execution_attempt_id)
            attempt_strategy[attempt_id] = strategy_key
            attempt_payload = payloads[strategy_key]["attempts"]
            _bump_count(attempt_payload["attempt_status_counts"], status)
            _bump_nested_count(attempt_payload["attempt_intent_status_counts"], trade_intent, status)
            if _newer_desc_asc(
                requested_at,
                execution_attempt_id,
                latest_attempt_at.get(strategy_key),
                attempt_payload.get("latest_execution_attempt_id"),
            ):
                latest_attempt_at[strategy_key] = requested_at
                attempt_payload["latest_execution_attempt_id"] = attempt_id
                attempt_payload["latest_requested_at"] = _render_datetime(requested_at)
            _set_latest_activity(latest_activity, strategy_key, requested_at)

        if attempt_strategy:
            order_rows = session.execute(
                select(ExecutionOrderModel.execution_attempt_id, func.count())
                .where(ExecutionOrderModel.execution_attempt_id.in_(list(attempt_strategy)))
                .group_by(ExecutionOrderModel.execution_attempt_id)
            )
            for execution_attempt_id, count in order_rows:
                strategy_key = attempt_strategy.get(str(execution_attempt_id))
                if strategy_key is not None:
                    payloads[strategy_key]["attempts"]["order_count"] += int(count or 0)

            fill_rows = session.execute(
                select(ExecutionFillModel.execution_attempt_id, func.count())
                .where(ExecutionFillModel.execution_attempt_id.in_(list(attempt_strategy)))
                .group_by(ExecutionFillModel.execution_attempt_id)
            )
            for execution_attempt_id, count in fill_rows:
                strategy_key = attempt_strategy.get(str(execution_attempt_id))
                if strategy_key is not None:
                    payloads[strategy_key]["attempts"]["fill_count"] += int(count or 0)

    if portfolio_schema_ready:
        latest_position_at: dict[str, datetime] = {}
        latest_marked_at: dict[str, datetime] = {}
        stale_after = now - timedelta(seconds=LEDGER_MARK_STALE_AFTER_SECONDS)
        position_rows = session.execute(
            select(
                PortfolioPositionModel.trading_strategy_id,
                PortfolioPositionModel.position_id,
                PortfolioPositionModel.status,
                PortfolioPositionModel.realized_pnl,
                PortfolioPositionModel.unrealized_pnl,
                PortfolioPositionModel.close_mark,
                PortfolioPositionModel.close_marked_at,
                PortfolioPositionModel.updated_at,
            )
            .where(PortfolioPositionModel.trading_strategy_id.in_(strategy_ids))
            .where(
                or_(
                    PortfolioPositionModel.market_date_opened == market_day,
                    PortfolioPositionModel.market_date_closed == market_day,
                    PortfolioPositionModel.status.in_(sorted(OPEN_POSITION_STATUSES)),
                )
            )
            .order_by(PortfolioPositionModel.trading_strategy_id.asc(), PortfolioPositionModel.updated_at.desc(), PortfolioPositionModel.position_id.asc())
        )
        for strategy_id, position_id, status, realized_pnl, unrealized_pnl, close_mark, close_marked_at, updated_at in position_rows:
            strategy_key = str(strategy_id)
            position_payload = payloads[strategy_key]["positions"]
            pnl_payload = payloads[strategy_key]["pnl"]
            status_key = str(status or "unknown")
            _bump_count(position_payload["position_status_counts"], status_key)
            pnl_payload["realized_pnl"] += coerce_float(realized_pnl) or 0.0
            if status_key in OPEN_POSITION_STATUSES:
                pnl_payload["unrealized_pnl"] += coerce_float(unrealized_pnl) or 0.0
                if close_mark is None:
                    position_payload["missing_mark_count"] += 1
                else:
                    position_payload["mark_count"] += 1
                if close_marked_at is not None:
                    if close_marked_at < stale_after:
                        position_payload["stale_mark_count"] += 1
                    if close_marked_at > latest_marked_at.get(strategy_key, datetime.min.replace(tzinfo=UTC)):
                        latest_marked_at[strategy_key] = close_marked_at
                        position_payload["latest_marked_at"] = _render_datetime(close_marked_at)
            if _newer_desc_asc(updated_at, position_id, latest_position_at.get(strategy_key), position_payload.get("latest_position_id")):
                latest_position_at[strategy_key] = updated_at
                position_payload["latest_position_id"] = str(position_id)
                position_payload["latest_updated_at"] = _render_datetime(updated_at)
            _set_latest_activity(latest_activity, strategy_key, updated_at)

        latest_close_at: dict[str, datetime] = {}
        close_rows = session.execute(
            select(
                PortfolioPositionModel.trading_strategy_id,
                PositionCloseModel.position_close_id,
                PositionCloseModel.closed_at,
            )
            .join(PortfolioPositionModel, PositionCloseModel.position_id == PortfolioPositionModel.position_id)
            .where(PortfolioPositionModel.trading_strategy_id.in_(strategy_ids))
            .where(PositionCloseModel.closed_at >= start)
            .where(PositionCloseModel.closed_at < end)
            .order_by(
                PortfolioPositionModel.trading_strategy_id.asc(),
                PositionCloseModel.closed_at.desc(),
                PositionCloseModel.position_close_id.desc(),
            )
        )
        for strategy_id, position_close_id, closed_at in close_rows:
            strategy_key = str(strategy_id)
            close_payload = payloads[strategy_key]["closes"]
            close_payload["close_count"] += 1
            if _newer_desc_desc(closed_at, position_close_id, latest_close_at.get(strategy_key), close_payload.get("latest_position_close_id")):
                latest_close_at[strategy_key] = closed_at
                close_payload["latest_position_close_id"] = int(position_close_id)
                close_payload["latest_closed_at"] = _render_datetime(closed_at)
            _set_latest_activity(latest_activity, strategy_key, closed_at)

    for strategy_id, payload in payloads.items():
        intent_state_counts = dict(sorted((str(state), int(count)) for state, count in as_mapping(payload["intents"]["intent_state_counts"]).items()))
        payload["intents"]["intent_state_counts"] = intent_state_counts
        payload["intents"]["intent_action_state_counts"] = _sort_nested_counts(payload["intents"]["intent_action_state_counts"])
        payload["intents"]["intent_count"] = int(sum(intent_state_counts.values()))

        attempt_status_counts = dict(sorted((str(state), int(count)) for state, count in as_mapping(payload["attempts"]["attempt_status_counts"]).items()))
        payload["attempts"]["attempt_status_counts"] = attempt_status_counts
        payload["attempts"]["attempt_intent_status_counts"] = _sort_nested_counts(payload["attempts"]["attempt_intent_status_counts"])
        payload["attempts"]["attempt_count"] = int(sum(attempt_status_counts.values()))

        position_status_counts = dict(sorted((str(state), int(count)) for state, count in as_mapping(payload["positions"]["position_status_counts"]).items()))
        payload["positions"]["position_status_counts"] = position_status_counts
        payload["positions"]["position_count"] = int(sum(position_status_counts.values()))
        payload["positions"]["open_position_count"] = int(sum(count for state, count in position_status_counts.items() if state in OPEN_POSITION_STATUSES))
        payload["positions"]["closed_position_count"] = int(position_status_counts.get("closed", 0))

        realized_pnl = round(coerce_float(payload["pnl"]["realized_pnl"]) or 0.0, 2)
        unrealized_pnl = round(coerce_float(payload["pnl"]["unrealized_pnl"]) or 0.0, 2)
        payload["pnl"] = {
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
            "net_pnl": round(realized_pnl + unrealized_pnl, 2),
        }
        payload["latest_activity_at"] = _render_datetime(latest_activity.get(strategy_id))
    return payloads


def _strategy_row(
    *,
    strategy: Any,
    engine_payload: Mapping[str, Any],
    execution_payload: Mapping[str, Any],
) -> dict[str, Any]:
    source = as_mapping(engine_payload.get("source"))
    candidates = as_mapping(engine_payload.get("candidates"))
    signals = as_mapping(engine_payload.get("signals"))
    decisions = as_mapping(engine_payload.get("decisions"))
    admissions = as_mapping(engine_payload.get("admissions"))
    intents = as_mapping(execution_payload.get("intents"))
    attempts = as_mapping(execution_payload.get("attempts"))
    positions = as_mapping(execution_payload.get("positions"))
    closes = as_mapping(execution_payload.get("closes"))
    latest_activity_candidates = [
        as_text(engine_payload.get("latest_activity_at")),
        as_text(execution_payload.get("latest_activity_at")),
    ]
    latest_activity = max((value for value in latest_activity_candidates if value), default=None)
    return {
        "trading_strategy_id": strategy.trading_strategy_id,
        "name": strategy.name,
        "trade_structure": strategy.trade_structure,
        "config_hash": strategy.config_hash,
        "execution_mode": strategy.execution.mode,
        "execution_runtime": strategy.execution.runtime,
        "approval_mode": strategy.execution.approval,
        "source": source,
        "candidates": candidates,
        "signals": signals,
        "decisions": decisions,
        "admissions": admissions,
        "intents": intents,
        "attempts": attempts,
        "positions": positions,
        "closes": closes,
        "pnl": as_mapping(execution_payload.get("pnl")),
        "top_blocker_reasons": dict(as_mapping(engine_payload.get("top_blocker_reasons"))),
        "latest_lifecycle_ids": {
            "ticker_source_run_id": source.get("latest_ticker_source_run_id"),
            "candidate_run_id": candidates.get("latest_candidate_run_id"),
            "trade_signal_id": signals.get("latest_trade_signal_id"),
            "trade_decision_id": decisions.get("latest_trade_decision_id"),
            "admission_decision_id": admissions.get("latest_admission_decision_id"),
            "execution_intent_id": intents.get("latest_execution_intent_id"),
            "execution_attempt_id": attempts.get("latest_execution_attempt_id"),
            "position_id": positions.get("latest_position_id"),
            "position_close_id": closes.get("latest_position_close_id"),
        },
        "latest_activity_at": latest_activity,
    }


@with_storage()
def build_strategy_evidence_ledger(
    *,
    db_target: str | None = None,
    market_date: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    resolved_market_date = _market_date_or_today(market_date)
    market_day, start, end = _window(resolved_market_date)
    now = datetime.now(UTC)
    strategies = load_active_trading_strategies()
    engine_schema_ready = storage.engine_facts.schema_has_tables(*ENGINE_LEDGER_TABLES)
    execution_schema_ready = storage.execution.schema_ready()
    intent_schema_ready = storage.execution.intent_schema_ready()
    portfolio_schema_ready = storage.execution.portfolio_schema_ready()

    rows: list[dict[str, Any]] = []
    with storage.engine_facts.session_factory() as engine_session, storage.execution.session_factory() as execution_session:
        strategy_list = list(strategies.values())
        engine_payloads = (
            _build_engine_strategy_ledgers(
                session=engine_session,
                strategies=strategy_list,
                market_day=market_day,
                start=start,
                end=end,
            )
            if engine_schema_ready
            else {strategy.trading_strategy_id: {} for strategy in strategy_list}
        )
        execution_payloads = _build_execution_strategy_ledgers(
            session=execution_session,
            strategies=strategy_list,
            market_day=market_day,
            start=start,
            end=end,
            now=now,
            execution_schema_ready=execution_schema_ready,
            intent_schema_ready=intent_schema_ready,
            portfolio_schema_ready=portfolio_schema_ready,
        )
        for strategy in strategy_list:
            rows.append(
                _strategy_row(
                    strategy=strategy,
                    engine_payload=engine_payloads.get(strategy.trading_strategy_id, {}),
                    execution_payload=execution_payloads.get(strategy.trading_strategy_id, {}),
                )
            )

    total_realized = round(sum(coerce_float(as_mapping(row.get("pnl")).get("realized_pnl")) or 0.0 for row in rows), 2)
    total_unrealized = round(sum(coerce_float(as_mapping(row.get("pnl")).get("unrealized_pnl")) or 0.0 for row in rows), 2)
    schema = {
        "engine_facts": "ready" if engine_schema_ready else "blocked",
        "execution": "ready" if execution_schema_ready else "blocked",
        "execution_intents": "ready" if intent_schema_ready else "blocked",
        "portfolio": "ready" if portfolio_schema_ready else "blocked",
    }
    status = "healthy" if all(value == "ready" for value in schema.values()) else "blocked"
    return {
        "status": status,
        "market_date": resolved_market_date,
        "generated_at": utc_now_iso(),
        "strategy_count": len(strategies),
        "schema": schema,
        "summary": {
            "source_run_count": sum(coerce_int(as_mapping(row.get("source")).get("source_run_count")) or 0 for row in rows),
            "candidate_run_count": sum(coerce_int(as_mapping(row.get("candidates")).get("candidate_run_count")) or 0 for row in rows),
            "trade_candidate_count": sum(coerce_int(as_mapping(row.get("candidates")).get("trade_candidate_count")) or 0 for row in rows),
            "signal_count": sum(coerce_int(as_mapping(row.get("signals")).get("signal_count")) or 0 for row in rows),
            "decision_count": sum(coerce_int(as_mapping(row.get("decisions")).get("decision_count")) or 0 for row in rows),
            "selected_count": sum(coerce_int(as_mapping(row.get("decisions")).get("selected_count")) or 0 for row in rows),
            "admission_count": sum(coerce_int(as_mapping(row.get("admissions")).get("admission_count")) or 0 for row in rows),
            "intent_count": sum(coerce_int(as_mapping(row.get("intents")).get("intent_count")) or 0 for row in rows),
            "attempt_count": sum(coerce_int(as_mapping(row.get("attempts")).get("attempt_count")) or 0 for row in rows),
            "order_count": sum(coerce_int(as_mapping(row.get("attempts")).get("order_count")) or 0 for row in rows),
            "fill_count": sum(coerce_int(as_mapping(row.get("attempts")).get("fill_count")) or 0 for row in rows),
            "position_count": sum(coerce_int(as_mapping(row.get("positions")).get("position_count")) or 0 for row in rows),
            "open_position_count": sum(coerce_int(as_mapping(row.get("positions")).get("open_position_count")) or 0 for row in rows),
            "close_count": sum(coerce_int(as_mapping(row.get("closes")).get("close_count")) or 0 for row in rows),
            "realized_pnl": total_realized,
            "unrealized_pnl": total_unrealized,
            "net_pnl": round(total_realized + total_unrealized, 2),
        },
        "strategies": rows,
    }
