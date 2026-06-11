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


def _count_state_rows(session: Any, statement: Any, field: Any) -> dict[str, int]:
    rows = session.execute(statement.with_only_columns(field, func.count()).group_by(field)).all()
    return dict(sorted((str(key or "unknown"), int(count or 0)) for key, count in rows))


def _count_action_state_rows(session: Any, statement: Any, action_field: Any, state_field: Any) -> dict[str, dict[str, int]]:
    counts: dict[str, Counter[str]] = {}
    for action, state, count in session.execute(statement.with_only_columns(action_field, state_field, func.count()).group_by(action_field, state_field)):
        action_key = str(action or "unknown")
        counts.setdefault(action_key, Counter())[str(state or "unknown")] += int(count or 0)
    return {action: dict(sorted(counter.items())) for action, counter in sorted(counts.items())}


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


def _sum_float(rows: Iterable[Any], field_name: str) -> float:
    return round(sum(coerce_float(getattr(row, field_name)) or 0.0 for row in rows), 2)


def _source_symbols_for_run(session: Any, ticker_source_run_id: str | None) -> list[str]:
    if not ticker_source_run_id:
        return []
    rows = session.execute(
        select(TickerSourceObservationModel.symbol)
        .where(TickerSourceObservationModel.ticker_source_run_id == ticker_source_run_id)
        .where(TickerSourceObservationModel.observation_state == "selected")
        .order_by(
            TickerSourceObservationModel.rank.asc().nulls_last(),
            TickerSourceObservationModel.symbol.asc(),
        )
        .limit(SOURCE_SYMBOL_LIMIT)
    ).all()
    return [str(symbol) for (symbol,) in rows]


def _engine_strategy_ledger(
    *,
    session: Any,
    strategy: Any,
    market_day: date,
    start: datetime,
    end: datetime,
) -> dict[str, Any]:
    source_ref = strategy.source.ref
    source_base = select(TickerSourceRunModel).where(TickerSourceRunModel.generated_at >= start).where(TickerSourceRunModel.generated_at < end)
    source_statement = source_base.where(TickerSourceRunModel.ticker_source_id == source_ref)
    latest_source = session.scalars(
        source_statement.order_by(TickerSourceRunModel.generated_at.desc(), TickerSourceRunModel.ticker_source_run_id.asc()).limit(1)
    ).first()
    source_run_count = session.scalar(select(func.count()).select_from(source_statement.subquery())) or 0
    source_symbols = _source_symbols_for_run(session, None if latest_source is None else latest_source.ticker_source_run_id)
    configured_source_symbols = list(strategy.symbols[:SOURCE_SYMBOL_LIMIT])

    candidate_statement = (
        select(CandidateRunModel)
        .where(CandidateRunModel.trading_strategy_id == strategy.trading_strategy_id)
        .where(CandidateRunModel.routine == "entry")
        .where(CandidateRunModel.generated_at >= start)
        .where(CandidateRunModel.generated_at < end)
    )
    candidate_runs = session.scalars(candidate_statement.order_by(CandidateRunModel.generated_at.desc(), CandidateRunModel.candidate_run_id.asc())).all()
    latest_candidate = candidate_runs[0] if candidate_runs else None
    candidate_run_ids = [row.candidate_run_id for row in candidate_runs]
    candidate_count = sum(int(row.candidate_count or 0) for row in candidate_runs)
    trade_candidate_count = (
        session.scalar(
            select(func.count())
            .select_from(TradeCandidateModel)
            .where(TradeCandidateModel.trading_strategy_id == strategy.trading_strategy_id)
            .where(TradeCandidateModel.routine == "entry")
            .where(TradeCandidateModel.observed_at >= start)
            .where(TradeCandidateModel.observed_at < end)
        )
        or 0
    )

    signal_statement = select(TradeSignalModel).where(TradeSignalModel.trading_strategy_id == strategy.trading_strategy_id).where(
        TradeSignalModel.session_date == market_day
    )
    signal_state_counts = _count_state_rows(session, signal_statement, TradeSignalModel.signal_state)
    latest_signal = session.scalars(signal_statement.order_by(TradeSignalModel.observed_at.desc(), TradeSignalModel.trade_signal_id.asc()).limit(1)).first()

    decision_statement = (
        select(TradeDecisionModel)
        .where(TradeDecisionModel.trading_strategy_id == strategy.trading_strategy_id)
        .where(TradeDecisionModel.routine == "entry")
        .where(TradeDecisionModel.decided_at >= start)
        .where(TradeDecisionModel.decided_at < end)
    )
    decision_state_counts = _count_state_rows(session, decision_statement, TradeDecisionModel.decision_state)
    latest_decision = session.scalars(
        decision_statement.order_by(TradeDecisionModel.decided_at.desc(), TradeDecisionModel.trade_decision_id.asc()).limit(1)
    ).first()

    admission_statement = (
        select(TradeAdmissionModel)
        .join(TradeSignalModel, TradeAdmissionModel.trade_signal_id == TradeSignalModel.trade_signal_id)
        .where(TradeSignalModel.trading_strategy_id == strategy.trading_strategy_id)
        .where(TradeAdmissionModel.session_date == market_day)
    )
    admission_state_counts = _count_state_rows(session, admission_statement, TradeAdmissionModel.admission_state)
    latest_admission = session.scalars(
        admission_statement.order_by(TradeAdmissionModel.decided_at.desc(), TradeAdmissionModel.admission_decision_id.asc()).limit(1)
    ).first()

    blockers: Counter[str] = Counter()
    for run in candidate_runs:
        summary = as_mapping(run.summary_json)
        _add_count_mapping(blockers, summary.get("top_quality_blockers"))
        _add_count_mapping(blockers, summary.get("top_rejection_counts"))
    if candidate_run_ids:
        diagnostics = session.scalars(
            select(CandidateSymbolDiagnosticModel).where(CandidateSymbolDiagnosticModel.candidate_run_id.in_(candidate_run_ids))
        ).all()
        for diagnostic in diagnostics:
            _add_count_mapping(blockers, diagnostic.rejection_counts_json)
            _add_quality_waterfall_reasons(blockers, as_mapping(diagnostic.evidence_json).get("quality_waterfall"))
    candidate_rows = session.scalars(
        select(TradeCandidateModel)
        .where(TradeCandidateModel.trading_strategy_id == strategy.trading_strategy_id)
        .where(TradeCandidateModel.observed_at >= start)
        .where(TradeCandidateModel.observed_at < end)
    ).all()
    for row in candidate_rows:
        _add_reason_list(blockers, row.reason_codes_json)
        _add_reason_list(blockers, row.blockers_json)
    signal_rows = session.scalars(signal_statement).all()
    for row in signal_rows:
        _add_reason_list(blockers, row.reason_codes_json)
        _add_reason_list(blockers, row.blockers_json)
    decision_rows = session.scalars(decision_statement).all()
    for row in decision_rows:
        _add_reason_list(blockers, row.reason_codes_json)
        _add_reason_list(blockers, row.blockers_json)
    admission_rows = session.scalars(admission_statement).all()
    for row in admission_rows:
        _add_reason_list(blockers, row.reason_codes_json)
        _add_reason_list(blockers, row.blockers_json)

    latest_activity_at = max(
        (
            value
            for value in (
                None if latest_source is None else latest_source.generated_at,
                None if latest_candidate is None else latest_candidate.generated_at,
                None if latest_signal is None else latest_signal.observed_at,
                None if latest_decision is None else latest_decision.decided_at,
                None if latest_admission is None else latest_admission.decided_at,
            )
            if value is not None
        ),
        default=None,
    )
    source_symbol_count = (
        int(latest_source.selected_count or 0)
        if latest_source is not None
        else len(strategy.symbols)
    )
    return {
        "source": {
            "source_type": strategy.source.kind,
            "source_id": source_ref,
            "source_run_count": int(source_run_count),
            "configured_symbol_count": len(strategy.symbols),
            "latest_symbol_count": source_symbol_count,
            "symbols": source_symbols or configured_source_symbols,
            "latest_ticker_source_run_id": None if latest_source is None else latest_source.ticker_source_run_id,
            "latest_generated_at": None if latest_source is None else _render_datetime(latest_source.generated_at),
        },
        "candidates": {
            "candidate_run_count": len(candidate_runs),
            "candidate_count": int(candidate_count),
            "trade_candidate_count": int(trade_candidate_count),
            "latest_candidate_run_id": None if latest_candidate is None else latest_candidate.candidate_run_id,
            "latest_generated_at": None if latest_candidate is None else _render_datetime(latest_candidate.generated_at),
        },
        "signals": {
            "signal_count": int(sum(signal_state_counts.values())),
            "signal_state_counts": signal_state_counts,
            "latest_trade_signal_id": None if latest_signal is None else latest_signal.trade_signal_id,
            "latest_observed_at": None if latest_signal is None else _render_datetime(latest_signal.observed_at),
        },
        "decisions": {
            "decision_count": int(sum(decision_state_counts.values())),
            "decision_state_counts": decision_state_counts,
            "selected_count": int(decision_state_counts.get("selected", 0)),
            "latest_trade_decision_id": None if latest_decision is None else latest_decision.trade_decision_id,
            "latest_decided_at": None if latest_decision is None else _render_datetime(latest_decision.decided_at),
        },
        "admissions": {
            "admission_count": int(sum(admission_state_counts.values())),
            "admission_state_counts": admission_state_counts,
            "latest_admission_decision_id": None if latest_admission is None else latest_admission.admission_decision_id,
            "latest_decided_at": None if latest_admission is None else _render_datetime(latest_admission.decided_at),
        },
        "top_blocker_reasons": _top_blockers(blockers),
        "latest_activity_at": _render_datetime(latest_activity_at),
    }


def _execution_strategy_ledger(
    *,
    session: Any,
    strategy: Any,
    market_day: date,
    start: datetime,
    end: datetime,
    now: datetime,
    execution_schema_ready: bool,
    intent_schema_ready: bool,
    portfolio_schema_ready: bool,
) -> dict[str, Any]:
    strategy_id = strategy.trading_strategy_id
    intent_state_counts: dict[str, int] = {}
    intent_action_state_counts: dict[str, dict[str, int]] = {}
    latest_intent = None
    if intent_schema_ready:
        intent_statement = (
            select(ExecutionIntentModel)
            .where(ExecutionIntentModel.trading_strategy_id == strategy_id)
            .where(ExecutionIntentModel.created_at >= start)
            .where(ExecutionIntentModel.created_at < end)
        )
        intent_state_counts = _count_state_rows(session, intent_statement, ExecutionIntentModel.state)
        intent_action_state_counts = _count_action_state_rows(session, intent_statement, ExecutionIntentModel.action_type, ExecutionIntentModel.state)
        latest_intent = session.scalars(
            intent_statement.order_by(ExecutionIntentModel.created_at.desc(), ExecutionIntentModel.execution_intent_id.asc()).limit(1)
        ).first()

    attempt_status_counts: dict[str, int] = {}
    attempt_intent_status_counts: dict[str, dict[str, int]] = {}
    latest_attempt = None
    attempt_ids: list[str] = []
    order_count = 0
    fill_count = 0
    if execution_schema_ready:
        attempt_statement = (
            select(ExecutionAttemptModel)
            .where(ExecutionAttemptModel.trading_strategy_id == strategy_id)
            .where(
                or_(
                    ExecutionAttemptModel.market_date == market_day,
                    and_(
                        ExecutionAttemptModel.requested_at >= start,
                        ExecutionAttemptModel.requested_at < end,
                    ),
                )
            )
        )
        attempt_rows = session.scalars(attempt_statement.order_by(ExecutionAttemptModel.requested_at.desc())).all()
        attempt_status_counts = dict(sorted(Counter(str(row.status or "unknown") for row in attempt_rows).items()))
        intent_counts: dict[str, Counter[str]] = {}
        for row in attempt_rows:
            intent_counts.setdefault(str(row.trade_intent or "unknown"), Counter())[str(row.status or "unknown")] += 1
        attempt_intent_status_counts = {intent: dict(sorted(counter.items())) for intent, counter in sorted(intent_counts.items())}
        latest_attempt = attempt_rows[0] if attempt_rows else None
        attempt_ids = [row.execution_attempt_id for row in attempt_rows]
        if attempt_ids:
            order_count = int(
                session.scalar(select(func.count()).select_from(ExecutionOrderModel).where(ExecutionOrderModel.execution_attempt_id.in_(attempt_ids)))
                or 0
            )
            fill_count = int(
                session.scalar(select(func.count()).select_from(ExecutionFillModel).where(ExecutionFillModel.execution_attempt_id.in_(attempt_ids)))
                or 0
            )

    position_status_counts: dict[str, int] = {}
    latest_position = None
    close_count = 0
    latest_close = None
    realized_pnl = 0.0
    unrealized_pnl = 0.0
    mark_count = 0
    missing_mark_count = 0
    stale_mark_count = 0
    latest_marked_at = None
    if portfolio_schema_ready:
        position_statement = (
            select(PortfolioPositionModel)
            .where(PortfolioPositionModel.trading_strategy_id == strategy_id)
            .where(
                or_(
                    PortfolioPositionModel.market_date_opened == market_day,
                    PortfolioPositionModel.market_date_closed == market_day,
                    PortfolioPositionModel.status.in_(sorted(OPEN_POSITION_STATUSES)),
                )
            )
        )
        positions = session.scalars(position_statement.order_by(PortfolioPositionModel.updated_at.desc(), PortfolioPositionModel.position_id.asc())).all()
        position_status_counts = dict(sorted(Counter(str(row.status or "unknown") for row in positions).items()))
        latest_position = positions[0] if positions else None
        open_positions = [row for row in positions if str(row.status or "") in OPEN_POSITION_STATUSES]
        realized_pnl = _sum_float(positions, "realized_pnl")
        unrealized_pnl = _sum_float(open_positions, "unrealized_pnl")
        mark_count = sum(1 for row in open_positions if row.close_mark is not None)
        missing_mark_count = sum(1 for row in open_positions if row.close_mark is None)
        mark_times = [row.close_marked_at for row in open_positions if row.close_marked_at is not None]
        latest_marked_at = max(mark_times, default=None)
        stale_after = now - timedelta(seconds=LEDGER_MARK_STALE_AFTER_SECONDS)
        stale_mark_count = sum(1 for row in open_positions if row.close_marked_at is not None and row.close_marked_at < stale_after)

        close_statement = (
            select(PositionCloseModel)
            .join(PortfolioPositionModel, PositionCloseModel.position_id == PortfolioPositionModel.position_id)
            .where(PortfolioPositionModel.trading_strategy_id == strategy_id)
            .where(PositionCloseModel.closed_at >= start)
            .where(PositionCloseModel.closed_at < end)
        )
        close_count = int(session.scalar(select(func.count()).select_from(close_statement.subquery())) or 0)
        latest_close = session.scalars(
            close_statement.order_by(PositionCloseModel.closed_at.desc(), PositionCloseModel.position_close_id.desc()).limit(1)
        ).first()

    latest_activity_at = max(
        (
            value
            for value in (
                None if latest_intent is None else latest_intent.created_at,
                None if latest_attempt is None else latest_attempt.requested_at,
                None if latest_position is None else latest_position.updated_at,
                None if latest_close is None else latest_close.closed_at,
            )
            if value is not None
        ),
        default=None,
    )
    return {
        "intents": {
            "intent_count": int(sum(intent_state_counts.values())),
            "intent_state_counts": intent_state_counts,
            "intent_action_state_counts": intent_action_state_counts,
            "latest_execution_intent_id": None if latest_intent is None else latest_intent.execution_intent_id,
            "latest_created_at": None if latest_intent is None else _render_datetime(latest_intent.created_at),
        },
        "attempts": {
            "attempt_count": int(sum(attempt_status_counts.values())),
            "attempt_status_counts": attempt_status_counts,
            "attempt_intent_status_counts": attempt_intent_status_counts,
            "order_count": order_count,
            "fill_count": fill_count,
            "latest_execution_attempt_id": None if latest_attempt is None else latest_attempt.execution_attempt_id,
            "latest_requested_at": None if latest_attempt is None else _render_datetime(latest_attempt.requested_at),
        },
        "positions": {
            "position_count": int(sum(position_status_counts.values())),
            "open_position_count": int(sum(count for state, count in position_status_counts.items() if state in OPEN_POSITION_STATUSES)),
            "closed_position_count": int(position_status_counts.get("closed", 0)),
            "position_status_counts": position_status_counts,
            "mark_count": int(mark_count),
            "missing_mark_count": int(missing_mark_count),
            "stale_mark_count": int(stale_mark_count),
            "latest_position_id": None if latest_position is None else latest_position.position_id,
            "latest_marked_at": _render_datetime(latest_marked_at),
            "latest_updated_at": None if latest_position is None else _render_datetime(latest_position.updated_at),
        },
        "closes": {
            "close_count": close_count,
            "latest_position_close_id": None if latest_close is None else latest_close.position_close_id,
            "latest_closed_at": None if latest_close is None else _render_datetime(latest_close.closed_at),
        },
        "pnl": {
            "realized_pnl": round(realized_pnl, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "net_pnl": round(realized_pnl + unrealized_pnl, 2),
        },
        "latest_activity_at": _render_datetime(latest_activity_at),
    }


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
        for strategy in strategies.values():
            engine_payload = (
                _engine_strategy_ledger(
                    session=engine_session,
                    strategy=strategy,
                    market_day=market_day,
                    start=start,
                    end=end,
                )
                if engine_schema_ready
                else {}
            )
            execution_payload = _execution_strategy_ledger(
                session=execution_session,
                strategy=strategy,
                market_day=market_day,
                start=start,
                end=end,
                now=now,
                execution_schema_ready=execution_schema_ready,
                intent_schema_ready=intent_schema_ready,
                portfolio_schema_ready=portfolio_schema_ready,
            )
            rows.append(_strategy_row(strategy=strategy, engine_payload=engine_payload, execution_payload=execution_payload))

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
