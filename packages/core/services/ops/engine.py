from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select

from core.services.positions import enrich_position_row
from core.services.trading_strategies import load_active_trading_strategies
from core.value_coercion import as_text, utc_iso
from core.storage.capture_models import CaptureSummaryModel
from core.storage.engine_models import (
    CandidateRunModel,
    TickerSourceObservationModel,
    TickerSourceRunModel,
    TickerSourceStateModel,
    TradeCandidateModel,
)
from core.storage.lifecycle_models import TradeDecisionModel, TradeSignalModel
from core.storage.serializers import parse_date, parse_datetime

from .shared import _combine_statuses

OPEN_POSITION_STATUSES = ("open", "partial_close")
ENGINE_RECENT_ROW_LIMIT = 12
SOURCE_SYMBOL_LIMIT = 25


def _window(market_date: str) -> tuple[datetime, datetime]:
    start = datetime.combine(parse_date(market_date), datetime.min.time(), tzinfo=UTC)
    return start, start + timedelta(days=1)


def _in_window(row: Mapping[str, Any], field_name: str, *, start: datetime, end: datetime) -> bool:
    parsed = parse_datetime(row.get(field_name))
    return parsed is not None and start <= parsed < end


def _count_rows(rows: list[Mapping[str, Any]], field_name: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(field_name) or "unknown") for row in rows).items()))


def _render_datetime(value: datetime | None) -> str | None:
    return utc_iso(value)


def _ticker_source_run_row(row: TickerSourceRunModel, *, symbols: list[str]) -> dict[str, Any]:
    return {
        "ticker_source_run_id": row.ticker_source_run_id,
        "ticker_source_type": row.ticker_source_type,
        "ticker_source_id": row.ticker_source_id,
        "job_run_id": row.job_run_id,
        "status": row.status,
        "config_hash": row.config_hash,
        "generated_at": _render_datetime(row.generated_at),
        "completed_at": _render_datetime(row.completed_at),
        "observed_count": row.observed_count,
        "selected_count": row.selected_count,
        "excluded_count": row.excluded_count,
        "symbols": symbols[:SOURCE_SYMBOL_LIMIT],
        "summary": dict(row.summary_json or {}),
        "created_at": _render_datetime(row.created_at),
        "updated_at": _render_datetime(row.updated_at),
    }


def _candidate_run_row(row: CandidateRunModel) -> dict[str, Any]:
    return {
        "candidate_run_id": row.candidate_run_id,
        "run_key": row.run_key,
        "trading_strategy_id": row.trading_strategy_id,
        "trade_structure": row.trade_structure,
        "routine": row.routine,
        "ticker_source_run_id": row.ticker_source_run_id,
        "ticker_source_kind": row.ticker_source_kind,
        "ticker_source_id": row.ticker_source_id,
        "status": row.status,
        "config_hash": row.config_hash,
        "generated_at": _render_datetime(row.generated_at),
        "completed_at": _render_datetime(row.completed_at),
        "symbol_count": row.symbol_count,
        "candidate_count": row.candidate_count,
        "summary": dict(row.summary_json or {}),
        "created_at": _render_datetime(row.created_at),
        "updated_at": _render_datetime(row.updated_at),
    }


def _ticker_source_state_row(row: TickerSourceStateModel) -> dict[str, Any]:
    return {
        "ticker_source_id": row.ticker_source_id,
        "symbol": row.symbol,
        "active": row.active,
        "last_state": row.last_state,
        "last_rank": row.last_rank,
        "best_rank": row.best_rank,
        "last_score": row.last_score,
        "best_score": row.best_score,
        "seen_count": row.seen_count,
        "selected_count": row.selected_count,
        "consecutive_missing_count": row.consecutive_missing_count,
        "last_ticker_source_run_id": row.last_ticker_source_run_id,
        "last_seen_at": _render_datetime(row.last_seen_at),
        "last_selected_at": _render_datetime(row.last_selected_at),
        "updated_at": _render_datetime(row.updated_at),
        "metrics": dict(row.last_metrics_json or {}),
    }


def _decision_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "trade_decision_id": row.get("trade_decision_id"),
        "trade_signal_id": row.get("trade_signal_id"),
        "candidate_run_id": row.get("candidate_run_id"),
        "trading_strategy_id": row.get("trading_strategy_id"),
        "trade_structure": row.get("trade_structure"),
        "routine": row.get("routine"),
        "underlying_symbol": row.get("underlying_symbol"),
        "decision_state": row.get("decision_state"),
        "selection_rank": row.get("selection_rank"),
        "confidence": row.get("confidence"),
        "reason_codes": row.get("reason_codes"),
        "blockers": row.get("blockers"),
        "decided_at": row.get("decided_at"),
        "expires_at": row.get("expires_at"),
    }


def _signal_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "trade_signal_id": row.get("trade_signal_id"),
        "candidate_run_id": row.get("candidate_run_id"),
        "trading_strategy_id": row.get("trading_strategy_id"),
        "trade_structure": row.get("trade_structure"),
        "routine": row.get("routine"),
        "underlying_symbol": row.get("underlying_symbol"),
        "signal_state": row.get("signal_state"),
        "confidence": row.get("confidence"),
        "reason_codes": row.get("reason_codes"),
        "blockers": row.get("blockers"),
        "observed_at": row.get("observed_at"),
        "expires_at": row.get("expires_at"),
    }


def _engine_fact_summary(
    *,
    engine_facts: Any,
    market_date: str,
    now: datetime,
) -> dict[str, Any]:
    start, end = _window(market_date)
    if not engine_facts.schema_ready():
        return {
            "status": "blocked",
            "reason": "engine_fact_schema_unavailable",
            "ticker_source_runs": [],
            "ticker_source_state": [],
            "candidate_runs": [],
            "trade_signals": [],
            "selected_decisions": [],
            "signal_state_counts": {},
            "decision_state_counts": {},
            "ticker_source_run_count": 0,
            "candidate_run_count": 0,
            "trade_candidate_count": 0,
            "signal_count": 0,
            "decision_count": 0,
            "selected_count": 0,
        }

    market_day = parse_date(market_date)
    with engine_facts.session_factory() as session:
        ticker_source_runs = session.scalars(
            select(TickerSourceRunModel)
            .where(TickerSourceRunModel.generated_at >= start)
            .where(TickerSourceRunModel.generated_at < end)
            .order_by(TickerSourceRunModel.generated_at.desc(), TickerSourceRunModel.ticker_source_run_id.asc())
            .limit(ENGINE_RECENT_ROW_LIMIT)
        ).all()
        ticker_source_run_count = (
            session.scalar(
                select(func.count())
                .select_from(TickerSourceRunModel)
                .where(TickerSourceRunModel.generated_at >= start)
                .where(TickerSourceRunModel.generated_at < end)
            )
            or 0
        )
        ticker_source_state = session.scalars(
            select(TickerSourceStateModel)
            .where(TickerSourceStateModel.active.is_(True))
            .order_by(TickerSourceStateModel.last_rank.asc().nullslast(), TickerSourceStateModel.last_seen_at.desc())
            .limit(ENGINE_RECENT_ROW_LIMIT)
        ).all()
        candidate_runs = session.scalars(
            select(CandidateRunModel)
            .where(CandidateRunModel.generated_at >= start)
            .where(CandidateRunModel.generated_at < end)
            .order_by(CandidateRunModel.generated_at.desc(), CandidateRunModel.candidate_run_id.asc())
            .limit(ENGINE_RECENT_ROW_LIMIT)
        ).all()
        candidate_run_count = (
            session.scalar(
                select(func.count())
                .select_from(CandidateRunModel)
                .where(CandidateRunModel.generated_at >= start)
                .where(CandidateRunModel.generated_at < end)
            )
            or 0
        )
        signal_counts = dict(
            session.execute(
                select(TradeSignalModel.signal_state, func.count())
                .where(TradeSignalModel.session_date == market_day)
                .group_by(TradeSignalModel.signal_state)
            ).all()
        )
        decision_counts = dict(
            session.execute(
                select(TradeDecisionModel.decision_state, func.count())
                .where(TradeDecisionModel.decided_at >= start)
                .where(TradeDecisionModel.decided_at < end)
                .group_by(TradeDecisionModel.decision_state)
            ).all()
        )
        trade_candidate_count = (
            session.scalar(
                select(func.count())
                .select_from(TradeCandidateModel)
                .where(TradeCandidateModel.observed_at >= start)
                .where(TradeCandidateModel.observed_at < end)
            )
            or 0
        )
        ticker_source_run_ids = [row.ticker_source_run_id for row in ticker_source_runs]
        symbols_by_ticker_source_run: dict[str, list[str]] = {ticker_source_run_id: [] for ticker_source_run_id in ticker_source_run_ids}
        if ticker_source_run_ids:
            for ticker_source_run_id, symbol in session.execute(
                select(TickerSourceObservationModel.ticker_source_run_id, TickerSourceObservationModel.symbol)
                .where(TickerSourceObservationModel.ticker_source_run_id.in_(ticker_source_run_ids))
                .where(TickerSourceObservationModel.observation_state == "selected")
                .order_by(
                    TickerSourceObservationModel.ticker_source_run_id.asc(),
                    TickerSourceObservationModel.rank.asc().nulls_last(),
                    TickerSourceObservationModel.symbol.asc(),
                )
            ):
                symbols = symbols_by_ticker_source_run.setdefault(str(ticker_source_run_id), [])
                if len(symbols) < SOURCE_SYMBOL_LIMIT:
                    symbols.append(str(symbol))

    now_iso = utc_iso(now)
    selected_decisions = [
        _decision_row(row)
        for row in engine_facts.list_trade_decisions_with_signals(
            decision_states=["selected"],
            routine="entry",
            as_of=now_iso,
            limit=ENGINE_RECENT_ROW_LIMIT,
        )
    ]
    watch_signals = [
        _signal_row(row)
        for row in engine_facts.list_trade_signals(
            signal_states=["ready", "observed"],
            routine="entry",
            as_of=now_iso,
            limit=ENGINE_RECENT_ROW_LIMIT,
        )
    ]
    ticker_source_rows = [
        _ticker_source_run_row(row, symbols=symbols_by_ticker_source_run.get(row.ticker_source_run_id, [])) for row in ticker_source_runs
    ]
    ticker_source_state_rows = [_ticker_source_state_row(row) for row in ticker_source_state]
    candidate_rows = [_candidate_run_row(row) for row in candidate_runs]
    ticker_source_statuses = {str(row.get("status") or "unknown") for row in ticker_source_rows}
    ticker_source_status = "idle"
    if ticker_source_rows:
        ticker_source_status = "degraded" if ticker_source_statuses - {"ready", "fallback", "completed", "ok"} else "healthy"
    return {
        "status": ticker_source_status,
        "ticker_source_runs": ticker_source_rows,
        "ticker_source_state": ticker_source_state_rows,
        "candidate_runs": candidate_rows,
        "trade_signals": watch_signals,
        "selected_decisions": selected_decisions,
        "signal_state_counts": dict(sorted((str(key), int(value)) for key, value in signal_counts.items())),
        "decision_state_counts": dict(sorted((str(key), int(value)) for key, value in decision_counts.items())),
        "ticker_source_run_count": int(ticker_source_run_count),
        "candidate_run_count": int(candidate_run_count),
        "trade_candidate_count": int(trade_candidate_count),
        "signal_count": int(sum(signal_counts.values())),
        "decision_count": int(sum(decision_counts.values())),
        "selected_count": int(decision_counts.get("selected", 0)),
    }


def _execution_summary(*, execution_store: Any, market_date: str) -> dict[str, Any]:
    start, end = _window(market_date)
    summary = {
        "status": "healthy",
        "intent_count": 0,
        "intent_state_counts": {},
        "entry_intent_count": 0,
        "entry_intent_state_counts": {},
        "management_intent_count": 0,
        "management_intent_state_counts": {},
        "open_position_count": 0,
        "open_position_symbols": {},
    }
    if not execution_store.intent_schema_ready():
        summary["status"] = "blocked"
        return summary

    intent_counts: Counter[tuple[str, str]] = Counter()
    for intent in execution_store.list_execution_intents(limit=1000):
        row = dict(intent)
        if not _in_window(row, "created_at", start=start, end=end):
            continue
        action_type = str(row.get("action_type") or "")
        state = str(row.get("state") or "unknown")
        intent_counts[(action_type, state)] += 1
    entry_counts = Counter({state: count for (action, state), count in intent_counts.items() if action == "open"})
    management_counts = Counter({state: count for (action, state), count in intent_counts.items() if action == "close"})
    state_counts: Counter[str] = Counter()
    for (_, state), count in intent_counts.items():
        state_counts[state] += count
    summary.update(
        {
            "intent_count": int(sum(state_counts.values())),
            "intent_state_counts": dict(sorted(state_counts.items())),
            "entry_intent_count": int(sum(entry_counts.values())),
            "entry_intent_state_counts": dict(sorted(entry_counts.items())),
            "management_intent_count": int(sum(management_counts.values())),
            "management_intent_state_counts": dict(sorted(management_counts.items())),
        }
    )

    if execution_store.portfolio_schema_ready():
        symbol_counts: Counter[str] = Counter()
        for position in execution_store.list_positions(statuses=list(OPEN_POSITION_STATUSES), limit=500):
            enriched = enrich_position_row(dict(position))
            symbol_counts[str(enriched.get("underlying_symbol") or enriched.get("root_symbol") or "unknown")] += 1
        summary["open_position_count"] = int(sum(symbol_counts.values()))
        summary["open_position_symbols"] = dict(sorted(symbol_counts.items()))
    else:
        summary["status"] = "blocked"
    return summary


def _capture_summary(*, capture_store: Any, now: datetime) -> dict[str, Any]:
    if not capture_store.target_schema_ready():
        return {
            "status": "blocked",
            "reason": "capture_schema_unavailable",
            "active_target_count": 0,
            "active_target_counts": {},
            "latest_summary": None,
        }
    active_targets = capture_store.list_active_capture_targets(limit=2000)
    target_counts = _count_rows(active_targets, "reason")
    latest_summary = None
    if capture_store.schema_ready():
        with capture_store.session_factory() as session:
            latest = session.scalars(select(CaptureSummaryModel).order_by(CaptureSummaryModel.captured_at.desc()).limit(1)).first()
        latest_summary = None if latest is None else capture_store.row(latest)
    latest_status = None if latest_summary is None else as_text(latest_summary.get("status"))
    if active_targets:
        status = "healthy" if latest_status in {None, "ok", "idle"} else "degraded"
    else:
        status = "idle"
    return {
        "status": status,
        "active_target_count": len(active_targets),
        "active_target_counts": target_counts,
        "latest_summary": latest_summary,
    }


def build_engine_ops_state(
    *,
    storage: Any,
    market_date: str,
    now: datetime,
) -> dict[str, Any]:
    strategies = load_active_trading_strategies()
    fact_summary = _engine_fact_summary(
        engine_facts=storage.engine_facts,
        market_date=market_date,
        now=now,
    )
    execution_summary = _execution_summary(
        execution_store=storage.execution,
        market_date=market_date,
    )
    capture_summary = _capture_summary(
        capture_store=storage.capture,
        now=now,
    )
    status = _combine_statuses(
        str(fact_summary.get("status") or "unknown"),
        str(execution_summary.get("status") or "unknown"),
        str(capture_summary.get("status") or "unknown"),
    )
    summary = {
        "strategy_count": len(strategies),
        "entry_strategy_count": sum(1 for strategy in strategies.values() if strategy.entry is not None and strategy.entry.enabled),
        "management_strategy_count": sum(1 for strategy in strategies.values() if strategy.management is not None and strategy.management.enabled),
        "ticker_source_run_count": fact_summary.get("ticker_source_run_count"),
        "candidate_run_count": fact_summary.get("candidate_run_count"),
        "trade_candidate_count": fact_summary.get("trade_candidate_count"),
        "signal_count": fact_summary.get("signal_count"),
        "signal_state_counts": fact_summary.get("signal_state_counts"),
        "decision_count": fact_summary.get("decision_count"),
        "decision_state_counts": fact_summary.get("decision_state_counts"),
        "selected_count": fact_summary.get("selected_count"),
        "intent_count": execution_summary.get("intent_count"),
        "intent_state_counts": execution_summary.get("intent_state_counts"),
        "entry_intent_count": execution_summary.get("entry_intent_count"),
        "entry_intent_state_counts": execution_summary.get("entry_intent_state_counts"),
        "management_intent_count": execution_summary.get("management_intent_count"),
        "management_intent_state_counts": execution_summary.get("management_intent_state_counts"),
        "open_position_count": execution_summary.get("open_position_count"),
        "open_position_symbols": execution_summary.get("open_position_symbols"),
        "capture_active_target_count": capture_summary.get("active_target_count"),
        "capture_target_counts": capture_summary.get("active_target_counts"),
        "capture_status": capture_summary.get("status"),
        "latest_capture_summary_id": (
            None if capture_summary.get("latest_summary") is None else dict(capture_summary["latest_summary"]).get("capture_summary_id")
        ),
    }
    return {
        "status": status,
        "summary": summary,
        "details": {
            "ticker_source_runs": fact_summary.get("ticker_source_runs"),
            "ticker_source_state": fact_summary.get("ticker_source_state"),
            "candidate_runs": fact_summary.get("candidate_runs"),
            "trade_signals": fact_summary.get("trade_signals"),
            "selected_decisions": fact_summary.get("selected_decisions"),
            "capture": capture_summary,
            "execution": execution_summary,
        },
    }


__all__ = ["build_engine_ops_state"]
