from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select

from core.money import money_sum_float
from core.services.execution_intents.shared import ACTIVE_INTENT_STATES
from core.services.trading_strategies import load_active_trading_strategies, routine_should_run_now
from core.storage.engine_models import (
    CandidateRunModel,
    CandidateSymbolDiagnosticModel,
    TickerSourceObservationModel,
    TickerSourceRunModel,
    TradeCandidateModel,
)
from core.storage.lifecycle_models import TradeAdmissionModel, TradeDecisionModel, TradeSignalModel
from core.storage.serializers import parse_date
from core.value_coercion import (
    as_list,
    as_mapping,
    as_text,
    coerce_float,
    coerce_int,
    utc_iso,
)

from core.services.ops.shared import (
    _attention,
    _combine_statuses,
)


from core.services.ops.trading.models import (
    ENTRY_QUALITY_STAGE_ORDER,
    NO_ENTRY_GROUP_CATEGORIES,
    NO_ENTRY_REASON_GROUPS,
    OPEN_POSITION_STATUSES,
    SOURCE_SYMBOL_LIMIT,
    _FlowProjection,
)
from core.services.ops.trading.account import _age_seconds
from core.services.ops.trading.execution_contract import _execution_contract_status, _strategy_execution_contract
from core.services.ops.trading.market_context import _market_context_reference_from_summary, _market_context_regime_fit

def _symbols_from_ticker_source_run(ticker_source_run: Mapping[str, Any] | None) -> list[str]:
    if ticker_source_run is None:
        return []
    evidence = as_mapping(ticker_source_run.get("evidence"))
    snapshot = as_mapping(evidence.get("snapshot"))
    entries = as_list(snapshot.get("entries"))
    symbols = [str(as_mapping(entry).get("symbol") or "").strip().upper() for entry in entries if str(as_mapping(entry).get("symbol") or "").strip()]
    if symbols:
        return list(dict.fromkeys(symbols))
    tickers = as_list(ticker_source_run.get("symbols"))
    return [str(symbol).strip().upper() for symbol in tickers if str(symbol or "").strip()]


def _normalized_symbols(symbols: tuple[str, ...] | list[str]) -> list[str]:
    normalized = [str(symbol).strip().upper() for symbol in symbols if str(symbol or "").strip()]
    return list(dict.fromkeys(normalized))

def _ticker_source_run_payload(row: TickerSourceRunModel, *, symbols: list[str]) -> dict[str, Any]:
    return {
        "ticker_source_run_id": row.ticker_source_run_id,
        "ticker_source_type": row.ticker_source_type,
        "ticker_source_id": row.ticker_source_id,
        "job_run_id": row.job_run_id,
        "status": row.status,
        "config_hash": row.config_hash,
        "generated_at": utc_iso(row.generated_at),
        "completed_at": utc_iso(row.completed_at),
        "observed_count": row.observed_count,
        "selected_count": row.selected_count,
        "excluded_count": row.excluded_count,
        "symbols": symbols[:SOURCE_SYMBOL_LIMIT],
        "summary": dict(row.summary_json or {}),
        "created_at": utc_iso(row.created_at),
        "updated_at": utc_iso(row.updated_at),
    }


def _candidate_symbol_diagnostic_payload(row: CandidateSymbolDiagnosticModel) -> dict[str, Any]:
    return {
        "candidate_run_id": row.candidate_run_id,
        "underlying_symbol": row.underlying_symbol,
        "trading_strategy_id": row.trading_strategy_id,
        "trade_structure": row.trade_structure,
        "routine": row.routine,
        "ticker_source_run_id": row.ticker_source_run_id,
        "ticker_source_kind": row.ticker_source_kind,
        "ticker_source_id": row.ticker_source_id,
        "diagnostic_status": row.diagnostic_status,
        "observed_at": utc_iso(row.observed_at),
        "spot_price": row.spot_price,
        "expiration_count": row.expiration_count,
        "contract_count": row.contract_count,
        "snapshot_count": row.snapshot_count,
        "raw_candidate_count": row.raw_candidate_count,
        "postprocess_candidate_count": row.postprocess_candidate_count,
        "runtime_candidate_count": row.runtime_candidate_count,
        "returned_candidate_count": row.returned_candidate_count,
        "setup": dict(row.setup_json or {}),
        "market_data": dict(row.market_data_json or {}),
        "rejection_counts": dict(row.rejection_counts_json or {}),
        "ranking_gate": dict(row.ranking_gate_json or {}),
        "examples": dict(row.examples_json or {}),
        "evidence": dict(row.evidence_json or {}),
        "created_at": utc_iso(row.created_at),
        "updated_at": utc_iso(row.updated_at),
    }

def _int_count_map(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    counts: dict[str, int] = {}
    for key, raw_count in value.items():
        name = as_text(key)
        count = coerce_int(raw_count)
        if name is None or count is None or count <= 0:
            continue
        counts[name] = int(count)
    return dict(sorted(counts.items()))


def _quality_blockers_by_stage(diagnostics: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    by_stage: dict[str, Counter[str]] = {}
    for diagnostic in diagnostics:
        evidence = as_mapping(diagnostic.get("evidence"))
        waterfall = as_mapping(evidence.get("quality_waterfall"))
        for result in as_list(waterfall.get("results")):
            if not isinstance(result, Mapping):
                continue
            if str(result.get("status") or "").strip().lower() != "block":
                continue
            stage = as_text(result.get("stage"))
            if stage is None:
                continue
            stage_counts = by_stage.setdefault(stage, Counter())
            for reason in as_list(result.get("reason_codes")):
                reason_code = as_text(reason)
                if reason_code is not None:
                    stage_counts[reason_code] += 1
    return {stage: dict(counts.most_common(8)) for stage, counts in sorted(by_stage.items())}


def _quality_profile_from_diagnostics(diagnostics: list[dict[str, Any]]) -> str | None:
    for diagnostic in diagnostics:
        evidence = as_mapping(diagnostic.get("evidence"))
        waterfall = as_mapping(evidence.get("quality_waterfall"))
        profile_id = as_text(evidence.get("quality_profile_id") or waterfall.get("profile_id"))
        if profile_id is not None:
            return profile_id
    return None


def _quality_waterfall_state(
    *,
    summary: Mapping[str, Any],
    diagnostics: list[dict[str, Any]],
    selection_counts: Mapping[str, Any] | None,
    admission_counts: Mapping[str, Any] | None,
) -> dict[str, Any]:
    raw_stage_counts = as_mapping(summary.get("filter_stage_counts"))
    stage_counts = {stage: _int_count_map(counts) for stage, counts in raw_stage_counts.items()}
    stage_blockers = _quality_blockers_by_stage(diagnostics)
    stage_order = tuple(dict.fromkeys((*ENTRY_QUALITY_STAGE_ORDER, *stage_counts.keys(), *stage_blockers.keys())))
    stage_rows = []
    for stage in stage_order:
        counts = stage_counts.get(stage, {})
        blockers = stage_blockers.get(stage, {})
        stage_rows.append(
            {
                "stage": stage,
                "counts": counts,
                "total": sum(counts.values()),
                "top_blocker_reasons": blockers,
            }
        )

    top_blockers = _int_count_map(summary.get("top_quality_blockers"))
    if not top_blockers:
        combined = Counter[str]()
        for blockers in stage_blockers.values():
            combined.update(blockers)
        top_blockers = dict(combined.most_common(12))

    selected_counts = _int_count_map(selection_counts)
    admitted_counts = _int_count_map(admission_counts)
    profile_id = as_text(summary.get("quality_profile_id")) or _quality_profile_from_diagnostics(diagnostics)
    return {
        "profile_id": profile_id,
        "snapshot_count": coerce_int(summary.get("quality_snapshot_count")),
        "blocked_snapshot_count": coerce_int(summary.get("quality_blocked_snapshot_count")),
        "stage_counts": stage_counts,
        "stage_rows": stage_rows,
        "top_blocker_reasons": top_blockers,
        "top_watch_reasons": _int_count_map(summary.get("top_quality_watch_reasons")),
        "selection": {
            "decision_state_counts": selected_counts,
            "total": sum(selected_counts.values()),
        },
        "admission": {
            "admission_state_counts": admitted_counts,
            "total": sum(admitted_counts.values()),
        },
    }


def _candidate_run_payload(
    row: CandidateRunModel,
    *,
    diagnostics: list[dict[str, Any]] | None = None,
    selection_counts: Mapping[str, Any] | None = None,
    admission_counts: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
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
        "generated_at": utc_iso(row.generated_at),
        "completed_at": utc_iso(row.completed_at),
        "symbol_count": row.symbol_count,
        "candidate_count": row.candidate_count,
        "summary": dict(row.summary_json or {}),
        "diagnostics": list(diagnostics or []),
        "selection_counts": _int_count_map(selection_counts),
        "admission_counts": _int_count_map(admission_counts),
        "created_at": utc_iso(row.created_at),
        "updated_at": utc_iso(row.updated_at),
    }


def _market_date_window(market_date: str) -> tuple[datetime, datetime]:
    start = datetime.combine(parse_date(market_date), datetime.min.time(), tzinfo=UTC)
    return start, start + timedelta(days=1)


def _latest_flow_facts(
    *,
    storage: Any,
    market_date: str,
    ticker_source_ids: set[str],
    strategy_ids: set[str],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    if not storage.engine_facts.schema_ready():
        return {}, {}
    start, end = _market_date_window(market_date)
    latest_sources: dict[str, TickerSourceRunModel] = {}
    latest_candidates: dict[str, CandidateRunModel] = {}
    with storage.engine_facts.session_factory() as session:
        for ticker_source_id in sorted(ticker_source_ids):
            row = session.scalars(
                select(TickerSourceRunModel)
                .where(TickerSourceRunModel.ticker_source_id == ticker_source_id)
                .where(TickerSourceRunModel.generated_at >= start)
                .where(TickerSourceRunModel.generated_at < end)
                .order_by(TickerSourceRunModel.generated_at.desc(), TickerSourceRunModel.ticker_source_run_id.asc())
                .limit(1)
            ).first()
            if row is not None:
                latest_sources[ticker_source_id] = row
        for strategy_id in sorted(strategy_ids):
            row = session.scalars(
                select(CandidateRunModel)
                .where(CandidateRunModel.trading_strategy_id == strategy_id)
                .where(CandidateRunModel.routine == "entry")
                .where(CandidateRunModel.generated_at >= start)
                .where(CandidateRunModel.generated_at < end)
                .order_by(CandidateRunModel.generated_at.desc(), CandidateRunModel.candidate_run_id.asc())
                .limit(1)
            ).first()
            if row is not None:
                latest_candidates[strategy_id] = row

        ticker_source_run_ids = [row.ticker_source_run_id for row in latest_sources.values()]
        candidate_run_ids = [row.candidate_run_id for row in latest_candidates.values()]
        symbols_by_ticker_source_run: dict[str, list[str]] = {ticker_source_run_id: [] for ticker_source_run_id in ticker_source_run_ids}
        diagnostics_by_candidate_run: dict[str, list[dict[str, Any]]] = {candidate_run_id: [] for candidate_run_id in candidate_run_ids}
        selection_counts_by_candidate_run: dict[str, dict[str, int]] = {candidate_run_id: {} for candidate_run_id in candidate_run_ids}
        admission_counts_by_candidate_run: dict[str, dict[str, int]] = {candidate_run_id: {} for candidate_run_id in candidate_run_ids}
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
        if candidate_run_ids:
            diagnostic_rows = session.scalars(
                select(CandidateSymbolDiagnosticModel)
                .where(CandidateSymbolDiagnosticModel.candidate_run_id.in_(candidate_run_ids))
                .order_by(
                    CandidateSymbolDiagnosticModel.candidate_run_id.asc(),
                    CandidateSymbolDiagnosticModel.returned_candidate_count.desc(),
                    CandidateSymbolDiagnosticModel.postprocess_candidate_count.desc(),
                    CandidateSymbolDiagnosticModel.raw_candidate_count.desc(),
                    CandidateSymbolDiagnosticModel.underlying_symbol.asc(),
                )
            ).all()
            for diagnostic in diagnostic_rows:
                rows = diagnostics_by_candidate_run.setdefault(diagnostic.candidate_run_id, [])
                if len(rows) < SOURCE_SYMBOL_LIMIT:
                    rows.append(_candidate_symbol_diagnostic_payload(diagnostic))
            candidate_run_ref = func.coalesce(TradeCandidateModel.candidate_run_id, TradeSignalModel.source_id)
            for candidate_run_id, decision_state, count in session.execute(
                select(candidate_run_ref, TradeDecisionModel.decision_state, func.count())
                .join(TradeSignalModel, TradeDecisionModel.trade_signal_id == TradeSignalModel.trade_signal_id)
                .outerjoin(TradeCandidateModel, TradeSignalModel.trade_candidate_id == TradeCandidateModel.trade_candidate_id)
                .where(candidate_run_ref.in_(candidate_run_ids))
                .group_by(candidate_run_ref, TradeDecisionModel.decision_state)
            ):
                counts = selection_counts_by_candidate_run.setdefault(str(candidate_run_id), {})
                counts[str(decision_state or "unknown")] = int(count or 0)
            for candidate_run_id, admission_state, count in session.execute(
                select(candidate_run_ref, TradeAdmissionModel.admission_state, func.count())
                .join(TradeSignalModel, TradeAdmissionModel.trade_signal_id == TradeSignalModel.trade_signal_id)
                .outerjoin(TradeCandidateModel, TradeSignalModel.trade_candidate_id == TradeCandidateModel.trade_candidate_id)
                .where(candidate_run_ref.in_(candidate_run_ids))
                .group_by(candidate_run_ref, TradeAdmissionModel.admission_state)
            ):
                counts = admission_counts_by_candidate_run.setdefault(str(candidate_run_id), {})
                counts[str(admission_state or "unknown")] = int(count or 0)

    return (
        {
            ticker_source_id: _ticker_source_run_payload(row, symbols=symbols_by_ticker_source_run.get(row.ticker_source_run_id, []))
            for ticker_source_id, row in latest_sources.items()
        },
        {
            strategy_id: _candidate_run_payload(
                row,
                diagnostics=diagnostics_by_candidate_run.get(row.candidate_run_id, []),
                selection_counts=selection_counts_by_candidate_run.get(row.candidate_run_id, {}),
                admission_counts=admission_counts_by_candidate_run.get(row.candidate_run_id, {}),
            )
            for strategy_id, row in latest_candidates.items()
        },
    )


def _portfolio_admission_state(row: TradeAdmissionModel) -> dict[str, Any]:
    evidence = as_mapping(row.evidence_json)
    portfolio_admission = as_mapping(evidence.get("portfolio_admission"))
    allocation_plan = as_mapping(portfolio_admission.get("allocation_plan")) or as_mapping(
        as_mapping(portfolio_admission.get("evidence")).get("allocation_plan")
    )
    allocation_decision = as_mapping(as_mapping(portfolio_admission.get("evidence")).get("allocation_decision")) or as_mapping(
        allocation_plan.get("current_decision")
    )
    status = as_text(evidence.get("portfolio_admission_status")) or as_text(portfolio_admission.get("status")) or "not_evaluated"
    reason = as_text(evidence.get("portfolio_admission_reason")) or as_text(portfolio_admission.get("reason"))
    return {
        "status": status,
        "reason": reason,
        "message": as_text(portfolio_admission.get("message")),
        "latest_admission_decision_id": row.admission_decision_id,
        "admission_state": row.admission_state,
        "decided_at": utc_iso(row.decided_at),
        "policy": as_mapping(portfolio_admission.get("policy")),
        "metrics": as_mapping(portfolio_admission.get("metrics")),
        "allocation_plan": allocation_plan,
        "allocation_decision": allocation_decision,
        "blockers": as_list(portfolio_admission.get("blockers")),
        "reason_codes": as_list(portfolio_admission.get("reason_codes")),
    }


def _protection_admission_state(row: TradeAdmissionModel) -> dict[str, Any]:
    evidence = as_mapping(row.evidence_json)
    protection_admission = as_mapping(evidence.get("protection_admission"))
    status = as_text(evidence.get("protection_admission_status")) or as_text(protection_admission.get("status")) or "not_evaluated"
    reason = as_text(evidence.get("protection_admission_reason")) or as_text(protection_admission.get("reason"))
    return {
        "status": status,
        "reason": reason,
        "message": as_text(protection_admission.get("message")),
        "latest_admission_decision_id": row.admission_decision_id,
        "admission_state": row.admission_state,
        "decided_at": utc_iso(row.decided_at),
        "policy": as_mapping(protection_admission.get("policy")),
        "metrics": as_mapping(protection_admission.get("metrics")),
        "blockers": as_list(protection_admission.get("blockers")),
        "reason_codes": as_list(protection_admission.get("reason_codes")),
    }


def _latest_entry_admission_states(
    *,
    storage: Any,
    market_date: str,
    strategy_ids: set[str],
    state_builder: Any,
) -> dict[str, dict[str, Any]]:
    if not strategy_ids or not storage.engine_facts.schema_ready():
        return {}
    start, end = _market_date_window(market_date)
    latest: dict[str, dict[str, Any]] = {}
    with storage.engine_facts.session_factory() as session:
        rows = session.execute(
            select(TradeSignalModel.trading_strategy_id, TradeAdmissionModel)
            .join(TradeSignalModel, TradeAdmissionModel.trade_signal_id == TradeSignalModel.trade_signal_id)
            .where(TradeSignalModel.trading_strategy_id.in_(strategy_ids))
            .where(TradeAdmissionModel.admission_kind == "entry_open")
            .where(TradeAdmissionModel.decided_at >= start)
            .where(TradeAdmissionModel.decided_at < end)
            .order_by(
                TradeAdmissionModel.decided_at.desc(),
                TradeAdmissionModel.admission_decision_id.asc(),
            )
            .limit(500)
        ).all()
    for strategy_id, row in rows:
        key = str(strategy_id)
        if key in latest:
            continue
        state = state_builder(row)
        if state.get("status") == "not_evaluated" and not state.get("reason"):
            continue
        latest[key] = state
    return latest


def _latest_portfolio_admissions(
    *,
    storage: Any,
    market_date: str,
    strategy_ids: set[str],
) -> dict[str, dict[str, Any]]:
    return _latest_entry_admission_states(
        storage=storage,
        market_date=market_date,
        strategy_ids=strategy_ids,
        state_builder=_portfolio_admission_state,
    )


def _latest_protection_admissions(
    *,
    storage: Any,
    market_date: str,
    strategy_ids: set[str],
) -> dict[str, dict[str, Any]]:
    return _latest_entry_admission_states(
        storage=storage,
        market_date=market_date,
        strategy_ids=strategy_ids,
        state_builder=_protection_admission_state,
    )


def _admission_flow_status(state: Mapping[str, Any]) -> str:
    status = as_text(as_mapping(state).get("status"))
    if status in {"blocked", "unknown"}:
        return status
    return "healthy"


def _source_state(
    *,
    ticker_source_run: Mapping[str, Any] | None,
    source_kind: str,
    configured_symbols: tuple[str, ...] | list[str],
    max_age_seconds: int | None,
    market_open: bool,
    now: datetime,
) -> dict[str, Any]:
    normalized_source_kind = str(source_kind or "").strip().lower()
    configured = _normalized_symbols(configured_symbols)
    if normalized_source_kind == "static":
        symbol_count = len(configured)
        return {
            "status": "healthy" if symbol_count > 0 else ("degraded" if market_open else "idle"),
            "raw_status": "configured" if symbol_count > 0 else "empty",
            "source_kind": "static",
            "source_basis": "configured_universe",
            "source_evidence_state": "static_symbols_configured" if symbol_count > 0 else "no_source_symbols",
            "age_seconds": None,
            "max_age_seconds": None,
            "stale": False,
            "symbol_count": symbol_count,
            "symbols": configured[:SOURCE_SYMBOL_LIMIT],
            "latest_run": None,
            "reason": None if symbol_count > 0 else "source_empty",
        }

    if ticker_source_run is None:
        return {
            "status": "degraded" if market_open and normalized_source_kind == "dynamic" else "idle",
            "raw_status": "missing",
            "source_kind": normalized_source_kind or source_kind,
            "source_basis": "ticker_source_run",
            "source_evidence_state": "no_recent_source_run",
            "age_seconds": None,
            "max_age_seconds": max_age_seconds,
            "stale": bool(market_open and normalized_source_kind == "dynamic"),
            "symbol_count": 0,
            "symbols": [],
            "latest_run": None,
            "reason": "no_recent_source_run",
        }
    raw_status = str(ticker_source_run.get("status") or "unknown")
    age_seconds = _age_seconds(ticker_source_run.get("generated_at") or ticker_source_run.get("completed_at"), now=now)
    stale = bool(market_open and max_age_seconds is not None and age_seconds is not None and age_seconds > max_age_seconds)
    status = "healthy" if raw_status in {"ready", "fallback", "completed", "ok"} else "degraded"
    if stale and status == "healthy":
        status = "degraded"
    symbols = _symbols_from_ticker_source_run(ticker_source_run)
    symbol_count = coerce_int(ticker_source_run.get("selected_count")) or len(symbols)
    empty = status == "healthy" and symbol_count == 0
    source_evidence_state = "source_symbols_available" if symbol_count > 0 else "no_source_symbols"
    if stale:
        source_evidence_state = "source_stale"
    return {
        "status": status,
        "raw_status": raw_status,
        "source_kind": normalized_source_kind or source_kind,
        "source_basis": "ticker_source_run",
        "source_evidence_state": source_evidence_state,
        "age_seconds": age_seconds,
        "max_age_seconds": max_age_seconds,
        "stale": stale,
        "symbol_count": symbol_count,
        "symbols": symbols[:25],
        "latest_run": dict(ticker_source_run),
        "reason": "source_stale" if stale else "source_empty" if empty else (None if status == "healthy" else "source_degraded"),
    }


def _candidate_state(
    *,
    candidate_run: Mapping[str, Any] | None,
    source_state: Mapping[str, Any] | None,
    cadence_minutes: int | None,
    market_open: bool,
    now: datetime,
) -> dict[str, Any]:
    max_age_seconds = None if cadence_minutes is None else max(cadence_minutes * 60 * 2, 300)
    if candidate_run is None:
        source_status = str((source_state or {}).get("status") or "unknown")
        source_symbol_count = coerce_int((source_state or {}).get("symbol_count"))
        if market_open and source_status == "healthy" and source_symbol_count == 0:
            return {
                "status": "healthy",
                "raw_status": "source_empty",
                "age_seconds": None,
                "max_age_seconds": max_age_seconds,
                "symbol_count": 0,
                "candidate_count": 0,
                "diagnostic_status": "no_source_symbols",
                "symbol_status_counts": {},
                "top_rejection_counts": {},
                "diagnostics": [],
                "latest_run": None,
                "reason": "source_has_no_symbols",
            }
        return {
            "status": "degraded" if market_open else "idle",
            "age_seconds": None,
            "max_age_seconds": max_age_seconds,
            "symbol_count": 0,
            "candidate_count": 0,
            "latest_run": None,
            "reason": "candidate_run_missing",
        }
    raw_status = str(candidate_run.get("status") or "unknown")
    age_seconds = _age_seconds(candidate_run.get("generated_at") or candidate_run.get("completed_at"), now=now)
    stale = bool(market_open and max_age_seconds is not None and age_seconds is not None and age_seconds > max_age_seconds)
    status = "healthy" if raw_status in {"completed", "ready", "ok"} else "degraded"
    if stale and status == "healthy":
        status = "degraded"
    candidate_count = coerce_int(candidate_run.get("candidate_count")) or 0
    summary = as_mapping(candidate_run.get("summary"))
    diagnostics = [dict(row) for row in as_list(candidate_run.get("diagnostics")) if isinstance(row, Mapping)]
    quality_waterfall = _quality_waterfall_state(
        summary=summary,
        diagnostics=diagnostics,
        selection_counts=as_mapping(candidate_run.get("selection_counts")),
        admission_counts=as_mapping(candidate_run.get("admission_counts")),
    )
    market_context = _market_context_reference_from_summary(summary)
    regime_fit = _market_context_regime_fit(diagnostics)
    if regime_fit:
        market_context = {**market_context, "regime_fit": regime_fit}
    return {
        "status": status,
        "raw_status": raw_status,
        "age_seconds": age_seconds,
        "max_age_seconds": max_age_seconds,
        "stale": stale,
        "symbol_count": coerce_int(candidate_run.get("symbol_count")) or 0,
        "candidate_count": candidate_count,
        "diagnostic_status": summary.get("diagnostic_status"),
        "symbol_status_counts": as_mapping(summary.get("symbol_status_counts")),
        "top_rejection_counts": as_mapping(summary.get("top_rejection_counts")),
        "quality_profile_id": quality_waterfall.get("profile_id"),
        "filter_stage_counts": quality_waterfall.get("stage_counts"),
        "top_quality_blockers": quality_waterfall.get("top_blocker_reasons"),
        "quality_waterfall": quality_waterfall,
        "market_context": market_context,
        "diagnostics": diagnostics,
        "latest_run": dict(candidate_run),
        "reason": "candidate_run_stale" if stale else ("no_candidates" if candidate_count == 0 else None),
    }


def _join_labels(labels: list[str]) -> str:
    if not labels:
        return ""
    if len(labels) == 1:
        return labels[0]
    if len(labels) == 2:
        return f"{labels[0]} and {labels[1]}"
    return f"{', '.join(labels[:-1])}, and {labels[-1]}"


def _reason_matches_group(reason: str, prefixes: tuple[str, ...], exact: tuple[str, ...]) -> bool:
    return reason in exact or any(reason.startswith(prefix) for prefix in prefixes)


def _expected_move_coverage(candidate_state: Mapping[str, Any]) -> dict[str, int]:
    counts: list[int] = []
    for diagnostic in as_list(candidate_state.get("diagnostics")):
        if not isinstance(diagnostic, Mapping):
            continue
        market_data = as_mapping(diagnostic.get("market_data"))
        count = coerce_int(market_data.get("expected_move_count") or diagnostic.get("expected_move_count"))
        if count is not None:
            counts.append(int(count))
    return {
        "diagnostic_count": len(counts),
        "positive_symbol_count": sum(1 for count in counts if count > 0),
        "zero_symbol_count": sum(1 for count in counts if count <= 0),
        "expected_move_count": sum(max(count, 0) for count in counts),
    }


def _entry_blocker_counts(candidate_state: Mapping[str, Any]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for source_key in ("top_quality_blockers", "top_rejection_counts"):
        for reason, raw_count in as_mapping(candidate_state.get(source_key)).items():
            reason_text = str(reason or "").strip()
            count = coerce_int(raw_count) or 0
            if reason_text and count > 0:
                counts[reason_text] += count

    coverage = _expected_move_coverage(candidate_state)
    if coverage["diagnostic_count"] > 0 and coverage["positive_symbol_count"] > 0 and coverage["zero_symbol_count"] == 0:
        partial_count = 0
        for reason in ("no_expected_move", "target_dte_expected_move_missing"):
            partial_count += counts.pop(reason, 0)
        if partial_count > 0:
            counts["partial_expected_move_coverage_gap"] += partial_count
    return counts


def _entry_blocker_groups(candidate_state: Mapping[str, Any]) -> list[dict[str, Any]]:
    counts = _entry_blocker_counts(candidate_state)

    groups: list[dict[str, Any]] = []
    matched_reasons: set[str] = set()
    for group_id, label, prefixes, exact in NO_ENTRY_REASON_GROUPS:
        reasons = {reason: count for reason, count in counts.items() if _reason_matches_group(reason, prefixes, exact)}
        if not reasons:
            continue
        matched_reasons.update(reasons)
        groups.append(
            {
                "group": group_id,
                "label": label,
                "count": sum(reasons.values()),
                "reason_codes": dict(sorted(reasons.items(), key=lambda item: (-item[1], item[0]))),
            }
        )

    other_reasons = {reason: count for reason, count in counts.items() if reason not in matched_reasons}
    if other_reasons:
        groups.append(
            {
                "group": "other",
                "label": "other policy filters",
                "count": sum(other_reasons.values()),
                "reason_codes": dict(sorted(other_reasons.items(), key=lambda item: (-item[1], item[0]))),
            }
        )

    groups.sort(key=lambda item: (-int(item.get("count") or 0), str(item.get("group") or "")))
    return groups


def _entry_posture_state(
    *,
    source_state: Mapping[str, Any],
    candidate_state: Mapping[str, Any],
    market_open: bool,
    entry_due: bool,
) -> dict[str, Any]:
    source_status = str(source_state.get("status") or "unknown")
    candidate_status = str(candidate_state.get("status") or "unknown")
    source_symbol_count = coerce_int(source_state.get("symbol_count")) or 0
    candidate_count = coerce_int(candidate_state.get("candidate_count")) or 0
    blocker_groups = _entry_blocker_groups(candidate_state)

    if candidate_status in {"degraded", "blocked", "halted"}:
        return {
            "status": candidate_status,
            "state": "entry_evidence_needs_attention",
            "message": "Entry evidence is stale, missing, or degraded.",
            "healthy_flat": False,
            "entry_due": entry_due,
            "primary_blocker_group": None,
            "blocker_groups": blocker_groups,
            "reason": candidate_state.get("reason"),
        }
    if source_status in {"degraded", "blocked", "halted"}:
        return {
            "status": source_status,
            "state": "source_needs_attention",
            "message": "Ticker source evidence is stale, missing, or degraded.",
            "healthy_flat": False,
            "entry_due": entry_due,
            "primary_blocker_group": None,
            "blocker_groups": blocker_groups,
            "reason": source_state.get("reason"),
        }
    if not market_open:
        return {
            "status": "idle",
            "state": "market_closed",
            "message": "Market is closed; entry evaluation is idle.",
            "healthy_flat": False,
            "entry_due": entry_due,
            "primary_blocker_group": None,
            "blocker_groups": blocker_groups,
            "reason": "market_closed",
        }
    if candidate_count > 0:
        return {
            "status": "healthy",
            "state": "candidates_available",
            "message": f"{candidate_count} entry candidate(s) are available for selection and admission.",
            "healthy_flat": False,
            "entry_due": entry_due,
            "primary_blocker_group": None,
            "blocker_groups": blocker_groups,
            "reason": None,
        }
    if source_symbol_count == 0:
        return {
            "status": "healthy",
            "state": "flat_no_source_symbols",
            "message": "No entries: the latest source run retained no symbols.",
            "healthy_flat": True,
            "entry_due": entry_due,
            "primary_blocker_group": None,
            "blocker_groups": blocker_groups,
            "reason": candidate_state.get("reason") or source_state.get("reason"),
        }

    labels = [str(group.get("label")) for group in blocker_groups[:3] if group.get("label")]
    message = "No entries: latest run produced no candidates."
    if labels:
        message = f"No entries: {_join_labels(labels)} blocked the latest run."
    return {
        "status": "healthy",
        "state": "flat_by_policy",
        "message": message,
        "healthy_flat": True,
        "entry_due": entry_due,
        "primary_blocker_group": None if not blocker_groups else blocker_groups[0].get("group"),
        "blocker_groups": blocker_groups[:8],
        "reason": candidate_state.get("reason") or "no_candidates",
    }


def _top_reason_codes(group: Mapping[str, Any] | None, *, limit: int = 3) -> dict[str, int]:
    reason_counts = as_mapping(None if group is None else group.get("reason_codes"))
    ranked = sorted(
        (
            (str(reason), coerce_int(count) or 0)
            for reason, count in reason_counts.items()
            if str(reason or "").strip() and (coerce_int(count) or 0) > 0
        ),
        key=lambda item: (-item[1], item[0]),
    )
    return dict(ranked[:limit])


def _admission_no_entry_reason(flow: Mapping[str, Any]) -> tuple[str | None, str | None]:
    for key in ("protection_admission", "portfolio_admission"):
        admission = as_mapping(flow.get(key))
        status = as_text(admission.get("status"))
        if status in {"blocked", "unknown"}:
            return "admission", as_text(admission.get("reason")) or status
    return None, None


def _strategy_no_entry_category(
    *,
    flow: Mapping[str, Any],
    entry_posture: Mapping[str, Any],
    source_state: Mapping[str, Any],
    candidate_state: Mapping[str, Any],
    top_group: Mapping[str, Any] | None,
) -> tuple[str, str | None]:
    admission_category, admission_reason = _admission_no_entry_reason(flow)
    if admission_category is not None:
        return admission_category, admission_reason

    state = as_text(entry_posture.get("state"))
    if state == "market_closed":
        return "market", "market_closed"
    if state in {"source_needs_attention", "flat_no_source_symbols"}:
        return "source", as_text(source_state.get("reason")) or state
    if state == "entry_evidence_needs_attention":
        return "data_quality", as_text(candidate_state.get("reason")) or state
    if state == "candidates_available":
        return "selection_ready", None

    group = as_text(None if top_group is None else top_group.get("group"))
    if group is not None:
        return NO_ENTRY_GROUP_CATEGORIES.get(group, "policy"), group
    return "policy", as_text(candidate_state.get("reason")) or as_text(entry_posture.get("reason")) or "no_candidates"


def _strategy_no_entry_summary(flows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for flow in flows:
        entry_posture = as_mapping(flow.get("entry_posture"))
        source_state = as_mapping(flow.get("source_state"))
        candidate_state = as_mapping(flow.get("candidate_state"))
        market_context = as_mapping(flow.get("market_context") or candidate_state.get("market_context"))
        blocker_groups = [as_mapping(group) for group in as_list(entry_posture.get("blocker_groups")) if isinstance(group, Mapping)]
        top_group = blocker_groups[0] if blocker_groups else None
        category, reason = _strategy_no_entry_category(
            flow=flow,
            entry_posture=entry_posture,
            source_state=source_state,
            candidate_state=candidate_state,
            top_group=top_group,
        )
        rows.append(
            {
                "trading_strategy_id": flow.get("trading_strategy_id"),
                "trade_structure": flow.get("trade_structure"),
                "state": entry_posture.get("state"),
                "status": entry_posture.get("status"),
                "category": category,
                "reason": reason,
                "message": entry_posture.get("message"),
                "top_blocker_group": None if top_group is None else top_group.get("group"),
                "top_blocker_label": None if top_group is None else top_group.get("label"),
                "top_reason_codes": _top_reason_codes(top_group),
                "source_status": source_state.get("status"),
                "source_reason": source_state.get("reason"),
                "candidate_status": candidate_state.get("status"),
                "candidate_reason": candidate_state.get("reason"),
                "market_context_snapshot_id": market_context.get("market_context_snapshot_id"),
                "market_context_regime_label": market_context.get("regime_label"),
                "market_context_risk_posture": market_context.get("risk_posture"),
                "market_context_fit": market_context.get("regime_fit"),
            }
        )
    rows.sort(key=lambda row: (str(row.get("category") or ""), str(row.get("trading_strategy_id") or "")))
    return rows


def _flow_position_summary(
    *,
    execution_store: Any,
    trading_strategy_id: str,
    market_date: str,
) -> dict[str, Any]:
    if not execution_store.portfolio_schema_ready():
        return {
            "status": "blocked",
            "position_count": 0,
            "open_position_count": 0,
            "closed_position_count": 0,
            "latest_exit_reason": None,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "net_pnl": 0.0,
        }
    day_positions = [
        dict(row)
        for row in execution_store.list_positions(
            trading_strategy_id=trading_strategy_id,
            market_date=market_date,
            limit=500,
        )
    ]
    open_positions = [
        dict(row)
        for row in execution_store.list_positions(
            trading_strategy_id=trading_strategy_id,
            statuses=OPEN_POSITION_STATUSES,
            limit=500,
        )
    ]
    closed_positions = [row for row in day_positions if str(row.get("status") or "") == "closed"]
    closed_positions.sort(key=lambda row: str(row.get("closed_at") or ""), reverse=True)
    realized = money_sum_float(coerce_float(row.get("realized_pnl")) for row in day_positions)
    unrealized = money_sum_float(coerce_float(row.get("unrealized_pnl")) for row in open_positions)
    return {
        "status": "healthy",
        "position_count": len(day_positions),
        "open_position_count": len(open_positions),
        "closed_position_count": len(closed_positions),
        "latest_exit_reason": None if not closed_positions else as_text(closed_positions[0].get("last_exit_reason")),
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "net_pnl": money_sum_float([realized, unrealized]),
    }


def _flow_intent_summary(
    *,
    execution_store: Any,
    trading_strategy_id: str,
) -> dict[str, Any]:
    if not execution_store.intent_schema_ready():
        return {
            "status": "blocked",
            "active_intent_count": 0,
            "active_intent_state_counts": {},
        }
    active_intents = [
        dict(row)
        for row in execution_store.list_execution_intents(
            trading_strategy_id=trading_strategy_id,
            states=sorted(ACTIVE_INTENT_STATES),
            limit=500,
        )
    ]
    state_counts = Counter(str(row.get("state") or "unknown") for row in active_intents)
    return {
        "status": "healthy",
        "active_intent_count": len(active_intents),
        "active_intent_state_counts": dict(sorted(state_counts.items())),
        "active_intents": active_intents[:20],
    }


def _build_trading_flows(
    *,
    storage: Any,
    engine_ops: Mapping[str, Any],
    market_date: str,
    market_open: bool,
    broker_environment: str,
    broker_environment_source: str,
    now: datetime,
) -> list[dict[str, Any]]:
    del engine_ops
    strategies = [strategy for strategy in load_active_trading_strategies().values() if strategy.enabled]
    latest_sources, latest_candidates = _latest_flow_facts(
        storage=storage,
        market_date=market_date,
        ticker_source_ids={strategy.source.ref for strategy in strategies},
        strategy_ids={strategy.trading_strategy_id for strategy in strategies},
    )
    latest_portfolio_admissions = _latest_portfolio_admissions(
        storage=storage,
        market_date=market_date,
        strategy_ids={strategy.trading_strategy_id for strategy in strategies},
    )
    latest_protection_admissions = _latest_protection_admissions(
        storage=storage,
        market_date=market_date,
        strategy_ids={strategy.trading_strategy_id for strategy in strategies},
    )
    flows: list[dict[str, Any]] = []
    for strategy in strategies:
        latest_source = latest_sources.get(strategy.source.ref)
        latest_entry = latest_candidates.get(strategy.trading_strategy_id)
        entry_cadence_minutes = None if strategy.entry is None else strategy.entry.schedule.cadence_minutes
        entry_due = bool(strategy.entry is not None and strategy.entry.enabled and routine_should_run_now(strategy.entry, now=now))
        source_state = _source_state(
            ticker_source_run=latest_source,
            source_kind=strategy.source.kind,
            configured_symbols=strategy.symbols,
            max_age_seconds=strategy.source.max_age_seconds,
            market_open=market_open,
            now=now,
        )
        candidate_state = _candidate_state(
            candidate_run=latest_entry,
            source_state=source_state,
            cadence_minutes=entry_cadence_minutes,
            market_open=market_open and entry_due,
            now=now,
        )
        entry_posture = _entry_posture_state(
            source_state=source_state,
            candidate_state=candidate_state,
            market_open=market_open,
            entry_due=entry_due,
        )
        intent_summary = _flow_intent_summary(
            execution_store=storage.execution,
            trading_strategy_id=strategy.trading_strategy_id,
        )
        position_summary = _flow_position_summary(
            execution_store=storage.execution,
            trading_strategy_id=strategy.trading_strategy_id,
            market_date=market_date,
        )
        execution_contract = _strategy_execution_contract(
            strategy=strategy,
            broker_environment=broker_environment,
            broker_environment_source=broker_environment_source,
            now=now,
        )
        max_entries = strategy.risk_limits.max_new_entries_per_day
        used_entries = coerce_int(position_summary.get("position_count")) or 0
        remaining_entries = None if max_entries is None else max(max_entries - used_entries - int(intent_summary.get("active_intent_count") or 0), 0)
        portfolio_admission = latest_portfolio_admissions.get(
            strategy.trading_strategy_id,
            {
                "status": "not_evaluated",
                "reason": "no_entry_admission_today",
                "message": "No selected entry has reached portfolio admission today.",
            },
        )
        protection_admission = latest_protection_admissions.get(
            strategy.trading_strategy_id,
            {
                "status": "not_evaluated",
                "reason": "no_entry_admission_today",
                "message": "No selected entry has reached protection admission today.",
            },
        )
        flows.append(
            {
                "trading_strategy_id": strategy.trading_strategy_id,
                "name": strategy.name,
                "trade_structure": strategy.trade_structure,
                "enabled": strategy.enabled,
                "runtime": strategy.runtime.model_dump(exclude_none=True),
                "protection": strategy.protection.model_dump(exclude_none=True, by_alias=True),
                "execution": strategy.execution.model_dump(exclude_none=True),
                "execution_contract": execution_contract,
                "source": strategy.source.model_dump(exclude_none=True, by_alias=True),
                "entry": (
                    None
                    if strategy.entry is None
                    else {
                        "enabled": strategy.entry.enabled,
                        "schedule": strategy.entry.schedule.as_dict(),
                        "selection": strategy.entry.selection.model_dump(exclude_none=True),
                    }
                ),
                "management": (
                    None
                    if strategy.management is None
                    else {
                        "enabled": strategy.management.enabled,
                        "schedule": strategy.management.schedule.as_dict(),
                    }
                ),
                "risk_limits": strategy.risk_limits.dump_config(),
                "source_state": source_state,
                "candidate_state": candidate_state,
                "market_context": candidate_state.get("market_context"),
                "entry_posture": entry_posture,
                "intent_state": intent_summary,
                "position_state": position_summary,
                "protection_admission": protection_admission,
                "portfolio_admission": portfolio_admission,
                "capacity": {
                    "open_position_count": position_summary.get("open_position_count"),
                    "max_open_positions": strategy.risk_limits.max_open_positions,
                    "session_entry_count": used_entries,
                    "max_daily_entries": max_entries,
                    "remaining_daily_entries": remaining_entries,
                    "protection_admission": protection_admission,
                    "portfolio_admission": portfolio_admission,
                },
                "status": _combine_statuses(
                    str(source_state.get("status") or "unknown"),
                    str(candidate_state.get("status") or "unknown"),
                    _admission_flow_status(protection_admission),
                    str(intent_summary.get("status") or "unknown"),
                    str(position_summary.get("status") or "unknown"),
                    _execution_contract_status(execution_contract),
                ),
            }
        )
    return flows

def _project_flows(
    *,
    storage: Any,
    engine_ops: Mapping[str, Any],
    market_date: str,
    market_open: bool,
    broker_environment: str,
    broker_environment_source: str,
    now: datetime,
) -> _FlowProjection:
    trading_flows = _build_trading_flows(
        storage=storage,
        engine_ops=engine_ops,
        market_date=market_date,
        market_open=market_open,
        broker_environment=broker_environment,
        broker_environment_source=broker_environment_source,
        now=now,
    )
    degraded_flows = [flow for flow in trading_flows if str(flow.get("status") or "") in {"degraded", "blocked", "halted"}]
    attention: list[dict[str, str]] = []
    statuses: list[str] = []
    if degraded_flows:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="trading_flows_need_attention",
                message=f"{len(degraded_flows)} trading flow(s) are degraded or blocked.",
            )
        )
    return _FlowProjection(
        trading_flows=trading_flows,
        degraded_flows=degraded_flows,
        statuses=tuple(statuses),
        attention=attention,
    )
