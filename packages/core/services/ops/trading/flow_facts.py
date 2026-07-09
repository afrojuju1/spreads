from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select

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
    coerce_int,
    utc_iso,
)

from core.services.ops.trading.account import _age_seconds
from core.services.ops.trading.market_context import _market_context_reference_from_summary, _market_context_regime_fit
from core.services.ops.trading.models import ENTRY_QUALITY_STAGE_ORDER, SOURCE_SYMBOL_LIMIT


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
