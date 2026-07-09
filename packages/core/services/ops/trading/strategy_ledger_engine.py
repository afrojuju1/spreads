from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from typing import Any

from sqlalchemy import func, select

from core.storage.engine_models import (
    CandidateRunModel,
    CandidateSymbolDiagnosticModel,
    TickerSourceObservationModel,
    TickerSourceRunModel,
    TradeCandidateModel,
    TradingFeatureSnapshotModel,
)
from core.storage.lifecycle_models import TradeAdmissionModel, TradeDecisionModel, TradeSignalModel
from core.value_coercion import as_list, as_mapping, as_text, coerce_float, coerce_int, utc_iso

from .strategy_ledger_common import (
    bump_count as _bump_count,
    newer_desc_asc as _newer_desc_asc,
    set_latest_activity as _set_latest_activity,
    sorted_counts as _sorted_counts,
    top_blockers as _top_blockers,
)

SOURCE_SYMBOL_LIMIT = 25
MARKET_CONTEXT_FILTER_ID = "market_context_regime_fit"
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


def _market_context_reference_from_summary(summary: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = as_mapping(summary)
    snapshot_id = as_text(payload.get("market_context_snapshot_id") or payload.get("snapshot_id"))
    regime_label = as_text(payload.get("market_context_regime_label") or payload.get("regime_label"))
    risk_posture = as_text(payload.get("market_context_risk_posture") or payload.get("risk_posture"))
    confidence = coerce_float(payload.get("market_context_confidence") or payload.get("confidence"))
    observed_at = payload.get("market_context_observed_at") or payload.get("observed_at")
    expires_at = payload.get("market_context_expires_at") or payload.get("expires_at")
    freshness = as_text(payload.get("market_context_freshness") or payload.get("freshness"))
    data_quality = as_text(payload.get("market_context_data_quality") or payload.get("data_quality"))
    if not any((snapshot_id, regime_label, risk_posture, confidence is not None, observed_at, expires_at, freshness, data_quality)):
        return {}
    return {
        "market_context_snapshot_id": snapshot_id,
        "observed_at": observed_at,
        "expires_at": expires_at,
        "scope": as_text(payload.get("market_context_scope") or payload.get("scope")),
        "regime_label": regime_label,
        "risk_posture": risk_posture,
        "confidence": confidence,
        "freshness": freshness,
        "data_quality": data_quality,
    }


def _market_context_reference_from_payload(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    market_context = as_mapping(payload)
    if not market_context:
        return {}
    regime = as_mapping(market_context.get("regime"))
    data_quality = as_mapping(market_context.get("data_quality"))
    return _market_context_reference_from_summary(
        {
            "market_context_snapshot_id": market_context.get("snapshot_id") or market_context.get("market_context_snapshot_id"),
            "market_context_observed_at": market_context.get("observed_at"),
            "market_context_expires_at": market_context.get("expires_at"),
            "market_context_scope": market_context.get("scope"),
            "market_context_regime_label": regime.get("regime_label"),
            "market_context_risk_posture": regime.get("risk_posture"),
            "market_context_confidence": regime.get("confidence"),
            "market_context_freshness": data_quality.get("freshness"),
            "market_context_data_quality": data_quality.get("state"),
        }
    )


def _record_market_context_link(
    *,
    section_payload: dict[str, Any],
    strategy_context: dict[str, Any],
    stage: str,
    context: Mapping[str, Any],
    latest: bool = False,
) -> None:
    context_ref = dict(context)
    if not context_ref:
        return
    snapshot_id = as_text(context_ref.get("market_context_snapshot_id"))
    if latest or not section_payload.get("latest_market_context"):
        section_payload["latest_market_context"] = context_ref
    if latest or not strategy_context.get("latest"):
        strategy_context["latest"] = context_ref
    if snapshot_id is not None:
        _bump_count(section_payload["market_context_snapshot_ids"], snapshot_id)
        _bump_count(strategy_context[f"{stage}_snapshot_ids"], snapshot_id)


def _add_market_context_regime_fit(counter: Counter[str], status_counter: Counter[str], waterfall: Any) -> None:
    for result in as_list(as_mapping(waterfall).get("results")):
        if not isinstance(result, Mapping):
            continue
        if as_text(result.get("filter_id")) != MARKET_CONTEXT_FILTER_ID:
            continue
        status_counter[as_text(result.get("status")) or "unknown"] += 1
        for reason in as_list(result.get("reason_codes")):
            reason_code = as_text(reason)
            if reason_code is not None:
                counter[reason_code] += 1


def _first_non_empty_mapping(*values: Any) -> Mapping[str, Any]:
    for value in values:
        mapping = as_mapping(value)
        if mapping:
            return mapping
    return {}


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
            "source_evidence_state": (
                "static_symbols_configured" if strategy.source.kind == "static" and configured_source_symbols else "not_observed"
            ),
            "latest_ticker_source_run_id": None,
            "latest_generated_at": None,
        },
        "candidates": {
            "candidate_run_count": 0,
            "candidate_count": 0,
            "trade_candidate_count": 0,
            "diagnostic_symbol_count": 0,
            "diagnostic_status_counts": {},
            "raw_candidate_count": 0,
            "postprocess_candidate_count": 0,
            "runtime_candidate_count": 0,
            "returned_candidate_count": 0,
            "candidate_productivity_state": "not_evaluated",
            "top_raw_rejection_counts": {},
            "data_quality_status_counts": {},
            "top_data_quality_reasons": {},
            "calendar_policy_status_counts": {},
            "top_calendar_policy_reasons": {},
            "ranking_policy_status_counts": {},
            "top_ranking_policy_blockers": {},
            "market_data_coverage": {},
            "feature_snapshot_count": 0,
            "feature_quality_status_counts": {},
            "market_data_quality_state_counts": {},
            "top_market_data_quality_reasons": {},
            "market_data_quality_component_state_counts": {},
            "latest_market_context": {},
            "market_context_snapshot_ids": {},
            "market_context_regime_fit_status_counts": {},
            "top_market_context_reasons": {},
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
            "latest_market_context": {},
            "market_context_snapshot_ids": {},
            "latest_trade_decision_id": None,
            "latest_decided_at": None,
        },
        "admissions": {
            "admission_count": 0,
            "admission_state_counts": {},
            "latest_market_context": {},
            "market_context_snapshot_ids": {},
            "latest_admission_decision_id": None,
            "latest_decided_at": None,
        },
        "market_context": {
            "latest": {},
            "candidate_snapshot_ids": {},
            "decision_snapshot_ids": {},
            "admission_snapshot_ids": {},
            "regime_fit_status_counts": {},
            "top_regime_fit_reasons": {},
        },
        "top_blocker_reasons": {},
        "latest_activity_at": None,
    }


def _finalize_state_counts(payload: dict[str, Any], key: str, count_key: str) -> None:
    state_counts = dict(sorted((str(state), int(count)) for state, count in as_mapping(payload[key].get(count_key)).items()))
    payload[key][count_key] = state_counts
    payload[key][key[:-1] + "_count"] = int(sum(state_counts.values()))


def _candidate_productivity_state(candidate_payload: Mapping[str, Any]) -> str:
    if coerce_int(candidate_payload.get("candidate_run_count")) in (None, 0):
        return "not_evaluated"
    candidate_count = int(coerce_int(candidate_payload.get("candidate_count")) or 0)
    trade_candidate_count = int(coerce_int(candidate_payload.get("trade_candidate_count")) or 0)
    returned_candidate_count = int(coerce_int(candidate_payload.get("returned_candidate_count")) or 0)
    if candidate_count > 0 or trade_candidate_count > 0 or returned_candidate_count > 0:
        return "candidates_available"

    raw_candidate_count = int(coerce_int(candidate_payload.get("raw_candidate_count")) or 0)
    postprocess_candidate_count = int(coerce_int(candidate_payload.get("postprocess_candidate_count")) or 0)
    runtime_candidate_count = int(coerce_int(candidate_payload.get("runtime_candidate_count")) or 0)
    if raw_candidate_count > 0:
        diagnostic_status_counts = as_mapping(candidate_payload.get("diagnostic_status_counts"))
        if int(coerce_int(diagnostic_status_counts.get("ranking_rejected")) or 0) > 0:
            return "ranking_policy_filtered"
        if int(coerce_int(diagnostic_status_counts.get("postprocess_rejected")) or 0) > 0 or postprocess_candidate_count == 0:
            return "postprocess_filtered"
        if runtime_candidate_count == 0:
            return "runtime_filtered"
        return "selection_filtered"

    diagnostic_symbol_count = int(coerce_int(candidate_payload.get("diagnostic_symbol_count")) or 0)
    if diagnostic_symbol_count <= 0:
        return "diagnostics_missing"
    diagnostic_status_counts = as_mapping(candidate_payload.get("diagnostic_status_counts"))
    data_unavailable_count = int(coerce_int(diagnostic_status_counts.get("data_unavailable")) or 0)
    no_raw_count = int(coerce_int(diagnostic_status_counts.get("no_raw_candidates")) or 0)
    if data_unavailable_count >= diagnostic_symbol_count:
        return "data_unavailable"
    if no_raw_count > 0 and data_unavailable_count > 0:
        return "mixed_data_and_no_raw_candidates"
    if no_raw_count > 0:
        return "no_raw_candidates"
    return "no_candidate_output"


def build_engine_strategy_ledgers(
    *,
    session: Any,
    strategies: Iterable[Any],
    market_day: date,
    start: datetime,
    end: datetime,
    feature_store_schema_ready: bool,
) -> dict[str, dict[str, Any]]:
    strategy_list = list(strategies)
    payloads = {strategy.trading_strategy_id: _empty_engine_strategy_ledger(strategy) for strategy in strategy_list}
    strategy_ids = list(payloads)
    if not strategy_ids:
        return payloads

    latest_activity: dict[str, datetime] = {}
    blockers_by_strategy = {strategy_id: Counter() for strategy_id in strategy_ids}
    candidate_diagnostic_counters = {
        strategy_id: {
            "raw_rejection_counts": Counter(),
            "data_quality_status_counts": Counter(),
            "data_quality_reasons": Counter(),
            "calendar_policy_status_counts": Counter(),
            "calendar_policy_reasons": Counter(),
            "ranking_policy_status_counts": Counter(),
            "ranking_policy_blockers": Counter(),
            "market_data_coverage": Counter(),
            "feature_quality_status_counts": Counter(),
            "market_data_quality_state_counts": Counter(),
            "market_data_quality_reasons": Counter(),
            "market_data_quality_component_state_counts": Counter(),
            "market_context_regime_fit_status_counts": Counter(),
            "market_context_regime_fit_reasons": Counter(),
        }
        for strategy_id in strategy_ids
    }
    candidate_context_by_run: dict[str, dict[str, Any]] = {}

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
                if source_payload["source_type"] == "static":
                    source_payload["source_evidence_state"] = (
                        "static_symbols_configured" if int(source_payload.get("latest_symbol_count") or 0) > 0 else "no_source_symbols"
                    )
                else:
                    source_payload["source_evidence_state"] = "no_recent_source_run"
                continue
            source_run_id, selected_count, generated_at = latest_source
            source_payload["latest_symbol_count"] = int(selected_count)
            source_payload["symbols"] = source_symbols_by_run.get(source_run_id) or source_payload["symbols"]
            source_payload["source_evidence_state"] = "source_symbols_available" if int(selected_count) > 0 else "no_source_symbols"
            source_payload["latest_ticker_source_run_id"] = source_run_id
            source_payload["latest_generated_at"] = utc_iso(generated_at)
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
        is_latest_candidate = _newer_desc_asc(
            generated_at,
            candidate_run_id,
            latest_candidate_at.get(strategy_key),
            candidate_payload.get("latest_candidate_run_id"),
        )
        if is_latest_candidate:
            latest_candidate_at[strategy_key] = generated_at
            candidate_payload["latest_candidate_run_id"] = run_id
            candidate_payload["latest_generated_at"] = utc_iso(generated_at)
        _set_latest_activity(latest_activity, strategy_key, generated_at)
        summary = as_mapping(summary_json)
        context_ref = _market_context_reference_from_summary(summary)
        if context_ref:
            candidate_context_by_run[run_id] = context_ref
            _record_market_context_link(
                section_payload=candidate_payload,
                strategy_context=payloads[strategy_key]["market_context"],
                stage="candidate",
                context=context_ref,
                latest=is_latest_candidate,
            )
        _add_count_mapping(blockers_by_strategy[strategy_key], summary.get("top_quality_blockers"))
        _add_count_mapping(blockers_by_strategy[strategy_key], summary.get("top_rejection_counts"))

    if candidate_run_strategy:
        diagnostic_rows = session.execute(
            select(
                CandidateSymbolDiagnosticModel.candidate_run_id,
                CandidateSymbolDiagnosticModel.diagnostic_status,
                CandidateSymbolDiagnosticModel.raw_candidate_count,
                CandidateSymbolDiagnosticModel.postprocess_candidate_count,
                CandidateSymbolDiagnosticModel.runtime_candidate_count,
                CandidateSymbolDiagnosticModel.returned_candidate_count,
                CandidateSymbolDiagnosticModel.expiration_count,
                CandidateSymbolDiagnosticModel.contract_count,
                CandidateSymbolDiagnosticModel.snapshot_count,
                CandidateSymbolDiagnosticModel.market_data_json,
                CandidateSymbolDiagnosticModel.rejection_counts_json,
                CandidateSymbolDiagnosticModel.ranking_gate_json,
                CandidateSymbolDiagnosticModel.evidence_json,
            ).where(CandidateSymbolDiagnosticModel.candidate_run_id.in_(list(candidate_run_strategy)))
        )
        for (
            candidate_run_id,
            diagnostic_status,
            raw_candidate_count,
            postprocess_candidate_count,
            runtime_candidate_count,
            returned_candidate_count,
            expiration_count,
            contract_count,
            snapshot_count,
            market_data_json,
            rejection_counts_json,
            ranking_gate_json,
            evidence_json,
        ) in diagnostic_rows:
            strategy_key = candidate_run_strategy.get(str(candidate_run_id))
            if strategy_key is None:
                continue
            candidate_payload = payloads[strategy_key]["candidates"]
            candidate_payload["diagnostic_symbol_count"] += 1
            _bump_count(candidate_payload["diagnostic_status_counts"], diagnostic_status)
            candidate_payload["raw_candidate_count"] += int(raw_candidate_count or 0)
            candidate_payload["postprocess_candidate_count"] += int(postprocess_candidate_count or 0)
            candidate_payload["runtime_candidate_count"] += int(runtime_candidate_count or 0)
            candidate_payload["returned_candidate_count"] += int(returned_candidate_count or 0)
            _add_count_mapping(blockers_by_strategy[strategy_key], rejection_counts_json)
            evidence = as_mapping(evidence_json)
            _add_quality_waterfall_reasons(blockers_by_strategy[strategy_key], evidence.get("quality_waterfall"))

            counters = candidate_diagnostic_counters[strategy_key]
            diagnostic_context = _market_context_reference_from_payload(evidence.get("market_context"))
            if diagnostic_context:
                candidate_context_by_run.setdefault(str(candidate_run_id), diagnostic_context)
                _record_market_context_link(
                    section_payload=candidate_payload,
                    strategy_context=payloads[strategy_key]["market_context"],
                    stage="candidate",
                    context=diagnostic_context,
                )
            _add_market_context_regime_fit(
                counters["market_context_regime_fit_reasons"],
                counters["market_context_regime_fit_status_counts"],
                evidence.get("quality_waterfall"),
            )
            rejection_counts = as_mapping(rejection_counts_json)
            ranking_gate = as_mapping(ranking_gate_json)
            replay_details = as_mapping(evidence.get("replay_details"))
            market_data = as_mapping(market_data_json)

            _add_count_mapping(counters["raw_rejection_counts"], rejection_counts.get("raw"))
            _add_count_mapping(counters["data_quality_status_counts"], replay_details.get("data_status_counts"))
            _add_count_mapping(
                counters["data_quality_reasons"],
                _first_non_empty_mapping(replay_details.get("data_reason_counts"), rejection_counts.get("data")),
            )
            _add_count_mapping(counters["calendar_policy_status_counts"], replay_details.get("calendar_status_counts"))
            _add_count_mapping(
                counters["calendar_policy_reasons"],
                _first_non_empty_mapping(replay_details.get("calendar_reason_counts"), rejection_counts.get("calendar")),
            )
            _add_count_mapping(
                counters["ranking_policy_status_counts"],
                _first_non_empty_mapping(ranking_gate.get("status_counts"), replay_details.get("ranking_policy_status_counts")),
            )
            _add_count_mapping(
                counters["ranking_policy_blockers"],
                _first_non_empty_mapping(
                    ranking_gate.get("blocker_counts"),
                    replay_details.get("ranking_policy_blocker_counts"),
                    rejection_counts.get("ranking_policy"),
                ),
            )

            coverage = counters["market_data_coverage"]
            expiration_total = int(expiration_count or 0)
            contract_total = int(contract_count or 0)
            snapshot_total = int(snapshot_count or 0)
            delta_snapshot_total = int(coerce_int(market_data.get("delta_snapshot_count")) or 0)
            expected_move_total = int(coerce_int(market_data.get("expected_move_count")) or 0)
            coverage["expiration_count"] += expiration_total
            coverage["contract_count"] += contract_total
            coverage["snapshot_count"] += snapshot_total
            coverage["delta_snapshot_count"] += delta_snapshot_total
            coverage["expected_move_count"] += expected_move_total
            if expiration_total > 0:
                coverage["symbols_with_expirations_count"] += 1
            if contract_total > 0:
                coverage["symbols_with_contracts_count"] += 1
            if snapshot_total > 0:
                coverage["symbols_with_snapshots_count"] += 1
            if delta_snapshot_total > 0:
                coverage["symbols_with_delta_snapshots_count"] += 1
            if expected_move_total > 0:
                coverage["symbols_with_expected_moves_count"] += 1

    if candidate_run_strategy and feature_store_schema_ready:
        feature_rows = session.execute(
            select(
                TradingFeatureSnapshotModel.candidate_run_id,
                TradingFeatureSnapshotModel.quality_status,
                TradingFeatureSnapshotModel.market_data_quality_state,
                TradingFeatureSnapshotModel.market_data_quality_reason,
                TradingFeatureSnapshotModel.market_data_quality_json,
            ).where(TradingFeatureSnapshotModel.candidate_run_id.in_(list(candidate_run_strategy)))
        )
        for candidate_run_id, quality_status, market_data_quality_state, market_data_quality_reason, market_data_quality_json in feature_rows:
            strategy_key = candidate_run_strategy.get(str(candidate_run_id))
            if strategy_key is None:
                continue
            candidate_payload = payloads[strategy_key]["candidates"]
            candidate_payload["feature_snapshot_count"] += 1
            counters = candidate_diagnostic_counters[strategy_key]
            counters["feature_quality_status_counts"][str(quality_status or "unknown")] += 1
            counters["market_data_quality_state_counts"][str(market_data_quality_state or "unknown")] += 1
            reason = as_text(market_data_quality_reason)
            if reason is not None:
                counters["market_data_quality_reasons"][reason] += 1
                if str(market_data_quality_state or "").strip().lower() == "block":
                    blockers_by_strategy[strategy_key][reason] += 1
            for component_name, component in as_mapping(as_mapping(market_data_quality_json).get("components")).items():
                component_state = as_text(as_mapping(component).get("state"))
                if component_state is not None:
                    counters["market_data_quality_component_state_counts"][f"{component_name}:{component_state}"] += 1

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
            signal_payload["latest_observed_at"] = utc_iso(observed_at)
        _set_latest_activity(latest_activity, strategy_key, observed_at)
        _add_reason_list(blockers_by_strategy[strategy_key], reason_codes_json)
        _add_reason_list(blockers_by_strategy[strategy_key], blockers_json)

    latest_decision_at: dict[str, datetime] = {}
    candidate_run_ref = func.coalesce(TradeCandidateModel.candidate_run_id, TradeSignalModel.source_id)
    decision_rows = session.execute(
        select(
            TradeDecisionModel.trading_strategy_id,
            TradeDecisionModel.decision_state,
            TradeDecisionModel.trade_decision_id,
            TradeDecisionModel.decided_at,
            TradeDecisionModel.reason_codes_json,
            TradeDecisionModel.blockers_json,
            TradeDecisionModel.evidence_json,
            candidate_run_ref,
        )
        .join(TradeSignalModel, TradeDecisionModel.trade_signal_id == TradeSignalModel.trade_signal_id)
        .outerjoin(TradeCandidateModel, TradeSignalModel.trade_candidate_id == TradeCandidateModel.trade_candidate_id)
        .where(TradeDecisionModel.trading_strategy_id.in_(strategy_ids))
        .where(TradeDecisionModel.routine == "entry")
        .where(TradeDecisionModel.decided_at >= start)
        .where(TradeDecisionModel.decided_at < end)
        .order_by(TradeDecisionModel.trading_strategy_id.asc(), TradeDecisionModel.decided_at.desc(), TradeDecisionModel.trade_decision_id.asc())
    )
    for (
        strategy_id,
        decision_state,
        trade_decision_id,
        decided_at,
        reason_codes_json,
        blockers_json,
        evidence_json,
        candidate_run_id,
    ) in decision_rows:
        strategy_key = str(strategy_id)
        decision_payload = payloads[strategy_key]["decisions"]
        _bump_count(decision_payload["decision_state_counts"], decision_state)
        is_latest_decision = _newer_desc_asc(
            decided_at,
            trade_decision_id,
            latest_decision_at.get(strategy_key),
            decision_payload.get("latest_trade_decision_id"),
        )
        if is_latest_decision:
            latest_decision_at[strategy_key] = decided_at
            decision_payload["latest_trade_decision_id"] = str(trade_decision_id)
            decision_payload["latest_decided_at"] = utc_iso(decided_at)
        _set_latest_activity(latest_activity, strategy_key, decided_at)
        context_ref = candidate_context_by_run.get(str(candidate_run_id)) or _market_context_reference_from_payload(
            as_mapping(evidence_json).get("market_context")
        )
        if context_ref:
            _record_market_context_link(
                section_payload=decision_payload,
                strategy_context=payloads[strategy_key]["market_context"],
                stage="decision",
                context=context_ref,
                latest=is_latest_decision,
            )
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
            TradeAdmissionModel.evidence_json,
            candidate_run_ref,
        )
        .join(TradeSignalModel, TradeAdmissionModel.trade_signal_id == TradeSignalModel.trade_signal_id)
        .outerjoin(TradeCandidateModel, TradeSignalModel.trade_candidate_id == TradeCandidateModel.trade_candidate_id)
        .where(TradeSignalModel.trading_strategy_id.in_(strategy_ids))
        .where(TradeAdmissionModel.session_date == market_day)
        .order_by(
            TradeSignalModel.trading_strategy_id.asc(),
            TradeAdmissionModel.decided_at.desc(),
            TradeAdmissionModel.admission_decision_id.asc(),
        )
    )
    for (
        strategy_id,
        admission_state,
        admission_decision_id,
        decided_at,
        reason_codes_json,
        blockers_json,
        evidence_json,
        candidate_run_id,
    ) in admission_rows:
        strategy_key = str(strategy_id)
        admission_payload = payloads[strategy_key]["admissions"]
        _bump_count(admission_payload["admission_state_counts"], admission_state)
        is_latest_admission = _newer_desc_asc(
            decided_at,
            admission_decision_id,
            latest_admission_at.get(strategy_key),
            admission_payload.get("latest_admission_decision_id"),
        )
        if is_latest_admission:
            latest_admission_at[strategy_key] = decided_at
            admission_payload["latest_admission_decision_id"] = str(admission_decision_id)
            admission_payload["latest_decided_at"] = utc_iso(decided_at)
        _set_latest_activity(latest_activity, strategy_key, decided_at)
        context_ref = candidate_context_by_run.get(str(candidate_run_id)) or _market_context_reference_from_payload(
            as_mapping(evidence_json).get("market_context")
        )
        if context_ref:
            _record_market_context_link(
                section_payload=admission_payload,
                strategy_context=payloads[strategy_key]["market_context"],
                stage="admission",
                context=context_ref,
                latest=is_latest_admission,
            )
        _add_reason_list(blockers_by_strategy[strategy_key], reason_codes_json)
        _add_reason_list(blockers_by_strategy[strategy_key], blockers_json)

    for strategy_id, payload in payloads.items():
        _finalize_state_counts(payload, "signals", "signal_state_counts")
        _finalize_state_counts(payload, "decisions", "decision_state_counts")
        _finalize_state_counts(payload, "admissions", "admission_state_counts")
        candidate_payload = payload["candidates"]
        candidate_payload["diagnostic_status_counts"] = dict(
            sorted((str(state), int(count)) for state, count in as_mapping(candidate_payload.get("diagnostic_status_counts")).items())
        )
        candidate_payload["candidate_productivity_state"] = _candidate_productivity_state(candidate_payload)
        diagnostic_counters = candidate_diagnostic_counters[strategy_id]
        candidate_payload["top_raw_rejection_counts"] = _top_blockers(diagnostic_counters["raw_rejection_counts"])
        candidate_payload["data_quality_status_counts"] = _sorted_counts(diagnostic_counters["data_quality_status_counts"])
        candidate_payload["top_data_quality_reasons"] = _top_blockers(diagnostic_counters["data_quality_reasons"])
        candidate_payload["calendar_policy_status_counts"] = _sorted_counts(diagnostic_counters["calendar_policy_status_counts"])
        candidate_payload["top_calendar_policy_reasons"] = _top_blockers(diagnostic_counters["calendar_policy_reasons"])
        candidate_payload["ranking_policy_status_counts"] = _sorted_counts(diagnostic_counters["ranking_policy_status_counts"])
        candidate_payload["top_ranking_policy_blockers"] = _top_blockers(diagnostic_counters["ranking_policy_blockers"])
        candidate_payload["market_data_coverage"] = _sorted_counts(diagnostic_counters["market_data_coverage"])
        candidate_payload["feature_quality_status_counts"] = _sorted_counts(diagnostic_counters["feature_quality_status_counts"])
        candidate_payload["market_data_quality_state_counts"] = _sorted_counts(diagnostic_counters["market_data_quality_state_counts"])
        candidate_payload["top_market_data_quality_reasons"] = _top_blockers(diagnostic_counters["market_data_quality_reasons"])
        candidate_payload["market_data_quality_component_state_counts"] = _sorted_counts(
            diagnostic_counters["market_data_quality_component_state_counts"]
        )
        candidate_payload["market_context_regime_fit_status_counts"] = _sorted_counts(diagnostic_counters["market_context_regime_fit_status_counts"])
        candidate_payload["top_market_context_reasons"] = _top_blockers(diagnostic_counters["market_context_regime_fit_reasons"])
        for section_name in ("candidates", "decisions", "admissions"):
            section_payload = payload[section_name]
            section_payload["market_context_snapshot_ids"] = dict(
                sorted(
                    (str(snapshot_id), int(count)) for snapshot_id, count in as_mapping(section_payload.get("market_context_snapshot_ids")).items()
                )
            )
        market_context_payload = payload["market_context"]
        market_context_payload["regime_fit_status_counts"] = _sorted_counts(diagnostic_counters["market_context_regime_fit_status_counts"])
        market_context_payload["top_regime_fit_reasons"] = _top_blockers(diagnostic_counters["market_context_regime_fit_reasons"])
        for key in ("candidate_snapshot_ids", "decision_snapshot_ids", "admission_snapshot_ids"):
            market_context_payload[key] = dict(
                sorted((str(snapshot_id), int(count)) for snapshot_id, count in as_mapping(market_context_payload.get(key)).items())
            )
        payload["decisions"]["selected_count"] = int(payload["decisions"]["decision_state_counts"].get("selected", 0))
        payload["top_blocker_reasons"] = _top_blockers(blockers_by_strategy[strategy_id])
        payload["latest_activity_at"] = utc_iso(latest_activity.get(strategy_id))
    return payloads
