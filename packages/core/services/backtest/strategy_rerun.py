from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from typing import Any

from sqlalchemy import select

from core.integrations.calendar_events.models import CalendarPolicyDecision
from core.integrations.calendar_events.resolver import CalendarEventResolver
from core.integrations.calendar_events.store import CalendarEventStore
from core.integrations.greeks import build_local_greeks_provider
from core.services.backtest.market_slices import (
    HistoricalMarketSliceProvider,
    HistoricalMarketSliceRequest,
    load_historical_trade_candidate_payloads,
)
from core.services.backtest.strategy_scope import load_backtest_strategy_scope, strategy_profile, strategy_variant_id
from core.services.backtest.windows import normalize_backtest_window
from core.services.entry_planner import plan_entry_selection
from core.services.market_dates import NEW_YORK
from core.services.strategy_builders import (
    build_entry_runtime_candidates_with_diagnostics_from_market_slices,
    build_symbol_market_slice_parameters,
    runtime_owner_key,
)
from core.services.trading_engine.candidate_identity import resolve_candidate_identity
from core.services.trading_engine.data import CandidateBuildRequest, CandidateBuildResult, ResolvedTickerSet
from core.services.trading_engine.data_runtime import (
    entry_candidate_build_parameters,
    entry_engine_label,
    entry_engine_strategy_run_id,
    entry_runtime_with_symbols,
    ticker_source_spec_from_strategy_source,
)
from core.services.trading_engine.entry_admission import build_selected_entry_admission_snapshot
from core.services.trading_engine.entry_selection import EntrySelectionEngine, candidate_result_summary
from core.services.trading_engine.entry_signals import (
    ENTRY_MONITOR_LIMIT,
    build_entry_signal_row_from_selection,
    candidate_payload,
    entry_selection_summary,
)
from core.services.trading_engine.facts import entry_trade_decision_id, entry_trade_signal_id
from core.services.trading_engine.kernel import EngineComponentRole, EngineRunRef
from core.services.trading_strategy_runtime import build_entry_runtime
from core.storage.engine_models import TickerSourceObservationModel
from core.storage.serializers import parse_date, render_value
from core.value_coercion import as_mapping, coerce_int, utc_now_iso

DEFAULT_STRATEGY_RERUN_AS_OF_TIME_ET = time(16, 0)


@dataclass(frozen=True)
class _ResolvedSymbols:
    symbols: tuple[str, ...]
    ticker_source_run_id: str | None
    reason_codes: tuple[str, ...]
    blockers: tuple[str, ...]
    evidence: dict[str, Any]


class _StoredCalendarResolver:
    def __init__(self, database_url: str) -> None:
        self.store = CalendarEventStore(database_url)
        self._resolver = CalendarEventResolver(store=self.store, adapters=[])

    def resolve_calendar_context(self, **kwargs: Any) -> Any:
        return self._resolver.resolve_calendar_context(**kwargs)

    def close(self) -> None:
        self.store.close()


def _session_as_of(market_date: str | date) -> datetime:
    parsed = parse_date(market_date)
    return datetime.combine(parsed, DEFAULT_STRATEGY_RERUN_AS_OF_TIME_ET, tzinfo=NEW_YORK).astimezone(UTC)


def _limit_symbols(symbols: tuple[str, ...], *, limit: int) -> tuple[str, ...]:
    normalized = tuple(dict.fromkeys(str(symbol).upper().strip() for symbol in symbols if str(symbol or "").strip()))
    return normalized[: max(int(limit), 1)]


def _stored_dynamic_source_symbols(
    *,
    storage: Any,
    source_id: str,
    as_of: datetime,
    limit: int,
) -> _ResolvedSymbols:
    with storage.session_factory() as session:
        latest_run_id = session.scalar(
            select(TickerSourceObservationModel.ticker_source_run_id)
            .where(TickerSourceObservationModel.ticker_source_id == source_id)
            .where(TickerSourceObservationModel.observation_state == "selected")
            .where(TickerSourceObservationModel.created_at <= as_of)
            .order_by(TickerSourceObservationModel.created_at.desc())
            .limit(1)
        )
        if latest_run_id in (None, ""):
            return _ResolvedSymbols(
                symbols=(),
                ticker_source_run_id=None,
                reason_codes=("stored_dynamic_source_missing",),
                blockers=("no_historical_ticker_source_observations",),
                evidence={
                    "source": "stored_ticker_source_observations",
                    "source_id": source_id,
                    "as_of": as_of.isoformat().replace("+00:00", "Z"),
                    "status": "missing",
                },
            )
        rows = list(
            session.execute(
                select(TickerSourceObservationModel)
                .where(TickerSourceObservationModel.ticker_source_run_id == str(latest_run_id))
                .where(TickerSourceObservationModel.observation_state == "selected")
                .order_by(TickerSourceObservationModel.rank.asc().nullslast(), TickerSourceObservationModel.symbol.asc())
                .limit(max(int(limit), 1))
            ).scalars()
        )
    symbols = tuple(str(row.symbol).upper() for row in rows)
    return _ResolvedSymbols(
        symbols=symbols,
        ticker_source_run_id=str(latest_run_id),
        reason_codes=("stored_dynamic_source_observation",),
        blockers=() if symbols else ("stored_ticker_source_run_empty",),
        evidence={
            "source": "stored_ticker_source_observations",
            "source_id": source_id,
            "ticker_source_run_id": str(latest_run_id),
            "selected_count": len(symbols),
            "as_of": as_of.isoformat().replace("+00:00", "Z"),
            "symbols": [
                {
                    "symbol": row.symbol,
                    "rank": row.rank,
                    "score": row.score,
                    "created_at": render_value(row.created_at),
                }
                for row in rows
            ],
        },
    )


def _resolve_strategy_symbols(
    *,
    storage: Any,
    strategy: Any,
    requested_symbols: tuple[str, ...] | None,
    as_of: datetime,
    limit: int,
) -> _ResolvedSymbols:
    if requested_symbols:
        symbols = _limit_symbols(requested_symbols, limit=limit)
        return _ResolvedSymbols(
            symbols=symbols,
            ticker_source_run_id=None,
            reason_codes=("request_symbols",),
            blockers=() if symbols else ("request_symbols_empty",),
            evidence={"source": "request_symbols", "symbol_count": len(symbols)},
        )
    if strategy.source.is_dynamic:
        resolved = _stored_dynamic_source_symbols(
            storage=storage,
            source_id=strategy.source.ref,
            as_of=as_of,
            limit=limit,
        )
        if resolved.symbols:
            return resolved
    symbols = _limit_symbols(tuple(strategy.symbols), limit=limit)
    reason = "static_strategy_source" if strategy.source.is_static else "strategy_fallback_universe"
    return _ResolvedSymbols(
        symbols=symbols,
        ticker_source_run_id=None,
        reason_codes=(reason,),
        blockers=() if symbols else ("strategy_symbol_scope_empty",),
        evidence={
            "source": reason,
            "source_id": strategy.source.ref,
            "symbol_count": len(symbols),
        },
    )


def _ticker_set(
    *,
    strategy: Any,
    resolved_symbols: _ResolvedSymbols,
    as_of: datetime,
) -> ResolvedTickerSet:
    return ResolvedTickerSet(
        symbols=resolved_symbols.symbols,
        source=ticker_source_spec_from_strategy_source(strategy.source),
        resolved_at=as_of,
        ticker_source_run_id=resolved_symbols.ticker_source_run_id,
        reason_codes=resolved_symbols.reason_codes,
        blockers=resolved_symbols.blockers,
        evidence=resolved_symbols.evidence,
    )


def _ticker_summary(ticker_set: ResolvedTickerSet) -> dict[str, Any]:
    return {
        "ticker_source_kind": ticker_set.source.source_type,
        "ticker_source_id": ticker_set.source.ref,
        "ticker_source_run_id": ticker_set.ticker_source_run_id,
        "resolved_at": ticker_set.resolved_at.isoformat().replace("+00:00", "Z"),
        "symbol_count": len(ticker_set.symbols),
        "symbols": list(ticker_set.symbols),
        "reason_codes": list(ticker_set.reason_codes),
        "blockers": list(ticker_set.blockers),
        "evidence": dict(ticker_set.evidence),
    }


def _calendar_decision_payload(value: Any) -> Any:
    if isinstance(value, CalendarPolicyDecision):
        return {
            "status": value.status,
            "reasons": [
                {
                    "code": reason.code,
                    "event_type": reason.event_type,
                    "severity": reason.severity,
                    "message": reason.message,
                    "scheduled_at": reason.scheduled_at,
                    "source": reason.source,
                }
                for reason in value.reasons
            ],
            "days_to_nearest_event": value.days_to_nearest_event,
            "events_before_expiry": value.events_before_expiry,
            "assignment_risk": value.assignment_risk,
            "macro_regime": value.macro_regime,
            "source_confidence": value.source_confidence,
            "sources": list(value.sources),
            "last_updated": value.last_updated,
            "earnings_phase": value.earnings_phase,
            "earnings_event_date": value.earnings_event_date,
            "earnings_session_timing": value.earnings_session_timing,
            "earnings_cohort_key": value.earnings_cohort_key,
            "earnings_days_to_event": value.earnings_days_to_event,
            "earnings_days_since_event": value.earnings_days_since_event,
            "earnings_timing_confidence": value.earnings_timing_confidence,
            "earnings_horizon_crosses_report": value.earnings_horizon_crosses_report,
            "earnings_primary_source": value.earnings_primary_source,
            "earnings_supporting_sources": list(value.earnings_supporting_sources),
            "earnings_consensus_status": value.earnings_consensus_status,
            "earnings_enrichment": dict(value.earnings_enrichment),
        }
    if isinstance(value, Mapping):
        return {str(key): _calendar_decision_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_calendar_decision_payload(item) for item in value]
    return value


def _json_ready(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return _calendar_decision_payload(value)


def _candidate_build_result(
    *,
    storage: Any,
    runtime: Any,
    ticker_set: ResolvedTickerSet,
    market_date: str,
    as_of: datetime,
    run_key: str,
    db_target: str,
    config_root: str | None,
    calendar_resolver: Any,
    greeks_provider: Any,
    candidate_limit: int,
    per_symbol_top: int,
) -> tuple[CandidateBuildResult, dict[str, Any]]:
    candidate_request = CandidateBuildRequest(
        run_ref=EngineRunRef(
            role=EngineComponentRole.DATA,
            run_id=run_key,
            trading_strategy_id=runtime.trading_strategy_id,
            source_id=ticker_set.source.ref,
            config_hash=runtime.config_hash,
        ),
        trading_strategy_id=runtime.trading_strategy_id,
        trade_structure=runtime.trade_structure,
        symbols=tuple(ticker_set.symbols),
        entry_runtime=runtime,
        candidate_limit=candidate_limit,
        per_symbol_top=per_symbol_top,
        greeks_source="auto",
        source_evidence=ticker_set.evidence,
    )
    base_parameters = entry_candidate_build_parameters(
        runtime=runtime,
        symbols=tuple(ticker_set.symbols),
        request=candidate_request,
        db_target=db_target,
        config_root=config_root,
    ).model_copy(
        update={
            "evaluation_date": market_date,
            "evaluation_timestamp": as_of.isoformat().replace("+00:00", "Z"),
        }
    )
    provider = HistoricalMarketSliceProvider(
        storage=storage,
        request=HistoricalMarketSliceRequest(
            market_date=market_date,
            as_of=as_of.isoformat().replace("+00:00", "Z"),
            trading_strategy_id=runtime.trading_strategy_id,
            routine="entry",
            label=runtime.trading_strategy_id,
            profile=runtime.build_settings.build_profile,
        ),
        greeks_provider=greeks_provider,
    )
    market_slices_by_symbol: dict[str, Any] = {}
    market_slice_diagnostics: dict[str, Any] = {}
    failures: list[dict[str, Any]] = []
    for symbol in ticker_set.symbols:
        try:
            market_slice_parameters = build_symbol_market_slice_parameters(
                symbol=symbol,
                base_parameters=base_parameters,
                runtimes=[runtime],
            )
            market_slices_by_symbol[symbol] = provider.get_symbol_market_slice(
                symbol=symbol,
                parameters=market_slice_parameters,
            )
            diagnostics = provider.diagnostics_for_symbol(symbol)
            if diagnostics is not None:
                market_slice_diagnostics[symbol] = diagnostics
        except Exception as exc:
            failures.append(
                {
                    "symbol": symbol,
                    "stage": "market_slice",
                    "error": str(exc),
                    "fidelity": "missing_or_invalid_historical_market_slice",
                }
            )

    owner_key = runtime_owner_key(runtime)
    candidates_by_owner, diagnostics_by_owner = build_entry_runtime_candidates_with_diagnostics_from_market_slices(
        entry_runtimes=[runtime],
        base_parameters=base_parameters,
        calendar_resolver=calendar_resolver,
        market_slices_by_symbol=market_slices_by_symbol,
        per_runtime_limit=candidate_limit,
    )
    owner_candidates = candidates_by_owner.get(owner_key, {})
    owner_diagnostics = tuple(_json_ready(dict(row)) for row in diagnostics_by_owner.get(owner_key, ()))
    flattened = tuple(dict(row) for rows in owner_candidates.values() for row in list(rows or []))
    candidate_source = "current_builder_rerun"
    if not flattened:
        fallback_rows: list[dict[str, Any]] = []
        for symbol in ticker_set.symbols:
            for row in load_historical_trade_candidate_payloads(
                storage=storage,
                symbol=symbol,
                strategy_id=runtime.trading_strategy_id,
                routine="entry",
                market_date=parse_date(market_date),
                as_of=as_of,
                limit=candidate_limit,
            ):
                fallback_rows.append(
                    {
                        **dict(row),
                        "backtest_candidate_source": "stored_trade_candidate_fallback",
                        "candidate_fidelity": "stored_trade_candidate_fallback",
                    }
                )
        if fallback_rows:
            flattened = tuple(fallback_rows[: max(int(candidate_limit), 1)])
            candidate_source = "stored_trade_candidate_fallback"
    summary = {
        "status": "completed",
        "symbol_count": len(ticker_set.symbols),
        "market_slice_symbol_count": len(market_slices_by_symbol),
        "candidate_count": len(flattened),
        "candidate_source": candidate_source,
        "failure_count": len(failures),
        "symbol_candidate_counts": {str(symbol): len(list(rows or [])) for symbol, rows in sorted(owner_candidates.items())},
        "label": entry_engine_label(runtime),
        "candidate_builder": runtime.build_settings.candidate_builder_key,
        "build_profile": runtime.build_settings.build_profile,
        "greeks_source": base_parameters.greeks_source,
        "market_slice_fidelity": {
            symbol: dict(as_mapping(diagnostics).get("fidelity_labels"))
            for symbol, diagnostics in market_slice_diagnostics.items()
        },
    }
    if failures and not flattened:
        summary["status"] = "failed"
        summary["reason"] = "market_slice_failures"
    return (
        CandidateBuildResult(
            run_ref=candidate_request.run_ref,
            candidate_run_id=f"candidate_run:{run_key}",
            candidates=flattened,
            diagnostics=owner_diagnostics,
            failures=tuple(failures),
            summary=summary,
        ),
        {
            "base_parameters": base_parameters.to_payload(),
            "market_slice_diagnostics": market_slice_diagnostics,
        },
    )


def _attach_trade_signal_ids(
    *,
    runtime: Any,
    market_date: str,
    signal_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in signal_rows:
        candidate = candidate_payload(row)
        symbol = str(row.get("underlying_symbol") or candidate.get("underlying_symbol") or "").upper()
        candidate_identity = str(
            row.get("candidate_identity")
            or resolve_candidate_identity(candidate, strategy=candidate.get("strategy"))
        ).strip()
        if not symbol or not candidate_identity:
            rows.append(dict(row))
            continue
        rows.append(
            {
                **dict(row),
                "trade_signal_id": entry_trade_signal_id(
                    trading_strategy_id=runtime.trading_strategy_id,
                    market_date=market_date,
                    underlying_symbol=symbol,
                    candidate_identity=candidate_identity,
                ),
            }
        )
    return rows


def _decision_artifacts(
    *,
    runtime: Any,
    market_date: str,
    run_key: str,
    signals: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    min_score = float(runtime.trigger_policy.get("min_signal_score") or 0.0)
    plan = plan_entry_selection(
        signals=signals,
        controls_allowed=True,
        controls_reason=None,
        bot_metrics={
            "fidelity": "backtest_controls_not_evaluated",
            "reason": "strategy_rerun_artifact",
        },
        min_score=min_score,
        eligible_selection_states=("promotable",),
    )
    rows: list[dict[str, Any]] = []
    for decision_plan, signal in zip(plan["decisions"], signals, strict=False):
        trade_signal_id = str(signal.get("trade_signal_id") or "")
        if not trade_signal_id:
            continue
        state = str(decision_plan["state"])
        decision_state = "selected" if state == "selected" else ("skip" if state == "blocked" else "no_entry")
        rows.append(
            {
                "trade_decision_id": entry_trade_decision_id(run_key=run_key, trade_signal_id=trade_signal_id),
                "trade_signal_id": trade_signal_id,
                "trading_strategy_id": runtime.trading_strategy_id,
                "trade_structure": runtime.trade_structure,
                "routine": "entry",
                "session_date": market_date,
                "decision_state": decision_state,
                "score": float(decision_plan["score"]),
                "rank": int(decision_plan["rank"]),
                "reason_codes": list(decision_plan["reason_codes"]),
                "blockers": list(decision_plan["reason_codes"]) if decision_state in {"skip", "selected_blocked"} else [],
                "evidence": {
                    "decision_plan": dict(decision_plan["payload"]),
                    "fidelity": "artifact_decision_from_strategy_rerun",
                    "candidate_identity": signal.get("candidate_identity"),
                    "underlying_symbol": signal.get("underlying_symbol"),
                },
            }
        )
    return rows, plan


def _admission_artifacts(
    *,
    storage: Any,
    runtime: Any,
    market_date: str,
    decisions: list[dict[str, Any]],
    signals: list[dict[str, Any]],
    generated_at: str,
) -> list[dict[str, Any]]:
    signals_by_id = {str(signal.get("trade_signal_id") or ""): signal for signal in signals}
    rows: list[dict[str, Any]] = []
    for decision in decisions:
        if str(decision.get("decision_state") or "") != "selected":
            continue
        signal = signals_by_id.get(str(decision.get("trade_signal_id") or ""))
        if signal is None:
            continue
        snapshot = build_selected_entry_admission_snapshot(
            engine_facts=storage.engine_facts,
            execution_store=storage.execution,
            runtime=runtime,
            decision=decision,
            signal=signal,
            market_date=market_date,
            evaluate_execution_capacity=False,
            evaluated_at=generated_at,
        )
        status = str(snapshot.get("status") or "unknown")
        admission_state = "approved" if status in {"admissible", "approved", "ok", "pass", "passed"} else status
        rows.append(
            {
                "admission_decision_id": f"backtest_admission:{decision['trade_decision_id']}",
                "trade_signal_id": decision.get("trade_signal_id"),
                "trade_decision_id": decision.get("trade_decision_id"),
                "trading_strategy_id": runtime.trading_strategy_id,
                "trade_structure": runtime.trade_structure,
                "routine": "entry",
                "session_date": market_date,
                "admission_kind": "entry_open",
                "admission_state": admission_state,
                "requested_quantity": snapshot.get("admissible_quantity"),
                "requested_notional": snapshot.get("required_buying_power"),
                "max_loss": snapshot.get("required_buying_power"),
                "reason_codes": list(snapshot.get("reason_codes") or []),
                "blockers": list(snapshot.get("blockers") or []),
                "policy_snapshot": {
                    "fidelity": "current_policy_historical_market_data",
                    "execution_capacity": "deferred_to_execution_simulation",
                },
                "metrics": {
                    "required_buying_power": snapshot.get("required_buying_power"),
                    "available_buying_power": snapshot.get("available_buying_power"),
                    "protection_admission": dict(snapshot.get("protection_admission") or {}),
                    "portfolio_admission": dict(snapshot.get("portfolio_admission") or {}),
                },
                "evidence": {
                    "fidelity": "artifact_admission_from_strategy_rerun",
                    "admission_snapshot": snapshot,
                },
                "decided_at": generated_at,
            }
        )
    return rows


def _state_counts(rows: list[Mapping[str, Any]], field: str) -> dict[str, int]:
    counts = Counter(str(row.get(field) or "unknown") for row in rows)
    return dict(sorted(counts.items()))


def _strategy_day_result(
    *,
    storage: Any,
    strategy: Any,
    requested_symbols: tuple[str, ...] | None,
    market_date: str,
    db_target: str,
    config_root: str | None,
    calendar_resolver: Any,
    greeks_provider: Any,
    market_data_symbol_limit: int,
    candidate_limit: int,
    per_symbol_top: int,
) -> dict[str, Any]:
    generated_at = utc_now_iso()
    as_of = _session_as_of(market_date)
    runtime = build_entry_runtime(strategy)
    resolved_symbols = _resolve_strategy_symbols(
        storage=storage,
        strategy=strategy,
        requested_symbols=requested_symbols,
        as_of=as_of,
        limit=market_data_symbol_limit,
    )
    ticker_set = _ticker_set(strategy=strategy, resolved_symbols=resolved_symbols, as_of=as_of)
    ticker_summary = _ticker_summary(ticker_set)
    run_key = f"backtest:strategy_rerun:{strategy.trading_strategy_id}:{market_date}:{strategy.config_hash[:12]}"
    runtime_with_symbols = entry_runtime_with_symbols(runtime, ticker_set.symbols)
    if ticker_set.blockers:
        return {
            "status": "skipped",
            "reason": "ticker_source_blocked",
            "market_date": market_date,
            "as_of": as_of.isoformat().replace("+00:00", "Z"),
            "ticker_set": ticker_summary,
            "candidate_build": candidate_result_summary(None),
            "signals": [],
            "decisions": [],
            "admissions": [],
            "fidelity_labels": {
                "source": "stored_or_config_symbol_scope",
                "candidate": "not_run",
                "signal": "not_run",
                "decision": "not_run",
                "admission": "not_run",
            },
        }

    candidate_result, build_context = _candidate_build_result(
        storage=storage,
        runtime=runtime_with_symbols,
        ticker_set=ticker_set,
        market_date=market_date,
        as_of=as_of,
        run_key=run_key,
        db_target=db_target,
        config_root=config_root,
        calendar_resolver=calendar_resolver,
        greeks_provider=greeks_provider,
        candidate_limit=candidate_limit,
        per_symbol_top=per_symbol_top,
    )
    selection_result = EntrySelectionEngine().select(
        runtime=runtime_with_symbols,
        ticker_set=ticker_set,
        ticker_summary=ticker_summary,
        candidate_result=candidate_result,
        label=entry_engine_label(runtime_with_symbols),
        cycle_id=run_key,
        generated_at=generated_at,
        previous_promotable={},
        previous_selection_memory={},
        top_promotable=candidate_limit,
        top_monitor=ENTRY_MONITOR_LIMIT,
    )
    strategy_run_id = entry_engine_strategy_run_id(run_key, runtime_with_symbols.trading_strategy_id)
    selected_rows = [
        build_entry_signal_row_from_selection(
            runtime=runtime_with_symbols,
            market_date=market_date,
            generated_at=generated_at,
            strategy_run_id=strategy_run_id,
            row=dict(row),
        )
        for row in list(selection_result.selection.get("signals") or [])
        if isinstance(row, Mapping)
    ]
    signals = _attach_trade_signal_ids(
        runtime=runtime_with_symbols,
        market_date=market_date,
        signal_rows=selected_rows,
    )
    decisions, decision_plan = _decision_artifacts(
        runtime=runtime_with_symbols,
        market_date=market_date,
        run_key=run_key,
        signals=signals,
    )
    admissions = _admission_artifacts(
        storage=storage,
        runtime=runtime_with_symbols,
        market_date=market_date,
        decisions=decisions,
        signals=signals,
        generated_at=generated_at,
    )
    selection_summary = entry_selection_summary(
        candidate_rows_by_symbol={symbol: [dict(row) for row in rows] for symbol, rows in selection_result.symbol_candidates.items()},
        selected_rows=signals,
        selection_memory=dict(selection_result.selection.get("selection_memory") or {}),
    )
    return {
        "status": "ok",
        "market_date": market_date,
        "as_of": as_of.isoformat().replace("+00:00", "Z"),
        "ticker_set": ticker_summary,
        "candidate_build": {
            **candidate_result_summary(selection_result.candidate_result),
            "failures": list(candidate_result.failures),
        },
        "strategy_run": {
            "strategy_run_id": strategy_run_id,
            "selection_summary": selection_summary,
            "entry_selection": {
                "selected_candidate_count": len(selection_result.selected_candidates),
                "monitored_candidate_count": len(selection_result.monitored_candidates),
                "rejected_candidate_count": len(selection_result.rejected_candidates),
            },
            "decision_plan": decision_plan,
        },
        "candidates": [dict(row) for row in selection_result.candidate_result.candidates],
        "diagnostics": [dict(row) for row in selection_result.candidate_result.diagnostics],
        "market_slice_diagnostics": dict(build_context["market_slice_diagnostics"]),
        "signals": signals,
        "decisions": decisions,
        "admissions": admissions,
        "fidelity_labels": {
            "source": "stored_dynamic_source_or_config_scope",
            "calendar": "stored_calendar_no_refresh",
            "market_data": "historical_clickhouse_slices",
            "candidate": str(selection_result.candidate_result.summary.get("candidate_source") or "current_builder_rerun"),
            "entry_quality": "current_entry_quality_pipeline",
            "signal": "current_entry_selection_engine",
            "decision": "artifact_decision_from_current_entry_policy",
            "admission": "current_protection_portfolio_policy_execution_capacity_deferred",
            "live_writes": "none",
        },
    }


def _aggregate_strategy_result(strategy: Any, day_results: list[dict[str, Any]]) -> dict[str, Any]:
    signal_rows = [row for result in day_results for row in list(result.get("signals") or []) if isinstance(row, Mapping)]
    decision_rows = [row for result in day_results for row in list(result.get("decisions") or []) if isinstance(row, Mapping)]
    admission_rows = [row for result in day_results for row in list(result.get("admissions") or []) if isinstance(row, Mapping)]
    candidate_count = sum(coerce_int(as_mapping(result.get("candidate_build")).get("candidate_count")) or 0 for result in day_results)
    failure_count = sum(coerce_int(as_mapping(result.get("candidate_build")).get("failure_count")) or 0 for result in day_results)
    selected_count = sum(1 for row in decision_rows if str(row.get("decision_state") or "") == "selected")
    approved_count = sum(1 for row in admission_rows if str(row.get("admission_state") or "") == "approved")
    candidate_fidelity = "current_builder_rerun"
    if any(as_mapping(result.get("fidelity_labels")).get("candidate") == "stored_trade_candidate_fallback" for result in day_results):
        candidate_fidelity = "stored_trade_candidate_fallback"
    return {
        **strategy_profile(strategy),
        "variant_id": strategy_variant_id(strategy),
        "market_dates": [str(result.get("market_date")) for result in day_results],
        "day_results": day_results,
        "candidate_productivity": {
            "candidate_run_count": len(day_results),
            "trade_candidate_count": candidate_count,
            "candidate_failure_count": failure_count,
        },
        "selection_quality": {
            "signal_count": len(signal_rows),
            "decision_count": len(decision_rows),
            "selected_count": selected_count,
            "signal_state_counts": _state_counts(signal_rows, "selection_state"),
            "decision_state_counts": _state_counts(decision_rows, "decision_state"),
            "selection_rate": None if not decision_rows else round(selected_count / len(decision_rows), 4),
        },
        "admissions": {
            "admission_count": len(admission_rows),
            "approved_count": approved_count,
            "admission_state_counts": _state_counts(admission_rows, "admission_state"),
            "approval_rate": None if not admission_rows else round(approved_count / len(admission_rows), 4),
        },
        "pnl": {"net_pnl": 0.0},
        "execution": {"attempt_count": 0, "fill_count": 0},
        "fidelity_labels": {
            "mode": "strategy_rerun_current_config",
            "candidate": candidate_fidelity,
            "signal": "current_entry_selection_engine",
            "decision": "artifact_decision_from_current_entry_policy",
            "admission": "current_policy_execution_capacity_deferred",
            "execution": "not_simulated_in_strategy_rerun",
            "pnl": "not_simulated_in_strategy_rerun",
        },
        "outcome_label": (
            "admission_artifacts_created"
            if admission_rows
            else ("selected_without_admission" if selected_count else ("signals_without_selected_decision" if signal_rows else "no_signals"))
        ),
    }


def build_strategy_rerun_backtest(
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
    window = normalize_backtest_window(start_date, end_date, max_days=max_days)
    strategies = load_backtest_strategy_scope(strategy_ids)
    entry_strategies = {
        strategy_id: strategy
        for strategy_id, strategy in strategies.items()
        if strategy.entry is not None and strategy.entry.enabled
    }
    calendar_resolver = _StoredCalendarResolver(storage.database_url or db_target)
    greeks_provider = build_local_greeks_provider()
    try:
        strategy_results: list[dict[str, Any]] = []
        for strategy in entry_strategies.values():
            day_results = [
                _strategy_day_result(
                    storage=storage,
                    strategy=strategy,
                    requested_symbols=symbols,
                    market_date=market_date,
                    db_target=db_target,
                    config_root=config_root,
                    calendar_resolver=calendar_resolver,
                    greeks_provider=greeks_provider,
                    market_data_symbol_limit=market_data_symbol_limit,
                    candidate_limit=candidate_limit,
                    per_symbol_top=per_symbol_top,
                )
                for market_date in window.market_dates
            ]
            strategy_results.append(_aggregate_strategy_result(strategy, day_results))
    finally:
        calendar_resolver.close()

    total_candidates = sum(
        coerce_int(as_mapping(result.get("candidate_productivity")).get("trade_candidate_count")) or 0
        for result in strategy_results
    )
    total_signals = sum(coerce_int(as_mapping(result.get("selection_quality")).get("signal_count")) or 0 for result in strategy_results)
    total_decisions = sum(coerce_int(as_mapping(result.get("selection_quality")).get("decision_count")) or 0 for result in strategy_results)
    total_admissions = sum(coerce_int(as_mapping(result.get("admissions")).get("admission_count")) or 0 for result in strategy_results)
    candidate_fidelity = "current_builder_rerun"
    if any(as_mapping(result.get("fidelity_labels")).get("candidate") == "stored_trade_candidate_fallback" for result in strategy_results):
        candidate_fidelity = "stored_trade_candidate_fallback"
    return {
        "status": "ready",
        "evaluation_mode": "strategy_rerun_current_model",
        "generated_at": utc_now_iso(),
        "window": {
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "market_dates": list(window.market_dates),
        },
        "summary": {
            "strategy_count": len(strategy_results),
            "candidate_count": total_candidates,
            "signal_count": total_signals,
            "decision_count": total_decisions,
            "admission_count": total_admissions,
            "net_pnl": 0.0,
        },
        "strategies": strategy_results,
        "fidelity_labels": {
            "mode": "strategy_rerun_current_model",
            "source": "stored_dynamic_source_or_config_scope",
            "market_data": "historical_clickhouse_slices",
            "calendar": "stored_calendar_no_refresh",
            "candidate": candidate_fidelity,
            "signal": "current_entry_selection_engine",
            "decision": "artifact_decision_from_current_entry_policy",
            "admission": "current_protection_portfolio_policy_execution_capacity_deferred",
            "execution": "not_simulated",
            "pnl": "not_simulated",
            "live_writes": "none",
        },
    }


__all__ = ["build_strategy_rerun_backtest"]
