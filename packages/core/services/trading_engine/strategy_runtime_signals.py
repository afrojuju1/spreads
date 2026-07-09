from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from core.services.trading_engine.data import CandidateBuildRequest
from core.services.trading_engine.data_runtime import (
    DataEngine,
    entry_engine_label,
    entry_engine_strategy_run_id,
    entry_runtime_with_symbols,
    ticker_source_spec_from_strategy_source,
)
from core.services.candidate_identity import resolve_candidate_identity
from core.services.trading_engine.entry_selection import EntrySelectionEngine, candidate_result_summary
from core.services.trading_engine.entry_signals import (
    ENTRY_MONITOR_LIMIT,
    NATURAL_ENTRY_PROVENANCE,
    OBSERVATION_ENTRY_PROVENANCE,
    build_entry_signal_row_from_selection,
    candidate_payload,
    entry_selection_summary,
)
from core.services.trading_engine.facts import persist_entry_engine_facts
from core.services.trading_engine.kernel import EngineComponentRole, EngineContext, EngineRunRef
from core.services.trading_strategy_runtime_models import EntryRuntime
from core.value_coercion import utc_now_iso as _utc_now


from core.services.trading_engine.strategy_runtime_support import (
    _entry_candidate_limit,
    _read_previous_entry_selection,
    _record_skipped_strategy_run,
    _ticker_set_summary,
    _trade_signal_id_for_signal,
)


def _refresh_entry_runtime_signals(
    *,
    db_target: str,
    storage: Any,
    runtime: EntryRuntime,
    market_date: str,
    run_key: str,
    planner_job_run_id: str | None,
    observation_only: bool = False,
) -> dict[str, Any]:
    generated_at = _utc_now()
    provenance = OBSERVATION_ENTRY_PROVENANCE if observation_only else NATURAL_ENTRY_PROVENANCE
    context = EngineContext(
        db_target=db_target,
        storage=storage,
        job_run_id=planner_job_run_id,
        metadata={
            "config_hash": runtime.config_hash,
            "entry_run_mode": "observation" if observation_only else "natural",
            "validation_provenance": provenance,
            "observation_only": observation_only,
        },
    )
    data_engine = DataEngine(context)
    source_spec = ticker_source_spec_from_strategy_source(runtime.strategy.source)
    ticker_set = data_engine.resolve_tickers(
        source=source_spec,
        as_of=datetime.now(UTC),
    )
    ticker_summary = _ticker_set_summary(ticker_set)
    if ticker_set.blockers:
        engine_fact_summary = persist_entry_engine_facts(
            engine_facts=getattr(storage, "engine_facts", None),
            runtime=runtime,
            market_date=market_date,
            run_key=run_key,
            generated_at=generated_at,
            ticker_set=ticker_set,
            candidate_result=None,
            signal_rows=[],
        )
        _record_skipped_strategy_run(
            signal_store=storage.signals,
            runtime=runtime,
            run_key=run_key,
            market_date=market_date,
            planner_job_run_id=planner_job_run_id,
            generated_at=generated_at,
            reason="ticker_source_blocked",
            ticker_set=ticker_set,
            observation_only=observation_only,
        )
        return {
            "status": "skipped",
            "reason": "ticker_source_blocked",
            "entry_run_mode": "observation" if observation_only else "natural",
            "validation_provenance": provenance,
            "observation_only": observation_only,
            "ticker_set": ticker_summary,
            "candidate_build": candidate_result_summary(None),
            "strategy_run": {},
            "engine_facts": engine_fact_summary,
        }

    runtime_with_symbols = entry_runtime_with_symbols(runtime, ticker_set.symbols)
    candidate_request = CandidateBuildRequest(
        run_ref=EngineRunRef(
            role=EngineComponentRole.DATA,
            run_id=run_key,
            trading_strategy_id=runtime.trading_strategy_id,
            job_run_id=planner_job_run_id,
            source_id=source_spec.ref,
            config_hash=runtime.config_hash,
        ),
        trading_strategy_id=runtime.trading_strategy_id,
        trade_structure=runtime.trade_structure,
        symbols=tuple(ticker_set.symbols),
        entry_runtime=runtime_with_symbols,
        candidate_limit=_entry_candidate_limit(runtime_with_symbols),
        per_symbol_top=1,
        greeks_source="auto",
        source_evidence=ticker_set.evidence,
    )
    candidate_result = data_engine.build_entry_trade_candidates(
        request=candidate_request,
        runtime=runtime_with_symbols,
    )
    previous_promotable, previous_selection_memory = _read_previous_entry_selection(
        signal_store=storage.signals,
        runtime=runtime_with_symbols,
        session_date=market_date,
    )
    selection_result = EntrySelectionEngine().select(
        runtime=runtime_with_symbols,
        ticker_set=ticker_set,
        ticker_summary=ticker_summary,
        candidate_result=candidate_result,
        label=entry_engine_label(runtime_with_symbols),
        cycle_id=run_key,
        generated_at=generated_at,
        previous_promotable=previous_promotable,
        previous_selection_memory=previous_selection_memory,
        top_promotable=_entry_candidate_limit(runtime_with_symbols),
        top_monitor=ENTRY_MONITOR_LIMIT,
    )
    candidate_result = selection_result.candidate_result
    quality_analysis = selection_result.quality_analysis
    symbol_candidates = selection_result.symbol_candidates
    selection = dict(selection_result.selection)
    selected_rows = [
        build_entry_signal_row_from_selection(
            runtime=runtime_with_symbols,
            market_date=market_date,
            generated_at=generated_at,
            strategy_run_id=entry_engine_strategy_run_id(run_key, runtime_with_symbols.trading_strategy_id),
            row=dict(row),
            observation_only=observation_only,
        )
        for row in list(selection.get("signals") or [])
        if isinstance(row, dict)
    ]
    selection_memory = dict(selection.get("selection_memory") or {})
    selection_summary = entry_selection_summary(
        candidate_rows_by_symbol=symbol_candidates,
        selected_rows=selected_rows,
        selection_memory=selection_memory,
    )
    strategy_run = storage.signals.upsert_strategy_run(
        strategy_run_id=entry_engine_strategy_run_id(run_key, runtime_with_symbols.trading_strategy_id),
        trading_strategy_id=runtime_with_symbols.trading_strategy_id,
        trigger_type="trading_strategy_entry",
        job_run_id=planner_job_run_id,
        cycle_id=run_key,
        label=entry_engine_label(runtime_with_symbols),
        session_date=market_date,
        started_at=generated_at,
        completed_at=generated_at,
        status="completed",
        result={
            "entry_run_mode": "observation" if observation_only else "natural",
            "validation_provenance": provenance,
            "observation_only": observation_only,
            **selection_summary,
            "selected_signal_rows": selected_rows,
            "entry_selection": {
                "selected_candidate_count": len(selection_result.selected_candidates),
                "monitored_candidate_count": len(selection_result.monitored_candidates),
                "rejected_candidate_count": len(selection_result.rejected_candidates),
            },
        },
        config_hash=runtime_with_symbols.config_hash,
    )
    engine_fact_summary = persist_entry_engine_facts(
        engine_facts=getattr(storage, "engine_facts", None),
        runtime=runtime_with_symbols,
        market_date=market_date,
        run_key=run_key,
        generated_at=generated_at,
        ticker_set=ticker_set,
        candidate_result=candidate_result,
        signal_rows=selected_rows,
        quality_analysis=quality_analysis,
    )
    signal_refs_by_key = {
        (
            str(ref.get("underlying_symbol") or "").upper(),
            str(ref.get("candidate_identity") or ""),
        ): dict(ref)
        for ref in list(engine_fact_summary.get("trade_signals") or [])
        if isinstance(ref, dict) and ref.get("trade_signal_id") not in (None, "")
    }
    signals = []
    for row in selected_rows:
        candidate = candidate_payload(row)
        candidate_identity = str(row.get("candidate_identity") or resolve_candidate_identity(candidate, strategy=candidate.get("strategy"))).strip()
        key = (str(row.get("underlying_symbol") or "").upper(), candidate_identity)
        signal_ref = signal_refs_by_key.get(key)
        trade_signal_id = (
            str(signal_ref["trade_signal_id"])
            if signal_ref is not None
            else _trade_signal_id_for_signal(
                candidate_generation={"engine_facts": engine_fact_summary},
                runtime=runtime_with_symbols,
                market_date=market_date,
                signal=row,
            )
        )
        if trade_signal_id is None:
            continue
        quality_evidence = (
            {}
            if signal_ref is None
            else {
                "quality_profile_id": signal_ref.get("quality_profile_id"),
                "quality_waterfall_blocked": signal_ref.get("quality_waterfall_blocked"),
                "quality_waterfall_stage_counts": dict(signal_ref.get("quality_waterfall_stage_counts") or {}),
            }
        )
        signals.append(
            {
                **row,
                "trade_signal_id": trade_signal_id,
                "evidence": {
                    **dict(row.get("evidence") or {}),
                    **quality_evidence,
                },
            }
        )
    return {
        "status": "ok",
        "reason": None,
        "entry_run_mode": "observation" if observation_only else "natural",
        "validation_provenance": provenance,
        "observation_only": observation_only,
        "ticker_set": ticker_summary,
        "candidate_build": candidate_result_summary(candidate_result),
        "strategy_run": {
            "strategy_run_id": strategy_run.get("strategy_run_id"),
            "signal_count": len(signals),
            "selection_summary": selection_summary,
            "selected_candidate_count": len(selection_result.selected_candidates),
            "monitored_candidate_count": len(selection_result.monitored_candidates),
            "rejected_candidate_count": len(selection_result.rejected_candidates),
        },
        "engine_facts": engine_fact_summary,
        "signals": signals,
    }


__all__ = ["_refresh_entry_runtime_signals"]
