from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any

from core.alerts.runtime import plan_runtime_entry_selected_alert
from core.db.decorators import with_storage
from core.services.admission_lifecycle import admission_allows_attempt, normalize_lifecycle_admission
from core.services.entry_planner import plan_entry_selection
from core.services.execution_intents import request_execution_intent_dispatch
from core.services.execution_intents.shared import (
    ACTIVE_INTENT_STATES,
    issue_pending_execution_intent,
)
from core.services.live_selection import select_live_signals
from core.services.management_recipes import build_exit_policy_from_recipe_refs
from core.services.option_structures import candidate_legs, payload_structure_identity
from core.services.candidate_fields import (
    candidate_economics,
    candidate_evidence_metrics,
    candidate_policy_context,
    candidate_strategy_metrics,
    risk_hints,
)
from core.services.runtime_policy import resolve_runtime_policy_fields
from core.services.risk_manager import (
    build_execution_admission_snapshot,
    resolve_position_size_policy,
)
from core.services.runtime_policy import build_runtime_policy_ref
from core.services.strategy_analytics import evaluate_trading_strategy_entry_controls
from core.services.trading_engine.data import CandidateBuildRequest, CandidateBuildResult, ResolvedTickerSet
from core.services.trading_engine.data_runtime import (
    PostgresDataEngine,
    entry_engine_label,
    entry_engine_strategy_run_id,
    entry_runtime_with_symbols,
    ticker_source_spec_from_strategy_source,
)
from core.services.trading_engine.facts import entry_trade_signal_id, persist_entry_engine_facts
from core.services.trading_engine.kernel import EngineComponentRole, EngineContext, EngineRunRef
from core.services.trading_engine.strategy import StrategyEntryRequest, StrategyEntryResult
from core.services.trading_strategies import routine_should_run_now
from core.services.trading_strategy_runtime import EntryRuntime, resolve_entry_runtime
from core.services.value_coercion import utc_now, utc_now_iso as _utc_now

ENTRY_INTENT_TTL_MINUTES = 5
ENTRY_MONITOR_LIMIT = 12


def _expires_in(minutes: int) -> str:
    return (utc_now() + timedelta(minutes=max(minutes, 1))).isoformat(timespec="seconds").replace("+00:00", "Z")


def _market_date_today() -> str:
    return utc_now().date().isoformat()


def _trade_decision_id(run_key: str, trade_signal_id: str) -> str:
    material = f"{run_key}|{trade_signal_id}".encode("utf-8")
    return f"trade_decision:{hashlib.sha1(material).hexdigest()[:24]}"


def _intent_id(trade_decision_id: str) -> str:
    return f"execution_intent:{trade_decision_id}"


def _slot_key(trading_strategy_id: str, underlying_symbol: str) -> str:
    return f"entry:{trading_strategy_id}:{underlying_symbol}"


def _entry_candidate_limit(runtime: EntryRuntime) -> int:
    max_symbols = runtime.strategy.source.max_symbols
    if max_symbols is not None:
        return max(int(max_symbols), 1)
    return 10


def _ticker_set_summary(ticker_set: ResolvedTickerSet) -> dict[str, Any]:
    evidence = dict(ticker_set.evidence or {})
    summary = evidence.get("summary") if isinstance(evidence.get("summary"), dict) else {}
    degradation = evidence.get("degradation") if isinstance(evidence.get("degradation"), dict) else {}
    return {
        "ticker_source_kind": ticker_set.source.source_type,
        "ticker_source_id": ticker_set.source.ref,
        "ticker_source_run_id": ticker_set.ticker_source_run_id,
        "resolved_at": ticker_set.resolved_at.isoformat().replace("+00:00", "Z"),
        "symbol_count": len(ticker_set.symbols),
        "symbols": list(ticker_set.symbols),
        "reason_codes": list(ticker_set.reason_codes),
        "blockers": list(ticker_set.blockers),
        "summary": dict(summary),
        "degradation": dict(degradation),
    }


def _candidate_result_summary(candidate_result: CandidateBuildResult | None) -> dict[str, Any]:
    if candidate_result is None:
        return {
            "status": "not_run",
            "candidate_count": 0,
            "symbol_count": 0,
        }
    return {
        "candidate_run_id": candidate_result.candidate_run_id,
        "candidate_count": len(candidate_result.candidates),
        **dict(candidate_result.summary or {}),
    }


def _group_candidate_rows(candidates: tuple[Any, ...]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        symbol = str(candidate.get("underlying_symbol") or "").upper().strip()
        if not symbol:
            continue
        grouped.setdefault(symbol, []).append(dict(candidate))
    return grouped


def _candidate_result_runtime_filter_reason_counts(candidate_result: CandidateBuildResult) -> dict[str, int]:
    counts: dict[str, int] = {}
    for diagnostic in candidate_result.diagnostics:
        if not isinstance(diagnostic, dict):
            continue
        rejection_counts = diagnostic.get("rejection_counts")
        if not isinstance(rejection_counts, dict):
            continue
        runtime_filter = rejection_counts.get("runtime_filter")
        if not isinstance(runtime_filter, dict):
            continue
        for reason, count in runtime_filter.items():
            rendered = str(reason or "").strip()
            if not rendered:
                continue
            try:
                counts[rendered] = counts.get(rendered, 0) + int(count)
            except (TypeError, ValueError):
                continue
    return dict(sorted(counts.items()))


def _candidate_payload(row: dict[str, Any]) -> dict[str, Any]:
    candidate = row.get("candidate")
    if isinstance(candidate, dict):
        return dict(candidate)
    return dict(row)


def _candidate_identity(candidate: dict[str, Any]) -> str:
    return str(
        candidate.get("candidate_identity")
        or candidate.get("structure_identity")
        or payload_structure_identity(candidate, strategy=candidate.get("strategy"))
        or ""
    ).strip()


def _candidate_identity_from_signal(signal: dict[str, Any]) -> str:
    candidate = _candidate_payload(signal)
    return str(signal.get("candidate_identity") or _candidate_identity(candidate)).strip()


def _trade_signal_refs(candidate_generation: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    engine_facts = candidate_generation.get("engine_facts") if isinstance(candidate_generation.get("engine_facts"), dict) else {}
    refs = engine_facts.get("trade_signals") if isinstance(engine_facts, dict) else None
    if not isinstance(refs, list):
        return ()
    return tuple(dict(ref) for ref in refs if isinstance(ref, dict))


def _trade_signal_id_for_signal(
    *,
    candidate_generation: dict[str, Any],
    runtime: EntryRuntime,
    market_date: str,
    signal: dict[str, Any],
) -> str | None:
    symbol = str(signal.get("underlying_symbol") or "").upper()
    candidate_identity = _candidate_identity_from_signal(signal)
    for ref in _trade_signal_refs(candidate_generation):
        if (
            symbol
            and candidate_identity
            and str(ref.get("underlying_symbol") or "").upper() == symbol
            and str(ref.get("candidate_identity") or "") == candidate_identity
        ):
            return None if ref.get("trade_signal_id") in (None, "") else str(ref["trade_signal_id"])
    if symbol and candidate_identity:
        return entry_trade_signal_id(
            trading_strategy_id=runtime.trading_strategy_id,
            market_date=market_date,
            underlying_symbol=symbol,
            candidate_identity=candidate_identity,
        )
    return None


def _trade_decision_state(decision_state: Any) -> str:
    normalized = str(decision_state or "").strip().lower()
    if normalized == "selected":
        return "selected"
    if normalized == "blocked":
        return "skip"
    return "no_entry"


def _persist_trade_admission(
    *,
    engine_facts: Any,
    runtime: EntryRuntime,
    market_date: str,
    policy_ref: dict[str, Any],
    trade_signal_id: str,
    trade_decision_id: str,
    execution_intent_id: str,
    slot_key: str,
    admission_snapshot: dict[str, Any],
    signal: dict[str, Any],
    expires_at: str,
) -> dict[str, Any]:
    normalized = normalize_lifecycle_admission(
        admission_snapshot,
        admission_kind="entry_open",
        source_object_type="trade_decision",
        source_object_id=trade_decision_id,
        session_date=market_date,
        requested_quantity=1,
        requested_notional=admission_snapshot.get("required_buying_power"),
        max_loss=admission_snapshot.get("required_buying_power"),
        policy_snapshot=policy_ref,
        metrics={
            "admissible_quantity": admission_snapshot.get("admissible_quantity"),
            "required_buying_power": admission_snapshot.get("required_buying_power"),
            "available_buying_power": admission_snapshot.get("available_buying_power"),
        },
        evidence={
            "trade_signal_id": trade_signal_id,
            "trade_decision_id": trade_decision_id,
            "execution_intent_id": execution_intent_id,
            "slot_key": slot_key,
            "underlying_symbol": signal.get("underlying_symbol"),
            "candidate_identity": _candidate_identity_from_signal(signal),
        },
    )
    target_intent_state = "pending" if admission_allows_attempt(normalized) else "revoked"
    now = _utc_now()
    engine_facts.upsert_trade_execution_intent(
        execution_intent_id=execution_intent_id,
        intent_kind="open",
        source_object_type="trade_decision",
        source_object_id=trade_decision_id,
        trade_signal_id=trade_signal_id,
        trade_decision_id=trade_decision_id,
        position_id=None,
        trading_strategy_id=runtime.trading_strategy_id,
        trade_structure=runtime.trade_structure,
        routine="entry",
        account_id=None,
        slot_key=slot_key,
        idempotency_key=execution_intent_id,
        intent_state=target_intent_state,
        claim_token=None,
        claimed_at=None,
        expires_at=expires_at,
        supersedes_intent_id=None,
        superseded_by_intent_id=None,
        payload={
            "underlying_symbol": signal.get("underlying_symbol"),
            "candidate_identity": _candidate_identity_from_signal(signal),
            "execution_runtime": runtime.strategy.execution.runtime,
        },
        policy_snapshot=policy_ref,
        config_hash=runtime.config_hash,
        created_at=now,
        updated_at=now,
    )
    admission = engine_facts.upsert_trade_admission(
        admission_decision_id=str(normalized["admission_decision_id"]),
        execution_intent_id=execution_intent_id,
        trade_signal_id=trade_signal_id,
        trade_decision_id=trade_decision_id,
        position_id=None,
        admission_kind=str(normalized["admission_kind"]),
        admission_state=str(normalized["admission_state"]),
        account_id=None,
        session_date=market_date,
        requested_quantity=normalized.get("requested_quantity"),
        requested_notional=normalized.get("requested_notional"),
        max_loss=normalized.get("max_loss"),
        policy_snapshot=dict(normalized.get("policy_snapshot") or {}),
        capability_snapshot=dict(normalized.get("capability_snapshot") or {}),
        metrics=dict(normalized.get("metrics") or {}),
        reason_codes=list(normalized.get("reason_codes") or []),
        blockers=list(normalized.get("blockers") or []),
        evidence=dict(normalized.get("evidence") or {}),
        note=normalized.get("message") or normalized.get("reason"),
        execution_attempt_id=None,
        decided_at=str(normalized["decided_at"]),
    )
    return {
        **dict(normalized),
        "admission_decision_id": admission["admission_decision_id"],
        "execution_intent_id": execution_intent_id,
    }


def _record_skipped_strategy_run(
    *,
    signal_store: Any,
    runtime: EntryRuntime,
    run_key: str,
    market_date: str,
    planner_job_run_id: str | None,
    generated_at: str,
    reason: str,
    ticker_set: ResolvedTickerSet,
) -> None:
    signal_store.upsert_strategy_run(
        strategy_run_id=entry_engine_strategy_run_id(run_key, runtime.trading_strategy_id),
        trading_strategy_id=runtime.trading_strategy_id,
        trigger_type="trading_strategy_entry",
        job_run_id=planner_job_run_id,
        cycle_id=run_key,
        label=entry_engine_label(runtime),
        session_date=market_date,
        started_at=generated_at,
        completed_at=generated_at,
        status="skipped",
        result={
            "reason": reason,
            "ticker_set": _ticker_set_summary(ticker_set),
            "candidate_count": 0,
            "signal_count": 0,
        },
        config_hash=runtime.config_hash,
    )


def _runtime_signal_eligibility(runtime: EntryRuntime, row: dict[str, Any]) -> str:
    eligibility = str(row.get("eligibility") or "live").strip().lower() or "live"
    if runtime.strategy.execution.mode == "shadow" and eligibility == "live":
        return "analysis_only"
    return eligibility


def _signal_blockers(candidate: dict[str, Any], *, eligibility: str | None = None) -> list[str]:
    blockers: list[str] = []
    if str(eligibility or "live").strip().lower() != "live":
        blockers.append("analysis_only")
    for field in ("scoring_blockers", "execution_blockers", "ranking_policy_blockers"):
        for blocker in _normalized_blockers(candidate.get(field)):
            if blocker not in blockers:
                blockers.append(blocker)
    return blockers


def _execution_shape(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "underlying_symbol": candidate.get("underlying_symbol"),
        "structure_identity": _candidate_identity(candidate),
        "legs": candidate_legs(candidate),
        "order_payload": dict(candidate.get("order_payload") or {}),
    }


def _read_previous_entry_selection(
    *,
    signal_store: Any,
    runtime: EntryRuntime,
    session_date: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    previous_runs = [
        dict(row)
        for row in signal_store.list_strategy_runs(
            trading_strategy_id=runtime.trading_strategy_id,
            session_date=session_date,
            limit=1,
        )
    ]
    if not previous_runs:
        return {}, {}
    result_payload = previous_runs[0].get("result")
    if not isinstance(result_payload, dict):
        result_payload = previous_runs[0].get("result_json")
    if not isinstance(result_payload, dict):
        return {}, {}
    selection_memory = {
        str(symbol): dict(state)
        for symbol, state in dict(result_payload.get("selection_memory") or {}).items()
        if isinstance(symbol, str) and isinstance(state, dict)
    }
    previous_promotable: dict[str, dict[str, Any]] = {}
    for row in list(result_payload.get("selected_signal_rows") or []):
        if not isinstance(row, dict) or str(row.get("selection_state") or "") != "promotable":
            continue
        candidate = _candidate_payload(row)
        symbol = str(row.get("underlying_symbol") or candidate.get("underlying_symbol") or "").upper()
        if symbol:
            previous_promotable[symbol] = candidate
    return previous_promotable, selection_memory


def _signal_row_from_selection(
    *,
    runtime: EntryRuntime,
    market_date: str,
    generated_at: str,
    strategy_run_id: str,
    row: dict[str, Any],
) -> dict[str, Any]:
    candidate = _candidate_payload(row)
    symbol = str(candidate.get("underlying_symbol") or row.get("underlying_symbol") or "").upper()
    eligibility = _runtime_signal_eligibility(runtime, row)
    policy_fields = resolve_runtime_policy_fields(
        profile=runtime.build_settings.scanner_profile,
        root_symbol=symbol,
    )
    return {
        **dict(row),
        "label": entry_engine_label(runtime),
        "market_date": market_date,
        "session_date": market_date,
        "root_symbol": symbol,
        "trading_strategy_id": runtime.trading_strategy_id,
        "strategy_run_id": strategy_run_id,
        "config_hash": runtime.config_hash,
        "strategy_family": runtime.trade_structure,
        "trade_structure": runtime.trade_structure,
        "profile": runtime.build_settings.scanner_profile,
        "style_profile": str(policy_fields["style_profile"]),
        "horizon_intent": str(policy_fields["horizon_intent"]),
        "product_class": str(policy_fields["product_class"]),
        "expiration_date": candidate.get("expiration_date"),
        "underlying_symbol": symbol,
        "selection_state": str(row.get("selection_state") or "monitor"),
        "selection_rank": (None if row.get("selection_rank") in (None, "") else int(row["selection_rank"])),
        "state_reason": str(row.get("state_reason") or "selected_runtime_signal"),
        "origin": "engine_selection",
        "eligibility": eligibility,
        "eligibility_state": eligibility,
        "promotion_score": candidate.get("promotion_score"),
        "execution_score": candidate.get("execution_score"),
        "confidence": candidate.get("confidence"),
        "created_at": generated_at,
        "updated_at": generated_at,
        "expires_at": None,
        "reason_codes": [str(row.get("state_reason") or "selected_runtime_signal")],
        "blockers": _signal_blockers(candidate, eligibility=eligibility),
        "legs": candidate_legs(candidate),
        "economics": candidate_economics(candidate),
        "strategy_metrics": candidate_strategy_metrics(candidate),
        "order_payload": dict(candidate.get("order_payload") or {}),
        "evidence": {
            "runtime_kind": "entry",
            "trading_strategy_id": runtime.trading_strategy_id,
            "trade_structure": runtime.trade_structure,
            "entry_recipe_refs": list(runtime.entry_recipe_refs),
            "trigger_policy": dict(runtime.trigger_policy),
            "execution_mode": runtime.strategy.execution.mode,
            "approval_mode": runtime.strategy.execution.approval,
            "selection_state": row.get("selection_state"),
            "selection_rank": row.get("selection_rank"),
            "generated_at": generated_at,
            "last_present_at": generated_at,
            **candidate_evidence_metrics(candidate),
            **candidate_policy_context(candidate),
        },
        "execution_shape": _execution_shape(candidate),
        "risk_hints": risk_hints(candidate),
        "source_cycle_id": strategy_run_id,
        "source_selection_state": row.get("selection_state"),
        "candidate_identity": _candidate_identity(candidate),
        "candidate": candidate,
    }


def _selection_summary(
    *,
    candidate_rows_by_symbol: dict[str, list[dict[str, Any]]],
    runtime_filter_reason_counts: dict[str, int],
    selected_rows: list[dict[str, Any]],
    selection_memory: dict[str, Any],
) -> dict[str, Any]:
    candidate_count = sum(len(rows) for rows in candidate_rows_by_symbol.values())
    signal_count = len(selected_rows)
    if signal_count:
        status = "signals_selected"
        message = f"{signal_count} signal{' was' if signal_count == 1 else 's were'} selected from {candidate_count} candidates."
    elif candidate_count:
        status = "no_entry_signals"
        message = "Candidates existed, but none cleared live selection for this strategy run."
    else:
        status = "no_candidates"
        message = "No candidates matched this strategy in the current run."
    return {
        "status": status,
        "message": message,
        "candidate_symbol_count": len(candidate_rows_by_symbol),
        "candidate_count": candidate_count,
        "signal_count": signal_count,
        "runtime_filter_reason_counts": dict(runtime_filter_reason_counts),
        "selection_memory": dict(selection_memory),
    }


def _refresh_entry_runtime_signals(
    *,
    db_target: str,
    storage: Any,
    runtime: EntryRuntime,
    market_date: str,
    run_key: str,
    planner_job_run_id: str | None,
) -> dict[str, Any]:
    generated_at = _utc_now()
    context = EngineContext(
        db_target=db_target,
        storage=storage,
        job_run_id=planner_job_run_id,
        metadata={"config_hash": runtime.config_hash},
    )
    data_engine = PostgresDataEngine(context)
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
        )
        return {
            "status": "skipped",
            "reason": "ticker_source_blocked",
            "ticker_set": ticker_summary,
            "candidate_build": _candidate_result_summary(None),
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
    symbol_candidates = _group_candidate_rows(candidate_result.candidates)
    runtime_filter_reason_counts = _candidate_result_runtime_filter_reason_counts(candidate_result)
    previous_promotable, previous_selection_memory = _read_previous_entry_selection(
        signal_store=storage.signals,
        runtime=runtime_with_symbols,
        session_date=market_date,
    )
    selection = select_live_signals(
        label=entry_engine_label(runtime_with_symbols),
        cycle_id=run_key,
        generated_at=generated_at,
        symbol_candidates=symbol_candidates,
        previous_promotable=previous_promotable,
        previous_selection_memory=previous_selection_memory,
        top_promotable=_entry_candidate_limit(runtime_with_symbols),
        top_monitor=ENTRY_MONITOR_LIMIT,
        profile=runtime_with_symbols.build_settings.scanner_profile,
        signal_cycle_context={
            "ticker_set": ticker_summary,
            "candidate_build": _candidate_result_summary(candidate_result),
        },
    )
    selected_rows = [
        _signal_row_from_selection(
            runtime=runtime_with_symbols,
            market_date=market_date,
            generated_at=generated_at,
            strategy_run_id=entry_engine_strategy_run_id(run_key, runtime_with_symbols.trading_strategy_id),
            row=dict(row),
        )
        for row in list(selection.get("signals") or [])
        if isinstance(row, dict)
    ]
    selection_memory = dict(selection.get("selection_memory") or {})
    selection_summary = _selection_summary(
        candidate_rows_by_symbol=symbol_candidates,
        runtime_filter_reason_counts=runtime_filter_reason_counts,
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
            **selection_summary,
            "selected_signal_rows": selected_rows,
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
    )
    signal_refs_by_key = {
        (
            str(ref.get("underlying_symbol") or "").upper(),
            str(ref.get("candidate_identity") or ""),
        ): str(ref["trade_signal_id"])
        for ref in list(engine_fact_summary.get("trade_signals") or [])
        if isinstance(ref, dict) and ref.get("trade_signal_id") not in (None, "")
    }
    signals = []
    for row in selected_rows:
        key = (str(row.get("underlying_symbol") or "").upper(), _candidate_identity_from_signal(row))
        trade_signal_id = signal_refs_by_key.get(key) or _trade_signal_id_for_signal(
            candidate_generation={"engine_facts": engine_fact_summary},
            runtime=runtime_with_symbols,
            market_date=market_date,
            signal=row,
        )
        if trade_signal_id is None:
            continue
        signals.append({**row, "trade_signal_id": trade_signal_id})
    return {
        "status": "ok",
        "reason": None,
        "ticker_set": ticker_summary,
        "candidate_build": _candidate_result_summary(candidate_result),
        "strategy_run": {
            "strategy_run_id": strategy_run.get("strategy_run_id"),
            "signal_count": len(signals),
            "selection_summary": selection_summary,
        },
        "engine_facts": engine_fact_summary,
        "signals": signals,
    }


def _normalized_blockers(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    blockers: list[str] = []
    for item in value:
        rendered = str(item or "").strip()
        if rendered and rendered not in blockers:
            blockers.append(rendered)
    return blockers


def _selected_execution_admission(
    *,
    execution_store: Any,
    runtime: Any,
    signal: dict[str, Any],
) -> dict[str, Any]:
    position_size_policy = resolve_position_size_policy(getattr(runtime.build_settings, "risk_defaults", {}))
    try:
        return build_execution_admission_snapshot(
            execution_store=execution_store,
            candidate=signal,
            limit_price=None,
            strategy_risk_budget=position_size_policy["max_risk_per_trade"],
            position_size_pct_of_available_balance=position_size_policy["position_size_pct_of_available_balance"],
        )
    except Exception as exc:
        return {
            "status": "unknown",
            "reason": "execution_admission_unavailable",
            "message": str(exc),
            "evaluated_at": _utc_now(),
            "admissible_quantity": None,
            "required_buying_power": None,
            "available_buying_power": None,
            "account_available_buying_power": None,
            "reserved_buying_power": None,
            "buying_power_basis": None,
            "buying_power_source_field": None,
            "broker_buying_power_status": None,
            "limiting_constraint": None,
            "strategy_risk_budget": position_size_policy["max_risk_per_trade"],
            "position_size_pct_of_available_balance": position_size_policy["position_size_pct_of_available_balance"],
            "position_size_budget": None,
        }


def _run_trading_strategy_entry(
    *,
    db_target: str,
    trading_strategy_id: str,
    market_date: str | None = None,
    planner_job_run_id: str | None = None,
    run_key: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    signal_store = storage.signals
    execution_store = storage.execution
    job_store = storage.jobs
    if not signal_store.schema_ready():
        return {"status": "skipped", "reason": "signal_schema_unavailable"}
    if not signal_store.strategy_runtime_schema_ready():
        return {"status": "skipped", "reason": "strategy_runtime_schema_unavailable"}
    if not execution_store.intent_schema_ready():
        return {"status": "skipped", "reason": "execution_intent_schema_unavailable"}
    engine_facts = getattr(storage, "engine_facts", None)
    if engine_facts is None or not engine_facts.schema_ready():
        return {"status": "skipped", "reason": "engine_fact_schema_unavailable"}

    runtime = resolve_entry_runtime(trading_strategy_id=trading_strategy_id)
    if runtime.strategy.entry is None or not routine_should_run_now(runtime.strategy.entry):
        return {
            "status": "skipped",
            "reason": "outside_schedule_window",
            "trading_strategy_id": runtime.trading_strategy_id,
        }

    resolved_market_date = market_date or _market_date_today()
    run_key = run_key or f"strategy:{runtime.trading_strategy_id}:entry:{_utc_now()}"
    scope_key = f"entry:{runtime.trading_strategy_id}:{resolved_market_date}"
    policy_ref = build_runtime_policy_ref(
        trading_strategy_id=runtime.trading_strategy_id,
        trade_structure=runtime.trade_structure,
        routine="entry",
        market_date=resolved_market_date,
    )
    candidate_generation = _refresh_entry_runtime_signals(
        db_target=db_target,
        storage=storage,
        runtime=runtime,
        market_date=resolved_market_date,
        run_key=run_key,
        planner_job_run_id=planner_job_run_id,
    )
    if str(candidate_generation.get("status") or "") == "skipped":
        return {
            "status": "skipped",
            "reason": candidate_generation.get("reason"),
            "trading_strategy_id": runtime.trading_strategy_id,
            "market_date": resolved_market_date,
            "run_key": run_key,
            "candidate_generation": candidate_generation,
        }
    signals = [dict(row) for row in list(candidate_generation.get("signals") or []) if isinstance(row, dict)]
    min_score = float(runtime.trigger_policy.get("min_signal_score") or 0.0)
    controls_allowed, controls_reason, strategy_metrics = evaluate_trading_strategy_entry_controls(
        storage=storage,
        strategy=runtime.strategy,
        market_date=resolved_market_date,
    )
    plan = plan_entry_selection(
        signals=signals,
        controls_allowed=controls_allowed,
        controls_reason=controls_reason,
        bot_metrics=strategy_metrics,
        min_score=min_score,
        eligible_selection_states=("promotable",),
    )
    selected = plan["selected"]

    decisions: list[dict[str, Any]] = []
    admissions: list[dict[str, Any]] = []
    selected_intent: dict[str, Any] | None = None
    selected_decision: dict[str, Any] | None = None
    selected_signal: dict[str, Any] | None = None
    selected_execution_admission: dict[str, Any] | None = None
    dispatch_job_run_id: str | None = None
    for decision_plan, signal in zip(plan["decisions"], signals, strict=False):
        slot_key = _slot_key(
            runtime.trading_strategy_id,
            str(signal.get("underlying_symbol") or ""),
        )
        trade_signal_id = _trade_signal_id_for_signal(
            candidate_generation=candidate_generation,
            runtime=runtime,
            market_date=resolved_market_date,
            signal=signal,
        )
        if trade_signal_id is None:
            continue
        trade_decision_state = _trade_decision_state(decision_plan["state"])
        reason_codes = list(decision_plan["reason_codes"])
        evidence = {
            "policy_ref": policy_ref,
            "decision_plan": dict(decision_plan["payload"]),
            "candidate_identity": _candidate_identity_from_signal(signal),
            "underlying_symbol": signal.get("underlying_symbol"),
            "candidate_generation": {
                "candidate_run_id": (
                    (candidate_generation.get("engine_facts") or {}).get("candidate_run_id")
                    if isinstance(candidate_generation.get("engine_facts"), dict)
                    else None
                ),
                "ticker_source_run_id": (
                    (candidate_generation.get("engine_facts") or {}).get("ticker_source_run_id")
                    if isinstance(candidate_generation.get("engine_facts"), dict)
                    else None
                ),
            },
        }
        if str(decision_plan["state"]) == "selected":
            existing_active = execution_store.list_execution_intents(
                slot_key=slot_key,
                states=sorted(ACTIVE_INTENT_STATES),
                limit=1,
            )
            if existing_active:
                trade_decision_state = "selected_blocked"
                reason_codes = ["active_execution_intent_exists"]
                evidence["slot_key"] = slot_key
        trade_decision_id = _trade_decision_id(run_key, trade_signal_id)
        decision = engine_facts.upsert_trade_decision(
            trade_decision_id=trade_decision_id,
            trade_signal_id=trade_signal_id,
            trading_strategy_id=runtime.trading_strategy_id,
            trade_structure=runtime.trade_structure,
            routine="entry",
            config_hash=runtime.config_hash,
            run_key=run_key,
            scope_key=scope_key,
            decision_state=trade_decision_state,
            score=float(decision_plan["score"]),
            rank=int(decision_plan["rank"]),
            selected_quantity=None,
            selected_execution_shape=dict(signal.get("execution_shape") or {}),
            reason_codes=reason_codes,
            blockers=(reason_codes if trade_decision_state in {"skip", "selected_blocked"} else []),
            evidence=evidence,
            metrics=dict(strategy_metrics),
            supersedes_decision_id=None,
            superseded_by_decision_id=None,
            decided_at=_utc_now(),
        )
        decisions.append(decision)
        if trade_decision_state == "selected_blocked":
            continue
        if trade_decision_state != "selected":
            continue
        selected_execution_admission = _selected_execution_admission(
            execution_store=execution_store,
            runtime=runtime,
            signal=signal,
        )
        execution_intent_id = _intent_id(str(decision["trade_decision_id"]))
        intent_expires_at = _expires_in(ENTRY_INTENT_TTL_MINUTES)
        selected_admission = _persist_trade_admission(
            engine_facts=engine_facts,
            runtime=runtime,
            market_date=resolved_market_date,
            policy_ref=policy_ref,
            trade_signal_id=trade_signal_id,
            trade_decision_id=str(decision["trade_decision_id"]),
            execution_intent_id=execution_intent_id,
            slot_key=slot_key,
            admission_snapshot=selected_execution_admission,
            signal=signal,
            expires_at=intent_expires_at,
        )
        admissions.append(selected_admission)
        if not admission_allows_attempt(selected_admission):
            selected_decision = decision
            selected_signal = signal
            continue
        selected_intent = issue_pending_execution_intent(
            execution_store,
            execution_intent_id=execution_intent_id,
            trading_strategy_id=runtime.trading_strategy_id,
            trade_signal_id=trade_signal_id,
            trade_decision_id=str(decision["trade_decision_id"]),
            strategy_position_id=None,
            execution_attempt_id=None,
            action_type="open",
            slot_key=slot_key,
            claim_token=None,
            policy_ref=policy_ref,
            config_hash=runtime.config_hash,
            state="pending",
            expires_at=intent_expires_at,
            superseded_by_id=None,
            payload={
                "trade_signal_id": trade_signal_id,
                "trade_decision_id": decision["trade_decision_id"],
                "admission_decision_id": selected_admission["admission_decision_id"],
                "underlying_symbol": signal.get("underlying_symbol"),
                "candidate_identity": _candidate_identity_from_signal(signal),
                "execution_mode": runtime.strategy.execution.mode,
                "approval_mode": runtime.strategy.execution.approval,
                "execution_runtime": runtime.strategy.execution.runtime,
                "execution_admission": selected_admission,
                "exit_policy": build_exit_policy_from_recipe_refs(tuple(runtime.strategy.management_recipe_refs)),
            },
            created_event_payload={
                "trade_signal_id": trade_signal_id,
                "trade_decision_id": decision["trade_decision_id"],
                "admission_decision_id": selected_admission["admission_decision_id"],
                "slot_key": slot_key,
                "execution_runtime": runtime.strategy.execution.runtime,
            },
        )
        selected_decision = decision
        selected_signal = signal
    if selected_intent is not None:
        dispatch_request = request_execution_intent_dispatch(
            job_store=job_store,
            requested_by={
                "reason": "entry_intent_created",
                "execution_intent_id": str(selected_intent["execution_intent_id"]),
                "trading_strategy_id": runtime.trading_strategy_id,
            },
        )
        if dispatch_request is not None:
            dispatch_job_run_id = None if dispatch_request.get("job_run_id") in (None, "") else str(dispatch_request["job_run_id"])
            execution_store.append_execution_intent_event(
                execution_intent_id=str(selected_intent["execution_intent_id"]),
                event_type=("dispatch_requested" if str(dispatch_request.get("status") or "") == "queued" else "dispatch_request_failed"),
                event_at=_utc_now(),
                payload={
                    "job_run_id": dispatch_job_run_id,
                    "job_key": dispatch_request.get("job_key"),
                    "status": dispatch_request.get("status"),
                    "error": dispatch_request.get("error"),
                },
            )

    runtime_alert: dict[str, Any] | None = None
    if selected_intent is not None and selected_decision is not None and selected_signal is not None:
        try:
            runtime_alert = plan_runtime_entry_selected_alert(
                alert_store=getattr(storage, "alerts", None),
                job_store=getattr(storage, "jobs", None),
                trading_strategy_id=runtime.trading_strategy_id,
                market_date=resolved_market_date,
                run_key=run_key,
                trade_signal=selected_signal,
                decision=selected_decision,
                execution_intent=selected_intent,
                execution_mode=runtime.strategy.execution.mode,
                approval_mode=runtime.strategy.execution.approval,
                planner_job_run_id=planner_job_run_id,
                dispatch_job_run_id=dispatch_job_run_id,
            )
        except Exception as exc:
            runtime_alert = {"status": "failed", "error": str(exc)}

    return {
        "status": "ok",
        "trading_strategy_id": runtime.trading_strategy_id,
        "market_date": resolved_market_date,
        "run_key": run_key,
        "signal_count": len(signals),
        "decision_count": len(decisions),
        "admission_count": len(admissions),
        "trade_decision_ids": [str(decision["trade_decision_id"]) for decision in decisions if decision.get("trade_decision_id") not in (None, "")],
        "selected_decision_ids": [
            str(decision["trade_decision_id"])
            for decision in decisions
            if decision.get("trade_decision_id") not in (None, "") and str(decision.get("decision_state") or "") == "selected"
        ],
        "selected_trade_signal_id": None if selected is None else str(selected.get("trade_signal_id")),
        "execution_intent_id": None if selected_intent is None else str(selected_intent.get("execution_intent_id")),
        "execution_admission": selected_execution_admission,
        "admission_decision_id": None if not admissions else admissions[-1].get("admission_decision_id"),
        "dispatch_job_run_id": dispatch_job_run_id,
        "runtime_alert": runtime_alert,
        "candidate_generation": candidate_generation,
    }


class PostgresStrategyEngine:
    def __init__(self, context: EngineContext) -> None:
        self.context = context

    def run_entry(self, request: StrategyEntryRequest) -> StrategyEntryResult:
        summary = _run_trading_strategy_entry(
            db_target=self.context.db_target,
            trading_strategy_id=request.trading_strategy_id,
            market_date=request.market_date.isoformat(),
            planner_job_run_id=request.run_ref.job_run_id,
            run_key=request.run_ref.run_id,
            storage=self.context.storage,
        )
        candidate_generation = summary.get("candidate_generation") if isinstance(summary.get("candidate_generation"), dict) else {}
        engine_facts = candidate_generation.get("engine_facts") if isinstance(candidate_generation.get("engine_facts"), dict) else {}
        strategy_run = candidate_generation.get("strategy_run") if isinstance(candidate_generation.get("strategy_run"), dict) else {}
        decisions = [dict(row) for row in list(engine_facts.get("trade_decisions") or []) if isinstance(row, dict)]
        return StrategyEntryResult(
            run_ref=request.run_ref,
            strategy_run_id=str(strategy_run.get("strategy_run_id") or summary.get("run_key") or request.run_ref.run_id),
            trade_signal_ids=tuple(
                str(row["trade_signal_id"])
                for row in list(engine_facts.get("trade_signals") or [])
                if isinstance(row, dict) and row.get("trade_signal_id") not in (None, "")
            ),
            trade_decision_ids=tuple(str(value) for value in list(summary.get("trade_decision_ids") or []))
            or tuple(str(row["trade_decision_id"]) for row in decisions if row.get("trade_decision_id") not in (None, "")),
            selected_decision_ids=tuple(str(value) for value in list(summary.get("selected_decision_ids") or [])),
            status=str(summary.get("status") or "unknown"),
            reason=None if summary.get("reason") in (None, "") else str(summary["reason"]),
            summary=summary,
        )


@with_storage()
def run_trading_strategy_entry(
    *,
    db_target: str,
    trading_strategy_id: str,
    market_date: str | None = None,
    planner_job_run_id: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    resolved_market_date = datetime.fromisoformat(market_date).date() if market_date else datetime.now(UTC).date()
    context = EngineContext(
        db_target=db_target,
        storage=storage,
        job_run_id=planner_job_run_id,
    )
    result = PostgresStrategyEngine(context).run_entry(
        StrategyEntryRequest(
            run_ref=EngineRunRef(
                role=EngineComponentRole.STRATEGY,
                run_id=f"strategy:{trading_strategy_id}:entry:{_utc_now()}",
                trading_strategy_id=trading_strategy_id,
                job_run_id=planner_job_run_id,
            ),
            trading_strategy_id=trading_strategy_id,
            market_date=resolved_market_date,
        )
    )
    return dict(result.summary)


__all__ = ["PostgresStrategyEngine", "run_trading_strategy_entry"]
