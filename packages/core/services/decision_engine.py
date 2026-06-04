from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any

from core.alerts.runtime import plan_runtime_entry_selected_alert
from core.db.decorators import with_storage
from core.services.entry_planner import plan_entry_selection, score_opportunity
from core.services.execution_intents import request_execution_intent_dispatch
from core.services.execution_intents.shared import (
    ACTIVE_INTENT_STATES,
    issue_pending_execution_intent,
)
from core.services.management_recipes import build_exit_policy_from_recipe_refs
from core.services.option_structures import normalize_strategy_family
from core.services.opportunity_generation import sync_entry_runtime_opportunities
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
from core.services.trading_engine.kernel import EngineComponentRole, EngineContext, EngineRunRef
from core.services.trading_strategies import load_active_trading_strategies, routine_should_run_now
from core.services.trading_strategy_runtime import EntryRuntime, resolve_entry_runtime

ENTRY_INTENT_TTL_MINUTES = 5
ENTRY_MONITOR_LIMIT = 12


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _expires_in(minutes: int) -> str:
    return (datetime.now(UTC) + timedelta(minutes=max(minutes, 1))).isoformat(timespec="seconds").replace("+00:00", "Z")


def _market_date_today() -> str:
    return datetime.now(UTC).date().isoformat()


def _decision_id(run_key: str, opportunity_id: str) -> str:
    material = f"{run_key}|{opportunity_id}".encode("utf-8")
    return f"opportunity_decision:{hashlib.sha1(material).hexdigest()[:24]}"


def _intent_id(opportunity_decision_id: str) -> str:
    return f"execution_intent:{opportunity_decision_id}"


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
        "source_type": ticker_set.source.source_type,
        "source_ref": ticker_set.source.ref,
        "source_run_id": ticker_set.source_run_id,
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
            "opportunity_count": 0,
        },
        config_hash=runtime.config_hash,
    )


def _refresh_entry_runtime_opportunities(
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
            "strategy_sync": {},
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
        build_policy={
            "entry_runtime": runtime_with_symbols,
            "top": _entry_candidate_limit(runtime_with_symbols),
            "per_runtime_limit": _entry_candidate_limit(runtime_with_symbols),
            "per_symbol_top": 1,
            "greeks_source": "auto",
        },
        source_evidence=ticker_set.evidence,
    )
    candidate_result = data_engine.build_entry_trade_candidates(
        request=candidate_request,
        runtime=runtime_with_symbols,
    )
    symbol_candidates = _group_candidate_rows(candidate_result.candidates)
    strategy_sync = sync_entry_runtime_opportunities(
        signal_store=storage.signals,
        label=entry_engine_label(runtime_with_symbols),
        session_date=market_date,
        generated_at=generated_at,
        cycle_id=run_key,
        entry_runtimes=[runtime_with_symbols],
        symbol_candidates=symbol_candidates,
        runtime_candidate_rows_by_owner=None,
        persisted_opportunities=[],
        job_run_id=planner_job_run_id,
        top_promotable=_entry_candidate_limit(runtime_with_symbols),
        top_monitor=ENTRY_MONITOR_LIMIT,
        selection_memory=None,
        signal_cycle_context={
            "ticker_set": ticker_summary,
            "candidate_build": _candidate_result_summary(candidate_result),
        },
        trigger_type="trading_strategy_entry",
    )
    return {
        "status": "ok",
        "reason": None,
        "ticker_set": ticker_summary,
        "candidate_build": _candidate_result_summary(candidate_result),
        "strategy_sync": {
            "strategy_runs_upserted": int(strategy_sync.get("strategy_runs_upserted") or 0),
            "runtime_opportunities_upserted": int(strategy_sync.get("runtime_opportunities_upserted") or 0),
            "runtime_opportunities_expired": int(strategy_sync.get("runtime_opportunities_expired") or 0),
            "opportunity_count": len(list(strategy_sync.get("opportunities") or [])),
        },
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
    opportunity: dict[str, Any],
) -> dict[str, Any]:
    position_size_policy = resolve_position_size_policy(getattr(runtime.build_settings, "risk_defaults", {}))
    try:
        return build_execution_admission_snapshot(
            execution_store=execution_store,
            candidate=opportunity,
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


def _opportunity_blockers(row: dict[str, Any]) -> list[str]:
    blockers = _normalized_blockers(row.get("blockers"))
    candidate = row.get("candidate")
    if isinstance(candidate, dict):
        for field in ("scoring_blockers", "execution_blockers"):
            for blocker in _normalized_blockers(candidate.get(field)):
                if blocker not in blockers:
                    blockers.append(blocker)
    evidence = row.get("evidence")
    if isinstance(evidence, dict):
        for blocker in _normalized_blockers(evidence.get("execution_blockers")):
            if blocker not in blockers:
                blockers.append(blocker)
    return blockers


def _matching_opportunities(
    *,
    signal_store: Any,
    market_date: str,
    symbols: tuple[str, ...] | None = None,
    strategy_family: str | None = None,
    allowed_labels: set[str] | None = None,
    trading_strategy_id: str | None = None,
    trade_structure: str | None = None,
    runtime_owned: bool | None = None,
) -> list[dict[str, Any]]:
    rows = [
        dict(row)
        for row in signal_store.list_opportunities(
            market_date=market_date,
            eligibility_state="live",
            trading_strategy_id=trading_strategy_id,
            trade_structure=trade_structure,
            runtime_owned=runtime_owned,
            limit=500,
        )
    ]
    allowed_symbols = set(symbols or ())
    filtered = [
        row
        for row in rows
        if (strategy_family is None or normalize_strategy_family(row.get("strategy_family")) == strategy_family)
        and (not allowed_symbols or str(row.get("underlying_symbol") or "").upper() in allowed_symbols)
        and (not allowed_labels or str(row.get("label") or "") in allowed_labels)
        and not _opportunity_blockers(row)
        and str(row.get("lifecycle_state") or "") in {"candidate", "ready", "blocked"}
        and row.get("consumed_by_execution_attempt_id") in (None, "")
    ]
    filtered.sort(
        key=lambda row: (
            -score_opportunity(row),
            int(row.get("selection_rank") or 999999),
            str(row.get("opportunity_id") or ""),
        )
    )
    return filtered


@with_storage()
def run_trading_strategy_entry_decision(
    *,
    db_target: str,
    trading_strategy_id: str,
    market_date: str | None = None,
    planner_job_run_id: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    signal_store = storage.signals
    execution_store = storage.execution
    job_store = storage.jobs
    if not signal_store.schema_ready() or not signal_store.decision_schema_ready():
        return {"status": "skipped", "reason": "signal_decision_schema_unavailable"}
    if not signal_store.strategy_runtime_schema_ready():
        return {"status": "skipped", "reason": "strategy_runtime_schema_unavailable"}
    if not execution_store.intent_schema_ready():
        return {"status": "skipped", "reason": "execution_intent_schema_unavailable"}

    runtime = resolve_entry_runtime(trading_strategy_id=trading_strategy_id)
    if runtime.strategy.entry is None or not routine_should_run_now(runtime.strategy.entry):
        return {
            "status": "skipped",
            "reason": "outside_schedule_window",
            "trading_strategy_id": runtime.trading_strategy_id,
        }

    resolved_market_date = market_date or _market_date_today()
    run_key = f"decision:{runtime.trading_strategy_id}:entry:{_utc_now()}"
    scope_key = f"entry:{runtime.trading_strategy_id}:{resolved_market_date}"
    policy_ref = build_runtime_policy_ref(
        trading_strategy_id=runtime.trading_strategy_id,
        trade_structure=runtime.trade_structure,
        routine="entry",
        market_date=resolved_market_date,
    )
    candidate_generation = _refresh_entry_runtime_opportunities(
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
    opportunities = _matching_opportunities(
        signal_store=signal_store,
        market_date=resolved_market_date,
        trading_strategy_id=runtime.trading_strategy_id,
        trade_structure=runtime.trade_structure,
        runtime_owned=True,
    )
    min_score = float(runtime.trigger_policy.get("min_opportunity_score") or 0.0)
    controls_allowed, controls_reason, strategy_metrics = evaluate_trading_strategy_entry_controls(
        storage=storage,
        strategy=runtime.strategy,
        market_date=resolved_market_date,
    )
    plan = plan_entry_selection(
        opportunities=opportunities,
        controls_allowed=controls_allowed,
        controls_reason=controls_reason,
        bot_metrics=strategy_metrics,
        min_score=min_score,
        eligible_selection_states=("promotable",),
    )
    selected = plan["selected"]

    decisions: list[dict[str, Any]] = []
    selected_intent: dict[str, Any] | None = None
    selected_decision: dict[str, Any] | None = None
    selected_opportunity: dict[str, Any] | None = None
    selected_execution_admission: dict[str, Any] | None = None
    dispatch_job_run_id: str | None = None
    for decision_plan, opportunity in zip(plan["decisions"], opportunities, strict=False):
        opportunity_id = str(opportunity["opportunity_id"])
        decision = signal_store.upsert_opportunity_decision(
            opportunity_decision_id=_decision_id(run_key, opportunity_id),
            opportunity_id=opportunity_id,
            trading_strategy_id=runtime.trading_strategy_id,
            run_key=run_key,
            scope_key=scope_key,
            policy_ref=policy_ref,
            config_hash=runtime.config_hash,
            state=str(decision_plan["state"]),
            score=float(decision_plan["score"]),
            rank=int(decision_plan["rank"]),
            reason_codes=list(decision_plan["reason_codes"]),
            superseded_by_id=None,
            decided_at=_utc_now(),
            payload=dict(decision_plan["payload"]),
        )
        decisions.append(decision)
        if str(decision_plan["state"]) != "selected":
            continue
        slot_key = _slot_key(
            runtime.trading_strategy_id,
            str(opportunity.get("underlying_symbol") or ""),
        )
        existing_active = execution_store.list_execution_intents(
            slot_key=slot_key,
            states=sorted(ACTIVE_INTENT_STATES),
            limit=1,
        )
        if existing_active:
            signal_store.upsert_opportunity_decision(
                opportunity_decision_id=str(decision["opportunity_decision_id"]),
                opportunity_id=opportunity_id,
                trading_strategy_id=runtime.trading_strategy_id,
                run_key=run_key,
                scope_key=scope_key,
                policy_ref=policy_ref,
                config_hash=runtime.config_hash,
                state="blocked",
                score=float(decision_plan["score"]),
                rank=int(decision_plan["rank"]),
                reason_codes=["active_execution_intent_exists"],
                superseded_by_id=None,
                decided_at=_utc_now(),
                payload={"slot_key": slot_key},
            )
            continue
        selected_execution_admission = _selected_execution_admission(
            execution_store=execution_store,
            runtime=runtime,
            opportunity=opportunity,
        )
        selected_intent = issue_pending_execution_intent(
            execution_store,
            execution_intent_id=_intent_id(str(decision["opportunity_decision_id"])),
            trading_strategy_id=runtime.trading_strategy_id,
            opportunity_decision_id=str(decision["opportunity_decision_id"]),
            strategy_position_id=None,
            execution_attempt_id=None,
            action_type="open",
            slot_key=slot_key,
            claim_token=None,
            policy_ref=policy_ref,
            config_hash=runtime.config_hash,
            state="pending",
            expires_at=_expires_in(ENTRY_INTENT_TTL_MINUTES),
            superseded_by_id=None,
            payload={
                "opportunity_id": opportunity_id,
                "opportunity_expires_at": opportunity.get("expires_at"),
                "underlying_symbol": opportunity.get("underlying_symbol"),
                "execution_mode": runtime.strategy.execution.mode,
                "approval_mode": runtime.strategy.execution.approval,
                "execution_runtime": runtime.strategy.execution.runtime,
                "execution_admission": selected_execution_admission,
                "exit_policy": build_exit_policy_from_recipe_refs(tuple(runtime.strategy.management_recipe_refs)),
            },
            created_event_payload={
                "opportunity_id": opportunity_id,
                "slot_key": slot_key,
                "execution_runtime": runtime.strategy.execution.runtime,
            },
        )
        selected_decision = decision
        selected_opportunity = opportunity
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
    if selected_intent is not None and selected_decision is not None and selected_opportunity is not None:
        try:
            runtime_alert = plan_runtime_entry_selected_alert(
                alert_store=getattr(storage, "alerts", None),
                job_store=getattr(storage, "jobs", None),
                trading_strategy_id=runtime.trading_strategy_id,
                market_date=resolved_market_date,
                run_key=run_key,
                opportunity=selected_opportunity,
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
        "opportunity_count": len(opportunities),
        "decision_count": len(decisions),
        "selected_opportunity_id": None if selected is None else str(selected.get("opportunity_id")),
        "execution_intent_id": None if selected_intent is None else str(selected_intent.get("execution_intent_id")),
        "execution_admission": selected_execution_admission,
        "dispatch_job_run_id": dispatch_job_run_id,
        "runtime_alert": runtime_alert,
        "candidate_generation": candidate_generation,
    }


@with_storage()
def run_active_entry_decisions(
    *,
    db_target: str,
    market_date: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    strategies = load_active_trading_strategies()
    results: list[dict[str, Any]] = []
    for strategy in strategies.values():
        if strategy.entry is None or not strategy.entry.enabled:
            continue
        results.append(
            run_trading_strategy_entry_decision(
                db_target=db_target,
                trading_strategy_id=strategy.trading_strategy_id,
                market_date=market_date,
                storage=storage,
            )
        )
    return {
        "status": "ok",
        "decision_runs": results,
        "decision_run_count": len(results),
    }


__all__ = ["run_active_entry_decisions", "run_trading_strategy_entry_decision"]
