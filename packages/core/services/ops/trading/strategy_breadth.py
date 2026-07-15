from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import datetime
from typing import Any


from core.services.trading_strategies import load_trading_strategies
from core.value_coercion import (
    as_mapping,
    as_text,
    coerce_int,
)



from core.services.ops.trading.models import (
    SOURCE_SYMBOL_LIMIT,
    _StrategyBreadthProjection,
)
from core.services.ops.trading.execution_contract import _strategy_execution_contract
from core.services.ops.trading.flow_facts import _candidate_state, _latest_flow_facts

def _strategy_not_active_reasons(strategy: Any, *, active: bool) -> list[str]:
    if active:
        return []
    reasons: list[str] = []
    if not bool(strategy.enabled):
        reasons.append("strategy_disabled")
    if bool(strategy.paused):
        reasons.append("strategy_paused")
    return reasons or ["strategy_not_scheduled"]


def _strategy_ops_posture(strategy: Any, *, active: bool) -> str:
    if active:
        return "active"
    if bool(strategy.paused):
        return "paused"
    mode = str(strategy.execution.mode or "shadow").strip().lower()
    if mode == "shadow":
        return "shadow_observation_candidate"
    if mode == "paper":
        return "paper_observation_candidate"
    if mode == "live":
        return "live_observation_candidate"
    return "observation_candidate"


def _strategy_not_active_message(reasons: list[str]) -> str | None:
    if not reasons:
        return None
    if "strategy_disabled" in reasons:
        return (
            "Authored strategy is disabled; listed for strategy-breadth observation only. "
            "No routine schedules, candidates, intents, or broker submission will be created."
        )
    if "strategy_paused" in reasons:
        return (
            "Authored strategy is paused; listed for operator context only. "
            "No routine schedules, candidates, intents, or broker submission will be created."
        )
    return "Strategy is not scheduled; no jobs, candidates, intents, or broker submission will be created."


def _strategy_routine_breadth_payload(routine: Any | None) -> dict[str, Any] | None:
    if routine is None:
        return None
    return {
        "enabled": routine.enabled,
        "schedule": routine.schedule.as_dict(),
        "quality_profile_id": routine.quality.profile_id,
        "quality_overrides": dict(routine.quality.overrides),
        "recipes": list(routine.recipes),
    }


def _latest_strategy_run_payloads(
    *,
    storage: Any,
    market_date: str,
    strategy_ids: set[str],
) -> dict[str, dict[str, Any]]:
    if not storage.signals.schema_ready():
        return {}
    latest_runs: dict[str, dict[str, Any]] = {}
    for strategy_id in sorted(strategy_ids):
        rows = storage.signals.list_strategy_runs(
            trading_strategy_id=strategy_id,
            session_date=market_date,
            limit=1,
        )
        if rows:
            latest_runs[strategy_id] = dict(rows[0])
    return latest_runs


def _latest_candidate_run_summary(candidate_run: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if candidate_run is None:
        return None
    return {
        "candidate_run_id": candidate_run.get("candidate_run_id"),
        "run_key": candidate_run.get("run_key"),
        "status": candidate_run.get("status"),
        "generated_at": candidate_run.get("generated_at"),
        "completed_at": candidate_run.get("completed_at"),
        "ticker_source_run_id": candidate_run.get("ticker_source_run_id"),
        "ticker_source_id": candidate_run.get("ticker_source_id"),
        "symbol_count": candidate_run.get("symbol_count"),
        "candidate_count": candidate_run.get("candidate_count"),
        "diagnostic_status": as_mapping(candidate_run.get("summary")).get("diagnostic_status"),
        "selection_counts": dict(as_mapping(candidate_run.get("selection_counts"))),
        "admission_counts": dict(as_mapping(candidate_run.get("admission_counts"))),
    }


def _strategy_latest_observation_state(
    *,
    strategy: Any,
    candidate_run: Mapping[str, Any] | None,
    strategy_run: Mapping[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    entry_cadence_minutes = None if strategy.entry is None else strategy.entry.schedule.cadence_minutes
    candidate_state = _candidate_state(
        candidate_run=candidate_run,
        source_state=None,
        cadence_minutes=entry_cadence_minutes,
        market_open=False,
        now=now,
    )
    run_result = as_mapping(None if strategy_run is None else strategy_run.get("result"))
    entry_selection = as_mapping(run_result.get("entry_selection"))
    selection_counts = as_mapping(None if candidate_run is None else candidate_run.get("selection_counts"))
    admission_counts = as_mapping(None if candidate_run is None else candidate_run.get("admission_counts"))
    if strategy_run is None and candidate_run is None:
        status = "missing"
        reason = "observation_run_missing"
    else:
        status = as_text(None if strategy_run is None else strategy_run.get("status")) or str(candidate_state.get("status") or "observed")
        reason = as_text(run_result.get("reason")) or as_text(candidate_state.get("reason"))
    return {
        "status": status,
        "reason": reason,
        "entry_run_mode": as_text(run_result.get("entry_run_mode")),
        "validation_provenance": as_text(run_result.get("validation_provenance")),
        "observation_only": bool(run_result.get("observation_only")),
        "strategy_run_id": None if strategy_run is None else strategy_run.get("strategy_run_id"),
        "candidate_run_id": None if candidate_run is None else candidate_run.get("candidate_run_id"),
        "generated_at": candidate_state.get("latest_run", {}).get("generated_at") if isinstance(candidate_state.get("latest_run"), Mapping) else None,
        "age_seconds": candidate_state.get("age_seconds"),
        "candidate_count": candidate_state.get("candidate_count"),
        "signal_count": coerce_int(run_result.get("signal_count")) or 0,
        "selected_candidate_count": coerce_int(entry_selection.get("selected_candidate_count")) or 0,
        "monitored_candidate_count": coerce_int(entry_selection.get("monitored_candidate_count")) or 0,
        "rejected_candidate_count": coerce_int(entry_selection.get("rejected_candidate_count")) or 0,
        "decision_state_counts": dict(selection_counts),
        "admission_state_counts": dict(admission_counts),
        "quality_profile_id": candidate_state.get("quality_profile_id"),
        "top_rejection_counts": dict(as_mapping(candidate_state.get("top_rejection_counts"))),
        "latest_strategy_run": None if strategy_run is None else dict(strategy_run),
        "latest_candidate_run": _latest_candidate_run_summary(candidate_run),
    }


def _strategy_breadth_row(
    *,
    strategy: Any,
    active_strategy_ids: set[str],
    broker_environment: str,
    broker_environment_source: str,
    latest_candidate_run: Mapping[str, Any] | None,
    latest_strategy_run: Mapping[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    active = strategy.trading_strategy_id in active_strategy_ids
    reasons = _strategy_not_active_reasons(strategy, active=active)
    ops_posture = _strategy_ops_posture(strategy, active=active)
    execution_contract = _strategy_execution_contract(
        strategy=strategy,
        broker_environment=broker_environment,
        broker_environment_source=broker_environment_source,
        now=now,
    )
    configured_automatic_submission_allowed = bool(execution_contract.get("automatic_submission_allowed"))
    execution_contract = {
        **execution_contract,
        "configured_automatic_submission_allowed": configured_automatic_submission_allowed,
        "automatic_submission_allowed": bool(active and configured_automatic_submission_allowed),
        "scheduled_active": active,
        "observation_only": not active,
        "rollout_blocker": None if active else reasons[0],
    }
    source_symbols = list(strategy.symbols[:SOURCE_SYMBOL_LIMIT])
    return {
        "trading_strategy_id": strategy.trading_strategy_id,
        "name": strategy.name,
        "trade_structure": strategy.trade_structure,
        "candidate_builder_key": strategy.candidate_builder_key,
        "build_profile": strategy.build_profile,
        "enabled": strategy.enabled,
        "paused": strategy.paused,
        "active": active,
        "status": "active" if active else ("paused" if "strategy_paused" in reasons else "available"),
        "ops_posture": ops_posture,
        "observation_only": not active,
        "scheduled_active": active,
        "not_active_reason": None if active else reasons[0],
        "not_active_reasons": reasons,
        "not_active_message": _strategy_not_active_message(reasons),
        "source": {
            **strategy.source.model_dump(exclude_none=True, by_alias=True),
            "symbol_count": len(strategy.symbols),
            "symbols": source_symbols,
            "symbol_limit": SOURCE_SYMBOL_LIMIT,
        },
        "entry": _strategy_routine_breadth_payload(strategy.entry),
        "management": _strategy_routine_breadth_payload(strategy.management),
        "risk_limits": strategy.risk_limits.dump_config(),
        "runtime": strategy.runtime.model_dump(exclude_none=True),
        "execution": strategy.execution.model_dump(exclude_none=True),
        "execution_mode": strategy.execution.mode,
        "approval_mode": strategy.execution.approval,
        "execution_runtime": strategy.execution.runtime,
        "execution_contract": execution_contract,
        "latest_observation": _strategy_latest_observation_state(
            strategy=strategy,
            candidate_run=latest_candidate_run,
            strategy_run=latest_strategy_run,
            now=now,
        ),
        "config_hash": strategy.config_hash,
        "config_path": str(strategy.config_path),
    }


def _project_strategy_breadth(
    *,
    storage: Any,
    market_date: str,
    broker_environment: str,
    broker_environment_source: str,
    trading_flows: list[dict[str, Any]],
    now: datetime,
) -> _StrategyBreadthProjection:
    strategies = list(load_trading_strategies().values())
    active_strategy_ids = {strategy_id for flow in trading_flows if (strategy_id := as_text(as_mapping(flow).get("trading_strategy_id"))) is not None}
    strategy_ids = {strategy.trading_strategy_id for strategy in strategies}
    _, latest_candidates = _latest_flow_facts(
        storage=storage,
        market_date=market_date,
        ticker_source_ids=set(),
        strategy_ids=strategy_ids,
    )
    latest_strategy_runs = _latest_strategy_run_payloads(
        storage=storage,
        market_date=market_date,
        strategy_ids=strategy_ids,
    )
    rows = [
        _strategy_breadth_row(
            strategy=strategy,
            active_strategy_ids=active_strategy_ids,
            broker_environment=broker_environment,
            broker_environment_source=broker_environment_source,
            latest_candidate_run=latest_candidates.get(strategy.trading_strategy_id),
            latest_strategy_run=latest_strategy_runs.get(strategy.trading_strategy_id),
            now=now,
        )
        for strategy in strategies
    ]
    rows.sort(
        key=lambda row: (
            0 if row.get("active") else 1,
            str(row.get("execution_mode") or ""),
            str(row.get("trade_structure") or ""),
            str(row.get("trading_strategy_id") or ""),
        )
    )
    available_rows = [row for row in rows if not bool(row.get("active")) and row.get("status") != "paused"]
    active_rows = [row for row in rows if bool(row.get("active"))]
    summary = {
        "strategy_count": len(rows),
        "active_strategy_count": len(active_rows),
        "inactive_strategy_count": len(rows) - len(active_rows),
        "available_strategy_count": len(available_rows),
        "available_shadow_strategy_count": sum(1 for row in available_rows if row.get("execution_mode") == "shadow"),
        "available_paper_strategy_count": sum(1 for row in available_rows if row.get("execution_mode") == "paper"),
        "available_live_strategy_count": sum(1 for row in available_rows if row.get("execution_mode") == "live"),
        "paused_strategy_count": sum(1 for row in rows if row.get("status") == "paused"),
        "trade_structure_counts": dict(Counter(str(row.get("trade_structure") or "unknown") for row in rows)),
        "execution_mode_counts": dict(Counter(str(row.get("execution_mode") or "unknown") for row in rows)),
        "ops_posture_counts": dict(Counter(str(row.get("ops_posture") or "unknown") for row in rows)),
    }
    payload = {
        "status": "ready",
        "summary": summary,
        "strategies": rows,
        "active_strategies": active_rows,
        "available_strategies": available_rows,
    }
    return _StrategyBreadthProjection(payload=payload, summary=summary)
