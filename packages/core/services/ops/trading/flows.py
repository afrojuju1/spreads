from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from core.money import money_sum_float
from core.services.execution_intents.shared import ACTIVE_INTENT_STATES
from core.services.trading_strategies import load_active_trading_strategies, routine_should_run_now
from core.value_coercion import as_text, coerce_float, coerce_int

from core.services.ops.shared import _attention, _combine_statuses
from core.services.ops.trading.execution_contract import _execution_contract_status, _strategy_execution_contract
from core.services.ops.trading.flow_admissions import (
    _admission_flow_status,
    _latest_portfolio_admissions,
    _latest_protection_admissions,
)
from core.services.ops.trading.flow_facts import _candidate_state, _latest_flow_facts, _source_state
from core.services.ops.trading.flow_posture import _entry_posture_state
from core.services.ops.trading.models import OPEN_POSITION_STATUSES, _FlowProjection


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
