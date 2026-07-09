from __future__ import annotations

from collections import Counter
from typing import Any


from core.db.decorators import with_storage
from core.services.execution.runtimes import resolve_execution_runtime_capabilities
from core.value_coercion import (
    as_list,
    as_mapping,
    as_text,
    coerce_int,
    utc_iso,
    utc_now,
    utc_now_iso,
)

from core.services.ops.shared import (
    _combine_statuses,
)


from core.services.ops.trading.account import _broker_exposure_state, _project_account
from core.services.ops.trading.execution import _project_execution
from core.services.ops.trading.execution_contract import (
    _broker_environment_source,
    _normalize_broker_environment,
    _project_execution_contract,
)
from core.services.ops.trading.flow_posture import _strategy_no_entry_summary
from core.services.ops.trading.flows import _project_flows
from core.services.ops.trading.market_context import _project_market_context
from core.services.ops.trading.positions import _project_positions
from core.services.ops.trading.runtime import (
    _project_alerts,
    _project_engine,
    _project_jobs,
    _project_market_control,
)
from core.services.ops.trading.strategy_breadth import _project_strategy_breadth



@with_storage()
def build_trading_ops_state(
    *,
    db_target: str | None = None,
    market_date: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    now = utc_now()
    generated_at = utc_iso(now) or utc_now_iso()
    market_control = _project_market_control(storage=storage, market_date=market_date, now=now)
    jobs = _project_jobs(db_target=db_target, storage=storage)
    account = _project_account(storage=storage, now=now, market_session=market_control.market_session)
    engine = _project_engine(
        storage=storage,
        market_date=market_control.market_date,
        now=now,
    )
    market_context = _project_market_context(
        storage=storage,
        now=now,
        market_open=market_control.market_open,
    )
    execution = _project_execution(
        storage=storage,
        market_date=market_control.market_date,
        now=now,
    )
    positions = _project_positions(
        storage=storage,
        now=now,
        broker_sync=account.broker_sync,
        market_session=market_control.market_session,
    )
    alerts = _project_alerts(storage=storage, now=now)
    broker_environment = _normalize_broker_environment(account.account_snapshot.get("environment"))
    broker_environment_source = _broker_environment_source(account.account_snapshot)
    flows = _project_flows(
        storage=storage,
        engine_ops=engine.payload,
        market_date=market_control.market_date,
        market_open=market_control.market_open,
        broker_environment=broker_environment,
        broker_environment_source=broker_environment_source,
        now=now,
    )
    execution_contract = _project_execution_contract(
        storage=storage,
        market_date=market_control.market_date,
        account_snapshot=account.account_snapshot,
        trading_flows=flows.trading_flows,
    )
    broker_exposure = _broker_exposure_state(
        account_snapshot=account.account_snapshot,
        open_positions=positions.open_positions,
        broker_sync=account.broker_sync,
    )
    strategy_breadth = _project_strategy_breadth(
        storage=storage,
        market_date=market_control.market_date,
        broker_environment=broker_environment,
        broker_environment_source=broker_environment_source,
        trading_flows=flows.trading_flows,
        now=now,
    )

    statuses: list[str] = []
    attention: list[dict[str, str]] = []
    for projection in (market_control, jobs, account, engine, market_context, execution, positions, alerts, flows, execution_contract):
        statuses.extend(projection.statuses)
        attention.extend(projection.attention)

    trading_allowed = True
    if market_control.kill_switch_reason is not None:
        trading_allowed = False
    elif str(market_control.control.get("mode") or "") != "normal":
        trading_allowed = False
    elif not market_control.market_open:
        trading_allowed = False
    elif account.broker_sync_status != "healthy":
        trading_allowed = False
    elif account.account_snapshot.get("status") != "ready":
        trading_allowed = False
    elif account.account.get("trading_blocked") or account.account.get("account_blocked"):
        trading_allowed = False
    elif execution_contract.summary.get("environment_compatible") is False:
        trading_allowed = False
    elif execution.stale_open_execution_count or execution.submit_unknown_execution_count:
        trading_allowed = False

    active_intent_count = sum(int(as_mapping(flow.get("intent_state")).get("active_intent_count") or 0) for flow in flows.trading_flows)
    portfolio_admission_states = [
        as_mapping(flow.get("portfolio_admission"))
        for flow in flows.trading_flows
        if as_mapping(flow.get("portfolio_admission")).get("status") not in {None, "", "not_evaluated"}
    ]
    protection_admission_states = [
        as_mapping(flow.get("protection_admission"))
        for flow in flows.trading_flows
        if as_mapping(flow.get("protection_admission")).get("status") not in {None, "", "not_evaluated"}
    ]
    portfolio_block_reasons = Counter(
        as_text(state.get("reason")) or "unknown" for state in portfolio_admission_states if as_text(state.get("status")) == "blocked"
    )
    protection_block_reasons = Counter(
        as_text(state.get("reason")) or "unknown" for state in protection_admission_states if as_text(state.get("status")) == "blocked"
    )
    primary_flow = next(
        (flow for flow in flows.trading_flows if flow.get("trading_strategy_id") == "momentum_long_calls"),
        flows.trading_flows[0] if flows.trading_flows else {},
    )
    strategy_no_entry_summary = _strategy_no_entry_summary(flows.trading_flows)
    strategy_no_entry_category_counts = Counter(str(row.get("category") or "unknown") for row in strategy_no_entry_summary)
    primary_entry_posture = as_mapping(primary_flow.get("entry_posture"))
    primary_capacity = as_mapping(primary_flow.get("capacity"))
    primary_position_state = as_mapping(primary_flow.get("position_state"))
    summary = {
        "market_date": market_control.market_date,
        "market_session_status": market_control.market_session.get("status"),
        "market_open_at": market_control.market_session.get("market_open_at"),
        "market_close_at": market_control.market_session.get("market_close_at"),
        "trading_allowed": trading_allowed,
        **execution_contract.summary,
        "control_mode": market_control.control.get("mode"),
        "temporal_schedule_status": as_mapping(jobs.details.get("schedules")).get("status"),
        "task_queue_count": jobs.summary.get("task_queue_count"),
        "disabled_task_queue_count": jobs.summary.get("disabled_task_queue_count"),
        "blocked_task_queue_count": sum(1 for row in as_list(jobs.details.get("task_queues")) if as_mapping(row).get("status") == "blocked"),
        "idle_task_queue_count": sum(1 for row in as_list(jobs.details.get("task_queues")) if as_mapping(row).get("status") == "idle"),
        "actionable_failed_job_count": jobs.summary.get("actionable_failed_count"),
        "broker_sync_status": account.broker_sync.get("status"),
        "broker_sync_age_seconds": account.broker_sync.get("age_seconds"),
        "account_snapshot_status": account.account_snapshot.get("status"),
        "account_snapshot_captured_at": account.account_snapshot.get("captured_at"),
        "primary_entry_state": primary_entry_posture.get("state"),
        "primary_entry_message": primary_entry_posture.get("message"),
        "primary_entry_primary_blocker_group": primary_entry_posture.get("primary_blocker_group"),
        "primary_entry_healthy_flat": primary_entry_posture.get("healthy_flat"),
        "primary_entry_blocker_groups": primary_entry_posture.get("blocker_groups"),
        "strategy_no_entry_category_counts": dict(sorted(strategy_no_entry_category_counts.items())),
        **market_context.summary,
        **strategy_breadth.summary,
        "broker_position_count": broker_exposure.get("broker_position_count"),
        "broker_option_position_count": broker_exposure.get("broker_option_position_count"),
        "spreads_managed_broker_option_position_count": broker_exposure.get("spreads_managed_option_position_count"),
        "external_manual_broker_option_position_count": broker_exposure.get("external_manual_option_position_count"),
        "open_position_count": len(positions.open_positions),
        "open_execution_count": len(execution.open_execution_attempts),
        "active_intent_count": active_intent_count,
        "protection_admission_evaluated_strategy_count": len(protection_admission_states),
        "protection_blocked_strategy_count": sum(1 for state in protection_admission_states if as_text(state.get("status")) == "blocked"),
        "protection_unknown_strategy_count": sum(1 for state in protection_admission_states if as_text(state.get("status")) == "unknown"),
        "protection_block_reasons": dict(sorted(protection_block_reasons.items())),
        "portfolio_admission_evaluated_strategy_count": len(portfolio_admission_states),
        "portfolio_blocked_strategy_count": sum(1 for state in portfolio_admission_states if as_text(state.get("status")) == "blocked"),
        "portfolio_unknown_strategy_count": sum(1 for state in portfolio_admission_states if as_text(state.get("status")) == "unknown"),
        "portfolio_block_reasons": dict(sorted(portfolio_block_reasons.items())),
        "max_open_positions": primary_capacity.get("max_open_positions"),
        "max_daily_entries": primary_capacity.get("max_daily_entries"),
        "session_entry_count": primary_capacity.get("session_entry_count"),
        "remaining_daily_entries": primary_capacity.get("remaining_daily_entries"),
        "closed_position_count": primary_position_state.get("closed_position_count"),
        "latest_exit_reason": primary_position_state.get("latest_exit_reason"),
        "realized_pnl": primary_position_state.get("realized_pnl"),
        "unrealized_pnl": primary_position_state.get("unrealized_pnl"),
        "net_pnl": primary_position_state.get("net_pnl"),
        "execution_health_status": execution.execution_health_status,
        "approved_admission_intent_gap_count": execution.approved_admission_intent_gap_count,
        "risk_breach_count": positions.risk_breach_count,
        "reconciliation_mismatch_count": positions.reconciliation_mismatch_count,
        "mark_health_status": positions.mark_health_status,
        "engine_status": engine.status,
        "engine_ticker_source_run_count": coerce_int(engine.summary.get("ticker_source_run_count")) or 0,
        "engine_candidate_run_count": coerce_int(engine.summary.get("candidate_run_count")) or 0,
        "engine_trade_candidate_count": coerce_int(engine.summary.get("trade_candidate_count")) or 0,
        "engine_signal_count": coerce_int(engine.summary.get("signal_count")) or 0,
        "engine_decision_count": coerce_int(engine.summary.get("decision_count")) or 0,
        "engine_selected_count": coerce_int(engine.summary.get("selected_count")) or 0,
        "engine_intent_count": coerce_int(engine.summary.get("intent_count")) or 0,
        "engine_entry_intent_count": coerce_int(engine.summary.get("entry_intent_count")) or 0,
        "engine_management_intent_count": coerce_int(engine.summary.get("management_intent_count")) or 0,
        "engine_event_count": coerce_int(engine.summary.get("engine_event_count")) or 0,
        "engine_workflow_event_count": coerce_int(engine.summary.get("engine_workflow_event_count")) or 0,
        "engine_outbox_pending_count": coerce_int(engine.summary.get("engine_outbox_pending_count")) or 0,
        "engine_outbox_retrying_count": coerce_int(engine.summary.get("engine_outbox_retrying_count")) or 0,
        "engine_open_position_count": coerce_int(engine.summary.get("open_position_count")) or 0,
        "capture_active_target_count": coerce_int(engine.summary.get("capture_active_target_count")) or 0,
        "capture_status": engine.summary.get("capture_status"),
    }

    details = {
        "market_session": market_control.market_session,
        "control": market_control.control,
        "jobs": jobs.payload,
        "schedules": jobs.details.get("schedules"),
        "task_queues": jobs.details.get("task_queues"),
        "disabled_task_queues": jobs.details.get("disabled_task_queues"),
        "running_jobs": [dict(row) for row in as_list(jobs.details.get("running_jobs")) if as_mapping(row).get("status") == "running"],
        "queued_jobs": [dict(row) for row in as_list(jobs.details.get("queued_jobs")) if as_mapping(row).get("status") == "queued"],
        "recent_job_runs": jobs.details.get("job_runs"),
        "broker_sync": account.broker_sync,
        "account_snapshot": account.account_snapshot,
        "broker_exposure": broker_exposure,
        "engine": engine.payload,
        "market_context": market_context.payload,
        "execution_contract": execution_contract.payload,
        "strategy_breadth": strategy_breadth.payload,
        "execution_runtimes": resolve_execution_runtime_capabilities(),
        "open_execution_attempts": execution.summarized_open_execution_attempts,
        "open_positions": positions.open_positions,
        "top_positions": positions.top_positions,
        "strategy_no_entry_summary": strategy_no_entry_summary,
        "trading_flows": flows.trading_flows,
        "primary_trading_flow": primary_flow,
        "protection_admission": {
            "evaluated_strategy_count": len(protection_admission_states),
            "blocked_strategy_count": sum(1 for state in protection_admission_states if as_text(state.get("status")) == "blocked"),
            "unknown_strategy_count": sum(1 for state in protection_admission_states if as_text(state.get("status")) == "unknown"),
            "block_reasons": dict(sorted(protection_block_reasons.items())),
            "latest_by_strategy": {
                str(flow.get("trading_strategy_id")): as_mapping(flow.get("protection_admission"))
                for flow in flows.trading_flows
                if as_mapping(flow.get("protection_admission")).get("status") not in {None, "", "not_evaluated"}
            },
        },
        "portfolio_admission": {
            "evaluated_strategy_count": len(portfolio_admission_states),
            "blocked_strategy_count": sum(1 for state in portfolio_admission_states if as_text(state.get("status")) == "blocked"),
            "unknown_strategy_count": sum(1 for state in portfolio_admission_states if as_text(state.get("status")) == "unknown"),
            "block_reasons": dict(sorted(portfolio_block_reasons.items())),
            "latest_by_strategy": {
                str(flow.get("trading_strategy_id")): as_mapping(flow.get("portfolio_admission"))
                for flow in flows.trading_flows
                if as_mapping(flow.get("portfolio_admission")).get("status") not in {None, "", "not_evaluated"}
            },
        },
        "alert_delivery": alerts.alert_delivery,
        "mark_health": {
            "status": positions.mark_health_status,
            "missing_mark_count": positions.missing_mark_count,
            "stale_mark_count": positions.stale_mark_count,
            "mark_freshness_required": positions.mark_freshness_required,
            "broker_unquoted_position_count": positions.broker_unquoted_positions,
            "mark_error": positions.mark_error,
        },
        "execution_health": {
            "status": execution.execution_health_status,
            "stale_open_execution_count": execution.stale_open_execution_count,
            "submit_unknown_execution_count": execution.submit_unknown_execution_count,
            "approved_admission_intent_gap_count": execution.approved_admission_intent_gap_count,
            "approved_admission_intent_gap_ids": execution.approved_admission_intent_gap_ids,
            "capacity_blocked_underlying_count": len(execution.capacity_blocked_underlyings),
            "capacity_blocked_underlyings": execution.capacity_blocked_underlyings,
        },
    }
    return {
        "status": _combine_statuses(*statuses),
        "generated_at": generated_at,
        "summary": summary,
        "attention": attention,
        "details": details,
    }


__all__ = ["build_trading_ops_state"]
