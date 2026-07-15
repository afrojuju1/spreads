from __future__ import annotations

import asyncio
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from core.engine.outbox_publisher import publish_pending_engine_outbox
from core.jobs.contracts import RoutineExecutionContext, RoutineHandler, RoutineOutcome
from core.jobs.registry import (
    ALERT_DELIVERY_JOB_TYPE,
    ALERT_RECONCILE_JOB_TYPE,
    BROKER_SYNC_JOB_TYPE,
    ENGINE_OUTBOX_PUBLISH_JOB_TYPE,
    EXECUTION_LIFECYCLE_START_JOB_TYPE,
    TRADING_STRATEGY_ENTRY_JOB_TYPE,
    TRADING_STRATEGY_MANAGE_JOB_TYPE,
)
from core.services.alert_delivery import (
    ALERT_DELIVERY_STALE_SECONDS,
    reconcile_alert_delivery,
    run_alert_delivery,
)
from core.services.broker_sync import run_broker_sync
from core.services.execution_intents import start_pending_execution_lifecycle_workflows
from core.services.exit_manager import run_trading_strategy_manage
from core.services.trading_engine.strategy_runtime import run_trading_strategy_entry
from core.storage.serializers import render_value


def _outcome(result: Mapping[str, Any], *, persisted_result: Mapping[str, Any] | None = None) -> RoutineOutcome:
    rendered = dict(render_value(persisted_result if persisted_result is not None else result))
    if result.get("status") == "skipped":
        return RoutineOutcome.skipped(rendered)
    return RoutineOutcome.succeeded(rendered)


def _broker_sync(context: RoutineExecutionContext) -> RoutineOutcome:
    context.heartbeat()
    result = run_broker_sync(
        db_target=context.database_url,
        history_range=str(context.payload.get("history_range", "1D")),
        activity_lookback_days=int(context.payload.get("activity_lookback_days", 1)),
    )
    return _outcome(result)


def _trading_strategy_entry(context: RoutineExecutionContext) -> RoutineOutcome:
    context.heartbeat()
    result = run_trading_strategy_entry(
        db_target=context.database_url,
        trading_strategy_id=str(context.payload["trading_strategy_id"]),
        market_date=context.payload.get("market_date"),
        planner_job_run_id=context.job_run_id,
    )
    return _outcome(result)


def _trading_strategy_manage(context: RoutineExecutionContext) -> RoutineOutcome:
    context.heartbeat()
    result = run_trading_strategy_manage(
        db_target=context.database_url,
        storage=context.storage,
        trading_strategy_id=str(context.payload["trading_strategy_id"]),
    )
    return _outcome(result)


def _execution_lifecycle_start(context: RoutineExecutionContext) -> RoutineOutcome:
    context.heartbeat()
    result = start_pending_execution_lifecycle_workflows(
        db_target=context.database_url,
        limit=int(context.payload.get("limit", 25) or 25),
    )
    return _outcome(result)


def _engine_outbox_publish(context: RoutineExecutionContext) -> RoutineOutcome:
    context.heartbeat()
    result = asyncio.run(
        publish_pending_engine_outbox(
            repository=context.storage.engine_events,
            nats_url=context.payload.get("nats_url"),
            limit=int(context.payload.get("limit", 100) or 100),
        )
    )
    return _outcome(result)


def _alert_delivery(context: RoutineExecutionContext) -> RoutineOutcome:
    context.heartbeat()
    result = run_alert_delivery(
        alert_store=context.storage.alerts,
        alert_id=int(context.payload["alert_id"]),
        delivery_job_run_id=context.job_run_id,
        worker_name=context.worker_name,
    )
    return _outcome(result, persisted_result=result)


def _alert_reconcile(context: RoutineExecutionContext) -> RoutineOutcome:
    context.heartbeat()
    result = reconcile_alert_delivery(
        alert_store=context.storage.alerts,
        job_store=context.storage.jobs,
        limit=int(context.payload.get("limit", 200)),
        stale_after_seconds=int(context.payload.get("stale_after_seconds", ALERT_DELIVERY_STALE_SECONDS)),
    )
    return _outcome(result, persisted_result=result)


HANDLERS: Mapping[str, RoutineHandler] = MappingProxyType(
    {
        BROKER_SYNC_JOB_TYPE: _broker_sync,
        ALERT_DELIVERY_JOB_TYPE: _alert_delivery,
        ALERT_RECONCILE_JOB_TYPE: _alert_reconcile,
        TRADING_STRATEGY_ENTRY_JOB_TYPE: _trading_strategy_entry,
        TRADING_STRATEGY_MANAGE_JOB_TYPE: _trading_strategy_manage,
        EXECUTION_LIFECYCLE_START_JOB_TYPE: _execution_lifecycle_start,
        ENGINE_OUTBOX_PUBLISH_JOB_TYPE: _engine_outbox_publish,
    }
)

__all__ = ["HANDLERS"]
