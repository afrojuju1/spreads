from __future__ import annotations

import asyncio
from collections.abc import Mapping
from types import MappingProxyType

from core.jobs.contracts import RoutineExecutionContext, RoutineHandler, RoutineOutcome
from core.jobs.registry import (
    OPS_HEALTH_SNAPSHOT_JOB_TYPE,
    OPS_LOG_RETENTION_JOB_TYPE,
    POSTGRES_BACKUP_JOB_TYPE,
    ROUTINE_SCHEDULE_RECONCILE_JOB_TYPE,
)
from core.runtime.config import default_workflow_address, default_workflow_namespace
from core.services.maintenance import run_ops_health_snapshot, run_ops_log_retention, run_postgres_backup
from core.storage.serializers import render_value
from core.workflow_runtime.provider import connect_provider
from core.workflow_runtime.routine_schedules import reconcile_routine_schedules


def _outcome(result: Mapping[str, object]) -> RoutineOutcome:
    rendered = dict(render_value(result))
    if result.get("status") == "skipped":
        return RoutineOutcome.skipped(rendered)
    return RoutineOutcome.succeeded(rendered)


def _postgres_backup(context: RoutineExecutionContext) -> RoutineOutcome:
    context.heartbeat()
    return _outcome(run_postgres_backup(database_url=context.database_url, payload=dict(context.payload)))


def _routine_schedule_reconcile(context: RoutineExecutionContext) -> RoutineOutcome:
    async def reconcile() -> dict[str, object]:
        client = await connect_provider(
            address=default_workflow_address(),
            namespace=default_workflow_namespace(),
        )
        return await reconcile_routine_schedules(client=client)

    context.heartbeat()
    return _outcome(asyncio.run(reconcile()))


def _ops_health_snapshot(context: RoutineExecutionContext) -> RoutineOutcome:
    context.heartbeat()
    return _outcome(run_ops_health_snapshot(database_url=context.database_url))


def _ops_log_retention(context: RoutineExecutionContext) -> RoutineOutcome:
    context.heartbeat()
    return _outcome(run_ops_log_retention(payload=dict(context.payload)))


HANDLERS: Mapping[str, RoutineHandler] = MappingProxyType(
    {
        ROUTINE_SCHEDULE_RECONCILE_JOB_TYPE: _routine_schedule_reconcile,
        POSTGRES_BACKUP_JOB_TYPE: _postgres_backup,
        OPS_HEALTH_SNAPSHOT_JOB_TYPE: _ops_health_snapshot,
        OPS_LOG_RETENTION_JOB_TYPE: _ops_log_retention,
    }
)

__all__ = ["HANDLERS"]
