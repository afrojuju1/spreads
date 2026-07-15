from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from core.jobs.contracts import RoutineExecutionContext, RoutineHandler, RoutineOutcome
from core.jobs.registry import TRADINGAGENTS_SCAN_JOB_TYPE
from core.services.tradingagents_scan import run_tradingagents_scan
from core.storage.serializers import render_value


def _tradingagents_scan(context: RoutineExecutionContext) -> RoutineOutcome:
    context.heartbeat()
    result = run_tradingagents_scan(
        storage=context.storage,
        job_run_id=context.job_run_id,
        payload=dict(context.payload),
        heartbeat=context.heartbeat,
    )
    projection = dict(render_value(result))
    if result.get("status") == "skipped":
        return RoutineOutcome.skipped(projection)
    return RoutineOutcome.succeeded(projection)


HANDLERS: Mapping[str, RoutineHandler] = MappingProxyType(
    {TRADINGAGENTS_SCAN_JOB_TYPE: _tradingagents_scan}
)

__all__ = ["HANDLERS"]
