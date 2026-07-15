from __future__ import annotations

import asyncio
from datetime import UTC
from typing import Any

from temporalio.api.enums.v1 import TaskQueueType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest

from core.jobs.registry import WORKFLOW_LANES
from core.jobs.specs import disabled_workflow_lanes
from core.runtime.config import default_workflow_address, default_workflow_namespace
from core.workflow_runtime.execution_health import collect_execution_health
from core.workflow_runtime.provider import PROVIDER_NAME, connect_provider, provider_queue_for_lane


async def _runtime_diagnostics(*, targets: list[dict[str, Any]]) -> dict[str, Any]:
    namespace = default_workflow_namespace()
    client = await connect_provider(address=default_workflow_address(), namespace=namespace)
    disabled_lanes = disabled_workflow_lanes()
    rows: list[dict[str, Any]] = []
    for spec in WORKFLOW_LANES:
        if spec.lane in disabled_lanes:
            rows.append(
                {
                    "lane": spec.lane,
                    "enabled": False,
                    "required_for_trading": spec.required_for_trading,
                    "required_for_deploy": spec.required_for_deploy,
                    "optional": spec.optional,
                    "poller_count": 0,
                    "pollers": [],
                    "status": "disabled",
                }
            )
            continue
        queue = provider_queue_for_lane(spec.lane)
        response = await client.workflow_service.describe_task_queue(
            DescribeTaskQueueRequest(
                namespace=namespace,
                task_queue=TaskQueue(name=queue),
                task_queue_type=TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
            )
        )
        pollers = [
            {
                "identity": poller.identity,
                "last_access_at": poller.last_access_time.ToDatetime(tzinfo=UTC).isoformat().replace("+00:00", "Z"),
            }
            for poller in response.pollers
        ]
        rows.append(
            {
                "lane": spec.lane,
                "enabled": True,
                "required_for_trading": spec.required_for_trading,
                "required_for_deploy": spec.required_for_deploy,
                "optional": spec.optional,
                "poller_count": len(pollers),
                "pollers": pollers,
                "status": "healthy" if pollers else "blocked",
                "provider": {"name": PROVIDER_NAME, "queue": queue},
            }
        )
    execution_health = await collect_execution_health(client, targets=targets)
    return {
        "status": "blocked" if execution_health["status"] == "blocked" else "healthy",
        "provider": PROVIDER_NAME,
        "lanes": rows,
        "executions": execution_health,
    }


def get_workflow_runtime_diagnostics(
    *,
    targets: list[dict[str, Any]] | None = None,
    timeout_seconds: float = 5.0,
) -> dict[str, Any]:
    try:
        return asyncio.run(
            asyncio.wait_for(
                _runtime_diagnostics(targets=list(targets or [])),
                timeout=timeout_seconds,
            )
        )
    except Exception as exc:
        disabled_lanes = disabled_workflow_lanes()
        return {
            "status": "blocked",
            "provider": PROVIDER_NAME,
            "error": str(exc),
            "executions": {
                "status": "blocked",
                "open_execution_count": 0,
                "correlated_target_count": len(targets or []),
                "stuck_workflow_task_count": 0,
                "pending_activity_retry_count": 0,
                "stuck_activity_count": 0,
                "retired_queue_execution_count": 0,
                "schedule_failure_count": 0,
                "projection_mismatch_count": 0,
                "issues": [],
                "error": str(exc),
            },
            "lanes": [
                {
                    "lane": spec.lane,
                    "enabled": spec.lane not in disabled_lanes,
                    "required_for_trading": spec.required_for_trading,
                    "required_for_deploy": spec.required_for_deploy,
                    "optional": spec.optional,
                    "poller_count": 0,
                    "pollers": [],
                    "status": "disabled" if spec.lane in disabled_lanes else "blocked",
                }
                for spec in WORKFLOW_LANES
            ],
        }


__all__ = ["get_workflow_runtime_diagnostics"]
