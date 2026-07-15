from __future__ import annotations

import asyncio
from typing import Any

from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy

from core.jobs.registry import get_job_spec
from core.runtime.config import default_workflow_address, default_workflow_namespace
from core.workflow_runtime.provider import connect_provider, provider_queue_for_lane
from core.workflows.scheduled_job import ScheduledJobWorkflow


def start_ad_hoc_job_workflow(
    *,
    job_type: str,
    job_key: str,
    job_run_id: str,
    orchestration_id: str,
    payload: dict[str, Any],
    workflow_address: str | None = None,
    workflow_namespace: str | None = None,
) -> Any:
    spec = get_job_spec(job_type)
    if spec is None:
        raise RuntimeError(f"Job type is not registered: {job_type}")

    async def _start() -> Any:
        client = await connect_provider(
            address=workflow_address or default_workflow_address(),
            namespace=workflow_namespace or default_workflow_namespace(),
        )
        return await client.start_workflow(
            ScheduledJobWorkflow.run,
            {
                "adhoc": True,
                "job_type": job_type,
                "job_key": job_key,
                "job_run_id": job_run_id,
                "orchestration_id": orchestration_id,
                "scheduled_for": payload.get("scheduled_for"),
                "payload": dict(payload),
            },
            id=orchestration_id,
            task_queue=provider_queue_for_lane(spec.workflow_lane),
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )

    return asyncio.run(_start())
