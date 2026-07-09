from __future__ import annotations

import asyncio
from typing import Any

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy

from core.jobs.registry import get_job_spec
from core.runtime.config import default_temporal_address, default_temporal_namespace
from core.workflows.scheduled_job import ScheduledJobWorkflow


def start_ad_hoc_job_workflow(
    *,
    job_type: str,
    job_key: str,
    job_run_id: str,
    orchestration_id: str,
    payload: dict[str, Any],
    temporal_address: str | None = None,
    temporal_namespace: str | None = None,
) -> Any:
    spec = get_job_spec(job_type)
    if spec is None:
        raise RuntimeError(f"Job type is not registered: {job_type}")

    async def _start() -> Any:
        client = await Client.connect(
            temporal_address or default_temporal_address(),
            namespace=temporal_namespace or default_temporal_namespace(),
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
            task_queue=spec.task_queue_name,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )

    return asyncio.run(_start())
