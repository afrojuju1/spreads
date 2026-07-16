from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy

from core.jobs.contracts import build_ad_hoc_job_run_id
from core.jobs.registry import get_job_spec
from core.runtime.config import default_workflow_address, default_workflow_namespace
from core.workflow_runtime.provider import connect_provider, provider_queue_for_lane
from core.workflow_runtime.wire import TEMPORAL_WORKFLOW_INPUT_LIMIT_BYTES, require_temporal_payload_budget
from core.workflows.scheduled_job import ScheduledJobWorkflow


@dataclass(frozen=True)
class StartedAdHocRoutine:
    workflow_id: str
    workflow_run_id: str
    job_run_id: str


def start_ad_hoc_job_workflow(
    *,
    job_type: str,
    job_key: str,
    workflow_id: str,
    payload: dict[str, Any],
    workflow_address: str | None = None,
    workflow_namespace: str | None = None,
) -> StartedAdHocRoutine:
    spec = get_job_spec(job_type)
    if spec is None:
        raise RuntimeError(f"Job type is not registered: {job_type}")

    async def _start() -> Any:
        client = await connect_provider(
            address=workflow_address or default_workflow_address(),
            namespace=workflow_namespace or default_workflow_namespace(),
        )
        workflow_input = {
            "adhoc": True,
            "job_type": job_type,
            "job_key": job_key,
            "scheduled_for": payload.get("scheduled_for"),
            "payload": dict(payload),
            "activity_retry": {
                "maximum_attempts": spec.activity_maximum_attempts,
            },
        }
        require_temporal_payload_budget(
            workflow_input,
            label=f"Ad-hoc routine {job_key} workflow input",
            limit_bytes=TEMPORAL_WORKFLOW_INPUT_LIMIT_BYTES,
        )
        handle = await client.start_workflow(
            ScheduledJobWorkflow.run,
            workflow_input,
            id=workflow_id,
            task_queue=provider_queue_for_lane(spec.workflow_lane),
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        workflow_run_id = str(handle.run_id or "").strip()
        if not workflow_run_id:
            workflow_run_id = str((await handle.describe()).run_id or "").strip()
        if not workflow_run_id:
            raise RuntimeError(f"Workflow runtime did not return a run ID for {workflow_id}.")
        return StartedAdHocRoutine(
            workflow_id=workflow_id,
            workflow_run_id=workflow_run_id,
            job_run_id=build_ad_hoc_job_run_id(job_key, workflow_run_id),
        )

    return asyncio.run(_start())


__all__ = ["StartedAdHocRoutine", "start_ad_hoc_job_workflow"]
