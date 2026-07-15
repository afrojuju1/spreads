from __future__ import annotations

import argparse
import asyncio
import os
from typing import Any

from temporalio import activity
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions

from core.jobs.registry import CAPTURE_WORKFLOW_LANE
from core.runtime.config import default_workflow_address, default_workflow_namespace
from core.services.market_capture import parse_args as parse_capture_args
from core.services.market_capture import run_capture_session
from core.workflow_runtime.provider import connect_provider, provider_queue_for_lane
from core.workflows.capture_session import CaptureSessionWorkflow


@activity.defn(name="run_capture_session_activity")
async def run_capture_session_activity(request: dict[str, Any]) -> dict[str, Any]:
    args = parse_capture_args(list(request.get("capture_args") or []))
    exit_code = await run_capture_session(args, heartbeat=activity.heartbeat)
    return {"status": "stopped", "exit_code": exit_code}


async def _start_session(client: Client, *, capture_args: list[str]) -> None:
    target = str(os.environ.get("SPREADS_DEPLOY_ENV") or "unknown")
    await client.start_workflow(
        CaptureSessionWorkflow.run,
        {"target": target, "capture_args": capture_args},
        id=f"capture-session:{target}",
        task_queue=provider_queue_for_lane(CAPTURE_WORKFLOW_LANE),
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )


async def run_capture_worker(*, capture_args: list[str]) -> None:
    client = await connect_provider(address=default_workflow_address(), namespace=default_workflow_namespace())
    worker = Worker(
        client,
        task_queue=provider_queue_for_lane(CAPTURE_WORKFLOW_LANE),
        workflows=[CaptureSessionWorkflow],
        activities=[run_capture_session_activity],
        workflow_runner=SandboxedWorkflowRunner(restrictions=SandboxRestrictions.default.with_passthrough_modules("core")),
    )
    async with worker:
        await _start_session(client, capture_args=capture_args)
        await asyncio.Future()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the workflow-supervised market capture owner.")
    args, capture_args = parser.parse_known_args(argv)
    del args
    asyncio.run(run_capture_worker(capture_args=capture_args))
