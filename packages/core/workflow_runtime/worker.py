from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor

from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions

from core.activities import (
    cancel_execution_attempt_activity,
    create_repriced_execution_intent_activity,
    ensure_execution_attempt_for_intent_activity,
    refresh_execution_attempt_activity,
    submit_execution_attempt_to_broker_activity,
)
from core.jobs.handlers import build_lane_handlers
from core.jobs.registry import CAPTURE_WORKFLOW_LANE, LIFECYCLE_WORKFLOW_LANE, WORKFLOW_LANES, get_workflow_lane
from core.runtime.config import default_lifecycle_workflow_lane, default_workflow_address, default_workflow_namespace
from core.workflow_runtime.provider import connect_provider, provider_queue_for_lane
from core.workflow_runtime.routine_activity import build_routine_activity
from core.workflows.close_lifecycle import CloseLifecycleWorkflow
from core.workflows.scheduled_job import ScheduledJobWorkflow
from core.workflows.trade_lifecycle import TradeLifecycleWorkflow


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a Spreads workflow lane worker.")
    parser.add_argument(
        "--lane",
        default=default_lifecycle_workflow_lane(),
        choices=tuple(spec.lane for spec in WORKFLOW_LANES if spec.lane != CAPTURE_WORKFLOW_LANE),
        help="Workflow lane to poll.",
    )
    return parser.parse_args(argv)


async def run_worker(*, lane: str) -> None:
    lane_spec = get_workflow_lane(lane)
    if lane_spec is None:
        raise ValueError(f"Unknown workflow lane: {lane}")
    client = await connect_provider(address=default_workflow_address(), namespace=default_workflow_namespace())
    lifecycle = lane == LIFECYCLE_WORKFLOW_LANE
    workflows = [TradeLifecycleWorkflow, CloseLifecycleWorkflow] if lifecycle else [ScheduledJobWorkflow]
    if lifecycle:
        activities = [
            ensure_execution_attempt_for_intent_activity,
            submit_execution_attempt_to_broker_activity,
            refresh_execution_attempt_activity,
            cancel_execution_attempt_activity,
            create_repriced_execution_intent_activity,
        ]
    else:
        handlers = build_lane_handlers(lane)
        activities = [build_routine_activity(expected_lane=lane, handlers=handlers)]
    with ThreadPoolExecutor(max_workers=lane_spec.max_concurrency, thread_name_prefix=f"{lane}-activity") as activity_executor:
        worker = Worker(
            client,
            task_queue=provider_queue_for_lane(lane),
            workflows=workflows,
            workflow_runner=SandboxedWorkflowRunner(restrictions=SandboxRestrictions.default.with_passthrough_modules("core")),
            activities=activities,
            activity_executor=activity_executor,
            max_concurrent_activities=lane_spec.max_concurrency,
        )
        await worker.run()


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    asyncio.run(run_worker(lane=str(args.lane)))


if __name__ == "__main__":
    main()
