from __future__ import annotations

import argparse
import asyncio

from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions

from core.activities import (
    cancel_execution_attempt_activity,
    create_repriced_execution_intent_activity,
    ensure_execution_attempt_for_intent_activity,
    refresh_execution_attempt_activity,
    run_scheduled_job_activity,
    submit_execution_attempt_to_broker_activity,
)
from core.jobs.registry import (
    DATA_TASK_QUEUE_NAME,
    RESEARCH_TASK_QUEUE_NAME,
    RUNTIME_TASK_QUEUE_NAME,
    VALUATION_TASK_QUEUE_NAME,
)
from core.runtime.config import default_temporal_address, default_temporal_namespace, default_temporal_task_queue
from core.workflows.close_lifecycle import CloseLifecycleWorkflow
from core.workflows.scheduled_job import ScheduledJobWorkflow
from core.workflows.trade_lifecycle import TradeLifecycleWorkflow


JOB_TASK_QUEUES = {
    RUNTIME_TASK_QUEUE_NAME,
    DATA_TASK_QUEUE_NAME,
    VALUATION_TASK_QUEUE_NAME,
    RESEARCH_TASK_QUEUE_NAME,
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Spreads Temporal workers.")
    parser.add_argument("--task-queue", default=default_temporal_task_queue(), help="Temporal task queue to poll.")
    parser.add_argument(
        "--kind",
        choices=("auto", "lifecycle", "jobs"),
        default="auto",
        help="Worker registration set. auto selects jobs for known job task queues.",
    )
    return parser.parse_args(argv)


async def run_worker(*, task_queue: str, kind: str = "auto") -> None:
    client = await Client.connect(
        default_temporal_address(),
        namespace=default_temporal_namespace(),
    )
    resolved_kind = "jobs" if kind == "auto" and task_queue in JOB_TASK_QUEUES else ("lifecycle" if kind == "auto" else kind)
    workflows = [ScheduledJobWorkflow] if resolved_kind == "jobs" else [TradeLifecycleWorkflow, CloseLifecycleWorkflow]
    activities = (
        [run_scheduled_job_activity]
        if resolved_kind == "jobs"
        else [
            ensure_execution_attempt_for_intent_activity,
            submit_execution_attempt_to_broker_activity,
            refresh_execution_attempt_activity,
            cancel_execution_attempt_activity,
            create_repriced_execution_intent_activity,
        ]
    )
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=workflows,
        workflow_runner=SandboxedWorkflowRunner(
            restrictions=SandboxRestrictions.default.with_passthrough_modules("core")
        ),
        activities=activities,
    )
    await worker.run()


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    asyncio.run(run_worker(task_queue=str(args.task_queue), kind=str(args.kind)))


if __name__ == "__main__":
    main()
