from __future__ import annotations

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions

from core.activities import (
    cancel_execution_attempt_activity,
    create_repriced_execution_intent_activity,
    ensure_execution_attempt_for_intent_activity,
    refresh_execution_attempt_activity,
    submit_execution_attempt_to_broker_activity,
)
from core.runtime.config import default_temporal_address, default_temporal_namespace, default_temporal_task_queue
from core.workflows.close_lifecycle import CloseLifecycleWorkflow
from core.workflows.trade_lifecycle import TradeLifecycleWorkflow


async def run_worker() -> None:
    client = await Client.connect(
        default_temporal_address(),
        namespace=default_temporal_namespace(),
    )
    worker = Worker(
        client,
        task_queue=default_temporal_task_queue(),
        workflows=[TradeLifecycleWorkflow, CloseLifecycleWorkflow],
        workflow_runner=SandboxedWorkflowRunner(
            restrictions=SandboxRestrictions.default.with_passthrough_modules("core")
        ),
        activities=[
            ensure_execution_attempt_for_intent_activity,
            submit_execution_attempt_to_broker_activity,
            refresh_execution_attempt_activity,
            cancel_execution_attempt_activity,
            create_repriced_execution_intent_activity,
        ],
    )
    await worker.run()


def main(argv: list[str] | None = None) -> None:
    _ = argv
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
