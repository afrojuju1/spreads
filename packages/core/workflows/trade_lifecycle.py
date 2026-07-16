from __future__ import annotations

from datetime import timedelta, timezone
from typing import Any

from temporalio import workflow

from core.workflows.contracts import (
    LifecycleActivityResult,
    LifecycleWorkflowResult,
    TradeLifecycleWorkflowInput,
)


@workflow.defn
class TradeLifecycleWorkflow:
    @workflow.run
    async def run(self, request_payload: dict[str, Any]) -> dict[str, Any]:
        request = TradeLifecycleWorkflowInput.model_validate(request_payload)
        execution_intent_id = request.execution_intent_id
        workflow_id = workflow.info().workflow_id
        activity_payload = {
            "execution_intent_id": execution_intent_id,
            "workflow_id": workflow_id,
        }
        prepared = LifecycleActivityResult.model_validate(
            await workflow.execute_activity(
                "ensure_execution_attempt_for_intent",
                activity_payload,
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=None,
            )
        )
        if prepared.execution_attempt_id is None:
            return LifecycleWorkflowResult(
                workflow_id=workflow_id,
                state=prepared.status,
                aggregate_type="execution_intent",
                aggregate_id=execution_intent_id,
                execution_intent_id=execution_intent_id,
                reason=prepared.reason,
                completed_at=workflow.now().astimezone(timezone.utc),
            ).to_payload()
        execution_attempt_id = prepared.execution_attempt_id
        submitted = LifecycleActivityResult.model_validate(
            await workflow.execute_activity(
                "submit_execution_attempt_to_broker",
                {
                    **activity_payload,
                    "execution_attempt_id": execution_attempt_id,
                },
                start_to_close_timeout=timedelta(minutes=5),
            )
        )
        return LifecycleWorkflowResult(
            workflow_id=workflow_id,
            state=submitted.attempt_status or submitted.status,
            aggregate_type="execution_attempt",
            aggregate_id=execution_attempt_id,
            execution_intent_id=execution_intent_id,
            execution_attempt_id=execution_attempt_id,
            position_id=submitted.position_id or prepared.position_id,
            reason=submitted.reason,
            completed_at=workflow.now().astimezone(timezone.utc),
        ).to_payload()


__all__ = ["TradeLifecycleWorkflow"]
