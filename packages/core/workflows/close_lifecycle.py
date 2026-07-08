from __future__ import annotations

from datetime import timezone

from temporalio import workflow

from core.workflows.contracts import CloseLifecycleWorkflowInput, LifecycleWorkflowResult


@workflow.defn
class CloseLifecycleWorkflow:
    @workflow.run
    async def run(self, request_payload: dict[str, object]) -> dict[str, object]:
        request = CloseLifecycleWorkflowInput.model_validate(request_payload)
        now = workflow.now().astimezone(timezone.utc)
        result = LifecycleWorkflowResult(
            workflow_id=request.workflow_id,
            state="accepted",
            aggregate_type="position",
            aggregate_id=request.position_id,
            completed_at=now,
            payload={
                "position_id": request.position_id,
                "execution_intent_id": request.execution_intent_id,
            },
        )
        return result.to_payload()


__all__ = ["CloseLifecycleWorkflow"]
