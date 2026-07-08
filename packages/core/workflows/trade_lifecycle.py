from __future__ import annotations

from datetime import timezone

from temporalio import workflow

from core.workflows.contracts import LifecycleWorkflowResult, TradeLifecycleWorkflowInput


@workflow.defn
class TradeLifecycleWorkflow:
    @workflow.run
    async def run(self, request_payload: dict[str, object]) -> dict[str, object]:
        request = TradeLifecycleWorkflowInput.model_validate(request_payload)
        now = workflow.now().astimezone(timezone.utc)
        result = LifecycleWorkflowResult(
            workflow_id=request.workflow_id,
            state="accepted",
            aggregate_type="execution_intent",
            aggregate_id=request.execution_intent_id,
            completed_at=now,
            payload={"execution_intent_id": request.execution_intent_id},
        )
        return result.to_payload()


__all__ = ["TradeLifecycleWorkflow"]
