from __future__ import annotations

from datetime import timedelta, timezone
from typing import Any

from temporalio import workflow


@workflow.defn
class TradeLifecycleWorkflow:
    @workflow.run
    async def run(self, request_payload: dict[str, Any]) -> dict[str, Any]:
        request = dict(request_payload)
        database_url = str(request["database_url"])
        execution_intent_id = str(request["execution_intent_id"])
        workflow_id = str(request["workflow_id"])
        correlation_id = str(request["correlation_id"])
        activity_payload = {
            "database_url": database_url,
            "execution_intent_id": execution_intent_id,
            "workflow_id": workflow_id,
            "correlation_id": correlation_id,
        }
        prepared = await workflow.execute_activity(
            "ensure_execution_attempt_for_intent",
            activity_payload,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=None,
        )
        prepared_attempt_id = prepared.get("execution_attempt_id")
        if prepared_attempt_id is None:
            now = workflow.now().astimezone(timezone.utc)
            return {
                "workflow_id": workflow_id,
                "state": str(prepared.get("status") or "failed"),
                "aggregate_type": "execution_intent",
                "aggregate_id": execution_intent_id,
                "completed_at": now.isoformat().replace("+00:00", "Z"),
                "payload": {
                    "execution_intent_id": execution_intent_id,
                    "execution_attempt_id": None,
                    "prepare": prepared,
                },
            }
        execution_attempt_id = str(prepared_attempt_id)
        submitted = await workflow.execute_activity(
            "submit_execution_attempt_to_broker",
            {
                **activity_payload,
                "execution_attempt_id": execution_attempt_id,
            },
            start_to_close_timeout=timedelta(minutes=5),
        )
        attempt = submitted.get("attempt") if isinstance(submitted, dict) else {}
        state = str(attempt.get("status") or submitted.get("status") or "submitted") if isinstance(attempt, dict) else "submitted"
        now = workflow.now().astimezone(timezone.utc)
        return {
            "workflow_id": workflow_id,
            "state": state,
            "aggregate_type": "execution_attempt",
            "aggregate_id": execution_attempt_id,
            "completed_at": now.isoformat().replace("+00:00", "Z"),
            "payload": {
                "execution_intent_id": execution_intent_id,
                "execution_attempt_id": execution_attempt_id,
                "prepare": prepared,
                "submit": submitted,
            },
        }


__all__ = ["TradeLifecycleWorkflow"]
