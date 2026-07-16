from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from temporalio import workflow

from core.workflows.contracts import (
    CloseLifecycleWorkflowInput,
    LifecycleActivityResult,
    LifecycleWorkflowResult,
)

TERMINAL_ATTEMPT_STATUSES = frozenset({"canceled", "cancelled", "done_for_day", "expired", "failed", "filled", "rejected"})
WORKING_ATTEMPT_STATUSES = frozenset(
    {
        "accepted",
        "accepted_for_bidding",
        "calculated",
        "held",
        "new",
        "partially_filled",
        "pending_cancel",
        "pending_new",
        "pending_replace",
        "replaced",
        "stopped",
        "submitted",
        "suspended",
    }
)


def _parse_time(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _workflow_result(
    *,
    workflow_id: str,
    state: str,
    execution_intent_id: str,
    position_id: str,
    execution_attempt_id: str,
    reason: str | None = None,
    replacement_execution_intent_id: str | None = None,
) -> dict[str, object]:
    return LifecycleWorkflowResult(
        workflow_id=workflow_id,
        state=state,
        aggregate_type="execution_attempt",
        aggregate_id=execution_attempt_id,
        execution_intent_id=execution_intent_id,
        execution_attempt_id=execution_attempt_id,
        replacement_execution_intent_id=replacement_execution_intent_id,
        position_id=position_id,
        reason=reason,
        completed_at=workflow.now().astimezone(timezone.utc),
    ).to_payload()


async def _apply_stale_close_policy(
    *,
    activity_payload: dict[str, object],
    workflow_id: str,
    position_id: str,
    execution_intent_id: str,
    execution_attempt_id: str,
    stale_order_action: str,
) -> dict[str, object]:
    if stale_order_action == "leave_working":
        return _workflow_result(
            workflow_id=workflow_id,
            state="working",
            execution_intent_id=execution_intent_id,
            position_id=position_id,
            execution_attempt_id=execution_attempt_id,
            reason="stale_order_left_working",
        )
    canceled = LifecycleActivityResult.model_validate(
        await workflow.execute_activity(
            "cancel_execution_attempt",
            {
                **activity_payload,
                "execution_attempt_id": execution_attempt_id,
            },
            start_to_close_timeout=timedelta(minutes=2),
        )
    )
    if stale_order_action == "fail_closed":
        return _workflow_result(
            workflow_id=workflow_id,
            state=canceled.attempt_status or "canceled",
            execution_intent_id=execution_intent_id,
            position_id=position_id,
            execution_attempt_id=execution_attempt_id,
            reason=canceled.reason or "stale_order_canceled_fail_closed",
        )
    replacement = LifecycleActivityResult.model_validate(
        await workflow.execute_activity(
            "create_repriced_execution_intent",
            {
                **activity_payload,
                "execution_attempt_id": execution_attempt_id,
            },
            start_to_close_timeout=timedelta(minutes=2),
        )
    )
    replacement_id = replacement.replacement_execution_intent_id
    return _workflow_result(
        workflow_id=workflow_id,
        state="superseded" if replacement_id is not None else replacement.status,
        execution_intent_id=execution_intent_id,
        position_id=position_id,
        execution_attempt_id=execution_attempt_id,
        reason=replacement.reason,
        replacement_execution_intent_id=replacement_id,
    )


@workflow.defn
class CloseLifecycleWorkflow:
    @workflow.run
    async def run(self, request_payload: dict[str, Any]) -> dict[str, Any]:
        request = CloseLifecycleWorkflowInput.model_validate(request_payload)
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
            )
        )
        if prepared.execution_attempt_id is None:
            return LifecycleWorkflowResult(
                workflow_id=workflow_id,
                state=prepared.status,
                aggregate_type="execution_intent",
                aggregate_id=execution_intent_id,
                execution_intent_id=execution_intent_id,
                position_id=prepared.position_id,
                reason=prepared.reason,
                completed_at=workflow.now().astimezone(timezone.utc),
            ).to_payload()
        execution_attempt_id = prepared.execution_attempt_id
        position_id = prepared.position_id
        if position_id is None:
            raise ValueError(f"Close lifecycle attempt {execution_attempt_id} is missing position_id")
        submit_ttl_seconds = prepared.submit_ttl_seconds or 300
        stale_after_seconds = prepared.stale_after_seconds or 75
        stale_order_action = prepared.stale_order_action or "cancel_and_reprice"
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
        started_at = workflow.now().astimezone(timezone.utc)
        deadline = started_at + timedelta(seconds=submit_ttl_seconds)
        latest = submitted
        while True:
            attempt_status = str(latest.attempt_status or "").strip().lower()
            if attempt_status in TERMINAL_ATTEMPT_STATUSES:
                return _workflow_result(
                    workflow_id=workflow_id,
                    state=attempt_status,
                    execution_intent_id=execution_intent_id,
                    position_id=position_id,
                    execution_attempt_id=execution_attempt_id,
                    reason=latest.reason,
                )

            now = workflow.now().astimezone(timezone.utc)
            submitted_at = _parse_time(latest.submitted_at) or _parse_time(latest.requested_at) or started_at
            stale_at = submitted_at + timedelta(seconds=stale_after_seconds)
            policy_due_at = min(deadline, stale_at)
            if now >= policy_due_at and (attempt_status in WORKING_ATTEMPT_STATUSES or now >= deadline):
                return await _apply_stale_close_policy(
                    activity_payload=activity_payload,
                    workflow_id=workflow_id,
                    position_id=position_id,
                    execution_intent_id=execution_intent_id,
                    execution_attempt_id=execution_attempt_id,
                    stale_order_action=stale_order_action,
                )

            sleep_seconds = max(min((policy_due_at - now).total_seconds(), 15.0), 1.0)
            await workflow.sleep(timedelta(seconds=sleep_seconds))
            latest = LifecycleActivityResult.model_validate(
                await workflow.execute_activity(
                    "refresh_execution_attempt",
                    {
                        **activity_payload,
                        "execution_attempt_id": execution_attempt_id,
                    },
                    start_to_close_timeout=timedelta(minutes=2),
                )
            )


__all__ = ["CloseLifecycleWorkflow"]
