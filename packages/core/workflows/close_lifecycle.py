from __future__ import annotations

from datetime import datetime, timedelta, timezone

from temporalio import workflow

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


def _as_mapping(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, dict) else {}


def _positive_int(value: object, default: int) -> int:
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _attempt_from_result(result: object) -> dict[str, object]:
    payload = _as_mapping(result)
    attempt = payload.get("attempt")
    return dict(attempt) if isinstance(attempt, dict) else payload


def _status(attempt: dict[str, object]) -> str:
    return str(attempt.get("status") or "").strip().lower()


def _parse_time(value: object) -> datetime | None:
    if value in (None, ""):
        return None
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
    execution_intent_id: str | None,
    position_id: str,
    execution_attempt_id: str,
    prepared: object,
    submitted: object,
    latest: object,
    replacement: object | None = None,
) -> dict[str, object]:
    now = workflow.now().astimezone(timezone.utc)
    payload: dict[str, object] = {
        "position_id": position_id,
        "execution_intent_id": execution_intent_id,
        "execution_attempt_id": execution_attempt_id,
        "prepare": prepared,
        "submit": submitted,
        "latest": latest,
    }
    if replacement is not None:
        payload["replacement"] = replacement
    return {
        "workflow_id": workflow_id,
        "state": state,
        "aggregate_type": "execution_attempt",
        "aggregate_id": execution_attempt_id,
        "completed_at": now.isoformat().replace("+00:00", "Z"),
        "payload": payload,
    }


async def _apply_stale_close_policy(
    *,
    activity_payload: dict[str, object],
    workflow_id: str,
    position_id: str,
    execution_intent_id: str | None,
    execution_attempt_id: str,
    stale_order_action: str,
    prepared: object,
    submitted: object,
    latest: object,
) -> dict[str, object]:
    if stale_order_action == "leave_working":
        return _workflow_result(
            workflow_id=workflow_id,
            state="working",
            execution_intent_id=execution_intent_id,
            position_id=position_id,
            execution_attempt_id=execution_attempt_id,
            prepared=prepared,
            submitted=submitted,
            latest=latest,
        )
    canceled = await workflow.execute_activity(
        "cancel_execution_attempt",
        {
            **activity_payload,
            "execution_attempt_id": execution_attempt_id,
        },
        start_to_close_timeout=timedelta(minutes=2),
    )
    if stale_order_action == "fail_closed":
        return _workflow_result(
            workflow_id=workflow_id,
            state="canceled",
            execution_intent_id=execution_intent_id,
            position_id=position_id,
            execution_attempt_id=execution_attempt_id,
            prepared=prepared,
            submitted=submitted,
            latest=canceled,
        )
    replacement = await workflow.execute_activity(
        "create_repriced_execution_intent",
        {
            **activity_payload,
            "execution_attempt_id": execution_attempt_id,
        },
        start_to_close_timeout=timedelta(minutes=2),
    )
    return _workflow_result(
        workflow_id=workflow_id,
        state="superseded",
        execution_intent_id=execution_intent_id,
        position_id=position_id,
        execution_attempt_id=execution_attempt_id,
        prepared=prepared,
        submitted=submitted,
        latest=canceled,
        replacement=replacement,
    )


@workflow.defn
class CloseLifecycleWorkflow:
    @workflow.run
    async def run(self, request_payload: dict[str, object]) -> dict[str, object]:
        request = dict(request_payload)
        database_url = str(request["database_url"])
        position_id = str(request["position_id"])
        execution_intent_id = None if request.get("execution_intent_id") is None else str(request["execution_intent_id"])
        workflow_id = str(request["workflow_id"])
        correlation_id = str(request["correlation_id"])
        payload = _as_mapping(request.get("payload"))
        execution_policy = _as_mapping(payload.get("execution_policy"))
        repricing_policy = _as_mapping(payload.get("repricing_policy"))
        if not repricing_policy:
            repricing_policy = _as_mapping(execution_policy.get("repricing_policy"))
        submit_ttl_seconds = _positive_int(execution_policy.get("submit_ttl_minutes"), 5) * 60
        stale_after_seconds = _positive_int(repricing_policy.get("stale_after_seconds"), 75)
        stale_order_action = str(
            execution_policy.get("stale_order_action") or repricing_policy.get("stale_order_action") or "cancel_and_reprice"
        ).strip().lower()
        if stale_order_action not in {"cancel_and_reprice", "fail_closed", "leave_working"}:
            stale_order_action = "cancel_and_reprice"
        activity_payload = {
            "database_url": database_url,
            "execution_intent_id": execution_intent_id,
            "position_id": position_id,
            "workflow_id": workflow_id,
            "correlation_id": correlation_id,
        }
        prepared = await workflow.execute_activity(
            "ensure_execution_attempt_for_intent",
            activity_payload,
            start_to_close_timeout=timedelta(minutes=2),
        )
        execution_attempt_id = str(prepared["execution_attempt_id"])
        submitted = await workflow.execute_activity(
            "submit_execution_attempt_to_broker",
            {
                **activity_payload,
                "execution_attempt_id": execution_attempt_id,
            },
            start_to_close_timeout=timedelta(minutes=5),
        )
        attempt = _attempt_from_result(submitted)
        started_at = workflow.now().astimezone(timezone.utc)
        deadline = started_at + timedelta(seconds=submit_ttl_seconds)
        latest: object = submitted
        while True:
            attempt_status = _status(attempt)
            if attempt_status in TERMINAL_ATTEMPT_STATUSES:
                return _workflow_result(
                    workflow_id=workflow_id,
                    state=attempt_status,
                    execution_intent_id=execution_intent_id,
                    position_id=position_id,
                    execution_attempt_id=execution_attempt_id,
                    prepared=prepared,
                    submitted=submitted,
                    latest=latest,
                )

            now = workflow.now().astimezone(timezone.utc)
            submitted_at = _parse_time(attempt.get("submitted_at")) or _parse_time(attempt.get("requested_at")) or started_at
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
                    prepared=prepared,
                    submitted=submitted,
                    latest=latest,
                )

            sleep_seconds = max(min((policy_due_at - now).total_seconds(), 15.0), 1.0)
            await workflow.sleep(timedelta(seconds=sleep_seconds))
            latest = await workflow.execute_activity(
                "refresh_execution_attempt",
                {
                    **activity_payload,
                    "execution_attempt_id": execution_attempt_id,
                },
                start_to_close_timeout=timedelta(minutes=2),
            )
            attempt = _attempt_from_result(latest)


__all__ = ["CloseLifecycleWorkflow"]
