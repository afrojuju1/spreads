from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio.api.enums.v1 import PendingActivityState, PendingWorkflowTaskState
from temporalio.client import Client, WorkflowExecution, WorkflowExecutionDescription
from temporalio.service import RPCError, RPCStatusCode

from core.value_coercion import utc_iso

WORKFLOW_TASK_STUCK_AFTER_SECONDS = 30
ACTIVITY_DISPATCH_STUCK_AFTER_SECONDS = 30
ACTIVITY_HEARTBEAT_STALE_AFTER_SECONDS = 600
TARGET_PROJECTION_GRACE_SECONDS = 60
RECENT_SCHEDULE_FAILURE_WINDOW = timedelta(hours=24)
RETIRED_QUEUE_SUFFIX = "-jobs"


def _age_seconds(value: datetime | None, *, now: datetime) -> int | None:
    if value is None:
        return None
    return max(int((now - value.astimezone(UTC)).total_seconds()), 0)


def _iso(value: datetime | None) -> str | None:
    return None if value is None else utc_iso(value)


def _timestamp(message: Any, field: str) -> datetime | None:
    try:
        if not message.HasField(field):
            return None
    except (AttributeError, ValueError):
        return None
    return getattr(message, field).ToDatetime(tzinfo=UTC)


def _pending_activity_state(value: int) -> str:
    return PendingActivityState.Name(value).removeprefix("PENDING_ACTIVITY_STATE_").lower()


def _pending_workflow_task_state(value: int) -> str:
    return PendingWorkflowTaskState.Name(value).removeprefix("PENDING_WORKFLOW_TASK_STATE_").lower()


def _workflow_row(execution: WorkflowExecution) -> dict[str, Any]:
    return {
        "workflow_id": execution.id,
        "workflow_run_id": execution.run_id,
        "workflow_type": execution.workflow_type,
        "task_queue": execution.task_queue,
        "status": None if execution.status is None else execution.status.name.lower(),
        "started_at": _iso(execution.start_time),
        "closed_at": _iso(execution.close_time),
    }


def _target_fields(target: Mapping[str, Any] | None) -> dict[str, Any]:
    if not target:
        return {}
    return {
        "correlation_kind": target.get("kind"),
        "correlation_id": target.get("correlation_id"),
        "projected_status": target.get("status"),
    }


def _inspect_description(
    description: WorkflowExecutionDescription,
    *,
    now: datetime,
    target: Mapping[str, Any] | None,
) -> tuple[list[dict[str, Any]], int]:
    execution = _workflow_row(description)
    issues: list[dict[str, Any]] = []
    pending_activity_retry_count = 0
    pending_workflow_task = description.raw_description.pending_workflow_task
    if pending_workflow_task.ByteSize():
        scheduled_at = _timestamp(pending_workflow_task, "original_scheduled_time") or _timestamp(
            pending_workflow_task,
            "scheduled_time",
        )
        age_seconds = _age_seconds(scheduled_at, now=now)
        attempt = max(int(pending_workflow_task.attempt or 0), 1)
        if attempt > 1 or (age_seconds is not None and age_seconds > WORKFLOW_TASK_STUCK_AFTER_SECONDS):
            issues.append(
                {
                    **execution,
                    **_target_fields(target),
                    "issue": "workflow_task_stuck",
                    "severity": "blocked",
                    "task_age_seconds": age_seconds,
                    "task_attempt": attempt,
                    "task_state": _pending_workflow_task_state(pending_workflow_task.state),
                    "note": "Temporal has not completed the current workflow task.",
                }
            )

    for activity in description.raw_description.pending_activities:
        attempt = max(int(activity.attempt or 0), 0)
        last_failure = str(activity.last_failure.message or "").strip()
        is_retry = bool(last_failure or attempt > 1)
        state = int(activity.state)
        scheduled_at = _timestamp(activity, "scheduled_time")
        next_attempt_at = _timestamp(activity, "next_attempt_schedule_time")
        last_heartbeat_at = _timestamp(activity, "last_heartbeat_time")
        due_at = next_attempt_at or scheduled_at
        due_age_seconds = _age_seconds(due_at, now=now)

        issue: str | None = None
        severity: str | None = None
        age_seconds: int | None = None
        if state == PendingActivityState.PENDING_ACTIVITY_STATE_SCHEDULED:
            if is_retry:
                pending_activity_retry_count += 1
            age_seconds = due_age_seconds
            if due_age_seconds is not None and due_age_seconds > ACTIVITY_DISPATCH_STUCK_AFTER_SECONDS:
                issue = "activity_dispatch_stuck"
                severity = "blocked"
            elif is_retry:
                issue = "activity_retrying"
                severity = "degraded"
        elif state == PendingActivityState.PENDING_ACTIVITY_STATE_STARTED and last_heartbeat_at is not None:
            age_seconds = _age_seconds(last_heartbeat_at, now=now)
            if age_seconds is not None and age_seconds > ACTIVITY_HEARTBEAT_STALE_AFTER_SECONDS:
                issue = "activity_heartbeat_stale"
                severity = "blocked"

        if issue is not None:
            issues.append(
                {
                    **execution,
                    **_target_fields(target),
                    "issue": issue,
                    "severity": severity,
                    "activity_id": activity.activity_id,
                    "activity_type": activity.activity_type.name,
                    "activity_state": _pending_activity_state(activity.state),
                    "activity_attempt": attempt,
                    "activity_maximum_attempts": int(activity.maximum_attempts or 0) or None,
                    "activity_age_seconds": age_seconds,
                    "last_failure": last_failure or None,
                    "next_attempt_at": _iso(next_attempt_at),
                    "last_heartbeat_at": _iso(last_heartbeat_at),
                }
            )
    return issues, pending_activity_retry_count


def _search_attribute(execution: WorkflowExecution, name: str) -> Any:
    for key, value in execution.typed_search_attributes:
        if key.name == name:
            return value
    return None


async def _recent_unresolved_schedule_failures(
    client: Client,
    *,
    now: datetime,
) -> list[dict[str, Any]]:
    cutoff = utc_iso(now - RECENT_SCHEDULE_FAILURE_WINDOW)
    query = f'WorkflowType="ScheduledJobWorkflow" AND ExecutionStatus="Failed" AND CloseTime > "{cutoff}"'
    failed_by_schedule: dict[str, WorkflowExecution] = {}
    async for execution in client.list_workflows(query, limit=100):
        schedule_id = str(_search_attribute(execution, "TemporalScheduledById") or "").strip()
        if schedule_id and schedule_id not in failed_by_schedule:
            failed_by_schedule[schedule_id] = execution

    unresolved: list[dict[str, Any]] = []
    for schedule_id, failed_execution in failed_by_schedule.items():
        try:
            description = await client.get_schedule_handle(schedule_id).describe()
        except RPCError as exc:
            if exc.status != RPCStatusCode.NOT_FOUND:
                raise
            unresolved.append(
                {
                    **_workflow_row(failed_execution),
                    "issue": "schedule_execution_failed",
                    "severity": "blocked",
                    "schedule_id": schedule_id,
                    "note": "The failed workflow's owning schedule no longer exists.",
                }
            )
            continue
        recent_actions = list(description.info.recent_actions)
        if not recent_actions:
            continue
        latest_action = recent_actions[-1].action
        latest_run_id = getattr(latest_action, "first_execution_run_id", None)
        latest_workflow_id = getattr(latest_action, "workflow_id", None)
        if not latest_run_id or not latest_workflow_id:
            continue
        latest = await client.get_workflow_handle(latest_workflow_id, run_id=latest_run_id).describe()
        if latest.status is None or latest.status.name.lower() != "failed":
            continue
        unresolved.append(
            {
                **_workflow_row(latest),
                "issue": "schedule_execution_failed",
                "severity": "blocked",
                "schedule_id": schedule_id,
                "scheduled_at": utc_iso(recent_actions[-1].scheduled_at),
                "note": "The schedule's latest started workflow execution failed.",
            }
        )
    return unresolved


async def _find_execution_by_run_id(client: Client, run_id: str) -> WorkflowExecution | None:
    async for execution in client.list_workflows(f'RunId="{run_id}"', limit=1):
        return execution
    return None


async def collect_execution_health(
    client: Client,
    *,
    targets: Sequence[Mapping[str, Any]] = (),
    now: datetime | None = None,
) -> dict[str, Any]:
    observed_at = (now or datetime.now(UTC)).astimezone(UTC)
    open_executions: list[WorkflowExecution] = []
    async for execution in client.list_workflows('ExecutionStatus="Running"', limit=500):
        open_executions.append(execution)

    open_by_run_id = {execution.run_id: execution for execution in open_executions}
    open_by_workflow_id = {execution.id: execution for execution in open_executions}
    target_by_run_id = {
        str(target.get("workflow_run_id")): target
        for target in targets
        if str(target.get("workflow_run_id") or "").strip()
    }
    target_by_workflow_id = {
        str(target.get("workflow_id")): target
        for target in targets
        if str(target.get("workflow_id") or "").strip()
    }

    issues: list[dict[str, Any]] = []
    pending_activity_retry_count = 0
    described_run_ids: set[str] = set()
    for execution in open_executions:
        target = target_by_run_id.get(execution.run_id) or target_by_workflow_id.get(execution.id)
        description = await client.get_workflow_handle(execution.id, run_id=execution.run_id).describe()
        execution_issues, retry_count = _inspect_description(description, now=observed_at, target=target)
        issues.extend(execution_issues)
        pending_activity_retry_count += retry_count
        described_run_ids.add(execution.run_id)
        if execution.task_queue.endswith(RETIRED_QUEUE_SUFFIX):
            issues.append(
                {
                    **_workflow_row(execution),
                    **_target_fields(target),
                    "issue": "retired_task_queue_execution",
                    "severity": "blocked",
                    "note": "An open workflow is still assigned to a retired task queue.",
                }
            )

    projection_mismatches: list[dict[str, Any]] = []
    for target in targets:
        run_id = str(target.get("workflow_run_id") or "").strip()
        workflow_id = str(target.get("workflow_id") or "").strip()
        execution = open_by_run_id.get(run_id) if run_id else None
        if execution is None and workflow_id:
            execution = open_by_workflow_id.get(workflow_id)
        if execution is None and run_id:
            execution = await _find_execution_by_run_id(client, run_id)
        if execution is None and workflow_id:
            try:
                execution = await client.get_workflow_handle(workflow_id).describe()
            except RPCError as exc:
                if exc.status != RPCStatusCode.NOT_FOUND:
                    raise
                execution = None
        if execution is not None and execution.run_id not in described_run_ids and execution.status is not None and execution.status.name.lower() == "running":
            description = await client.get_workflow_handle(execution.id, run_id=execution.run_id).describe()
            execution_issues, retry_count = _inspect_description(description, now=observed_at, target=target)
            issues.extend(execution_issues)
            pending_activity_retry_count += retry_count
            described_run_ids.add(execution.run_id)

        target_at = target.get("projected_at")
        target_age_seconds = _age_seconds(target_at if isinstance(target_at, datetime) else None, now=observed_at)
        target_is_old = target_age_seconds is None or target_age_seconds > TARGET_PROJECTION_GRACE_SECONDS
        execution_status = None if execution is None or execution.status is None else execution.status.name.lower()
        if target_is_old and execution_status != "running":
            row = {
                **({} if execution is None else _workflow_row(execution)),
                **_target_fields(target),
                "workflow_id": workflow_id or (None if execution is None else execution.id),
                "workflow_run_id": run_id or (None if execution is None else execution.run_id),
                "issue": "projection_mismatch",
                "severity": "blocked",
                "projection_age_seconds": target_age_seconds,
                "provider_status": execution_status or "missing",
                "note": "The database still projects active work but the correlated Temporal execution is not running.",
            }
            projection_mismatches.append(row)
            issues.append(row)

    schedule_failures = await _recent_unresolved_schedule_failures(client, now=observed_at)
    issues.extend(schedule_failures)
    severity = {str(row.get("severity")) for row in issues}
    status = "blocked" if "blocked" in severity else "degraded" if "degraded" in severity else "healthy"
    issues.sort(
        key=lambda row: (
            0 if row.get("severity") == "blocked" else 1,
            str(row.get("issue") or ""),
            str(row.get("workflow_id") or ""),
        )
    )
    return {
        "status": status,
        "observed_at": utc_iso(observed_at),
        "open_execution_count": len(open_executions),
        "correlated_target_count": len(targets),
        "stuck_workflow_task_count": sum(1 for row in issues if row.get("issue") == "workflow_task_stuck"),
        "pending_activity_retry_count": pending_activity_retry_count,
        "stuck_activity_count": sum(
            1 for row in issues if row.get("issue") in {"activity_dispatch_stuck", "activity_heartbeat_stale"}
        ),
        "retired_queue_execution_count": sum(1 for row in issues if row.get("issue") == "retired_task_queue_execution"),
        "schedule_failure_count": len(schedule_failures),
        "projection_mismatch_count": len(projection_mismatches),
        "issues": issues,
    }


__all__ = ["collect_execution_health"]
