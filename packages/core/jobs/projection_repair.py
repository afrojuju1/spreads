from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import UTC
from typing import Any

from temporalio.api.enums.v1 import EventType
from temporalio.api.failure.v1 import Failure
from temporalio.client import Client, WorkflowExecution, WorkflowExecutionDescription

from core.db.decorators import with_storage
from core.runtime.config import default_workflow_address, default_workflow_namespace
from core.value_coercion import utc_iso
from core.workflow_runtime.provider import PROVIDER_NAME, connect_provider

EXPECTED_WORKFLOW_TYPE = "ScheduledJobWorkflow"


def _failure_projection(failure: Failure | None) -> dict[str, Any] | None:
    if failure is None or not failure.ByteSize():
        return None
    payload: dict[str, Any] = {
        "message": str(failure.message or ""),
        "source": str(failure.source or "") or None,
    }
    if failure.HasField("application_failure_info"):
        payload["type"] = str(failure.application_failure_info.type or "") or None
        payload["non_retryable"] = bool(failure.application_failure_info.non_retryable)
    elif failure.HasField("activity_failure_info"):
        payload["type"] = "ActivityFailure"
        payload["activity_type"] = failure.activity_failure_info.activity_type.name
        payload["activity_id"] = failure.activity_failure_info.activity_id
        payload["retry_state"] = int(failure.activity_failure_info.retry_state)
    elif failure.HasField("timeout_failure_info"):
        payload["type"] = "TimeoutFailure"
        payload["timeout_type"] = int(failure.timeout_failure_info.timeout_type)
    elif failure.HasField("canceled_failure_info"):
        payload["type"] = "CanceledFailure"
    if failure.HasField("cause"):
        payload["cause"] = _failure_projection(failure.cause)
    return payload


def _failure_message(value: Any) -> str | None:
    if not isinstance(value, Mapping):
        return None
    nested = value.get("cause") or value.get("failure")
    nested_message = _failure_message(nested)
    if nested_message:
        return nested_message
    message = str(value.get("message") or value.get("reason") or "").strip()
    return message or None


async def _find_execution(client: Client, *, run_id: str) -> WorkflowExecution:
    matches: list[WorkflowExecution] = []
    async for execution in client.list_workflows(f'RunId="{run_id}"', limit=2):
        matches.append(execution)
    if not matches:
        raise ValueError(f"Temporal run not found: {run_id}")
    if len(matches) != 1:
        raise ValueError(f"Temporal run identity is not unique: {run_id}")
    return matches[0]


async def _history_projection(
    client: Client,
    *,
    workflow_id: str,
    run_id: str,
) -> tuple[int, dict[str, Any] | None]:
    handle = client.get_workflow_handle(workflow_id, run_id=run_id)
    maximum_activity_attempt = 0
    terminal_failure: dict[str, Any] | None = None
    async for event in handle.fetch_history_events():
        if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED:
            maximum_activity_attempt = max(
                maximum_activity_attempt,
                int(event.activity_task_started_event_attributes.attempt or 1),
            )
        elif event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
            attributes = event.workflow_execution_failed_event_attributes
            terminal_failure = {
                "retry_state": int(attributes.retry_state),
                "failure": _failure_projection(attributes.failure),
            }
        elif event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
            attributes = event.workflow_execution_timed_out_event_attributes
            terminal_failure = {
                "retry_state": int(attributes.retry_state),
                "timeout_type": int(attributes.timeout_type),
            }
        elif event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
            terminal_failure = {"type": "canceled"}
        elif event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
            attributes = event.workflow_execution_terminated_event_attributes
            terminal_failure = {
                "type": "terminated",
                "reason": str(attributes.reason or "") or None,
                "identity": str(attributes.identity or "") or None,
            }
    return maximum_activity_attempt, terminal_failure


def _validate_completed_result(
    value: Any,
    *,
    job_run_id: str,
    orchestration_id: str,
    provider_attempt: int,
) -> str:
    if not isinstance(value, Mapping):
        raise ValueError("Temporal workflow result is not a routine projection envelope.")
    envelope = dict(value)
    if int(envelope.get("schema_version") or 0) != 2:
        raise ValueError("Temporal workflow result predates the repair-safe projection contract.")
    if str(envelope.get("job_run_id") or "") != job_run_id:
        raise ValueError("Temporal workflow result belongs to a different job run.")
    if str(envelope.get("orchestration_id") or "") != orchestration_id:
        raise ValueError("Temporal workflow result belongs to a different orchestration run.")
    job_status = str(envelope.get("job_status") or "").strip().lower()
    if job_status not in {"succeeded", "skipped"}:
        raise ValueError(f"Temporal workflow returned an invalid terminal job status: {job_status or 'missing'}")
    envelope_attempt = max(int(envelope.get("provider_attempt") or 0), 0)
    if envelope_attempt != provider_attempt:
        raise ValueError(
            f"Temporal result attempt {envelope_attempt} does not match history attempt {provider_attempt}."
        )
    if str(envelope.get("result_store") or "") != "job_runs":
        raise ValueError("Temporal workflow result references an unsupported result store.")
    if str(envelope.get("result_ref") or "") != job_run_id:
        raise ValueError("Temporal workflow result references a different persisted job result.")
    return job_status


async def _read_terminal_projection(
    *,
    job_row: Mapping[str, Any],
    workflow_address: str,
    workflow_namespace: str,
) -> dict[str, Any]:
    job_run_id = str(job_row.get("job_run_id") or "").strip()
    orchestration_id = str(job_row.get("orchestration_id") or "").strip()
    if not orchestration_id:
        raise ValueError(f"Job run {job_run_id} has no Temporal orchestration ID.")
    payload = job_row.get("payload") if isinstance(job_row.get("payload"), Mapping) else {}
    expected_workflow_id = str(payload.get("workflow_id") or "").strip()
    if not expected_workflow_id:
        raise ValueError(f"Job run {job_run_id} has no workflow ID in its projection payload.")

    client = await connect_provider(address=workflow_address, namespace=workflow_namespace)
    execution = await _find_execution(client, run_id=orchestration_id)
    if execution.id != expected_workflow_id:
        raise ValueError(
            f"Job run {job_run_id} projects workflow {expected_workflow_id}, but Temporal run "
            f"{orchestration_id} belongs to {execution.id}."
        )
    if execution.workflow_type != EXPECTED_WORKFLOW_TYPE:
        raise ValueError(
            f"Temporal run {orchestration_id} is {execution.workflow_type}, not {EXPECTED_WORKFLOW_TYPE}."
        )
    handle = client.get_workflow_handle(execution.id, run_id=execution.run_id)
    description: WorkflowExecutionDescription = await handle.describe()
    provider_status = None if description.status is None else description.status.name.lower()
    if provider_status == "running" or provider_status is None:
        raise ValueError(f"Temporal run {orchestration_id} is still active; projection repair is refused.")
    if description.close_time is None:
        raise ValueError(f"Temporal run {orchestration_id} is terminal without a close time.")

    provider_attempt, terminal_failure = await _history_projection(
        client,
        workflow_id=execution.id,
        run_id=execution.run_id,
    )
    if provider_attempt < 1:
        raise ValueError(f"Temporal run {orchestration_id} has no Activity attempt to project.")

    error_text: str | None = None
    if provider_status == "completed":
        job_status = _validate_completed_result(
            await handle.result(),
            job_run_id=job_run_id,
            orchestration_id=orchestration_id,
            provider_attempt=provider_attempt,
        )
        persisted_result = job_row.get("result")
        if not isinstance(persisted_result, Mapping):
            raise ValueError(
                f"Job run {job_run_id} has no persisted Postgres result; Temporal history "
                "does not duplicate rich job evidence."
            )
        result = dict(persisted_result)
    else:
        job_status = "failed"
        failure_message = _failure_message(terminal_failure)
        error_text = f"Temporal workflow {provider_status}: {failure_message or execution.id}"
        result = {
            "status": "failed",
            "provider": {
                "name": PROVIDER_NAME,
                "status": provider_status,
                "workflow_id": execution.id,
                "workflow_run_id": execution.run_id,
                "attempt_count": provider_attempt,
                "failure": terminal_failure,
            },
        }
    return {
        "job_run_id": job_run_id,
        "orchestration_id": orchestration_id,
        "workflow_id": execution.id,
        "workflow_type": execution.workflow_type,
        "provider_status": provider_status,
        "job_status": job_status,
        "provider_attempt_count": provider_attempt,
        "retry_count": provider_attempt - 1,
        "finished_at": description.close_time.astimezone(UTC),
        "result": result,
        "error_text": error_text,
    }


@with_storage()
def repair_terminal_routine_projection(
    *,
    job_run_id: str,
    db_target: str | None = None,
    workflow_address: str | None = None,
    workflow_namespace: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    job_row = storage.jobs.get_job_run(job_run_id)
    if job_row is None:
        raise ValueError(f"Unknown job_run_id: {job_run_id}")
    projection = asyncio.run(
        _read_terminal_projection(
            job_row=dict(job_row),
            workflow_address=workflow_address or default_workflow_address(),
            workflow_namespace=workflow_namespace or default_workflow_namespace(),
        )
    )
    repaired, changed = storage.jobs.repair_terminal_job_run_projection(
        job_run_id=job_run_id,
        expected_orchestration_id=str(projection["orchestration_id"]),
        status=str(projection["job_status"]),
        retry_count=int(projection["retry_count"]),
        finished_at=projection["finished_at"],
        result=dict(projection["result"]),
        error_text=projection["error_text"],
    )
    return {
        "status": "repaired" if changed else "unchanged",
        "job_run_id": job_run_id,
        "workflow_id": projection["workflow_id"],
        "workflow_run_id": projection["orchestration_id"],
        "provider_status": projection["provider_status"],
        "provider_attempt_count": projection["provider_attempt_count"],
        "job_status": repaired.get("status"),
        "retry_count": repaired.get("retry_count"),
        "finished_at": utc_iso(repaired.get("finished_at")),
        "changed": changed,
    }


__all__ = ["repair_terminal_routine_projection"]
