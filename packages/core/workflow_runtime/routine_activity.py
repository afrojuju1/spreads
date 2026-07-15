from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
import socket
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

from core.jobs.contracts import ResolvedRoutineRequest, RoutineHandler
from core.jobs.execution import JobRunExecutor
from core.jobs.orchestration import due_job_payload
from core.jobs.registry import get_job_spec
from core.jobs.specs import get_declared_job_row
from core.runtime.config import default_database_url
from core.storage.serializers import parse_datetime

ROUTINE_ACTIVITY_NAME = "run_scheduled_job_activity"


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _non_retryable(message: str, *, error_type: str, details: Mapping[str, Any]) -> ApplicationError:
    return ApplicationError(
        message,
        dict(details),
        type=error_type,
        non_retryable=True,
    )


def _required_text(request: Mapping[str, Any], key: str) -> str:
    value = str(request.get(key) or "").strip()
    if not value:
        raise _non_retryable(
            f"Routine workflow request requires {key}",
            error_type="RoutineRequestInvalid",
            details={"field": key},
        )
    return value


def _validate_lane(
    *,
    expected_lane: str,
    job_key: str,
    job_type: str,
    orchestration_id: str,
) -> str:
    spec = get_job_spec(job_type)
    if spec is None:
        raise _non_retryable(
            f"Routine type is not registered: {job_type}",
            error_type="RoutineTypeNotRegistered",
            details={
                "job_key": job_key,
                "job_type": job_type,
                "orchestration_id": orchestration_id,
            },
        )
    if spec.workflow_lane != expected_lane:
        raise _non_retryable(
            f"Routine {job_key} belongs to {spec.workflow_lane}, not worker lane {expected_lane}",
            error_type="RoutineLaneMismatch",
            details={
                "job_key": job_key,
                "job_type": job_type,
                "expected_lane": expected_lane,
                "registered_lane": spec.workflow_lane,
                "orchestration_id": orchestration_id,
            },
        )
    return spec.workflow_lane


def _resolve_request(
    request: Mapping[str, Any],
    *,
    expected_lane: str,
    workflow_id: str,
) -> ResolvedRoutineRequest | None:
    base_orchestration_id = str(request.get("orchestration_id") or workflow_id)
    if bool(request.get("adhoc")):
        job_key = _required_text(request, "job_key")
        job_type = _required_text(request, "job_type")
        job_run_id = _required_text(request, "job_run_id")
        workflow_lane = _validate_lane(
            expected_lane=expected_lane,
            job_key=job_key,
            job_type=job_type,
            orchestration_id=base_orchestration_id,
        )
        raw_payload = request.get("payload") or {}
        if not isinstance(raw_payload, Mapping):
            raise _non_retryable(
                "Ad-hoc routine payload must be a mapping",
                error_type="RoutineRequestInvalid",
                details={"field": "payload", "job_key": job_key, "job_type": job_type},
            )
        payload = dict(raw_payload)
        scheduled_for = parse_datetime(request.get("scheduled_for")) or _utc_now()
        return ResolvedRoutineRequest(
            source="adhoc",
            job_run_id=job_run_id,
            job_key=job_key,
            job_type=job_type,
            workflow_lane=workflow_lane,
            orchestration_id=base_orchestration_id,
            scheduled_for=scheduled_for,
            singleton_scope=str(payload.get("singleton_scope") or "").strip() or None,
            payload=payload,
        )

    job_key = _required_text(request, "job_key")
    definition = get_declared_job_row(job_key)
    if definition is None:
        raise _non_retryable(
            f"Declared routine definition was not found: {job_key}",
            error_type="RoutineDefinitionNotFound",
            details={"job_key": job_key, "orchestration_id": base_orchestration_id},
        )
    job_type = str(definition.get("job_type") or "").strip()
    workflow_lane = _validate_lane(
        expected_lane=expected_lane,
        job_key=job_key,
        job_type=job_type,
        orchestration_id=base_orchestration_id,
    )
    if not bool(definition.get("enabled")):
        return None
    observed_at = parse_datetime(request.get("scheduled_for")) or _utc_now()
    due = due_job_payload(definition, now=observed_at)
    if due is None:
        return None
    job_run_id, scheduled_for, payload = due
    return ResolvedRoutineRequest(
        source="scheduled",
        job_run_id=str(job_run_id),
        job_key=job_key,
        job_type=job_type,
        workflow_lane=workflow_lane,
        orchestration_id=f"{base_orchestration_id}:{job_run_id}",
        scheduled_for=scheduled_for,
        singleton_scope=str(definition.get("singleton_scope") or "").strip() or None,
        payload=dict(payload),
    )


def build_routine_activity(
    *,
    expected_lane: str,
    handlers: Mapping[str, RoutineHandler],
) -> Any:
    @activity.defn(name=ROUTINE_ACTIVITY_NAME)
    def run_routine_activity(request: dict[str, Any]) -> dict[str, Any]:
        info = activity.info()
        workflow_id = str(info.workflow_id or "").strip()
        resolved = _resolve_request(
            request,
            expected_lane=expected_lane,
            workflow_id=workflow_id,
        )
        if resolved is None:
            return {
                "status": "skipped",
                "reason": "not_due_or_disabled",
                "job_key": request.get("job_key"),
            }
        handler = handlers.get(resolved.job_type)
        if handler is None:
            raise _non_retryable(
                f"Routine handler is unavailable for {resolved.job_type} on lane {expected_lane}",
                error_type="RoutineHandlerUnavailable",
                details={
                    "job_key": resolved.job_key,
                    "job_type": resolved.job_type,
                    "expected_lane": expected_lane,
                    "orchestration_id": resolved.orchestration_id,
                },
            )
        executor = JobRunExecutor(
            database_url=default_database_url(),
            worker_name=socket.gethostname(),
            provider_heartbeat=lambda details: activity.heartbeat(dict(details)),
        )
        return executor.execute(resolved, handler)

    return run_routine_activity


__all__ = ["ROUTINE_ACTIVITY_NAME", "build_routine_activity"]
