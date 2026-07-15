from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
import socket
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

from core.jobs.contracts import ResolvedRoutineRequest, RoutineHandler
from core.jobs.execution import RoutineActivityRunner, RoutineProjectionConflict
from core.jobs.orchestration import build_job_run_id
from core.jobs.registry import get_job_spec
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
    workflow_run_id: str,
    provider_attempt: int,
) -> ResolvedRoutineRequest:
    orchestration_id = str(request.get("orchestration_id") or workflow_run_id)
    if bool(request.get("adhoc")):
        job_key = _required_text(request, "job_key")
        job_type = _required_text(request, "job_type")
        job_run_id = _required_text(request, "job_run_id")
        workflow_lane = _validate_lane(
            expected_lane=expected_lane,
            job_key=job_key,
            job_type=job_type,
            orchestration_id=orchestration_id,
        )
        raw_payload = request.get("payload") or {}
        if not isinstance(raw_payload, Mapping):
            raise _non_retryable(
                "Ad-hoc routine payload must be a mapping",
                error_type="RoutineRequestInvalid",
                details={"field": "payload", "job_key": job_key, "job_type": job_type},
            )
        payload = dict(raw_payload)
        payload.setdefault("workflow_id", workflow_id)
        scheduled_for = parse_datetime(request.get("scheduled_for")) or _utc_now()
        return ResolvedRoutineRequest(
            job_run_id=job_run_id,
            job_key=job_key,
            job_type=job_type,
            workflow_lane=workflow_lane,
            orchestration_id=orchestration_id,
            scheduled_for=scheduled_for,
            provider_attempt=provider_attempt,
            payload=payload,
        )

    job_key = _required_text(request, "job_key")
    job_type = _required_text(request, "job_type")
    workflow_lane = _validate_lane(
        expected_lane=expected_lane,
        job_key=job_key,
        job_type=job_type,
        orchestration_id=orchestration_id,
    )
    raw_payload = request.get("payload") or {}
    if not isinstance(raw_payload, Mapping):
        raise _non_retryable(
            "Scheduled routine payload must be a mapping",
            error_type="RoutineRequestInvalid",
            details={"field": "payload", "job_key": job_key, "job_type": job_type},
        )
    scheduled_for = parse_datetime(request.get("scheduled_for"))
    if scheduled_for is None:
        raise _non_retryable(
            "Scheduled routine request requires scheduled_for",
            error_type="RoutineRequestInvalid",
            details={"field": "scheduled_for", "job_key": job_key, "job_type": job_type},
        )
    payload = dict(raw_payload)
    payload["job_key"] = job_key
    payload["scheduled_for"] = scheduled_for.isoformat().replace("+00:00", "Z")
    payload["workflow_id"] = workflow_id
    config_hash = str(request.get("config_hash") or "").strip()
    if config_hash:
        payload.setdefault("declared_config_hash", config_hash)
    job_run_id = build_job_run_id(job_key, scheduled_for)
    return ResolvedRoutineRequest(
        job_run_id=job_run_id,
        job_key=job_key,
        job_type=job_type,
        workflow_lane=workflow_lane,
        orchestration_id=orchestration_id,
        scheduled_for=scheduled_for,
        provider_attempt=provider_attempt,
        payload=payload,
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
        workflow_run_id = str(info.workflow_run_id or "").strip()
        resolved = _resolve_request(
            request,
            expected_lane=expected_lane,
            workflow_id=workflow_id,
            workflow_run_id=workflow_run_id,
            provider_attempt=max(int(info.attempt), 1),
        )
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
        executor = RoutineActivityRunner(
            database_url=default_database_url(),
            worker_name=socket.gethostname(),
            provider_heartbeat=lambda details: activity.heartbeat(dict(details)),
        )
        try:
            return executor.execute(resolved, handler)
        except RoutineProjectionConflict as exc:
            raise _non_retryable(
                str(exc),
                error_type="RoutineProjectionConflict",
                details={
                    "job_run_id": resolved.job_run_id,
                    "job_key": resolved.job_key,
                    "orchestration_id": resolved.orchestration_id,
                },
            ) from exc

    return run_routine_activity


__all__ = ["ROUTINE_ACTIVITY_NAME", "build_routine_activity"]
