from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from core.jobs.contracts import ResolvedRoutineRequest, RoutineExecutionContext, RoutineHandler, RoutineOutcome
from core.storage.factory import build_storage_context
from core.storage.job_repository import JobRepository

COMPLETED_JOB_STATUSES = {"succeeded", "skipped"}


def _utc_now() -> datetime:
    return datetime.now(UTC)


class RoutineProjectionConflict(RuntimeError):
    pass


class RoutineActivityRunner:
    def __init__(
        self,
        *,
        database_url: str,
        worker_name: str,
        provider_heartbeat: Callable[[Mapping[str, Any]], None],
    ) -> None:
        self._database_url = database_url
        self._worker_name = worker_name
        self._provider_heartbeat = provider_heartbeat

    def _prepare_job_run(
        self,
        job_store: JobRepository,
        request: ResolvedRoutineRequest,
        payload: dict[str, Any],
    ) -> RoutineOutcome | None:
        row, created = job_store.create_job_run(
            job_run_id=request.job_run_id,
            job_key=request.job_key,
            orchestration_id=request.orchestration_id,
            job_type=request.job_type,
            status="queued",
            scheduled_for=request.scheduled_for,
            retry_count=max(request.provider_attempt - 1, 0),
            session_id=payload.get("session_id") if isinstance(payload.get("session_id"), str) else None,
            payload=payload,
        )
        if created:
            return None
        row_orchestration_id = str(row.get("orchestration_id") or "")
        if row_orchestration_id != request.orchestration_id:
            raise RoutineProjectionConflict(
                f"Job run {request.job_run_id} belongs to Temporal run {row_orchestration_id}, "
                f"not {request.orchestration_id}."
            )
        row_status = str(row.get("status") or "")
        if row_status in COMPLETED_JOB_STATUSES:
            row = job_store.record_terminal_job_run_delivery_attempt(
                job_run_id=request.job_run_id,
                expected_orchestration_id=request.orchestration_id,
                provider_attempt=request.provider_attempt,
            )
            persisted_result = row.get("result")
            if isinstance(persisted_result, Mapping):
                return (
                    RoutineOutcome.skipped(persisted_result)
                    if row_status == "skipped"
                    else RoutineOutcome.succeeded(persisted_result)
                )
            fallback = {
                "status": row_status,
                "reason": "job_run_already_terminal",
                "job_run_id": request.job_run_id,
            }
            return RoutineOutcome.skipped(fallback) if row_status == "skipped" else RoutineOutcome.succeeded(fallback)
        return None

    @staticmethod
    def _provider_result(request: ResolvedRoutineRequest, outcome: RoutineOutcome) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "job_run_id": request.job_run_id,
            "orchestration_id": request.orchestration_id,
            "job_status": outcome.job_status,
            "provider_attempt": request.provider_attempt,
            "result": dict(outcome.persisted_result),
        }

    def _heartbeat_callback(
        self,
        job_store: JobRepository,
        request: ResolvedRoutineRequest,
    ) -> Callable[[], None]:
        def heartbeat() -> None:
            now = _utc_now()
            run_record = job_store.heartbeat_job_run(
                job_run_id=request.job_run_id,
                expected_orchestration_id=request.orchestration_id,
                heartbeat_at=now,
                worker_name=self._worker_name,
            )
            if run_record is None:
                raise RuntimeError(f"Job run {request.job_run_id} was superseded during execution.")
            self._provider_heartbeat(
                {
                    "job_run_id": request.job_run_id,
                    "heartbeat_at": now.isoformat(),
                    "provider_attempt": request.provider_attempt,
                }
            )

        return heartbeat

    def execute(
        self,
        request: ResolvedRoutineRequest,
        handler: RoutineHandler,
    ) -> dict[str, Any]:
        storage = build_storage_context(self._database_url)
        job_store = storage.jobs
        job_row_ready = False
        try:
            payload = dict(request.payload)
            early_result = self._prepare_job_run(job_store, request, payload)
            job_row_ready = True
            if early_result is not None:
                return self._provider_result(request, early_result)

            running = job_store.begin_job_run_attempt(
                job_run_id=request.job_run_id,
                expected_orchestration_id=request.orchestration_id,
                worker_name=self._worker_name,
                provider_attempt=request.provider_attempt,
                started_at=_utc_now(),
            )
            if running is None:
                raise RuntimeError(f"Job run {request.job_run_id} was superseded before execution.")

            heartbeat = self._heartbeat_callback(job_store, request)

            outcome = handler(
                RoutineExecutionContext(
                    job_run_id=request.job_run_id,
                    job_key=request.job_key,
                    job_type=request.job_type,
                    workflow_lane=request.workflow_lane,
                    scheduled_for=request.scheduled_for,
                    provider_attempt=request.provider_attempt,
                    worker_name=self._worker_name,
                    database_url=storage.database_url,
                    storage=storage,
                    payload=payload,
                    heartbeat=heartbeat,
                )
            )
            completed = job_store.update_job_run_status(
                job_run_id=request.job_run_id,
                status=outcome.job_status,
                expected_orchestration_id=request.orchestration_id,
                worker_name=self._worker_name,
                finished_at=_utc_now(),
                heartbeat_at=_utc_now(),
                result=outcome.persisted_result,
            )
            if completed is None:
                raise RuntimeError(f"Job run {request.job_run_id} was superseded before completion.")
            return self._provider_result(request, outcome)
        except Exception as exc:
            if job_row_ready:
                job_store.update_job_run_status(
                    job_run_id=request.job_run_id,
                    status="failed",
                    expected_orchestration_id=request.orchestration_id,
                    worker_name=self._worker_name,
                    finished_at=_utc_now(),
                    heartbeat_at=_utc_now(),
                    error_text=str(exc),
                )
            raise
        finally:
            storage.close()


__all__ = ["RoutineActivityRunner", "RoutineProjectionConflict"]
