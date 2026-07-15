from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from core.jobs.contracts import ResolvedRoutineRequest, RoutineExecutionContext, RoutineHandler
from core.jobs.orchestration import singleton_lease_key
from core.storage.factory import build_storage_context
from core.storage.job_repository import JobRepository

JOB_LEASE_TTL_SECONDS = 600
TERMINAL_JOB_STATUSES = {"succeeded", "skipped", "failed"}


def _utc_now() -> datetime:
    return datetime.now(UTC)


class JobRunExecutor:
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
    ) -> dict[str, Any] | None:
        row, created = job_store.create_job_run(
            job_run_id=request.job_run_id,
            job_key=request.job_key,
            orchestration_id=request.orchestration_id,
            job_type=request.job_type,
            status="queued",
            scheduled_for=request.scheduled_for,
            session_id=payload.get("session_id") if isinstance(payload.get("session_id"), str) else None,
            payload=payload,
        )
        if created:
            return None
        row_orchestration_id = str(row.get("orchestration_id") or "")
        row_status = str(row.get("status") or "")
        if request.source == "scheduled" and row_status in TERMINAL_JOB_STATUSES:
            return {
                "status": "skipped",
                "reason": "job_run_already_terminal",
                "job_run_id": request.job_run_id,
            }
        if request.source == "adhoc" and row_orchestration_id != request.orchestration_id:
            job_store.requeue_job_run(
                job_run_id=request.job_run_id,
                orchestration_id=request.orchestration_id,
                payload=payload,
            )
            return None
        if request.source == "adhoc" and row_status in TERMINAL_JOB_STATUSES:
            return {
                "status": "skipped",
                "reason": "job_run_already_terminal",
                "job_run_id": request.job_run_id,
            }
        return None

    def _acquire_singleton_lease(
        self,
        job_store: JobRepository,
        request: ResolvedRoutineRequest,
    ) -> tuple[str | None, dict[str, Any] | None]:
        if not request.singleton_scope:
            return None, None
        lease_key = singleton_lease_key(request.job_type, request.singleton_scope)
        acquired = job_store.acquire_lease(
            lease_key=lease_key,
            owner=request.job_run_id,
            job_run_id=request.job_run_id,
            expires_in_seconds=JOB_LEASE_TTL_SECONDS,
            state={
                "kind": "temporal_singleton_job",
                "job_key": request.job_key,
                "orchestration_id": request.orchestration_id,
            },
        )
        if acquired:
            return lease_key, None
        result = {"status": "skipped", "reason": "singleton_lease_unavailable"}
        job_store.update_job_run_status(
            job_run_id=request.job_run_id,
            status="skipped",
            expected_orchestration_id=request.orchestration_id,
            worker_name=self._worker_name,
            finished_at=_utc_now(),
            heartbeat_at=_utc_now(),
            result=result,
        )
        return None, result

    def _heartbeat_callback(
        self,
        job_store: JobRepository,
        request: ResolvedRoutineRequest,
        lease_key: str | None,
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
            if lease_key is not None:
                renewed = job_store.renew_lease(
                    lease_key=lease_key,
                    owner=request.job_run_id,
                    expires_in_seconds=JOB_LEASE_TTL_SECONDS,
                    state={
                        "kind": "temporal_singleton_job",
                        "orchestration_id": request.orchestration_id,
                    },
                )
                if renewed is None:
                    raise RuntimeError(f"Job run {request.job_run_id} lost its singleton lease.")
            self._provider_heartbeat(
                {
                    "job_run_id": request.job_run_id,
                    "heartbeat_at": now.isoformat(),
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
        lease_key: str | None = None
        job_row_ready = False
        try:
            payload = dict(request.payload)
            early_result = self._prepare_job_run(job_store, request, payload)
            job_row_ready = True
            if early_result is not None:
                return early_result

            lease_key, early_result = self._acquire_singleton_lease(job_store, request)
            if early_result is not None:
                return early_result

            running = job_store.update_job_run_status(
                job_run_id=request.job_run_id,
                status="running",
                expected_orchestration_id=request.orchestration_id,
                worker_name=self._worker_name,
                started_at=_utc_now(),
                heartbeat_at=_utc_now(),
            )
            if running is None:
                raise RuntimeError(f"Job run {request.job_run_id} was superseded before execution.")

            heartbeat = self._heartbeat_callback(job_store, request, lease_key)

            outcome = handler(
                RoutineExecutionContext(
                    job_run_id=request.job_run_id,
                    job_key=request.job_key,
                    job_type=request.job_type,
                    workflow_lane=request.workflow_lane,
                    scheduled_for=request.scheduled_for,
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
            return outcome.persisted_result
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
            if lease_key is not None:
                job_store.release_lease(lease_key, owner=request.job_run_id)
            storage.close()


__all__ = ["JOB_LEASE_TTL_SECONDS", "JobRunExecutor"]
