from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from core.jobs.orchestration import singleton_lease_key

from .observability import _emit_live_collector_observability, _publish_job_run_event

JOB_LEASE_TTL_SECONDS = 600


class ManagedJobFailure(RuntimeError):
    def __init__(self, message: str, *, result: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.result = result


class SupersededJobRun(RuntimeError):
    pass


async def _mark_running(
    job_store: Any, job_run_id: str, runtime_owner: str, arq_job_id: str
) -> Any:
    now = datetime.now(UTC)
    run_record = await asyncio.to_thread(
        job_store.update_job_run_status,
        job_run_id=job_run_id,
        status="running",
        expected_arq_job_id=arq_job_id,
        worker_name=runtime_owner,
        started_at=now,
        heartbeat_at=now,
    )
    if run_record is None:
        raise SupersededJobRun(f"Job run {job_run_id} was superseded before start.")
    return run_record


def _sync_job_heartbeat(
    job_store: Any,
    *,
    job_run_id: str,
    arq_job_id: str,
    runtime_owner: str,
    lease_key: str | None,
) -> None:
    now = datetime.now(UTC)
    run_record = job_store.heartbeat_job_run(
        job_run_id=job_run_id,
        expected_arq_job_id=arq_job_id,
        heartbeat_at=now,
        worker_name=runtime_owner,
    )
    if run_record is None:
        raise SupersededJobRun(f"Job run {job_run_id} was superseded during execution.")
    if lease_key is not None:
        job_store.renew_lease(
            lease_key=lease_key,
            owner=job_run_id,
            expires_in_seconds=JOB_LEASE_TTL_SECONDS,
            state={"kind": "singleton_job"},
        )


async def _execute_managed_job(
    ctx: dict[str, Any],
    *,
    job_key: str,
    job_run_id: str,
    arq_job_id: str,
    payload: dict[str, Any],
    runner: Any,
    compact_result: Any,
    on_running: Any | None = None,
    on_completed: Any | None = None,
    on_failed: Any | None = None,
) -> dict[str, Any]:
    job_store = ctx["job_store"]
    runtime_owner = ctx["worker_name"]
    lease_key = None
    scope = payload.get("singleton_scope")
    job_type = payload.get("job_type")
    if scope and job_type:
        lease_key = singleton_lease_key(str(job_type), str(scope))
        acquired = await asyncio.to_thread(
            job_store.acquire_lease,
            lease_key=lease_key,
            owner=job_run_id,
            job_run_id=job_run_id,
            expires_in_seconds=JOB_LEASE_TTL_SECONDS,
            state={"kind": "singleton_job", "job_key": job_key},
        )
        if not acquired:
            result = {"status": "skipped", "reason": "singleton_lease_unavailable"}
            skipped_record = await asyncio.to_thread(
                job_store.update_job_run_status,
                job_run_id=job_run_id,
                status="skipped",
                expected_arq_job_id=arq_job_id,
                worker_name=runtime_owner,
                finished_at=datetime.now(UTC),
                heartbeat_at=datetime.now(UTC),
                result=result,
            )
            await _publish_job_run_event(ctx, skipped_record)
            return result

    running_record = await _mark_running(
        job_store, job_run_id, runtime_owner, arq_job_id
    )
    await _publish_job_run_event(ctx, running_record)
    if on_running is not None:
        await on_running(running_record)
    try:
        result = await asyncio.to_thread(
            runner,
            lambda: _sync_job_heartbeat(
                job_store,
                job_run_id=job_run_id,
                arq_job_id=arq_job_id,
                runtime_owner=runtime_owner,
                lease_key=lease_key,
            ),
        )
        compact = compact_result(result)
        final_status = (
            "skipped"
            if isinstance(result, dict) and result.get("status") == "skipped"
            else "succeeded"
        )
        completed_record = await asyncio.to_thread(
            job_store.update_job_run_status,
            job_run_id=job_run_id,
            status=final_status,
            expected_arq_job_id=arq_job_id,
            worker_name=runtime_owner,
            finished_at=datetime.now(UTC),
            heartbeat_at=datetime.now(UTC),
            result=compact,
        )
        if completed_record is None:
            raise SupersededJobRun(
                f"Job run {job_run_id} was superseded before completion."
            )
        await _publish_job_run_event(ctx, completed_record)
        if on_completed is not None:
            await on_completed(completed_record, result)
        if payload.get("job_type") == "live_collector" and final_status == "succeeded":
            await _emit_live_collector_observability(ctx, completed_record)
        return compact
    except SupersededJobRun:
        return {"status": "superseded", "job_run_id": job_run_id}
    except Exception as exc:
        partial_result = exc.result if isinstance(exc, ManagedJobFailure) else None
        failed_record = await asyncio.to_thread(
            job_store.update_job_run_status,
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=arq_job_id,
            worker_name=runtime_owner,
            finished_at=datetime.now(UTC),
            heartbeat_at=datetime.now(UTC),
            result=None if partial_result is None else compact_result(partial_result),
            error_text=str(exc),
        )
        await _publish_job_run_event(ctx, failed_record)
        if on_failed is not None and failed_record is not None:
            await on_failed(failed_record, partial_result)
        raise
    finally:
        if lease_key is not None:
            await asyncio.to_thread(
                job_store.release_lease, lease_key, owner=job_run_id
            )
