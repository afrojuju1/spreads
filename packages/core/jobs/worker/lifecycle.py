from __future__ import annotations

import asyncio
import os
import socket
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import redis.asyncio as redis_async
from arq import create_pool

from core.jobs.orchestration import worker_runtime_lease_key
from core.jobs.registry import (
    COLLECTOR_RECOVERY_JOB_KEY,
    COLLECTOR_RECOVERY_JOB_TYPE,
    DISCOVERY_QUEUE_NAME,
    RUNTIME_QUEUE_NAME,
    get_job_spec,
)
from core.runtime.config import default_database_url, default_redis_url
from core.runtime.redis import build_redis_settings
from core.storage.factory import build_job_repository, build_storage_context

WORKER_HEARTBEAT_SECONDS = 30
WORKER_LEASE_TTL_SECONDS = 90


def worker_name() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


async def _heartbeat_runtime(job_store: Any, runtime_owner: str) -> None:
    state = {
        "kind": "worker",
        "lane": str(getattr(job_store, "_worker_lane", "") or "unknown"),
        "settings_name": str(
            getattr(job_store, "_worker_settings_name", "") or "unknown"
        ),
        "queue_name": str(getattr(job_store, "_worker_queue_name", "") or "unknown"),
    }
    while True:
        await asyncio.to_thread(
            job_store.acquire_lease,
            lease_key=worker_runtime_lease_key(runtime_owner),
            owner=runtime_owner,
            expires_in_seconds=WORKER_LEASE_TTL_SECONDS,
            state=state,
        )
        await asyncio.sleep(WORKER_HEARTBEAT_SECONDS)


async def startup(ctx: dict[str, Any]) -> None:
    ctx["database_url"] = default_database_url()
    ctx["redis_url"] = default_redis_url()
    ctx["worker_name"] = worker_name()
    ctx["storage"] = build_storage_context(ctx["database_url"])
    ctx["job_store"] = build_job_repository(context=ctx["storage"])
    setattr(ctx["job_store"], "_worker_lane", ctx.get("worker_lane", "unknown"))
    setattr(
        ctx["job_store"],
        "_worker_settings_name",
        ctx.get("worker_settings_name", "unknown"),
    )
    setattr(
        ctx["job_store"],
        "_worker_queue_name",
        ctx.get("worker_queue_name", "unknown"),
    )
    ctx["event_bus"] = redis_async.from_url(ctx["redis_url"], decode_responses=True)
    ctx["runtime_heartbeat_task"] = asyncio.create_task(
        _heartbeat_runtime(ctx["job_store"], ctx["worker_name"])
    )


async def _enqueue_startup_collector_recovery(ctx: dict[str, Any]) -> None:
    job_store = ctx["job_store"]
    definition = await asyncio.to_thread(
        job_store.get_job_definition,
        COLLECTOR_RECOVERY_JOB_KEY,
    )
    if definition is None or not bool(definition.get("enabled")):
        return
    latest_runs = await asyncio.to_thread(
        job_store.list_job_runs,
        job_key=COLLECTOR_RECOVERY_JOB_KEY,
        limit=1,
    )
    latest_run = latest_runs[0] if latest_runs else None
    if latest_run is not None and str(latest_run.get("status") or "") in {
        "queued",
        "running",
    }:
        return
    spec = get_job_spec(COLLECTOR_RECOVERY_JOB_TYPE)
    if spec is None:
        return
    redis = await create_pool(build_redis_settings(ctx["redis_url"]))
    try:
        scheduled_for = datetime.now(UTC)
        job_run_id = f"collector_recovery:start:{uuid4().hex}"
        payload = dict(definition.get("payload") or {})
        payload.update(
            {
                "job_key": COLLECTOR_RECOVERY_JOB_KEY,
                "job_type": COLLECTOR_RECOVERY_JOB_TYPE,
                "scheduled_for": scheduled_for.isoformat().replace("+00:00", "Z"),
                "singleton_scope": definition.get("singleton_scope"),
            }
        )
        run_record, created = await asyncio.to_thread(
            job_store.create_job_run,
            job_run_id=job_run_id,
            job_key=COLLECTOR_RECOVERY_JOB_KEY,
            arq_job_id=job_run_id,
            job_type=COLLECTOR_RECOVERY_JOB_TYPE,
            status="queued",
            scheduled_for=scheduled_for,
            payload=payload,
        )
        if not created:
            return
        await redis.enqueue_job(
            spec.task_name,
            COLLECTOR_RECOVERY_JOB_KEY,
            run_record["job_run_id"],
            run_record["payload"],
            run_record["arq_job_id"],
            _job_id=run_record["arq_job_id"],
            _queue_name=spec.queue_name,
        )
    finally:
        await redis.close()


async def runtime_startup(ctx: dict[str, Any]) -> None:
    ctx["worker_lane"] = "runtime"
    ctx["worker_settings_name"] = "RuntimeWorkerSettings"
    ctx["worker_queue_name"] = RUNTIME_QUEUE_NAME
    await startup(ctx)
    await _enqueue_startup_collector_recovery(ctx)


async def discovery_startup(ctx: dict[str, Any]) -> None:
    ctx["worker_lane"] = "discovery"
    ctx["worker_settings_name"] = "DiscoveryWorkerSettings"
    ctx["worker_queue_name"] = DISCOVERY_QUEUE_NAME
    await startup(ctx)


async def shutdown(ctx: dict[str, Any]) -> None:
    task = ctx.get("runtime_heartbeat_task")
    if task is not None:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    job_store = ctx.get("job_store")
    if job_store is not None:
        await asyncio.to_thread(
            job_store.release_lease,
            worker_runtime_lease_key(ctx["worker_name"]),
            owner=ctx["worker_name"],
        )
        await asyncio.to_thread(job_store.close)
    storage = ctx.get("storage")
    if storage is not None:
        await asyncio.to_thread(storage.close)
    event_bus = ctx.get("event_bus")
    if event_bus is not None:
        await event_bus.aclose()
