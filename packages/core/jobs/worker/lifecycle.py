from __future__ import annotations

import asyncio
import logging
import os
import socket
from typing import Any

import redis.asyncio as redis_async

from core.jobs.orchestration import worker_runtime_lease_key
from core.jobs.registry import (
    DATA_QUEUE_NAME,
    RESEARCH_QUEUE_NAME,
    RUNTIME_QUEUE_NAME,
    VALUATION_QUEUE_NAME,
)
from core.observability.logging import configure_logging, log_event
from core.runtime.config import default_database_url, default_redis_url
from core.storage.factory import build_job_repository, build_storage_context

WORKER_HEARTBEAT_SECONDS = 30
WORKER_LEASE_TTL_SECONDS = 90

logger = logging.getLogger(__name__)


def worker_name() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


async def _heartbeat_runtime(job_store: Any, runtime_owner: str) -> None:
    state = {
        "kind": "worker",
        "lane": str(getattr(job_store, "_worker_lane", "") or "unknown"),
        "settings_name": str(getattr(job_store, "_worker_settings_name", "") or "unknown"),
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
    configure_logging(service=f"worker-{ctx.get('worker_lane', 'unknown')}", force=True)
    ctx["database_url"] = default_database_url()
    ctx["redis_url"] = default_redis_url()
    ctx["worker_name"] = worker_name()
    ctx["storage"] = build_storage_context(ctx["database_url"])
    ctx["job_store"] = build_job_repository(context=ctx["storage"])
    ctx["job_store"]._worker_lane = ctx.get("worker_lane", "unknown")
    ctx["job_store"]._worker_settings_name = ctx.get("worker_settings_name", "unknown")
    ctx["job_store"]._worker_queue_name = ctx.get("worker_queue_name", "unknown")
    ctx["event_bus"] = redis_async.from_url(ctx["redis_url"], decode_responses=True)
    ctx["runtime_heartbeat_task"] = asyncio.create_task(_heartbeat_runtime(ctx["job_store"], ctx["worker_name"]))
    log_event(
        logger,
        logging.INFO,
        "worker_started",
        worker_name=ctx["worker_name"],
        lane=ctx.get("worker_lane"),
        settings_name=ctx.get("worker_settings_name"),
        queue_name=ctx.get("worker_queue_name"),
    )


async def runtime_startup(ctx: dict[str, Any]) -> None:
    ctx["worker_lane"] = "runtime"
    ctx["worker_settings_name"] = "RuntimeWorkerSettings"
    ctx["worker_queue_name"] = RUNTIME_QUEUE_NAME
    await startup(ctx)


async def data_startup(ctx: dict[str, Any]) -> None:
    ctx["worker_lane"] = "data"
    ctx["worker_settings_name"] = "DataWorkerSettings"
    ctx["worker_queue_name"] = DATA_QUEUE_NAME
    await startup(ctx)


async def valuation_startup(ctx: dict[str, Any]) -> None:
    ctx["worker_lane"] = "valuation"
    ctx["worker_settings_name"] = "ValuationWorkerSettings"
    ctx["worker_queue_name"] = VALUATION_QUEUE_NAME
    await startup(ctx)


async def research_startup(ctx: dict[str, Any]) -> None:
    ctx["worker_lane"] = "research"
    ctx["worker_settings_name"] = "ResearchWorkerSettings"
    ctx["worker_queue_name"] = RESEARCH_QUEUE_NAME
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
    log_event(
        logger,
        logging.INFO,
        "worker_stopped",
        worker_name=ctx.get("worker_name"),
        lane=ctx.get("worker_lane"),
        settings_name=ctx.get("worker_settings_name"),
        queue_name=ctx.get("worker_queue_name"),
    )
