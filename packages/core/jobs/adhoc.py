from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

from arq import create_pool

from core.jobs.registry import get_job_spec
from core.runtime.config import default_redis_url
from core.runtime.redis import build_redis_settings


def ensure_manual_job_definition(
    job_store: Any,
    *,
    job_key: str,
    job_type: str,
    payload: Mapping[str, Any] | None = None,
    singleton_scope: str | None = None,
) -> None:
    job_store.upsert_job_definition(
        job_key=job_key,
        job_type=job_type,
        enabled=False,
        schedule_type="manual",
        schedule={},
        payload=dict(payload or {}),
        singleton_scope=singleton_scope,
    )


def enqueue_ad_hoc_job(
    *,
    job_type: str,
    job_key: str,
    job_run_id: str,
    arq_job_id: str,
    payload: dict[str, Any],
    redis_url: str | None = None,
) -> Any:
    spec = get_job_spec(job_type)
    if spec is None:
        raise RuntimeError(f"Job type is not registered: {job_type}")

    async def _enqueue() -> Any:
        redis = await create_pool(
            build_redis_settings(redis_url or default_redis_url())
        )
        try:
            return await redis.enqueue_job(
                spec.task_name,
                job_key,
                job_run_id,
                payload,
                arq_job_id,
                _job_id=arq_job_id,
                _queue_name=spec.queue_name,
            )
        finally:
            await redis.aclose()

    return asyncio.run(_enqueue())
