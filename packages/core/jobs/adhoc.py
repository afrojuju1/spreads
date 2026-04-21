from __future__ import annotations

import asyncio
from typing import Any

from arq import create_pool

from core.jobs.registry import get_job_spec
from core.runtime.config import default_redis_url
from core.runtime.redis import build_redis_settings


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
