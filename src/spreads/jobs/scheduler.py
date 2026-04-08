from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime
from typing import Any

from arq import create_pool

from spreads.jobs.orchestration import (
    SCHEDULER_RUNTIME_LEASE_KEY,
    build_redis_settings,
    default_redis_url,
    due_job_payload,
    singleton_lease_key,
    utc_now,
)
from spreads.storage import build_job_repository, default_database_url
from spreads.storage.serializers import parse_datetime

DEFAULT_POLL_SECONDS = 30
SCHEDULER_LEASE_TTL_SECONDS = 90


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Schedule ARQ jobs for spreads collectors and analysis.")
    parser.add_argument("--db", default=default_database_url(), help="Postgres database URL.")
    parser.add_argument("--redis-url", default=default_redis_url(), help="Redis connection URL.")
    parser.add_argument("--poll-seconds", type=int, default=DEFAULT_POLL_SECONDS, help="Scheduler poll interval.")
    parser.add_argument("--once", action="store_true", help="Evaluate schedules once and exit.")
    return parser.parse_args(argv)


def _lease_is_active(lease: Any) -> bool:
    if lease is None:
        return False
    expires_at = parse_datetime(lease["expires_at"])
    return expires_at is not None and expires_at > utc_now()


async def enqueue_due_jobs(job_store: Any, redis: Any) -> dict[str, Any]:
    now = datetime.now(UTC)
    definitions = await asyncio.to_thread(job_store.list_job_definitions, enabled_only=True)
    enqueued: list[str] = []
    skipped: list[dict[str, str]] = []

    for definition in definitions:
        due = due_job_payload(definition, now=now)
        if due is None:
            continue
        job_run_id, scheduled_for, payload = due
        if definition.singleton_scope:
            lease = await asyncio.to_thread(
                job_store.get_lease,
                singleton_lease_key(definition.job_type, definition.singleton_scope),
            )
            if _lease_is_active(lease):
                skipped.append(
                    {
                        "job_key": definition.job_key,
                        "reason": "singleton_lease_active",
                    }
                )
                continue

        run_record, created = await asyncio.to_thread(
            job_store.create_job_run,
            job_run_id=job_run_id,
            job_key=definition.job_key,
            arq_job_id=job_run_id,
            job_type=definition.job_type,
            status="queued",
            scheduled_for=scheduled_for,
            payload=payload,
        )
        if not created:
            continue

        task_name = {
            "live_collector": "run_live_collector_job",
            "post_close_analysis": "run_post_close_analysis_job",
            "post_market_analysis": "run_post_market_analysis_job",
        }.get(definition.job_type)
        if task_name is None:
            await asyncio.to_thread(
                job_store.update_job_run_status,
                job_run_id=run_record["job_run_id"],
                status="failed",
                finished_at=datetime.now(UTC),
                error_text=f"Unsupported job_type: {definition.job_type}",
            )
            continue
        try:
            result = await redis.enqueue_job(
                task_name,
                definition.job_key,
                run_record["job_run_id"],
                payload,
                _job_id=run_record["job_run_id"],
            )
            if result is None:
                await asyncio.to_thread(
                    job_store.update_job_run_status,
                    job_run_id=run_record["job_run_id"],
                    status="skipped",
                    finished_at=datetime.now(UTC),
                    error_text="ARQ rejected duplicate job id",
                )
                continue
            enqueued.append(run_record["job_run_id"])
        except Exception as exc:
            await asyncio.to_thread(
                job_store.update_job_run_status,
                job_run_id=run_record["job_run_id"],
                status="failed",
                finished_at=datetime.now(UTC),
                error_text=str(exc),
            )
    return {"enqueued": enqueued, "skipped": skipped}


async def scheduler_loop(args: argparse.Namespace) -> int:
    job_store = build_job_repository(args.db)
    redis = await create_pool(build_redis_settings(args.redis_url))
    try:
        while True:
            owner = "scheduler"
            await asyncio.to_thread(
                job_store.acquire_lease,
                lease_key=SCHEDULER_RUNTIME_LEASE_KEY,
                owner=owner,
                expires_in_seconds=max(args.poll_seconds * 3, SCHEDULER_LEASE_TTL_SECONDS),
                state={"kind": "scheduler", "last_tick_at": datetime.now(UTC).isoformat()},
            )
            result = await enqueue_due_jobs(job_store, redis)
            if result["enqueued"] or result["skipped"]:
                print(
                    f"[{datetime.now(UTC).isoformat(timespec='seconds').replace('+00:00', 'Z')}] "
                    f"enqueued={len(result['enqueued'])} skipped={len(result['skipped'])}"
                )
            if args.once:
                break
            await asyncio.sleep(max(args.poll_seconds, 1))
    finally:
        await asyncio.to_thread(job_store.release_lease, SCHEDULER_RUNTIME_LEASE_KEY, owner="scheduler")
        await redis.close()
        job_store.close()
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(scheduler_loop(args))


if __name__ == "__main__":
    raise SystemExit(main())
