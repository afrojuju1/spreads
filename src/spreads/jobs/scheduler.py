from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any

from arq import create_pool

from spreads.events.bus import publish_global_event_async
from spreads.jobs.registry import get_job_spec
from spreads.jobs.orchestration import (
    SCHEDULER_RUNTIME_LEASE_KEY,
    build_job_attempt_id,
    build_job_run_id,
    due_job_payload,
    isoformat_utc,
    resolve_live_tick_plan,
    singleton_lease_key,
    utc_now,
)
from spreads.runtime.config import default_database_url, default_redis_url
from spreads.runtime.redis import build_redis_settings
from spreads.storage.factory import build_job_repository
from spreads.storage.serializers import parse_datetime

DEFAULT_POLL_SECONDS = 30
SCHEDULER_LEASE_TTL_SECONDS = 90
LIVE_SLOT_MAX_RETRIES = 3


def _log_scheduler_event(event: str, **payload: Any) -> None:
    record = {
        "event": event,
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        **payload,
    }
    print(json.dumps(record, separators=(",", ":"), sort_keys=True), flush=True)


async def _publish_job_run_update(redis: Any, run_record: Any) -> None:
    try:
        payload = dict(run_record.get("payload") or {})
        await publish_global_event_async(
            redis,
            topic="job.run.updated",
            event_class="control_event",
            entity_type="job_run",
            entity_id=run_record["job_run_id"],
            payload=run_record,
            timestamp=run_record.get("finished_at") or run_record.get("heartbeat_at") or run_record["scheduled_for"],
            source="scheduler",
            session_date=str(payload["session_date"]) if isinstance(payload.get("session_date"), str) else None,
            correlation_id=str(run_record["job_key"]),
        )
    except Exception:
        pass


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

async def _enqueue_job_run(
    *,
    job_store: Any,
    redis: Any,
    definition: Any,
    run_record: Any,
) -> bool:
    spec = get_job_spec(str(definition["job_type"]))
    if spec is None:
        failed_record = await asyncio.to_thread(
            job_store.update_job_run_status,
            job_run_id=run_record["job_run_id"],
            status="failed",
            finished_at=datetime.now(UTC),
            error_text=f"Unsupported job_type: {definition['job_type']}",
        )
        if failed_record is not None:
            await _publish_job_run_update(redis, failed_record)
        return False
    try:
        result = await redis.enqueue_job(
            spec.task_name,
            definition["job_key"],
            run_record["job_run_id"],
            run_record["payload"],
            run_record["arq_job_id"],
            _job_id=run_record["arq_job_id"],
            _queue_name=spec.queue_name,
        )
        if result is None:
            skipped_record = await asyncio.to_thread(
                job_store.update_job_run_status,
                job_run_id=run_record["job_run_id"],
                status="skipped",
                finished_at=datetime.now(UTC),
                error_text="ARQ rejected duplicate job id",
            )
            if skipped_record is not None:
                await _publish_job_run_update(redis, skipped_record)
            return False
        await _publish_job_run_update(redis, run_record)
        return True
    except Exception as exc:
        failed_record = await asyncio.to_thread(
            job_store.update_job_run_status,
            job_run_id=run_record["job_run_id"],
            status="failed",
            finished_at=datetime.now(UTC),
            error_text=str(exc),
        )
        if failed_record is not None:
            await _publish_job_run_update(redis, failed_record)
        return False


def _live_run_active(run_record: Any, *, now: datetime, interval_seconds: int) -> bool:
    if run_record["status"] == "queued":
        queued_at = parse_datetime(run_record["scheduled_for"])
        if queued_at is None:
            return False
        return queued_at >= now - timedelta(seconds=max(interval_seconds, DEFAULT_POLL_SECONDS * 2))
    if run_record["status"] == "running":
        last_seen = (
            parse_datetime(run_record.get("heartbeat_at"))
            or parse_datetime(run_record.get("started_at"))
            or parse_datetime(run_record.get("scheduled_for"))
        )
        if last_seen is None:
            return False
        return last_seen >= now - timedelta(seconds=max(interval_seconds * 2, DEFAULT_POLL_SECONDS * 4))
    return False


async def _reconcile_live_collector_jobs(job_store: Any, redis: Any, *, now: datetime) -> dict[str, Any]:
    definitions = await asyncio.to_thread(job_store.list_job_definitions, enabled_only=True, job_type="live_collector")
    enqueued: list[str] = []
    skipped: list[dict[str, str]] = []

    for definition in definitions:
        plan = resolve_live_tick_plan(definition, now=now)
        if plan is None:
            continue
        latest_session_runs = await asyncio.to_thread(
            job_store.list_job_runs,
            job_key=definition["job_key"],
            session_id=str(plan["session_id"]),
            limit=1,
        )
        latest_session_run = latest_session_runs[0] if latest_session_runs else None
        max_retries = max(int(definition["payload"].get("max_slot_retries", LIVE_SLOT_MAX_RETRIES)), 0)
        for slot_at in plan["slots"]:
            run_record = await asyncio.to_thread(
                job_store.get_job_run_for_slot,
                job_key=definition["job_key"],
                session_id=str(plan["session_id"]),
                slot_at=slot_at,
            )
            if run_record is None:
                if latest_session_run is not None and latest_session_run["status"] in {"queued", "running"} and _live_run_active(
                    latest_session_run,
                    now=now,
                    interval_seconds=int(plan["interval_seconds"]),
                ):
                    skipped.append(
                        {
                            "job_key": definition["job_key"],
                            "reason": "previous_slot_active",
                        }
                    )
                    break
                slot_iso = isoformat_utc(slot_at)
                payload = dict(plan["payload"])
                payload.update(
                    {
                        "job_key": definition["job_key"],
                        "job_type": "live_collector",
                        "label": plan["label"],
                        "session_id": plan["session_id"],
                        "scheduled_for": slot_iso,
                        "slot_at": slot_iso,
                        "singleton_scope": None,
                    }
                )
                job_run_id = build_job_run_id(definition["job_key"], slot_at)
                attempt_id = build_job_attempt_id(job_run_id, 0)
                created_record, created = await asyncio.to_thread(
                    job_store.create_job_run,
                    job_run_id=job_run_id,
                    job_key=definition["job_key"],
                    arq_job_id=attempt_id,
                    job_type="live_collector",
                    status="queued",
                    scheduled_for=slot_at,
                    session_id=str(plan["session_id"]),
                    slot_at=slot_at,
                    payload=payload,
                )
                if created:
                    if await _enqueue_job_run(
                        job_store=job_store,
                        redis=redis,
                        definition=definition,
                        run_record=created_record,
                    ):
                        enqueued.append(created_record["job_run_id"])
                    latest_session_run = created_record
                    break
                continue

            if run_record["status"] == "succeeded":
                latest_session_run = run_record
                continue
            if run_record["status"] == "failed" and int(run_record.get("retry_count", 0)) >= max_retries:
                latest_session_run = run_record
                continue
            if run_record["status"] in {"queued", "running"} and _live_run_active(
                run_record,
                now=now,
                interval_seconds=int(plan["interval_seconds"]),
            ):
                latest_session_run = run_record
                break

            next_retry_count = int(run_record.get("retry_count", 0)) + 1
            if run_record["status"] in {"failed", "skipped"} and next_retry_count > max_retries:
                latest_session_run = run_record
                continue
            attempt_id = build_job_attempt_id(run_record["job_run_id"], next_retry_count)
            requeued_record = await asyncio.to_thread(
                job_store.requeue_job_run,
                job_run_id=run_record["job_run_id"],
                arq_job_id=attempt_id,
                payload=dict(run_record["payload"]),
            )
            if await _enqueue_job_run(
                job_store=job_store,
                redis=redis,
                definition=definition,
                run_record=requeued_record,
            ):
                enqueued.append(requeued_record["job_run_id"])
            latest_session_run = requeued_record
            break

    return {"enqueued": enqueued, "skipped": skipped}


async def _enqueue_definition_jobs(job_store: Any, redis: Any, *, now: datetime) -> dict[str, Any]:
    definitions = await asyncio.to_thread(job_store.list_job_definitions, enabled_only=True)
    enqueued: list[str] = []
    skipped: list[dict[str, str]] = []

    for definition in definitions:
        if definition["job_type"] == "live_collector":
            continue
        due = due_job_payload(definition, now=now)
        if due is None:
            continue
        job_run_id, scheduled_for, payload = due
        if definition["singleton_scope"]:
            lease = await asyncio.to_thread(
                job_store.get_lease,
                singleton_lease_key(definition["job_type"], definition["singleton_scope"]),
            )
            if _lease_is_active(lease):
                skipped.append(
                    {
                        "job_key": definition["job_key"],
                        "reason": "singleton_lease_active",
                    }
                )
                continue

        run_record, created = await asyncio.to_thread(
            job_store.create_job_run,
            job_run_id=job_run_id,
            job_key=definition["job_key"],
            arq_job_id=build_job_attempt_id(job_run_id, 0),
            job_type=definition["job_type"],
            status="queued",
            scheduled_for=scheduled_for,
            payload=payload,
        )
        if not created:
            continue

        if await _enqueue_job_run(
            job_store=job_store,
            redis=redis,
            definition=definition,
            run_record=run_record,
        ):
            enqueued.append(run_record["job_run_id"])
    return {"enqueued": enqueued, "skipped": skipped}


async def enqueue_due_jobs(job_store: Any, redis: Any) -> dict[str, Any]:
    now = datetime.now(UTC)
    live_result, definition_result = await asyncio.gather(
        _reconcile_live_collector_jobs(job_store, redis, now=now),
        _enqueue_definition_jobs(job_store, redis, now=now),
    )
    return {
        "enqueued": [*live_result["enqueued"], *definition_result["enqueued"]],
        "skipped": [*live_result["skipped"], *definition_result["skipped"]],
    }


async def scheduler_loop(args: argparse.Namespace) -> int:
    job_store = build_job_repository(args.db)
    redis = await create_pool(build_redis_settings(args.redis_url))
    _log_scheduler_event(
        "scheduler_started",
        poll_seconds=max(args.poll_seconds, 1),
        once=bool(args.once),
    )
    try:
        while True:
            owner = "scheduler"
            tick_started = perf_counter()
            lease_seconds = max(args.poll_seconds * 3, SCHEDULER_LEASE_TTL_SECONDS)
            await asyncio.to_thread(
                job_store.acquire_lease,
                lease_key=SCHEDULER_RUNTIME_LEASE_KEY,
                owner=owner,
                expires_in_seconds=lease_seconds,
                state={"kind": "scheduler", "last_tick_at": datetime.now(UTC).isoformat()},
            )
            result = await enqueue_due_jobs(job_store, redis)
            _log_scheduler_event(
                "scheduler_tick",
                poll_seconds=max(args.poll_seconds, 1),
                lease_seconds=lease_seconds,
                elapsed_ms=round((perf_counter() - tick_started) * 1000, 1),
                enqueued_count=len(result["enqueued"]),
                skipped_count=len(result["skipped"]),
                enqueued_job_run_ids=result["enqueued"][:5],
                skipped_samples=result["skipped"][:5],
            )
            if args.once:
                break
            await asyncio.sleep(max(args.poll_seconds, 1))
    finally:
        await asyncio.to_thread(job_store.release_lease, SCHEDULER_RUNTIME_LEASE_KEY, owner="scheduler")
        await redis.close()
        job_store.close()
        _log_scheduler_event("scheduler_stopped")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return asyncio.run(scheduler_loop(args))


if __name__ == "__main__":
    raise SystemExit(main())
