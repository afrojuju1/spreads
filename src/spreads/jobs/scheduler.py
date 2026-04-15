from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any

from arq import create_pool

from spreads.events.bus import publish_global_event_async
from spreads.jobs.registry import (
    COLLECTOR_RECOVERY_JOB_KEY,
    COLLECTOR_RECOVERY_JOB_TYPE,
    get_job_spec,
)
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
from spreads.services.live_slot_updates import write_live_session_slot
from spreads.services.live_recovery import (
    LIVE_SLOT_STATUS_MISSED,
    LIVE_SLOT_STATUS_QUEUED,
    LIVE_SLOT_TERMINAL_STATUSES,
)
from spreads.services.value_coercion import as_text as _as_text
from spreads.storage.factory import build_storage_context
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


def _slot_is_terminal(slot_record: Any) -> bool:
    return str(slot_record.get("status") or "") in LIVE_SLOT_TERMINAL_STATUSES


def _run_slot_at(run_record: Any) -> datetime | None:
    return parse_datetime(run_record.get("slot_at") or run_record.get("scheduled_for"))


async def _supersede_queued_live_run(
    *,
    job_store: Any,
    recovery_store: Any,
    redis: Any,
    run_record: Any,
    session_id: str,
    session_date: str,
    label: str,
    now: datetime,
) -> Any:
    next_arq_job_id = build_job_attempt_id(
        str(run_record["job_run_id"]),
        int(run_record.get("retry_count", 0)) + 1,
    )
    await asyncio.to_thread(
        job_store.requeue_job_run,
        job_run_id=run_record["job_run_id"],
        arq_job_id=next_arq_job_id,
        payload=dict(run_record.get("payload") or {}),
    )
    superseded_record = await asyncio.to_thread(
        job_store.update_job_run_status,
        job_run_id=run_record["job_run_id"],
        status="skipped",
        expected_arq_job_id=next_arq_job_id,
        finished_at=now,
        error_text="Superseded by a newer live slot under scheduler coalescing.",
    )
    if superseded_record is not None:
        await _publish_job_run_update(redis, superseded_record)
    slot_at = _run_slot_at(run_record)
    slot_iso = isoformat_utc(slot_at) if slot_at is not None else _as_text(run_record.get("slot_at"))
    if slot_iso:
        await asyncio.to_thread(
            write_live_session_slot,
            recovery_store,
            job_key=str(run_record["job_key"]),
            session_id=session_id,
            session_date=session_date,
            label=label,
            slot_at=slot_iso,
            scheduled_for=slot_iso,
            status=LIVE_SLOT_STATUS_MISSED,
            job_run_id=str(run_record["job_run_id"]),
            capture_status=_as_text(run_record.get("capture_status")),
            recovery_note="Scheduler coalesced this stale queued slot in favor of the latest pending slot.",
            finished_at=isoformat_utc(now),
            updated_at=isoformat_utc(now),
        )
    return superseded_record


async def _enqueue_collector_recovery_if_needed(
    *,
    job_store: Any,
    redis: Any,
    now: datetime,
) -> str | None:
    definition = await asyncio.to_thread(
        job_store.get_job_definition,
        COLLECTOR_RECOVERY_JOB_KEY,
    )
    if definition is None or not bool(definition.get("enabled")):
        return None
    latest_runs = await asyncio.to_thread(
        job_store.list_job_runs,
        job_key=COLLECTOR_RECOVERY_JOB_KEY,
        limit=1,
    )
    latest_run = latest_runs[0] if latest_runs else None
    if latest_run is not None and str(latest_run.get("status") or "") in {"queued", "running"}:
        active_at = (
            parse_datetime(latest_run.get("heartbeat_at"))
            or parse_datetime(latest_run.get("started_at"))
            or parse_datetime(latest_run.get("scheduled_for"))
        )
        if active_at is not None and active_at >= now - timedelta(seconds=120):
            return None
    payload = dict(definition.get("payload") or {})
    payload.update(
        {
            "job_key": COLLECTOR_RECOVERY_JOB_KEY,
            "job_type": COLLECTOR_RECOVERY_JOB_TYPE,
            "scheduled_for": isoformat_utc(now),
            "singleton_scope": definition.get("singleton_scope"),
        }
    )
    job_run_id = build_job_run_id(COLLECTOR_RECOVERY_JOB_KEY, now)
    run_record, created = await asyncio.to_thread(
        job_store.create_job_run,
        job_run_id=job_run_id,
        job_key=COLLECTOR_RECOVERY_JOB_KEY,
        arq_job_id=build_job_attempt_id(job_run_id, 0),
        job_type=COLLECTOR_RECOVERY_JOB_TYPE,
        status="queued",
        scheduled_for=now,
        payload=payload,
    )
    if not created:
        return None
    enqueued = await _enqueue_job_run(
        job_store=job_store,
        redis=redis,
        definition=definition,
        run_record=run_record,
    )
    return None if not enqueued else str(run_record["job_run_id"])


async def _reconcile_live_collector_jobs(
    job_store: Any,
    recovery_store: Any,
    redis: Any,
    *,
    now: datetime,
) -> dict[str, Any]:
    definitions = await asyncio.to_thread(
        job_store.list_job_definitions,
        enabled_only=True,
        job_type="live_collector",
    )
    enqueued: list[str] = []
    skipped: list[dict[str, str]] = []
    recovery_enqueued: list[str] = []

    for definition in definitions:
        plan = resolve_live_tick_plan(definition, now=now)
        if plan is None:
            continue
        current_slot = plan.get("current_slot")
        if not isinstance(current_slot, datetime):
            continue
        all_slots = list(plan["slots"])
        session_id = str(plan["session_id"])
        session_date = str(plan["session_date"])
        label = str(plan["label"])
        await asyncio.to_thread(
            recovery_store.ensure_live_session_slots,
            job_key=str(definition["job_key"]),
            session_id=session_id,
            session_date=session_date,
            label=label,
            slots=[
                {
                    "slot_at": isoformat_utc(slot_at),
                    "scheduled_for": isoformat_utc(slot_at),
                }
                for slot_at in all_slots
            ],
        )
        latest_session_runs = await asyncio.to_thread(
            job_store.list_job_runs,
            job_key=definition["job_key"],
            session_id=session_id,
            limit=1,
        )
        latest_session_run = latest_session_runs[0] if latest_session_runs else None
        max_retries = max(int(definition["payload"].get("max_slot_retries", LIVE_SLOT_MAX_RETRIES)), 0)
        gap_detected = False

        for slot_at in all_slots:
            if slot_at >= current_slot:
                break
            slot_record = await asyncio.to_thread(
                recovery_store.get_live_session_slot,
                session_id=session_id,
                slot_at=slot_at,
            )
            if slot_record is not None and _slot_is_terminal(slot_record):
                continue
            run_record = await asyncio.to_thread(
                job_store.get_job_run_for_slot,
                job_key=definition["job_key"],
                session_id=session_id,
                slot_at=slot_at,
            )
            if run_record is not None and str(run_record.get("status") or "") in {"queued", "running"} and _live_run_active(
                run_record,
                now=now,
                interval_seconds=int(plan["interval_seconds"]),
            ):
                continue
            await asyncio.to_thread(
                write_live_session_slot,
                recovery_store,
                job_key=str(definition["job_key"]),
                session_id=session_id,
                session_date=session_date,
                label=label,
                slot_at=isoformat_utc(slot_at),
                scheduled_for=isoformat_utc(slot_at),
                status=LIVE_SLOT_STATUS_MISSED,
                job_run_id=None if run_record is None else str(run_record["job_run_id"]),
                capture_status=None if slot_record is None else _as_text(slot_record.get("capture_status")),
                recovery_note="Scheduler advanced past this live slot without a completed fresh run.",
                slot_details={} if slot_record is None else dict(slot_record.get("slot_details") or {}),
                queued_at=None if slot_record is None else _as_text(slot_record.get("queued_at")),
                started_at=None if slot_record is None else _as_text(slot_record.get("started_at")),
                finished_at=isoformat_utc(now),
                updated_at=isoformat_utc(now),
            )
            gap_detected = True

        slot_at = current_slot
        run_record = await asyncio.to_thread(
            job_store.get_job_run_for_slot,
            job_key=definition["job_key"],
            session_id=session_id,
            slot_at=slot_at,
        )
        slot_iso = isoformat_utc(slot_at)
        if run_record is None:
            latest_slot_at = (
                None
                if latest_session_run is None
                else _run_slot_at(latest_session_run)
            )
            if (
                latest_session_run is not None
                and str(latest_session_run.get("status") or "") == "queued"
                and latest_slot_at is not None
                and latest_slot_at < slot_at
            ):
                latest_session_run = await _supersede_queued_live_run(
                    job_store=job_store,
                    recovery_store=recovery_store,
                    redis=redis,
                    run_record=latest_session_run,
                    session_id=session_id,
                    session_date=session_date,
                    label=label,
                    now=now,
                )
                gap_detected = True
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
                if gap_detected:
                    recovery_run_id = await _enqueue_collector_recovery_if_needed(
                        job_store=job_store,
                        redis=redis,
                        now=now,
                    )
                    if recovery_run_id is not None:
                        recovery_enqueued.append(recovery_run_id)
                continue
            payload = dict(plan["payload"])
            payload.update(
                {
                    "job_key": definition["job_key"],
                    "job_type": "live_collector",
                    "label": label,
                    "session_id": session_id,
                    "session_date": session_date,
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
                session_id=session_id,
                slot_at=slot_at,
                payload=payload,
            )
            if created:
                await asyncio.to_thread(
                    write_live_session_slot,
                    recovery_store,
                    job_key=str(definition["job_key"]),
                    session_id=session_id,
                    session_date=session_date,
                    label=label,
                    slot_at=slot_iso,
                    scheduled_for=slot_iso,
                    status=LIVE_SLOT_STATUS_QUEUED,
                    job_run_id=str(created_record["job_run_id"]),
                    queued_at=isoformat_utc(now),
                    updated_at=isoformat_utc(now),
                )
                if await _enqueue_job_run(
                    job_store=job_store,
                    redis=redis,
                    definition=definition,
                    run_record=created_record,
                ):
                    enqueued.append(created_record["job_run_id"])
                latest_session_run = created_record
        elif run_record["status"] == "succeeded":
            latest_session_run = run_record
        elif run_record["status"] == "failed" and int(run_record.get("retry_count", 0)) >= max_retries:
            await asyncio.to_thread(
                write_live_session_slot,
                recovery_store,
                job_key=str(definition["job_key"]),
                session_id=session_id,
                session_date=session_date,
                label=label,
                slot_at=slot_iso,
                scheduled_for=slot_iso,
                status=LIVE_SLOT_STATUS_MISSED,
                job_run_id=str(run_record["job_run_id"]),
                recovery_note="Live slot exceeded its retry budget without a completed fresh run.",
                finished_at=isoformat_utc(now),
                updated_at=isoformat_utc(now),
            )
            gap_detected = True
            latest_session_run = run_record
        elif run_record["status"] in {"queued", "running"} and _live_run_active(
            run_record,
            now=now,
            interval_seconds=int(plan["interval_seconds"]),
        ):
            latest_session_run = run_record
        else:
            next_retry_count = int(run_record.get("retry_count", 0)) + 1
            if run_record["status"] in {"failed", "skipped"} and next_retry_count > max_retries:
                await asyncio.to_thread(
                    write_live_session_slot,
                    recovery_store,
                    job_key=str(definition["job_key"]),
                    session_id=session_id,
                    session_date=session_date,
                    label=label,
                    slot_at=slot_iso,
                    scheduled_for=slot_iso,
                    status=LIVE_SLOT_STATUS_MISSED,
                    job_run_id=str(run_record["job_run_id"]),
                    recovery_note="Live slot exhausted retries before it could complete fresh.",
                    finished_at=isoformat_utc(now),
                    updated_at=isoformat_utc(now),
                )
                gap_detected = True
                latest_session_run = run_record
            else:
                attempt_id = build_job_attempt_id(run_record["job_run_id"], next_retry_count)
                requeued_record = await asyncio.to_thread(
                    job_store.requeue_job_run,
                    job_run_id=run_record["job_run_id"],
                    arq_job_id=attempt_id,
                    payload=dict(run_record["payload"]),
                )
                await asyncio.to_thread(
                    write_live_session_slot,
                    recovery_store,
                    job_key=str(definition["job_key"]),
                    session_id=session_id,
                    session_date=session_date,
                    label=label,
                    slot_at=slot_iso,
                    scheduled_for=slot_iso,
                    status=LIVE_SLOT_STATUS_QUEUED,
                    job_run_id=str(requeued_record["job_run_id"]),
                    queued_at=isoformat_utc(now),
                    updated_at=isoformat_utc(now),
                )
                if await _enqueue_job_run(
                    job_store=job_store,
                    redis=redis,
                    definition=definition,
                    run_record=requeued_record,
                ):
                    enqueued.append(requeued_record["job_run_id"])
                latest_session_run = requeued_record

        if gap_detected:
            recovery_run_id = await _enqueue_collector_recovery_if_needed(
                job_store=job_store,
                redis=redis,
                now=now,
            )
            if recovery_run_id is not None:
                recovery_enqueued.append(recovery_run_id)

    return {
        "enqueued": enqueued,
        "skipped": skipped,
        "recovery_enqueued": recovery_enqueued,
    }


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


async def enqueue_due_jobs(job_store: Any, recovery_store: Any, redis: Any) -> dict[str, Any]:
    now = datetime.now(UTC)
    live_result, definition_result = await asyncio.gather(
        _reconcile_live_collector_jobs(job_store, recovery_store, redis, now=now),
        _enqueue_definition_jobs(job_store, redis, now=now),
    )
    return {
        "enqueued": [*live_result["enqueued"], *definition_result["enqueued"]],
        "skipped": [*live_result["skipped"], *definition_result["skipped"]],
        "recovery_enqueued": list(live_result.get("recovery_enqueued") or []),
    }


async def scheduler_loop(args: argparse.Namespace) -> int:
    storage = build_storage_context(args.db)
    job_store = storage.jobs
    recovery_store = storage.recovery
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
            result = await enqueue_due_jobs(job_store, recovery_store, redis)
            _log_scheduler_event(
                "scheduler_tick",
                poll_seconds=max(args.poll_seconds, 1),
                lease_seconds=lease_seconds,
                elapsed_ms=round((perf_counter() - tick_started) * 1000, 1),
                enqueued_count=len(result["enqueued"]),
                skipped_count=len(result["skipped"]),
                recovery_enqueued_count=len(result.get("recovery_enqueued") or []),
                enqueued_job_run_ids=result["enqueued"][:5],
                skipped_samples=result["skipped"][:5],
                recovery_job_run_ids=list(result.get("recovery_enqueued") or [])[:5],
            )
            if args.once:
                break
            await asyncio.sleep(max(args.poll_seconds, 1))
    finally:
        await asyncio.to_thread(job_store.release_lease, SCHEDULER_RUNTIME_LEASE_KEY, owner="scheduler")
        await redis.close()
        storage.close()
        _log_scheduler_event("scheduler_stopped")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return asyncio.run(scheduler_loop(args))


if __name__ == "__main__":
    raise SystemExit(main())
