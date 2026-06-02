from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any

from arq import create_pool

from core.events.bus import publish_global_event_async
from core.jobs.registry import (
    DISCOVERY_QUEUE_NAME,
    RESEARCH_QUEUE_NAME,
    RUNTIME_QUEUE_NAME,
    VALUATION_QUEUE_NAME,
    get_job_spec,
)
from core.jobs.specs import list_declared_job_rows
from core.jobs.orchestration import (
    build_job_attempt_id,
    build_job_run_id,
    due_job_payload,
    isoformat_utc,
    resolve_live_tick_plan,
    scheduler_runtime_lease_key,
    singleton_lease_key,
    utc_now,
)
from core.observability.logging import configure_logging, log_event
from core.runtime.config import default_database_url, default_redis_url
from core.runtime.redis import build_redis_settings
from core.services.live_slot_updates import write_live_session_slot
from core.services.discovery_recovery import (
    LIVE_SLOT_STATUS_MISSED,
    LIVE_SLOT_STATUS_QUEUED,
    LIVE_SLOT_TERMINAL_STATUSES,
)
from core.services.value_coercion import as_text as _as_text
from core.storage.factory import build_storage_context
from core.storage.serializers import parse_datetime

DEFAULT_POLL_SECONDS = 30
SCHEDULER_LEASE_TTL_SECONDS = 90
LIVE_SLOT_MAX_RETRIES = 3
DEFINITION_QUEUE_CLEANUP_LIMIT = 500
STALE_JOB_RECONCILE_LIMIT = 500
JOB_RUN_QUEUE_STALE_AFTER_SECONDS = 15 * 60
JOB_RUN_HEARTBEAT_STALE_AFTER_SECONDS = 10 * 60
VALID_QUEUE_DOMAINS = ("all", "runtime", "discovery", "research", "valuation")
QUEUE_DOMAIN_TO_QUEUE_NAME = {
    "runtime": RUNTIME_QUEUE_NAME,
    "discovery": DISCOVERY_QUEUE_NAME,
    "research": RESEARCH_QUEUE_NAME,
    "valuation": VALUATION_QUEUE_NAME,
}

logger = logging.getLogger(__name__)


def _log_scheduler_event(event: str, **payload: Any) -> None:
    log_event(logger, logging.INFO, event, **payload)

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
    parser = argparse.ArgumentParser(
        description="Schedule ARQ jobs for spreads discovery runs and analysis."
    )
    parser.add_argument("--db", default=default_database_url(), help="Postgres database URL.")
    parser.add_argument("--redis-url", default=default_redis_url(), help="Redis connection URL.")
    parser.add_argument("--poll-seconds", type=int, default=DEFAULT_POLL_SECONDS, help="Scheduler poll interval.")
    parser.add_argument("--once", action="store_true", help="Evaluate schedules once and exit.")
    parser.add_argument(
        "--queue-domain",
        "--queue",
        dest="queue_domain",
        choices=VALID_QUEUE_DOMAINS,
        default="all",
        help="Restrict scheduling and stale-run reconciliation to one queue domain.",
    )
    return parser.parse_args(argv)


def _matches_queue_domain(job_type: str, queue_domain: str) -> bool:
    normalized = str(queue_domain or "all").strip().lower()
    if normalized == "all":
        return True
    spec = get_job_spec(str(job_type or "").strip())
    if spec is None:
        return False
    return spec.queue_name == QUEUE_DOMAIN_TO_QUEUE_NAME[normalized]


def _lease_is_active(lease: Any) -> bool:
    if lease is None:
        return False
    expires_at = parse_datetime(lease["expires_at"])
    return expires_at is not None and expires_at > utc_now()


def _job_run_heartbeat_stale_after_seconds(run_record: Any) -> int:
    payload = dict(run_record.get("payload") or {})
    try:
        interval_seconds = max(int(payload.get("interval_seconds", 0)), 0)
    except (TypeError, ValueError):
        interval_seconds = 0
    return max(interval_seconds * 2, JOB_RUN_HEARTBEAT_STALE_AFTER_SECONDS)


def _job_run_is_stale(run_record: Any, *, now: datetime) -> bool:
    status = str(run_record.get("status") or "").strip().lower()
    if status == "queued":
        scheduled_for = parse_datetime(run_record.get("scheduled_for"))
        if scheduled_for is None:
            return True
        return scheduled_for < now - timedelta(seconds=JOB_RUN_QUEUE_STALE_AFTER_SECONDS)
    if status == "running":
        last_seen = (
            parse_datetime(run_record.get("heartbeat_at"))
            or parse_datetime(run_record.get("started_at"))
            or parse_datetime(run_record.get("scheduled_for"))
        )
        if last_seen is None:
            return True
        return last_seen < now - timedelta(
            seconds=_job_run_heartbeat_stale_after_seconds(run_record)
        )
    return False


async def _purge_arq_job_artifacts(redis: Any, *, arq_job_id: str) -> None:
    if not arq_job_id:
        return
    await redis.zrem(RUNTIME_QUEUE_NAME, arq_job_id)
    await redis.zrem(DISCOVERY_QUEUE_NAME, arq_job_id)
    await redis.zrem(VALUATION_QUEUE_NAME, arq_job_id)
    await redis.delete(
        f"arq:job:{arq_job_id}",
        f"arq:in-progress:{arq_job_id}",
        f"arq:retry:{arq_job_id}",
        f"arq:result:{arq_job_id}",
    )


async def _release_singleton_lease_for_run(job_store: Any, run_record: Any) -> None:
    payload = dict(run_record.get("payload") or {})
    singleton_scope = str(payload.get("singleton_scope") or "").strip()
    job_type = str(run_record.get("job_type") or "").strip()
    job_run_id = str(run_record.get("job_run_id") or "").strip()
    if not singleton_scope or not job_type or not job_run_id:
        return
    await asyncio.to_thread(
        job_store.release_lease,
        singleton_lease_key(job_type, singleton_scope),
        owner=job_run_id,
    )


async def _reconcile_stale_job_run(
    *,
    job_store: Any,
    redis: Any,
    run_record: Any,
    now: datetime,
) -> str | None:
    status = str(run_record.get("status") or "").strip().lower()
    arq_job_id = str(run_record.get("arq_job_id") or "")
    job_run_id = str(run_record.get("job_run_id") or "")
    if not job_run_id or status not in {"queued", "running"}:
        return None

    if status == "queued":
        reconciled_record = await asyncio.to_thread(
            job_store.update_job_run_status,
            job_run_id=job_run_id,
            status="skipped",
            expected_arq_job_id=arq_job_id,
            finished_at=now,
            result={
                "status": "skipped",
                "reason": "stale_queued_run_reconciled",
            },
            error_text="Marked skipped after stale queue reconciliation.",
        )
    else:
        reconciled_record = await asyncio.to_thread(
            job_store.update_job_run_status,
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=arq_job_id,
            finished_at=now,
            heartbeat_at=now,
            result={
                "status": "failed",
                "reason": "stale_running_heartbeat_reconciled",
            },
            error_text="Marked failed after stale heartbeat reconciliation.",
        )
    if reconciled_record is None:
        return None

    await _release_singleton_lease_for_run(job_store, run_record)
    await _purge_arq_job_artifacts(redis, arq_job_id=arq_job_id)
    await _publish_job_run_update(redis, reconciled_record)
    return job_run_id


async def _reconcile_stale_job_runs(job_store: Any, redis: Any, *, now: datetime) -> list[str]:
    reconciled: list[str] = []
    for status in ("running", "queued"):
        run_rows = await asyncio.to_thread(
            job_store.list_job_runs,
            status=status,
            limit=STALE_JOB_RECONCILE_LIMIT,
        )
        for run_record in run_rows:
            if not _matches_queue_domain(
                str(run_record.get("job_type") or ""),
                "all",
            ):
                continue
            if not _job_run_is_stale(run_record, now=now):
                continue
            reconciled_job_run_id = await _reconcile_stale_job_run(
                job_store=job_store,
                redis=redis,
                run_record=run_record,
                now=now,
            )
            if reconciled_job_run_id is not None:
                reconciled.append(reconciled_job_run_id)
    return reconciled

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


async def _reconcile_discovery_run_jobs(
    job_store: Any,
    recovery_store: Any,
    redis: Any,
    *,
    now: datetime,
    queue_domain: str,
) -> dict[str, Any]:
    if str(queue_domain).strip().lower() not in {"all", "discovery"}:
        return {
            "enqueued": [],
            "skipped": [],
            "recovery_enqueued": [],
        }
    definitions = await asyncio.to_thread(
        list_declared_job_rows,
        enabled_only=True,
        job_type="discovery_run",
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
                continue
            payload = dict(plan["payload"])
            payload.update(
                {
                    "job_key": definition["job_key"],
                    "job_type": "discovery_run",
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
                job_type="discovery_run",
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

    return {
        "enqueued": enqueued,
        "skipped": skipped,
        "recovery_enqueued": recovery_enqueued,
    }


async def _enqueue_definition_jobs(
    job_store: Any,
    redis: Any,
    *,
    now: datetime,
    queue_domain: str,
) -> dict[str, Any]:
    definitions = await asyncio.to_thread(list_declared_job_rows, enabled_only=True)
    enqueued: list[str] = []
    skipped: list[dict[str, str]] = []

    for definition in definitions:
        if not _matches_queue_domain(str(definition["job_type"]), queue_domain):
            continue
        if definition["job_type"] == "discovery_run":
            continue
        due = due_job_payload(definition, now=now)
        if due is None:
            continue
        latest_runs = await asyncio.to_thread(
            job_store.list_latest_runs_by_job_keys,
            job_keys=[str(definition["job_key"])],
            statuses=None,
        )
        latest_run = latest_runs[0] if latest_runs else None
        latest_scheduled_for = (
            None
            if latest_run is None
            else parse_datetime(latest_run.get("scheduled_for"))
        )
        superseded_stale_runs = 0
        if latest_scheduled_for is not None:
            queued_runs = await asyncio.to_thread(
                job_store.list_job_runs,
                job_key=str(definition["job_key"]),
                status="queued",
                limit=DEFINITION_QUEUE_CLEANUP_LIMIT,
            )
            for queued_run in queued_runs:
                queued_job_run_id = str(queued_run.get("job_run_id") or "")
                if latest_run is not None and queued_job_run_id == str(
                    latest_run.get("job_run_id") or ""
                ):
                    continue
                queued_scheduled_for = parse_datetime(queued_run.get("scheduled_for"))
                if (
                    queued_scheduled_for is None
                    or queued_scheduled_for >= latest_scheduled_for
                ):
                    continue
                superseded_record = await asyncio.to_thread(
                    job_store.update_job_run_status,
                    job_run_id=queued_job_run_id,
                    status="skipped",
                    expected_arq_job_id=str(queued_run.get("arq_job_id") or ""),
                    finished_at=now,
                    result={
                        "status": "skipped",
                        "reason": "superseded_by_newer_scheduled_run",
                    },
                    error_text="Superseded by a newer scheduled run.",
                )
                if superseded_record is None:
                    continue
                superseded_stale_runs += 1
                await _publish_job_run_update(redis, superseded_record)
        if superseded_stale_runs:
            skipped.append(
                {
                    "job_key": str(definition["job_key"]),
                    "reason": "superseded_stale_queued_runs",
                    "count": str(superseded_stale_runs),
                }
            )
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


async def enqueue_due_jobs(
    job_store: Any,
    recovery_store: Any,
    redis: Any,
    *,
    queue_domain: str,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    stale_reconciled, live_result, definition_result = await asyncio.gather(
        _reconcile_stale_job_runs_filtered(
            job_store,
            redis,
            now=now,
            queue_domain=queue_domain,
        ),
        _reconcile_discovery_run_jobs(
            job_store,
            recovery_store,
            redis,
            now=now,
            queue_domain=queue_domain,
        ),
        _enqueue_definition_jobs(job_store, redis, now=now, queue_domain=queue_domain),
    )
    return {
        "enqueued": [*live_result["enqueued"], *definition_result["enqueued"]],
        "skipped": [*live_result["skipped"], *definition_result["skipped"]],
        "recovery_enqueued": list(live_result.get("recovery_enqueued") or []),
        "reconciled": stale_reconciled,
    }


async def _reconcile_stale_job_runs_filtered(
    job_store: Any,
    redis: Any,
    *,
    now: datetime,
    queue_domain: str,
) -> list[str]:
    if str(queue_domain).strip().lower() == "all":
        return await _reconcile_stale_job_runs(job_store, redis, now=now)
    reconciled: list[str] = []
    for status in ("running", "queued"):
        run_rows = await asyncio.to_thread(
            job_store.list_job_runs,
            status=status,
            limit=STALE_JOB_RECONCILE_LIMIT,
        )
        for run_record in run_rows:
            if not _matches_queue_domain(
                str(run_record.get("job_type") or ""),
                queue_domain,
            ):
                continue
            if not _job_run_is_stale(run_record, now=now):
                continue
            reconciled_job_run_id = await _reconcile_stale_job_run(
                job_store=job_store,
                redis=redis,
                run_record=run_record,
                now=now,
            )
            if reconciled_job_run_id is not None:
                reconciled.append(reconciled_job_run_id)
    return reconciled


async def scheduler_loop(args: argparse.Namespace) -> int:
    storage = build_storage_context(args.db)
    job_store = storage.jobs
    recovery_store = storage.recovery
    redis = await create_pool(build_redis_settings(args.redis_url))
    _log_scheduler_event(
        "scheduler_started",
        poll_seconds=max(args.poll_seconds, 1),
        once=bool(args.once),
        queue_domain=str(args.queue_domain),
    )
    try:
        while True:
            owner = "scheduler"
            tick_started = perf_counter()
            lease_seconds = max(args.poll_seconds * 3, SCHEDULER_LEASE_TTL_SECONDS)
            lease_key = scheduler_runtime_lease_key(str(args.queue_domain))
            await asyncio.to_thread(
                job_store.acquire_lease,
                lease_key=lease_key,
                owner=owner,
                expires_in_seconds=lease_seconds,
                state={
                    "kind": "scheduler",
                    "last_tick_at": datetime.now(UTC).isoformat(),
                    "queue_domain": str(args.queue_domain),
                },
            )
            result = await enqueue_due_jobs(
                job_store,
                recovery_store,
                redis,
                queue_domain=str(args.queue_domain),
            )
            _log_scheduler_event(
                "scheduler_tick",
                poll_seconds=max(args.poll_seconds, 1),
                lease_seconds=lease_seconds,
                queue_domain=str(args.queue_domain),
                lease_key=lease_key,
                elapsed_ms=round((perf_counter() - tick_started) * 1000, 1),
                enqueued_count=len(result["enqueued"]),
                skipped_count=len(result["skipped"]),
                recovery_enqueued_count=len(result.get("recovery_enqueued") or []),
                reconciled_count=len(result.get("reconciled") or []),
                enqueued_job_run_ids=result["enqueued"][:5],
                skipped_samples=result["skipped"][:5],
                recovery_job_run_ids=list(result.get("recovery_enqueued") or [])[:5],
                reconciled_job_run_ids=list(result.get("reconciled") or [])[:5],
            )
            if args.once:
                break
            await asyncio.sleep(max(args.poll_seconds, 1))
    finally:
        await asyncio.to_thread(
            job_store.release_lease,
            scheduler_runtime_lease_key(str(args.queue_domain)),
            owner="scheduler",
        )
        await redis.close()
        storage.close()
        _log_scheduler_event("scheduler_stopped", queue_domain=str(args.queue_domain))
    return 0


def main(argv: list[str] | None = None) -> int:
    configure_logging(service="scheduler", force=True)
    args = parse_args(argv)
    return asyncio.run(scheduler_loop(args))


if __name__ == "__main__":
    raise SystemExit(main())
