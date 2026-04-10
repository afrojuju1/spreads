from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping

from arq import create_pool

from spreads.alerts.discord import build_discord_payload, send_discord_webhook
from spreads.events.bus import publish_global_event_sync
from spreads.jobs.orchestration import build_job_attempt_id
from spreads.jobs.registry import (
    ALERT_DELIVERY_ADHOC_JOB_KEY,
    ALERT_DELIVERY_JOB_TYPE,
    get_job_spec,
)
from spreads.runtime.config import default_redis_url
from spreads.runtime.redis import build_redis_settings
from spreads.services.live_pipelines import build_live_session_id
from spreads.storage.alert_repository import (
    ALERT_RECORD_KIND_DELIVERY,
    AlertRepository,
)
from spreads.storage.job_repository import JobRepository

DISCORD_DELIVERY_TARGET = "discord_webhook"
ALERT_DELIVERY_MAX_ATTEMPTS = 5
ALERT_DELIVERY_RETRY_BASE_SECONDS = 60
ALERT_DELIVERY_STALE_SECONDS = 5 * 60


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _utc_now_text() -> str:
    return _utc_now().isoformat(timespec="seconds").replace("+00:00", "Z")


def resolve_delivery_webhook_url(webhook_url: str | None = None) -> str | None:
    if webhook_url is not None:
        return _as_text(webhook_url)
    return _as_text(os.environ.get("SPREADS_DISCORD_WEBHOOK_URL") or os.environ.get("DISCORD_WEBHOOK_URL"))


def alert_delivery_job_run_id(alert_id: int) -> str:
    return f"alert_delivery:{alert_id}"


def _resolve_session_id(row: Mapping[str, Any], session_id: str | None = None) -> str:
    explicit = _as_text(session_id) or _as_text(row.get("session_id"))
    if explicit:
        return explicit
    return build_live_session_id(str(row["label"]), str(row["session_date"]))


def _ensure_alert_delivery_job_definition(job_store: JobRepository) -> None:
    job_store.upsert_job_definition(
        job_key=ALERT_DELIVERY_ADHOC_JOB_KEY,
        job_type=ALERT_DELIVERY_JOB_TYPE,
        enabled=False,
        schedule_type="manual",
        schedule={},
        payload={},
        singleton_scope=None,
    )


def _enqueue_ad_hoc_job(
    *,
    job_type: str,
    job_key: str,
    job_run_id: str,
    arq_job_id: str,
    payload: dict[str, Any],
) -> Any:
    spec = get_job_spec(job_type)
    if spec is None:
        raise RuntimeError(f"Job type is not registered: {job_type}")

    async def _enqueue() -> Any:
        redis = await create_pool(build_redis_settings(default_redis_url()))
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


def publish_alert_event(
    *,
    topic: str,
    row: Mapping[str, Any],
    source: str,
    correlation_id: str | None = None,
) -> None:
    session_id = _resolve_session_id(row)
    publish_global_event_sync(
        topic=topic,
        event_class="control_event",
        entity_type="alert_event",
        entity_id=str(row["alert_id"]),
        payload={
            **dict(row),
            "session_id": session_id,
        },
        timestamp=str(row["updated_at"] if topic == "alert.event.updated" else row["created_at"]),
        source=source,
        session_date=_as_text(row.get("session_date")),
        correlation_id=correlation_id,
    )


def enqueue_alert_delivery_job(
    *,
    alert_store: AlertRepository,
    job_store: JobRepository,
    alert_id: int,
    session_id: str | None = None,
    force_requeue: bool = False,
) -> dict[str, Any]:
    row = alert_store.get_delivery_event(alert_id)
    if row is None:
        raise ValueError(f"Unknown delivery alert: {alert_id}")
    if row["record_kind"] != ALERT_RECORD_KIND_DELIVERY:
        raise ValueError(f"Alert {alert_id} is not a delivery row")

    _ensure_alert_delivery_job_definition(job_store)
    job_run_id = alert_delivery_job_run_id(alert_id)
    resolved_session_id = _resolve_session_id(row, session_id=session_id)
    scheduled_for = _utc_now()
    payload = {
        "alert_id": int(alert_id),
        "session_id": resolved_session_id,
        "job_key": ALERT_DELIVERY_ADHOC_JOB_KEY,
        "job_type": ALERT_DELIVERY_JOB_TYPE,
        "scheduled_for": scheduled_for.isoformat().replace("+00:00", "Z"),
    }

    existing = job_store.get_job_run(job_run_id)
    if existing is None:
        job_run, _ = job_store.create_job_run(
            job_run_id=job_run_id,
            job_key=ALERT_DELIVERY_ADHOC_JOB_KEY,
            arq_job_id=job_run_id,
            job_type=ALERT_DELIVERY_JOB_TYPE,
            status="queued",
            scheduled_for=scheduled_for,
            session_id=resolved_session_id,
            payload=payload,
        )
    elif not force_requeue and existing["status"] in {"queued", "running"}:
        alert_store.mark_delivery_job_queued(
            alert_id=alert_id,
            delivery_job_run_id=job_run_id,
            queued_at=scheduled_for,
        )
        return dict(existing)
    else:
        next_retry_count = int(existing.get("retry_count", 0)) + 1
        job_run = job_store.requeue_job_run(
            job_run_id=job_run_id,
            arq_job_id=build_job_attempt_id(job_run_id, next_retry_count),
            payload=payload,
        )

    alert_store.mark_delivery_job_queued(
        alert_id=alert_id,
        delivery_job_run_id=job_run_id,
        queued_at=scheduled_for,
    )
    try:
        enqueued = _enqueue_ad_hoc_job(
            job_type=ALERT_DELIVERY_JOB_TYPE,
            job_key=ALERT_DELIVERY_ADHOC_JOB_KEY,
            job_run_id=job_run_id,
            arq_job_id=str(job_run["arq_job_id"]),
            payload=payload,
        )
    except Exception as exc:
        job_store.update_job_run_status(
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=str(job_run["arq_job_id"]),
            finished_at=_utc_now(),
            error_text=str(exc),
        )
        raise RuntimeError(f"Alert delivery queueing failed: {exc}") from exc
    if enqueued is None:
        job_store.update_job_run_status(
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=str(job_run["arq_job_id"]),
            finished_at=_utc_now(),
            error_text="Alert delivery job was not enqueued.",
        )
        raise RuntimeError("Alert delivery queueing failed: job was not enqueued.")
    return dict(job_store.get_job_run(job_run_id) or job_run)


def plan_alert_delivery(
    *,
    alert_store: AlertRepository,
    job_store: JobRepository,
    payload: dict[str, Any],
    dedupe_key: str,
    dedupe_state: dict[str, Any] | None,
    session_id: str | None,
    planner_job_run_id: str | None,
    source: str,
    correlation_id: str | None,
    webhook_url: str | None = None,
) -> tuple[dict[str, Any], bool]:
    resolved_session_id = _resolve_session_id(payload, session_id=session_id)
    resolved_webhook_url = resolve_delivery_webhook_url(webhook_url)
    status = "pending" if resolved_webhook_url else "suppressed"
    response = None if resolved_webhook_url else {"reason": "missing_SPREADS_DISCORD_WEBHOOK_URL"}
    row, created = alert_store.plan_delivery_event(
        created_at=payload["created_at"],
        session_date=payload["session_date"],
        label=str(payload["label"]),
        session_id=resolved_session_id,
        cycle_id=str(payload["cycle_id"]),
        symbol=str(payload["symbol"]),
        alert_type=str(payload["alert_type"]),
        dedupe_key=dedupe_key,
        delivery_target=DISCORD_DELIVERY_TARGET,
        status=status,
        payload=payload,
        state=dedupe_state,
        planner_job_run_id=planner_job_run_id,
        response=response,
    )
    if not created:
        return dict(row), False
    if status == "pending":
        try:
            enqueue_alert_delivery_job(
                alert_store=alert_store,
                job_store=job_store,
                alert_id=int(row["alert_id"]),
                session_id=resolved_session_id,
            )
            refreshed = alert_store.get_alert_event(int(row["alert_id"]))
            if refreshed is not None:
                row = refreshed
        except Exception:
            pass
    publish_alert_event(
        topic="alert.event.created",
        row=row,
        source=source,
        correlation_id=correlation_id,
    )
    return dict(row), True


def _retry_schedule(attempt_count: int) -> datetime:
    exponent = max(attempt_count - 1, 0)
    delay_seconds = ALERT_DELIVERY_RETRY_BASE_SECONDS * (2 ** exponent)
    return _utc_now() + timedelta(seconds=min(delay_seconds, 15 * 60))


def run_alert_delivery(
    *,
    alert_store: AlertRepository,
    alert_id: int,
    delivery_job_run_id: str,
    worker_name: str,
) -> dict[str, Any]:
    claimed = alert_store.claim_delivery_event(
        alert_id=alert_id,
        delivery_job_run_id=delivery_job_run_id,
        worker_name=worker_name,
        claimed_at=_utc_now(),
    )
    if claimed is None:
        return {
            "status": "skipped",
            "reason": "not_claimable",
            "alert_id": alert_id,
        }

    webhook_url = resolve_delivery_webhook_url()
    correlation_id = _as_text(claimed.get("cycle_id"))
    payload = dict(claimed.get("payload") or {})
    if not webhook_url:
        attempt_count = int(claimed.get("attempt_count") or 0)
        retry_at = _retry_schedule(attempt_count)
        final_status = "dead_letter" if attempt_count >= ALERT_DELIVERY_MAX_ATTEMPTS else "retry_wait"
        completed = alert_store.finish_delivery_event(
            alert_id=alert_id,
            status=final_status,
            finished_at=_utc_now(),
            error_text="Missing Discord webhook configuration.",
            next_attempt_at=None if final_status == "dead_letter" else retry_at,
            worker_name=worker_name,
        )
        publish_alert_event(
            topic="alert.event.updated",
            row=completed,
            source="alerts.delivery",
            correlation_id=correlation_id,
        )
        return dict(completed)
    try:
        response = send_discord_webhook(webhook_url, build_discord_payload(payload))
        completed = alert_store.finish_delivery_event(
            alert_id=alert_id,
            status="delivered",
            finished_at=_utc_now(),
            delivered_at=_utc_now(),
            response=response,
            worker_name=worker_name,
        )
    except Exception as exc:
        attempt_count = int(claimed.get("attempt_count") or 0)
        retry_at = _retry_schedule(attempt_count)
        final_status = "dead_letter" if attempt_count >= ALERT_DELIVERY_MAX_ATTEMPTS else "retry_wait"
        completed = alert_store.finish_delivery_event(
            alert_id=alert_id,
            status=final_status,
            finished_at=_utc_now(),
            error_text=str(exc),
            next_attempt_at=None if final_status == "dead_letter" else retry_at,
            worker_name=worker_name,
        )
    publish_alert_event(
        topic="alert.event.updated",
        row=completed,
        source="alerts.delivery",
        correlation_id=correlation_id,
    )
    return dict(completed)


def reconcile_alert_delivery(
    *,
    alert_store: AlertRepository,
    job_store: JobRepository,
    limit: int = 200,
    stale_after_seconds: int = ALERT_DELIVERY_STALE_SECONDS,
) -> dict[str, Any]:
    now = _utc_now()
    stale_before = now - timedelta(seconds=max(int(stale_after_seconds), 1))
    due_rows = alert_store.list_due_delivery_events(
        now=now,
        stale_dispatching_before=stale_before,
        limit=limit,
    )

    reconciled: list[int] = []
    requeued: list[int] = []
    skipped: list[int] = []
    failed: list[dict[str, Any]] = []

    for row in due_rows:
        current = dict(row)
        force_requeue = False
        if current["status"] == "dispatching":
            reset = alert_store.reset_stale_dispatching_event(
                alert_id=int(current["alert_id"]),
                reset_at=now,
            )
            if reset is None:
                skipped.append(int(current["alert_id"]))
                continue
            current = dict(reset)
            force_requeue = True
            reconciled.append(int(current["alert_id"]))

        job_run_id = _as_text(current.get("delivery_job_run_id")) or alert_delivery_job_run_id(int(current["alert_id"]))
        existing_job_run = job_store.get_job_run(job_run_id)
        if not force_requeue and existing_job_run is not None and existing_job_run["status"] in {"queued", "running"}:
            skipped.append(int(current["alert_id"]))
            continue
        try:
            enqueue_alert_delivery_job(
                alert_store=alert_store,
                job_store=job_store,
                alert_id=int(current["alert_id"]),
                session_id=_as_text(current.get("session_id")),
                force_requeue=force_requeue,
            )
            requeued.append(int(current["alert_id"]))
        except Exception as exc:
            failed.append(
                {
                    "alert_id": int(current["alert_id"]),
                    "error": str(exc),
                }
            )

    return {
        "status": "ok",
        "checked": len(due_rows),
        "reclaimed": reconciled,
        "requeued": requeued,
        "skipped": skipped,
        "failed": failed,
    }
