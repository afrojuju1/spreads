from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Mapping

from core.alerts.discord import build_discord_payload, send_discord_webhook
from core.events.bus import publish_global_event_sync
from core.jobs.adhoc import start_ad_hoc_job_workflow
from core.jobs.registry import (
    ALERT_DELIVERY_ADHOC_JOB_KEY,
    ALERT_DELIVERY_JOB_TYPE,
)
from core.observability.logging import log_event
from core.services.runtime_identity import build_live_run_scope_id
from core.storage.alert_repository import (
    ALERT_RECORD_KIND_DELIVERY,
    AlertRepository,
)
from core.value_coercion import as_text as _as_text, utc_now as _utc_now
from core.workflow_runtime.provider import routine_workflow_id

DISCORD_DELIVERY_TARGET = "discord_webhook"
ALERT_DELIVERY_MAX_ATTEMPTS = 5
ALERT_DELIVERY_RETRY_BASE_SECONDS = 60
ALERT_DELIVERY_STALE_SECONDS = 5 * 60

logger = logging.getLogger(__name__)


def resolve_deploy_env() -> str:
    return _as_text(os.environ.get("SPREADS_DEPLOY_ENV")) or "unknown"


def _payload_with_deploy_env(payload: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    normalized["deploy_env"] = _as_text(normalized.get("deploy_env")) or resolve_deploy_env()
    return normalized


def resolve_delivery_webhook_url(webhook_url: str | None = None) -> str | None:
    if webhook_url is not None:
        return _as_text(webhook_url)
    return _as_text(os.environ.get("SPREADS_DISCORD_WEBHOOK_URL") or os.environ.get("DISCORD_WEBHOOK_URL"))


def alert_delivery_attempt_id(alert_id: int, *, attempt_number: int = 1) -> str:
    return f"alert_delivery:{alert_id}:attempt:{max(int(attempt_number), 1)}"


def _resolve_session_id(row: Mapping[str, Any], session_id: str | None = None) -> str:
    explicit = _as_text(session_id) or _as_text(row.get("session_id"))
    if explicit:
        return explicit
    return build_live_run_scope_id(str(row["label"]), str(row["session_date"]))


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
    alert_id: int,
    session_id: str | None = None,
) -> dict[str, Any]:
    row = alert_store.get_delivery_event(alert_id)
    if row is None:
        raise ValueError(f"Unknown delivery alert: {alert_id}")
    if row["record_kind"] != ALERT_RECORD_KIND_DELIVERY:
        raise ValueError(f"Alert {alert_id} is not a delivery row")

    attempt_number = int(row.get("attempt_count") or 0) + 1
    delivery_attempt_id = alert_delivery_attempt_id(alert_id, attempt_number=attempt_number)
    resolved_session_id = _resolve_session_id(row, session_id=session_id)
    scheduled_for = _utc_now()
    payload = {
        "alert_id": int(alert_id),
        "session_id": resolved_session_id,
        "job_key": ALERT_DELIVERY_ADHOC_JOB_KEY,
        "job_type": ALERT_DELIVERY_JOB_TYPE,
        "delivery_attempt": attempt_number,
        "scheduled_for": scheduled_for.isoformat().replace("+00:00", "Z"),
    }

    workflow_id = routine_workflow_id(delivery_attempt_id)

    try:
        started = start_ad_hoc_job_workflow(
            job_type=ALERT_DELIVERY_JOB_TYPE,
            job_key=ALERT_DELIVERY_ADHOC_JOB_KEY,
            workflow_id=workflow_id,
            payload=payload,
        )
    except Exception as exc:
        raise RuntimeError(f"Alert delivery queueing failed: {exc}") from exc
    if started is None:
        raise RuntimeError("Alert delivery workflow start failed.")
    alert_store.mark_delivery_job_queued(
        alert_id=alert_id,
        delivery_job_run_id=started.job_run_id,
        queued_at=scheduled_for,
    )
    return {
        "job_run_id": started.job_run_id,
        "job_key": ALERT_DELIVERY_ADHOC_JOB_KEY,
        "job_type": ALERT_DELIVERY_JOB_TYPE,
        "workflow_id": workflow_id,
        "workflow_run_id": started.workflow_run_id,
        "status": "started",
        "scheduled_for": payload["scheduled_for"],
        "payload": payload,
    }


def plan_alert_delivery(
    *,
    alert_store: AlertRepository,
    payload: dict[str, Any],
    dedupe_key: str,
    dedupe_state: dict[str, Any] | None,
    session_id: str | None,
    planner_job_run_id: str | None,
    source: str,
    correlation_id: str | None,
    webhook_url: str | None = None,
) -> tuple[dict[str, Any], bool]:
    normalized_payload = _payload_with_deploy_env(payload)
    resolved_session_id = _resolve_session_id(normalized_payload, session_id=session_id)
    resolved_webhook_url = resolve_delivery_webhook_url(webhook_url)
    status = "pending" if resolved_webhook_url else "suppressed"
    response = None if resolved_webhook_url else {"reason": "missing_SPREADS_DISCORD_WEBHOOK_URL"}
    row, created = alert_store.plan_delivery_event(
        created_at=normalized_payload["created_at"],
        session_date=normalized_payload["session_date"],
        label=str(normalized_payload["label"]),
        session_id=resolved_session_id,
        cycle_id=str(normalized_payload["cycle_id"]),
        symbol=str(normalized_payload["symbol"]),
        alert_type=str(normalized_payload["alert_type"]),
        dedupe_key=dedupe_key,
        delivery_target=DISCORD_DELIVERY_TARGET,
        status=status,
        payload=normalized_payload,
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
                alert_id=int(row["alert_id"]),
                session_id=resolved_session_id,
            )
            refreshed = alert_store.get_alert_event(int(row["alert_id"]))
            if refreshed is not None:
                row = refreshed
        except Exception as exc:
            log_event(
                logger,
                logging.WARNING,
                "alert_delivery_enqueue_failed",
                exc_info=True,
                alert_id=row.get("alert_id"),
                session_id=resolved_session_id,
                session_date=normalized_payload.get("session_date"),
                label=normalized_payload.get("label"),
                symbol=normalized_payload.get("symbol"),
                alert_type=normalized_payload.get("alert_type"),
                cycle_id=normalized_payload.get("cycle_id"),
                error=str(exc),
            )
    publish_alert_event(
        topic="alert.event.created",
        row=row,
        source=source,
        correlation_id=correlation_id,
    )
    return dict(row), True


def _retry_schedule(attempt_count: int) -> datetime:
    exponent = max(attempt_count - 1, 0)
    delay_seconds = ALERT_DELIVERY_RETRY_BASE_SECONDS * (2**exponent)
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
    restarted: list[int] = []
    skipped: list[int] = []
    failed: list[dict[str, Any]] = []

    for row in due_rows:
        current = dict(row)
        if current["status"] == "dispatching":
            reset = alert_store.reset_stale_dispatching_event(
                alert_id=int(current["alert_id"]),
                reset_at=now,
            )
            if reset is None:
                skipped.append(int(current["alert_id"]))
                continue
            current = dict(reset)
            reconciled.append(int(current["alert_id"]))

        try:
            enqueue_alert_delivery_job(
                alert_store=alert_store,
                alert_id=int(current["alert_id"]),
                session_id=_as_text(current.get("session_id")),
            )
            restarted.append(int(current["alert_id"]))
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
        "restarted": restarted,
        "skipped": skipped,
        "failed": failed,
    }
