from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any

from core.db.decorators import with_storage
from core.jobs.orchestration import (
    NEW_YORK,
    SCHEDULER_RUNTIME_LEASE_KEY,
    WORKER_RUNTIME_LEASE_PREFIX,
)
from core.jobs.registry import get_job_spec
from core.jobs.specs import get_declared_job_row
from core.jobs.specs import excluded_declared_job_types
from core.services.broker_sync import BROKER_SYNC_KEY
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_int as _coerce_int,
    utc_now_iso as _utc_now,
)

from .jobs import (
    _job_definition_status,
    _job_run_requires_attention,
    _split_active_queued_jobs,
)
from .broker_sync import broker_sync_payload as _broker_sync_payload
from .engine import build_engine_ops_state
from .market_session import market_session_context as _market_session_context
from .shared import (
    RECENT_FAILURE_LIMIT,
    _activity_at,
    _attention,
    _combine_statuses,
    _control_status,
    _lease_status,
    _sorted_by_activity,
)
from .trading import _alert_delivery_payload

RECENT_ALERT_LIMIT = 200


def _scheduler_payload(job_store: Any, *, now: datetime) -> dict[str, Any]:
    active_leases = [dict(row) for row in job_store.list_active_leases(prefix=SCHEDULER_RUNTIME_LEASE_KEY)]
    primary = next(
        (row for row in active_leases if str(row.get("lease_key") or "") == SCHEDULER_RUNTIME_LEASE_KEY),
        None,
    )
    if primary is None and active_leases:
        primary = active_leases[0]
    fallback = None if primary is not None else job_store.get_lease(SCHEDULER_RUNTIME_LEASE_KEY)
    source = primary or fallback
    status = "healthy" if active_leases else _lease_status(fallback, now=now)
    return {
        "status": status,
        "expires_at": None if source is None else source.get("expires_at"),
        "owner": None if source is None else source.get("owner"),
        "job_run_id": None if source is None else source.get("job_run_id"),
        "lease_key": None if source is None else source.get("lease_key"),
        "active_scheduler_count": len(active_leases),
    }


def _actionable_recent_failures(
    job_store: Any,
    recent_failures: list[dict[str, Any]],
    *,
    now: datetime,
) -> list[dict[str, Any]]:
    candidates = [dict(row) for row in recent_failures if _job_run_requires_attention(row, now=now)]
    if not candidates:
        return []

    job_keys = sorted({str(row.get("job_key") or "").strip() for row in candidates if str(row.get("job_key") or "").strip()})
    latest_by_key = {
        str(row.get("job_key") or "").strip(): dict(row)
        for row in job_store.list_latest_runs_by_job_keys(
            job_keys=job_keys,
            statuses=None,
        )
    }

    actionable: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for row in candidates:
        job_type = str(row.get("job_type") or "").strip()
        if get_job_spec(job_type) is None:
            continue
        job_key = str(row.get("job_key") or "").strip()
        if not job_key:
            actionable.append(dict(row))
            continue
        if job_key in seen_keys:
            continue
        seen_keys.add(job_key)
        latest_row = latest_by_key.get(job_key)
        if latest_row is None:
            actionable.append(dict(row))
            continue
        definition = get_declared_job_row(job_key)
        if definition is not None and _job_definition_status(
            definition,
            latest_row,
            now=now,
        ) in {"healthy", "idle"}:
            continue
        if _job_run_requires_attention(latest_row, now=now):
            actionable.append(latest_row)
    return _sorted_by_activity(actionable)


@with_storage()
def build_system_status(
    *,
    db_target: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    from . import get_control_state_snapshot

    generated_at = _utc_now()
    now = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    market_date = now.astimezone(NEW_YORK).date().isoformat()
    market_session = _market_session_context(now=now)
    control = get_control_state_snapshot(storage=storage)
    attention: list[dict[str, str]] = []
    statuses = [_control_status(control)]
    details: dict[str, Any] = {
        "control": control,
        "market_session": market_session,
    }

    if str(control.get("mode") or "") == "halted":
        attention.append(
            _attention(
                severity="high",
                code="control_mode_halted",
                message=_as_text(control.get("note")) or "Control mode is halted.",
            )
        )
    elif str(control.get("mode") or "") == "degraded":
        attention.append(
            _attention(
                severity="medium",
                code="control_mode_degraded",
                message=_as_text(control.get("note")) or "Control mode is degraded.",
            )
        )

    job_store = storage.jobs
    if not job_store.schema_ready():
        scheduler_payload = {
            "status": "blocked",
            "expires_at": None,
            "owner": None,
        }
        workers = []
        running_jobs = []
        queued_jobs = []
        recent_failures = []
        actionable_recent_failures = []
        statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="job_schema_unavailable",
                message="Job storage is not available yet.",
            )
        )
    else:
        excluded_job_types = excluded_declared_job_types()
        scheduler_payload = _scheduler_payload(job_store, now=now)
        scheduler_status = str(scheduler_payload.get("status") or "unknown")
        if scheduler_status != "healthy":
            attention.append(
                _attention(
                    severity="high" if scheduler_status == "blocked" else "medium",
                    code="scheduler_unhealthy",
                    message="Scheduler lease is missing, expired, or close to expiring.",
                )
            )

        workers = [dict(row) for row in job_store.list_active_leases(prefix=WORKER_RUNTIME_LEASE_PREFIX)]
        worker_status = "healthy" if workers else "blocked"
        if worker_status != "healthy":
            attention.append(
                _attention(
                    severity="high",
                    code="workers_missing",
                    message="No active worker leases are present.",
                )
            )

        running_jobs = [
            dict(row)
            for row in job_store.list_job_runs(status="running", limit=100)
            if str(row.get("job_type") or "").strip() not in excluded_job_types
        ]
        queued_jobs = [
            dict(row)
            for row in job_store.list_job_runs(status="queued", limit=100)
            if str(row.get("job_type") or "").strip() not in excluded_job_types
        ]
        queued_jobs, _ = _split_active_queued_jobs(
            queued_jobs,
            now=now,
        )
        failed_jobs = [
            dict(row)
            for row in job_store.list_job_runs(
                status="failed",
                limit=RECENT_FAILURE_LIMIT,
            )
            if str(row.get("job_type") or "").strip() not in excluded_job_types
        ]
        skipped_jobs = [
            dict(row)
            for row in job_store.list_job_runs(
                status="skipped",
                limit=RECENT_FAILURE_LIMIT,
            )
            if str(row.get("job_type") or "").strip() not in excluded_job_types
        ]
        recent_failures = _sorted_by_activity(failed_jobs + skipped_jobs)[:RECENT_FAILURE_LIMIT]
        actionable_recent_failures = _actionable_recent_failures(
            job_store,
            recent_failures,
            now=now,
        )
        if actionable_recent_failures:
            attention.append(
                _attention(
                    severity="medium",
                    code="recent_job_failures",
                    message=(f"{len(actionable_recent_failures)} recent failed or skipped " "job runs need attention."),
                )
            )

        statuses.append(
            _combine_statuses(
                scheduler_payload["status"],
                worker_status,
                "degraded" if actionable_recent_failures else "healthy",
            )
        )

    broker_store = storage.broker
    if not broker_store.schema_ready():
        broker_sync_status = "blocked"
        broker_sync = {
            "status": "missing",
            "raw_status": None,
            "updated_at": None,
            "summary": {},
            "error_text": None,
            "age_seconds": None,
        }
        attention.append(
            _attention(
                severity="high",
                code="broker_sync_schema_unavailable",
                message="Broker sync storage is not available yet.",
            )
        )
    else:
        broker_sync_status, broker_sync = _broker_sync_payload(
            broker_store.get_sync_state(BROKER_SYNC_KEY),
            now=now,
            market_session=market_session,
        )
        if broker_sync_status not in {"healthy", "idle"}:
            attention.append(
                _attention(
                    severity="high" if broker_sync_status == "blocked" else "medium",
                    code="broker_sync_unhealthy",
                    message="Broker sync is missing, stale, or degraded.",
                )
            )
    statuses.append(broker_sync_status)

    alert_store = storage.alerts
    if alert_store.schema_ready():
        recent_alerts = [dict(row) for row in alert_store.list_alert_events(limit=RECENT_ALERT_LIMIT)]
        alert_delivery = _alert_delivery_payload(recent_alerts, now=now)
        if alert_delivery["status"] != "healthy":
            attention.append(
                _attention(
                    severity="medium",
                    code="alert_delivery_issues",
                    message="Recent alert delivery failures or retries were detected.",
                )
            )
    else:
        recent_alerts = []
        alert_delivery = {
            "status": "unknown",
            "count": 0,
            "status_counts": {},
            "dead_letter_count": 0,
            "retry_wait_count": 0,
            "dispatching_count": 0,
            "pending_count": 0,
        }

    engine_ops = build_engine_ops_state(
        storage=storage,
        market_date=market_date,
        now=now,
    )
    engine_status = str(engine_ops.get("status") or "unknown")
    if engine_status in {"degraded", "blocked"}:
        attention.append(
            _attention(
                severity="high" if engine_status == "blocked" else "medium",
                code="engine_unhealthy",
                message="Engine facts, execution storage, or capture targets need attention.",
            )
        )
    statuses.append(engine_status)

    details.update(
        {
            "scheduler": scheduler_payload,
            "workers": workers,
            "running_jobs": running_jobs,
            "queued_jobs": queued_jobs,
            "recent_failures": [
                {
                    **row,
                    "activity_at": _activity_at(row),
                }
                for row in actionable_recent_failures
            ],
            "engine": engine_ops,
            "broker_sync": broker_sync,
            "alert_delivery": alert_delivery,
        }
    )

    engine_summary = dict(engine_ops.get("summary") or {})
    summary = {
        "control_mode": control.get("mode"),
        "worker_count": len(workers),
        "running_job_count": len(running_jobs),
        "queued_job_count": len(queued_jobs),
        "running_jobs_by_type": dict(Counter(str(row.get("job_type") or "unknown") for row in running_jobs)),
        "queued_jobs_by_type": dict(Counter(str(row.get("job_type") or "unknown") for row in queued_jobs)),
        "recent_failure_count": len(actionable_recent_failures),
        "engine_status": engine_status,
        "engine_source_run_count": _coerce_int(engine_summary.get("source_run_count")) or 0,
        "engine_candidate_run_count": _coerce_int(engine_summary.get("candidate_run_count")) or 0,
        "engine_trade_candidate_count": _coerce_int(engine_summary.get("trade_candidate_count")) or 0,
        "engine_signal_count": _coerce_int(engine_summary.get("signal_count")) or 0,
        "engine_decision_count": _coerce_int(engine_summary.get("decision_count")) or 0,
        "engine_selected_count": _coerce_int(engine_summary.get("selected_count")) or 0,
        "engine_intent_count": _coerce_int(engine_summary.get("intent_count")) or 0,
        "engine_entry_intent_count": _coerce_int(engine_summary.get("entry_intent_count")) or 0,
        "engine_management_intent_count": _coerce_int(engine_summary.get("management_intent_count")) or 0,
        "engine_open_position_count": _coerce_int(engine_summary.get("open_position_count")) or 0,
        "capture_active_target_count": _coerce_int(engine_summary.get("capture_active_target_count")) or 0,
        "capture_status": engine_summary.get("capture_status"),
        "broker_sync_status": broker_sync.get("status"),
        "alert_delivery_status": alert_delivery.get("status"),
        "market_session_status": market_session.get("status"),
    }
    return {
        "status": _combine_statuses(*statuses),
        "generated_at": generated_at,
        "summary": summary,
        "attention": attention,
        "details": details,
    }
