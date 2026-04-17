from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from core.db.decorators import with_storage
from core.jobs.orchestration import (
    SCHEDULER_RUNTIME_LEASE_KEY,
    SINGLETON_LEASE_PREFIX,
    WORKER_RUNTIME_LEASE_PREFIX,
    singleton_lease_key,
)
from core.jobs.registry import WORKER_LANES, get_queue_name_for_job_type
from core.services.live_collector_health.enrichment import (
    enrich_live_collector_job_run_payload,
)
from core.services.selection_summary import selection_summary_payload as _selection_summary_payload
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_int as _coerce_int,
    utc_now_iso as _utc_now,
)

from .shared import (
    JOB_RUN_HEARTBEAT_STALE_AFTER_SECONDS,
    JOB_RUN_QUEUE_STALE_AFTER_SECONDS,
    OpsLookupError,
    _activity_at,
    _attention,
    _combine_statuses,
    _is_recent,
    _lease_status,
    _run_duration_seconds,
    _seconds_since,
    _sorted_by_activity,
    _stream_quote_events_saved,
    _stream_trade_events_saved,
)


def _skip_reason_text(run: Mapping[str, Any]) -> str | None:
    result = run.get("result") if isinstance(run.get("result"), Mapping) else {}
    return _as_text(result.get("reason"))


def _skip_is_benign(run: Mapping[str, Any]) -> bool:
    reason = str(_skip_reason_text(run) or "").strip().lower()
    if reason == "singleton_lease_unavailable":
        return True
    if reason == "outside_schedule_window":
        return True
    if reason == "stale_slot" and str(run.get("job_type") or "") == "live_collector":
        return True
    error_text = str(_as_text(run.get("error_text")) or "").strip().lower()
    return error_text in {
        "superseded during queue consolidation",
        "superseded by a newer live slot under scheduler coalescing.",
    }


def _job_run_requires_attention(
    run: Mapping[str, Any],
    *,
    now: datetime,
) -> bool:
    if not _is_recent(_activity_at(run), now=now):
        return False
    status = str(run.get("status") or "").strip().lower()
    if status == "failed":
        return True
    if status == "skipped":
        return not _skip_is_benign(run)
    return False


def _definition_requires_attention(
    definition: Mapping[str, Any],
    *,
    now: datetime,
) -> bool:
    operator_status = str(definition.get("operator_status") or "unknown").strip().lower()
    if operator_status not in {"degraded", "blocked"}:
        return False
    latest_run_at = definition.get("latest_run_at")
    if latest_run_at is None:
        return True
    return _is_recent(latest_run_at, now=now)


def _job_run_operator_status(
    run: Mapping[str, Any],
    *,
    now: datetime,
) -> tuple[str, str | None]:
    status = str(run.get("status") or "unknown").strip().lower()
    if status == "failed":
        error_text = _as_text(run.get("error_text"))
        return "blocked", error_text or "Job run failed."
    if status == "skipped":
        result = run.get("result") if isinstance(run.get("result"), Mapping) else {}
        reason = _as_text(result.get("reason"))
        if _skip_is_benign(run):
            if reason == "singleton_lease_unavailable":
                return (
                    "healthy",
                    "Job run was skipped because another singleton run already covered the slot.",
                )
            if reason == "outside_schedule_window":
                return (
                    "healthy",
                    "Job run was skipped because it was outside its configured schedule window.",
                )
            if reason == "stale_slot" and str(run.get("job_type") or "") == "live_collector":
                return (
                    "healthy",
                    "Stale live slot was intentionally marked missed instead of replayed.",
                )
            return "healthy", "Job run was superseded during queue consolidation."
        return "degraded", reason or "Job run was skipped."
    if status == "queued":
        queue_age_seconds = _seconds_since(run.get("scheduled_for"), now=now)
        if (
            queue_age_seconds is not None
            and queue_age_seconds > JOB_RUN_QUEUE_STALE_AFTER_SECONDS
        ):
            return "degraded", "Queued job run is older than 15 minutes."
        return "healthy", None
    if status == "running":
        payload = run.get("payload") if isinstance(run.get("payload"), Mapping) else {}
        interval_seconds = _coerce_int(payload.get("interval_seconds")) or 0
        stale_after_seconds = max(
            interval_seconds * 2,
            JOB_RUN_HEARTBEAT_STALE_AFTER_SECONDS,
        )
        heartbeat_age_seconds = _seconds_since(
            run.get("heartbeat_at")
            or run.get("started_at")
            or run.get("scheduled_for"),
            now=now,
        )
        if heartbeat_age_seconds is None or heartbeat_age_seconds > stale_after_seconds:
            return "degraded", "Running job heartbeat is stale."
        return "healthy", None
    if status == "succeeded":
        if str(run.get("job_type") or "") == "live_collector":
            live_action_gate = (
                run.get("live_action_gate")
                if isinstance(run.get("live_action_gate"), Mapping)
                else {}
            )
            if str(live_action_gate.get("status") or "") == "blocked":
                return (
                    "blocked",
                    _as_text(live_action_gate.get("message"))
                    or "Live collector actions are blocked.",
                )
            capture_status = str(run.get("capture_status") or "").strip().lower()
            if capture_status and capture_status not in {"healthy", "idle"}:
                return (
                    "degraded",
                    f"Live collector capture finished as {capture_status}.",
                )
        return "healthy", None
    return "unknown", None


def _summarize_job_run(
    run: Mapping[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    enriched = enrich_live_collector_job_run_payload(run)
    operator_status, operator_note = _job_run_operator_status(enriched, now=now)
    quote_capture = (
        enriched.get("quote_capture")
        if isinstance(enriched.get("quote_capture"), Mapping)
        else {}
    )
    trade_capture = (
        enriched.get("trade_capture")
        if isinstance(enriched.get("trade_capture"), Mapping)
        else {}
    )
    payload = (
        enriched.get("payload") if isinstance(enriched.get("payload"), Mapping) else {}
    )
    result = (
        enriched.get("result") if isinstance(enriched.get("result"), Mapping) else {}
    )
    live_action_gate = (
        enriched.get("live_action_gate")
        if isinstance(enriched.get("live_action_gate"), Mapping)
        else {}
    )
    stream_quote_events_saved = _stream_quote_events_saved(quote_capture)
    stream_trade_events_saved = _stream_trade_events_saved(trade_capture)
    return {
        "job_run_id": enriched.get("job_run_id"),
        "job_key": enriched.get("job_key"),
        "job_type": enriched.get("job_type"),
        "session_id": enriched.get("session_id"),
        "status": enriched.get("status"),
        "operator_status": operator_status,
        "operator_note": operator_note,
        "scheduled_for": enriched.get("scheduled_for"),
        "slot_at": enriched.get("slot_at"),
        "started_at": enriched.get("started_at"),
        "finished_at": enriched.get("finished_at"),
        "heartbeat_at": enriched.get("heartbeat_at"),
        "activity_at": _activity_at(enriched),
        "duration_seconds": _run_duration_seconds(enriched),
        "retry_count": _coerce_int(enriched.get("retry_count")) or 0,
        "worker_name": enriched.get("worker_name"),
        "arq_job_id": enriched.get("arq_job_id"),
        "error_text": enriched.get("error_text"),
        "capture_status": enriched.get("capture_status"),
        "singleton_scope": payload.get("singleton_scope"),
        "result_status": result.get("status"),
        "result_reason": result.get("reason"),
        "live_action_gate": dict(live_action_gate),
        "stream_quote_events_saved": stream_quote_events_saved,
        "websocket_quote_events_saved": stream_quote_events_saved,
        "baseline_quote_events_saved": _coerce_int(
            quote_capture.get("baseline_quote_events_saved")
        )
        or 0,
        "recovery_quote_events_saved": _coerce_int(
            quote_capture.get("recovery_quote_events_saved")
        )
        or 0,
        "total_trade_events_saved": _coerce_int(
            trade_capture.get("total_trade_events_saved")
        )
        or 0,
        "stream_trade_events_saved": stream_trade_events_saved,
        "websocket_trade_events_saved": stream_trade_events_saved,
    }


def _job_definition_status(
    definition: Mapping[str, Any],
    latest_run: Mapping[str, Any] | None,
    *,
    now: datetime,
) -> str:
    if not bool(definition.get("enabled")):
        return "idle"
    if latest_run is None:
        return "unknown"
    latest_status, _ = _job_run_operator_status(latest_run, now=now)
    return _combine_statuses("healthy", latest_status)


def _summarize_job_definition(
    definition: Mapping[str, Any],
    latest_run: Mapping[str, Any] | None,
    *,
    now: datetime,
) -> dict[str, Any]:
    enriched_latest_run = (
        None if latest_run is None else enrich_live_collector_job_run_payload(latest_run)
    )
    latest_summary = (
        None
        if enriched_latest_run is None
        else _summarize_job_run(enriched_latest_run, now=now)
    )
    return {
        "job_key": definition.get("job_key"),
        "job_type": definition.get("job_type"),
        "enabled": bool(definition.get("enabled")),
        "schedule_type": definition.get("schedule_type"),
        "schedule": dict(definition.get("schedule") or {}),
        "market_calendar": definition.get("market_calendar"),
        "singleton_scope": definition.get("singleton_scope"),
        "updated_at": definition.get("updated_at"),
        "operator_status": _job_definition_status(
            definition,
            enriched_latest_run,
            now=now,
        ),
        "latest_run_id": None if latest_summary is None else latest_summary.get("job_run_id"),
        "latest_run_status": None if latest_summary is None else latest_summary.get("status"),
        "latest_run_operator_status": None
        if latest_summary is None
        else latest_summary.get("operator_status"),
        "latest_run_at": None if latest_summary is None else latest_summary.get("activity_at"),
        "latest_capture_status": None
        if latest_summary is None
        else latest_summary.get("capture_status"),
    }


def _worker_lane_rows(
    *,
    workers: list[dict[str, Any]],
    queued_jobs: list[dict[str, Any]],
    running_jobs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    workers_by_settings: dict[str, list[dict[str, Any]]] = {}
    for worker in workers:
        lease_state = (
            worker.get("lease_state")
            if isinstance(worker.get("lease_state"), Mapping)
            else {}
        )
        settings_name = str(
            lease_state.get("settings_name") or lease_state.get("lane") or "unknown"
        )
        workers_by_settings.setdefault(settings_name, []).append(dict(worker))

    queued_by_queue = Counter(
        get_queue_name_for_job_type(str(row.get("job_type") or "unknown")) or "unknown"
        for row in queued_jobs
    )
    running_by_queue = Counter(
        get_queue_name_for_job_type(str(row.get("job_type") or "unknown")) or "unknown"
        for row in running_jobs
    )

    rows: list[dict[str, Any]] = []
    for lane in WORKER_LANES:
        lane_workers = workers_by_settings.get(str(lane.settings_name), [])
        active_worker_count = len(lane_workers)
        queued_job_count = int(queued_by_queue.get(str(lane.queue_name), 0))
        running_job_count = int(running_by_queue.get(str(lane.queue_name), 0))
        status = "healthy" if active_worker_count > 0 else "blocked"
        rows.append(
            {
                "settings_name": lane.settings_name,
                "lane": str(lane.settings_name).removesuffix("WorkerSettings").lower(),
                "queue_name": lane.queue_name,
                "task_names": list(lane.task_names),
                "task_count": len(lane.task_names),
                "max_jobs": lane.max_jobs,
                "active_worker_count": active_worker_count,
                "active_workers": [
                    str(worker.get("owner") or "-") for worker in lane_workers
                ],
                "queued_job_count": queued_job_count,
                "running_job_count": running_job_count,
                "status": status,
            }
        )
    return rows


@with_storage()
def build_jobs_overview(
    *,
    db_target: str | None = None,
    job_type: str | None = None,
    status: str | None = None,
    limit: int = 25,
    storage: Any | None = None,
) -> dict[str, Any]:
    generated_at = _utc_now()
    now = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    attention: list[dict[str, str]] = []

    job_store = storage.jobs
    if not job_store.schema_ready():
        attention.append(
            _attention(
                severity="high",
                code="job_schema_unavailable",
                message="Job storage is not available yet.",
            )
        )
        return {
            "status": "blocked",
            "generated_at": generated_at,
            "summary": {
                "view": "list",
                "job_type": job_type,
                "status_filter": status,
                "limit": limit,
                "definition_count": 0,
                "enabled_definition_count": 0,
                "run_count": 0,
                "singleton_lease_count": 0,
            },
            "attention": attention,
            "details": {
                "view": "list",
                "scheduler": None,
                "workers": [],
                "singleton_leases": [],
                "job_definitions": [],
                "job_runs": [],
            },
        }

    definitions = [
        dict(row)
        for row in job_store.list_job_definitions(
            enabled_only=None,
            job_type=job_type,
        )
    ]
    latest_run_by_key = {
        str(row["job_key"]): dict(row)
        for row in job_store.list_latest_runs_by_job_keys(
            job_keys=[str(row["job_key"]) for row in definitions],
            statuses=None,
        )
    }
    definition_rows = [
        _summarize_job_definition(
            definition,
            latest_run_by_key.get(str(definition["job_key"])),
            now=now,
        )
        for definition in definitions
    ]
    run_rows = [
        _summarize_job_run(dict(row), now=now)
        for row in job_store.list_job_runs(
            job_type=job_type,
            status=status,
            limit=limit,
        )
    ]
    run_rows = _sorted_by_activity(run_rows)

    scheduler_lease = job_store.get_lease(SCHEDULER_RUNTIME_LEASE_KEY)
    scheduler_payload = {
        "status": _lease_status(scheduler_lease, now=now),
        "expires_at": None if scheduler_lease is None else scheduler_lease.get("expires_at"),
        "owner": None if scheduler_lease is None else scheduler_lease.get("owner"),
        "job_run_id": None if scheduler_lease is None else scheduler_lease.get("job_run_id"),
    }
    workers = [
        dict(row)
        for row in job_store.list_active_leases(prefix=WORKER_RUNTIME_LEASE_PREFIX)
    ]
    singleton_leases = [
        dict(row) for row in job_store.list_active_leases(prefix=SINGLETON_LEASE_PREFIX)
    ]

    statuses = [scheduler_payload["status"]]
    if scheduler_payload["status"] != "healthy":
        attention.append(
            _attention(
                severity="high" if scheduler_payload["status"] == "blocked" else "medium",
                code="scheduler_unhealthy",
                message="Scheduler lease is missing, expired, or close to expiring.",
            )
        )

    worker_status = "healthy" if workers else "blocked"
    statuses.append(worker_status)
    if worker_status != "healthy":
        attention.append(
            _attention(
                severity="high",
                code="workers_missing",
                message="No active worker leases are present.",
            )
        )

    status_counts = Counter(str(row.get("status") or "unknown") for row in run_rows)
    operator_status_counts = Counter(
        str(row.get("operator_status") or "unknown") for row in run_rows
    )
    job_type_counts = Counter(str(row.get("job_type") or "unknown") for row in run_rows)
    stale_running_count = sum(
        1
        for row in run_rows
        if str(row.get("status") or "") == "running"
        and str(row.get("operator_status") or "") != "healthy"
    )
    degraded_capture_count = sum(
        1
        for row in run_rows
        if str(row.get("job_type") or "") == "live_collector"
        and str(row.get("capture_status") or "") not in {"", "healthy", "None"}
    )
    if status_counts.get("failed", 0):
        attention.append(
            _attention(
                severity="high",
                code="failed_job_runs_present",
                message=f"{status_counts['failed']} recent job run(s) failed.",
            )
        )
    actionable_skipped_count = sum(
        1
        for row in run_rows
        if str(row.get("status") or "") == "skipped"
        and str(row.get("operator_status") or "") != "healthy"
    )
    if actionable_skipped_count:
        attention.append(
            _attention(
                severity="medium",
                code="skipped_job_runs_present",
                message=f"{actionable_skipped_count} recent job run(s) were skipped.",
            )
        )
    if stale_running_count:
        attention.append(
            _attention(
                severity="medium",
                code="stale_running_jobs",
                message=f"{stale_running_count} running job run(s) have stale heartbeats.",
            )
        )
    if degraded_capture_count:
        attention.append(
            _attention(
                severity="medium",
                code="collector_capture_degraded",
                message=f"{degraded_capture_count} live collector run(s) completed with degraded capture.",
            )
        )

    actionable_definition_rows = [
        row for row in definition_rows if _definition_requires_attention(row, now=now)
    ]
    actionable_definition_status_counts = Counter(
        str(row.get("operator_status") or "unknown")
        for row in actionable_definition_rows
    )
    if actionable_definition_status_counts.get("degraded", 0) or actionable_definition_status_counts.get("blocked", 0):
        attention.append(
            _attention(
                severity="medium",
                code="job_definitions_need_attention",
                message=(
                    f"{actionable_definition_status_counts.get('degraded', 0) + actionable_definition_status_counts.get('blocked', 0)} "
                    "job definition(s) have an unhealthy latest run."
                ),
            )
        )

    lane_rows = _worker_lane_rows(
        workers=workers,
        queued_jobs=[
            dict(row) for row in job_store.list_job_runs(status="queued", limit=200)
        ],
        running_jobs=[
            dict(row) for row in job_store.list_job_runs(status="running", limit=200)
        ],
    )
    blocked_lane_count = sum(
        1 for row in lane_rows if str(row.get("status") or "") == "blocked"
    )
    if blocked_lane_count:
        attention.append(
            _attention(
                severity="high",
                code="worker_lanes_blocked",
                message=f"{blocked_lane_count} worker lane(s) have no active workers.",
            )
        )

    stale_singleton_leases: list[dict[str, Any]] = []
    for lease in singleton_leases:
        lease_run_id = _as_text(lease.get("job_run_id"))
        if lease_run_id is None:
            continue
        run_record = job_store.get_job_run(lease_run_id)
        if run_record is None or str(run_record.get("status") or "") not in {
            "queued",
            "running",
        }:
            stale_singleton_leases.append(dict(lease))
    if stale_singleton_leases:
        attention.append(
            _attention(
                severity="medium",
                code="stale_singleton_leases",
                message=f"{len(stale_singleton_leases)} singleton lease(s) point at inactive job runs.",
            )
        )

    statuses.append(
        _combine_statuses(
            "blocked" if status_counts.get("failed", 0) else "healthy",
            "degraded"
            if status_counts.get("skipped", 0)
            or stale_running_count
            or degraded_capture_count
            else "healthy",
            "degraded"
            if actionable_definition_status_counts.get("degraded", 0)
            or actionable_definition_status_counts.get("blocked", 0)
            else "healthy",
            "degraded" if stale_singleton_leases else "healthy",
        )
    )

    return {
        "status": _combine_statuses(*statuses),
        "generated_at": generated_at,
        "summary": {
            "view": "list",
            "job_type": job_type,
            "status_filter": status,
            "limit": limit,
            "definition_count": len(definition_rows),
            "enabled_definition_count": sum(
                1 for row in definition_rows if bool(row.get("enabled"))
            ),
            "run_count": len(run_rows),
            "status_counts": dict(status_counts),
            "operator_status_counts": dict(operator_status_counts),
            "job_type_counts": dict(job_type_counts),
            "singleton_lease_count": len(singleton_leases),
            "worker_lane_count": len(lane_rows),
            "stale_running_count": stale_running_count,
            "degraded_capture_count": degraded_capture_count,
        },
        "attention": attention,
        "details": {
            "view": "list",
            "scheduler": scheduler_payload,
            "workers": workers,
            "worker_lanes": lane_rows,
            "singleton_leases": singleton_leases,
            "stale_singleton_leases": stale_singleton_leases,
            "job_definitions": definition_rows,
            "job_runs": run_rows,
        },
    }


@with_storage()
def build_job_lanes_overview(
    *,
    db_target: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    payload = build_jobs_overview(db_target=db_target, storage=storage)
    details = dict(payload.get("details") or {})
    lane_rows = list(details.get("worker_lanes") or [])
    summary = dict(payload.get("summary") or {})
    return {
        "status": payload.get("status"),
        "generated_at": payload.get("generated_at"),
        "summary": {
            "view": "lanes",
            "worker_lane_count": len(lane_rows),
            "active_worker_count": sum(
                int(row.get("active_worker_count") or 0) for row in lane_rows
            ),
            "running_job_count": sum(
                int(row.get("running_job_count") or 0) for row in lane_rows
            ),
            "queued_job_count": sum(
                int(row.get("queued_job_count") or 0) for row in lane_rows
            ),
            "singleton_lease_count": summary.get("singleton_lease_count"),
        },
        "attention": list(payload.get("attention") or []),
        "details": {
            "view": "lanes",
            "scheduler": details.get("scheduler"),
            "workers": details.get("workers"),
            "worker_lanes": lane_rows,
            "singleton_leases": details.get("singleton_leases"),
        },
    }


@with_storage()
def build_job_run_view(
    *,
    job_run_id: str,
    db_target: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    generated_at = _utc_now()
    now = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    job_store = storage.jobs
    if not job_store.schema_ready():
        raise OpsLookupError("Job storage is not available yet.")

    run_record = job_store.get_job_run(job_run_id)
    if run_record is None:
        raise OpsLookupError(f"Unknown job run: {job_run_id}")

    run = enrich_live_collector_job_run_payload(run_record)
    run_summary = _summarize_job_run(run, now=now)
    attention: list[dict[str, str]] = []
    statuses = [str(run_summary.get("operator_status") or "unknown")]

    operator_note = _as_text(run_summary.get("operator_note"))
    if operator_note is not None:
        attention.append(
            _attention(
                severity="high" if run_summary["operator_status"] == "blocked" else "medium",
                code=f"job_run_{run_summary['operator_status']}",
                message=operator_note,
            )
        )

    definition = job_store.get_job_definition(str(run.get("job_key")))
    definition_summary = None
    if definition is None:
        attention.append(
            _attention(
                severity="medium",
                code="job_definition_missing",
                message=f"Job definition {run.get('job_key')} no longer exists.",
            )
        )
        statuses.append("degraded")
    else:
        latest_definition_runs = job_store.list_latest_runs_by_job_keys(
            job_keys=[str(run.get("job_key"))],
            statuses=None,
        )
        latest_definition_run = latest_definition_runs[0] if latest_definition_runs else None
        definition_summary = _summarize_job_definition(
            definition,
            latest_definition_run,
            now=now,
        )
        if (
            latest_definition_run is not None
            and latest_definition_run.get("job_run_id") != run_summary["job_run_id"]
        ):
            attention.append(
                _attention(
                    severity="medium",
                    code="historical_job_run",
                    message=(
                        f"{run_summary['job_run_id']} is not the latest run for {run_summary['job_key']}; "
                        f"latest is {latest_definition_run.get('job_run_id')}."
                    ),
                )
            )

    singleton_scope = _as_text((run.get("payload") or {}).get("singleton_scope"))
    singleton_lease = None
    if singleton_scope is not None and _as_text(run.get("job_type")) is not None:
        singleton_lease = job_store.get_lease(
            singleton_lease_key(str(run["job_type"]), singleton_scope)
        )
        if (
            singleton_lease is not None
            and singleton_lease.get("job_run_id") != run_summary["job_run_id"]
        ):
            statuses.append("degraded")
            attention.append(
                _attention(
                    severity="medium",
                    code="singleton_lease_held_elsewhere",
                    message=(
                        f"Singleton lease for {run.get('job_type')}:{singleton_scope} is currently held by "
                        f"{singleton_lease.get('job_run_id')}."
                    ),
                )
            )

    result = run.get("result") if isinstance(run.get("result"), Mapping) else {}
    if (
        str(run.get("status") or "") == "failed"
        and _as_text(run.get("error_text")) is None
    ):
        result_reason = _as_text(result.get("reason"))
        if result_reason is not None:
            attention.append(
                _attention(
                    severity="high",
                    code="job_run_failure_reason",
                    message=result_reason,
                )
            )

    return {
        "status": _combine_statuses(*statuses),
        "generated_at": generated_at,
        "summary": {
            "view": "detail",
            "job_run_id": run_summary.get("job_run_id"),
            "job_key": run_summary.get("job_key"),
            "job_type": run_summary.get("job_type"),
            "session_id": run_summary.get("session_id"),
            "status": run_summary.get("status"),
            "operator_status": run_summary.get("operator_status"),
            "scheduled_for": run_summary.get("scheduled_for"),
            "activity_at": run_summary.get("activity_at"),
            "worker_name": run_summary.get("worker_name"),
            "retry_count": run_summary.get("retry_count"),
            "capture_status": run_summary.get("capture_status"),
            "collector_opportunity_count": _coerce_int(
                (run.get("selection_summary") or {}).get("opportunity_count")
            )
            or 0,
        },
        "attention": attention,
        "details": {
            "view": "detail",
            "run": run_summary,
            "definition": definition_summary,
            "payload": dict(run.get("payload") or {}),
            "result": dict(result),
            "quote_capture": dict(run.get("quote_capture") or {}),
            "trade_capture": dict(run.get("trade_capture") or {}),
            "uoa_summary": dict(run.get("uoa_summary") or {}),
            "uoa_quote_summary": dict(run.get("uoa_quote_summary") or {}),
            "uoa_decisions": dict(run.get("uoa_decisions") or {}),
            "selection_summary": _selection_summary_payload(run.get("selection_summary")),
            "singleton_lease": None if singleton_lease is None else dict(singleton_lease),
        },
    }
