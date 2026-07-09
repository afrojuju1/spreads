from __future__ import annotations

from core.services.ops.jobs.state_support import (
    Any,
    BROKER_SYNC_KEY,
    RETIRED_LIFECYCLE_JOB_TYPES,
    Counter,
    _apply_broker_sync_definition_overrides,
    _apply_broker_sync_run_overrides,
    _apply_superseded_run_overrides,
    _attention,
    _broker_sync_payload,
    _combine_statuses,
    _definition_requires_attention,
    _disabled_task_queue_rows,
    _filter_excluded_job_runs,
    _job_run_health_attention,
    _market_session_context,
    _project_job_run_health,
    _project_worker_runtime,
    _sorted_by_activity,
    _split_active_queued_jobs,
    _summarize_job_definition,
    _summarize_job_run,
    excluded_declared_job_types,
    list_declared_job_rows,
    utc_iso,
    utc_now,
    utc_now_iso,
    with_storage,
)


@with_storage()
def build_jobs_overview(
    *,
    db_target: str | None = None,
    job_type: str | None = None,
    status: str | None = None,
    limit: int = 25,
    storage: Any | None = None,
) -> dict[str, Any]:
    now = utc_now()
    generated_at = utc_iso(now) or utc_now_iso()
    excluded_job_types = excluded_declared_job_types().union(RETIRED_LIFECYCLE_JOB_TYPES)
    attention: list[dict[str, str]] = []
    definitions = [dict(row) for row in list_declared_job_rows(enabled_only=None, job_type=job_type)]
    disabled_task_queue_rows = _disabled_task_queue_rows(excluded_job_types=excluded_job_types)
    definition_rows = [
        _summarize_job_definition(
            definition,
            latest_run=None,
            now=now,
        )
        for definition in definitions
    ]

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
                "definition_count": len(definition_rows),
                "enabled_definition_count": sum(1 for row in definition_rows if bool(row.get("enabled"))),
                "disabled_task_queue_count": len(disabled_task_queue_rows),
                "excluded_job_types": sorted(excluded_job_types),
                "run_count": 0,
                "singleton_lease_count": 0,
            },
            "attention": attention,
            "details": {
                "view": "list",
                "schedules": None,
                "task_queues": [],
                "singleton_leases": [],
                "disabled_task_queues": disabled_task_queue_rows,
                "declared_jobs": definition_rows,
                "job_runs": [],
            },
        }

    latest_run_by_key = {
        str(row["job_key"]): dict(row)
        for row in job_store.list_latest_runs_by_job_keys(
            job_keys=[str(row["job_key"]) for row in definitions],
            statuses=None,
        )
    }
    broker_sync: dict[str, Any] | None = None
    if any(str(row.get("job_key") or "") == BROKER_SYNC_KEY for row in definitions) and storage.broker.schema_ready():
        _, broker_sync = _broker_sync_payload(
            storage.broker.get_sync_state(BROKER_SYNC_KEY),
            now=now,
            market_session=_market_session_context(now=now),
        )
    definition_rows = [
        _summarize_job_definition(
            definition,
            latest_run_by_key.get(str(definition["job_key"])),
            now=now,
        )
        for definition in definitions
    ]
    definition_rows = _apply_broker_sync_definition_overrides(
        definition_rows,
        latest_run_by_key=latest_run_by_key,
        broker_sync=broker_sync,
    )
    run_records = [
        dict(row)
        for row in job_store.list_job_runs(
            job_type=job_type,
            status=status,
            limit=limit,
        )
    ]
    latest_success_run_by_key = {
        str(row["job_key"]): dict(row)
        for row in job_store.list_latest_runs_by_job_keys(
            job_keys=sorted({str(row.get("job_key") or "") for row in [*definitions, *run_records] if str(row.get("job_key") or "")}),
            statuses=["succeeded"],
        )
    }
    run_rows = [_summarize_job_run(row, now=now) for row in run_records]
    run_rows = _filter_excluded_job_runs(
        run_rows,
        excluded_job_types=excluded_job_types,
    )
    run_rows = _sorted_by_activity(run_rows)
    run_rows = _apply_superseded_run_overrides(
        run_rows,
        latest_run_by_key=latest_success_run_by_key,
    )
    run_rows = _apply_broker_sync_run_overrides(
        run_rows,
        broker_sync=broker_sync,
    )
    queued_run_rows = [dict(row) for row in job_store.list_job_runs(status="queued", limit=200)]
    queued_run_rows = _filter_excluded_job_runs(
        queued_run_rows,
        excluded_job_types=excluded_job_types,
    )
    active_queued_run_rows, stale_queued_run_rows = _split_active_queued_jobs(
        queued_run_rows,
        now=now,
    )
    running_run_rows = _filter_excluded_job_runs(
        [dict(row) for row in job_store.list_job_runs(status="running", limit=200)],
        excluded_job_types=excluded_job_types,
    )
    worker_runtime = _project_worker_runtime(
        job_store=job_store,
        definitions=definition_rows,
        queued_jobs=active_queued_run_rows,
        running_jobs=running_run_rows,
        excluded_job_types=excluded_job_types,
        now=now,
        include_singletons=True,
        include_blocked_task_queue_status=False,
    )
    run_health = _project_job_run_health(
        counted_runs=run_rows,
        running_health_rows=run_rows,
        stale_queued_rows=stale_queued_run_rows,
    )

    statuses = list(worker_runtime.statuses)
    attention.extend(worker_runtime.attention)
    attention.extend(_job_run_health_attention(run_health))

    actionable_definition_rows = [row for row in definition_rows if _definition_requires_attention(row, now=now)]
    actionable_definition_status_counts = Counter(str(row.get("operator_status") or "unknown") for row in actionable_definition_rows)
    if actionable_definition_status_counts.get("degraded", 0) or actionable_definition_status_counts.get("blocked", 0):
        attention.append(
            _attention(
                severity="medium",
                code="declared_jobs_need_attention",
                message=(
                    f"{actionable_definition_status_counts.get('degraded', 0) + actionable_definition_status_counts.get('blocked', 0)} "
                    "declared job(s) have an unhealthy latest run."
                ),
            )
        )

    statuses.append(
        _combine_statuses(
            "blocked" if run_health.actionable_failed_count else "healthy",
            "degraded" if run_health.actionable_skipped_count or run_health.stale_running_count else "healthy",
            (
                "degraded"
                if actionable_definition_status_counts.get("degraded", 0) or actionable_definition_status_counts.get("blocked", 0)
                else "healthy"
            ),
            "degraded" if worker_runtime.stale_singleton_leases else "healthy",
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
            "enabled_definition_count": sum(1 for row in definition_rows if bool(row.get("enabled"))),
            "run_count": len(run_rows),
            "status_counts": dict(run_health.status_counts),
            "operator_status_counts": dict(run_health.operator_status_counts),
            "job_type_counts": dict(run_health.job_type_counts),
            "singleton_lease_count": len(worker_runtime.singleton_leases),
            "task_queue_count": len(worker_runtime.task_queue_rows),
            "disabled_task_queue_count": len(disabled_task_queue_rows),
            "excluded_job_types": sorted(excluded_job_types),
            "stale_running_count": run_health.stale_running_count,
            "stale_queued_job_count": run_health.stale_queued_job_count,
            "actionable_failed_count": run_health.actionable_failed_count,
            "historical_failed_count": run_health.historical_failed_count,
        },
        "attention": attention,
        "details": {
            "view": "list",
            "schedules": worker_runtime.schedules_payload,
            "task_queues": worker_runtime.task_queue_rows,
            "disabled_task_queues": disabled_task_queue_rows,
            "singleton_leases": worker_runtime.singleton_leases,
            "stale_singleton_leases": worker_runtime.stale_singleton_leases,
            "stale_queued_job_runs": stale_queued_run_rows,
            "declared_jobs": definition_rows,
            "job_runs": run_rows,
        },
    }


__all__ = ["build_jobs_overview"]
