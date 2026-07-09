from __future__ import annotations

from core.services.ops.jobs.state_support import (
    Any,
    RETIRED_LIFECYCLE_JOB_TYPES,
    _attention,
    _combine_statuses,
    _disabled_task_queue_rows,
    _filter_excluded_job_runs,
    _job_run_health_attention,
    _project_job_run_health,
    _project_worker_runtime,
    _sorted_by_activity,
    _split_active_queued_jobs,
    _summarize_job_run,
    excluded_declared_job_types,
    list_declared_job_rows,
    utc_iso,
    utc_now,
    utc_now_iso,
    with_storage,
)


@with_storage()
def build_jobs_compact_state(
    *,
    db_target: str | None = None,
    limit: int = 25,
    storage: Any | None = None,
) -> dict[str, Any]:
    now = utc_now()
    generated_at = utc_iso(now) or utc_now_iso()
    excluded_job_types = excluded_declared_job_types().union(RETIRED_LIFECYCLE_JOB_TYPES)
    attention: list[dict[str, str]] = []
    definitions = [dict(row) for row in list_declared_job_rows(enabled_only=None, job_type=None)]
    disabled_task_queue_rows = _disabled_task_queue_rows(excluded_job_types=excluded_job_types)

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
                "view": "compact",
                "definition_count": len(definitions),
                "enabled_definition_count": sum(1 for row in definitions if bool(row.get("enabled"))),
                "disabled_task_queue_count": len(disabled_task_queue_rows),
                "excluded_job_types": sorted(excluded_job_types),
                "run_count": 0,
                "task_queue_count": 0,
                "stale_running_count": 0,
                "stale_queued_job_count": 0,
                "actionable_failed_count": 0,
                "historical_failed_count": 0,
            },
            "attention": attention,
            "details": {
                "view": "compact",
                "schedules": None,
                "task_queues": [],
                "disabled_task_queues": disabled_task_queue_rows,
                "running_jobs": [],
                "queued_jobs": [],
                "job_runs": [],
            },
        }

    recent_runs = [_summarize_job_run(dict(row), now=now) for row in job_store.list_job_runs(limit=limit)]
    recent_runs = _sorted_by_activity(
        _filter_excluded_job_runs(
            recent_runs,
            excluded_job_types=excluded_job_types,
        )
    )
    queued_runs = _filter_excluded_job_runs(
        [_summarize_job_run(dict(row), now=now) for row in job_store.list_job_runs(status="queued", limit=200)],
        excluded_job_types=excluded_job_types,
    )
    running_runs = _filter_excluded_job_runs(
        [_summarize_job_run(dict(row), now=now) for row in job_store.list_job_runs(status="running", limit=200)],
        excluded_job_types=excluded_job_types,
    )
    active_queued_runs, stale_queued_runs = _split_active_queued_jobs(
        queued_runs,
        now=now,
    )

    worker_runtime = _project_worker_runtime(
        job_store=job_store,
        definitions=definitions,
        queued_jobs=active_queued_runs,
        running_jobs=running_runs,
        excluded_job_types=excluded_job_types,
        now=now,
        include_singletons=False,
        include_blocked_task_queue_status=False,
    )
    run_health = _project_job_run_health(
        counted_runs=recent_runs,
        running_health_rows=running_runs,
        stale_queued_rows=stale_queued_runs,
    )

    statuses = list(worker_runtime.statuses)
    attention.extend(worker_runtime.attention)
    attention.extend(_job_run_health_attention(run_health))

    statuses.append(
        _combine_statuses(
            "blocked" if run_health.actionable_failed_count else "healthy",
            "degraded" if run_health.actionable_skipped_count or run_health.stale_running_count or run_health.stale_queued_job_count else "healthy",
            "blocked" if worker_runtime.blocked_task_queue_count else "healthy",
        )
    )

    return {
        "status": _combine_statuses(*statuses),
        "generated_at": generated_at,
        "summary": {
            "view": "compact",
            "definition_count": len(definitions),
            "enabled_definition_count": sum(1 for row in definitions if bool(row.get("enabled"))),
            "run_count": len(recent_runs),
            "status_counts": dict(run_health.status_counts),
            "operator_status_counts": dict(run_health.operator_status_counts),
            "job_type_counts": dict(run_health.job_type_counts),
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
            "view": "compact",
            "schedules": worker_runtime.schedules_payload,
            "task_queues": worker_runtime.task_queue_rows,
            "disabled_task_queues": disabled_task_queue_rows,
            "running_jobs": running_runs,
            "queued_jobs": active_queued_runs,
            "stale_queued_job_runs": stale_queued_runs,
            "job_runs": recent_runs,
        },
    }


__all__ = ["build_jobs_compact_state"]
