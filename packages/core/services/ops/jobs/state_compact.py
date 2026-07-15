from __future__ import annotations

from core.services.ops.jobs.state_support import (
    Any,
    RETIRED_LIFECYCLE_JOB_TYPES,
    _attention,
    _combine_statuses,
    _disabled_workflow_lane_rows,
    _filter_excluded_job_runs,
    _job_run_health_attention,
    _project_job_run_health,
    _project_workflow_runtime,
    _sorted_by_activity,
    _split_active_queued_jobs,
    _summarize_job_run,
    disabled_workflow_lanes,
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
    disabled_lanes = disabled_workflow_lanes()
    attention: list[dict[str, str]] = []
    definitions = [dict(row) for row in list_declared_job_rows(enabled_only=None, job_type=None)]
    disabled_lane_rows = _disabled_workflow_lane_rows(disabled_lanes=disabled_lanes)

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
                "disabled_workflow_lane_count": len(disabled_lane_rows),
                "run_count": 0,
                "workflow_lane_count": 0,
                "stale_running_count": 0,
                "stale_queued_job_count": 0,
                "actionable_failed_count": 0,
                "historical_failed_count": 0,
            },
            "attention": attention,
            "details": {
                "view": "compact",
                "routine_schedules": None,
                "workflow_lanes": [],
                "disabled_workflow_lanes": disabled_lane_rows,
                "running_jobs": [],
                "queued_jobs": [],
                "job_runs": [],
            },
        }

    recent_runs = [_summarize_job_run(dict(row), now=now) for row in job_store.list_job_runs(limit=limit)]
    recent_runs = _sorted_by_activity(
        _filter_excluded_job_runs(
            recent_runs,
            excluded_job_types=set(RETIRED_LIFECYCLE_JOB_TYPES),
        )
    )
    queued_runs = _filter_excluded_job_runs(
        [_summarize_job_run(dict(row), now=now) for row in job_store.list_job_runs(status="queued", limit=200)],
        excluded_job_types=set(RETIRED_LIFECYCLE_JOB_TYPES),
    )
    running_runs = _filter_excluded_job_runs(
        [_summarize_job_run(dict(row), now=now) for row in job_store.list_job_runs(status="running", limit=200)],
        excluded_job_types=set(RETIRED_LIFECYCLE_JOB_TYPES),
    )
    active_queued_runs, stale_queued_runs = _split_active_queued_jobs(
        queued_runs,
        now=now,
    )

    workflow_runtime = _project_workflow_runtime(
        job_store=job_store,
        definitions=definitions,
        routine_schedule_definitions=definitions,
        queued_jobs=active_queued_runs,
        running_jobs=running_runs,
        disabled_lanes=disabled_lanes,
        now=now,
        include_singletons=False,
        include_blocked_workflow_lane_status=True,
    )
    run_health = _project_job_run_health(
        counted_runs=recent_runs,
        running_health_rows=running_runs,
        stale_queued_rows=stale_queued_runs,
    )

    statuses = list(workflow_runtime.statuses)
    attention.extend(workflow_runtime.attention)
    attention.extend(_job_run_health_attention(run_health))

    statuses.append(
        _combine_statuses(
            "blocked" if run_health.actionable_failed_count else "healthy",
            "degraded" if run_health.actionable_skipped_count or run_health.stale_running_count or run_health.stale_queued_job_count else "healthy",
            "blocked" if workflow_runtime.blocked_workflow_lane_count else "healthy",
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
            "workflow_lane_count": len(workflow_runtime.workflow_lane_rows),
            "disabled_workflow_lane_count": len(disabled_lane_rows),
            "blocked_workflow_lane_count": workflow_runtime.blocked_workflow_lane_count,
            "due_routine_count": len(workflow_runtime.due_routines_missing),
            "stale_running_count": run_health.stale_running_count,
            "stale_queued_job_count": run_health.stale_queued_job_count,
            "actionable_failed_count": run_health.actionable_failed_count,
            "historical_failed_count": run_health.historical_failed_count,
        },
        "attention": attention,
        "details": {
            "view": "compact",
            "routine_schedules": workflow_runtime.routine_schedules_payload,
            "workflow_lanes": workflow_runtime.workflow_lane_rows,
            "disabled_workflow_lanes": disabled_lane_rows,
            "due_routines_missing": workflow_runtime.due_routines_missing,
            "running_jobs": running_runs,
            "queued_jobs": active_queued_runs,
            "stale_queued_job_runs": stale_queued_runs,
            "job_runs": recent_runs,
        },
    }


__all__ = ["build_jobs_compact_state"]
