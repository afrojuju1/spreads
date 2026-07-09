from __future__ import annotations

from core.services.ops.jobs.state_support import (
    Any,
    Mapping,
    OpsLookupError,
    _attention,
    _combine_statuses,
    _job_key_is_adhoc,
    _summarize_job_definition,
    _summarize_job_run,
    as_text,
    get_declared_job_row,
    singleton_lease_key,
    utc_iso,
    utc_now,
    utc_now_iso,
    with_storage,
)


@with_storage()
def build_job_run_view(
    *,
    job_run_id: str,
    db_target: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    now = utc_now()
    generated_at = utc_iso(now) or utc_now_iso()
    job_store = storage.jobs
    if not job_store.schema_ready():
        raise OpsLookupError("Job storage is not available yet.")

    run_record = job_store.get_job_run(job_run_id)
    if run_record is None:
        raise OpsLookupError(f"Unknown job run: {job_run_id}")

    run = dict(run_record)
    run_summary = _summarize_job_run(run, now=now)
    attention: list[dict[str, str]] = []
    statuses = [str(run_summary.get("operator_status") or "unknown")]

    operator_note = as_text(run_summary.get("operator_note"))
    if operator_note is not None:
        attention.append(
            _attention(
                severity="high" if run_summary["operator_status"] == "blocked" else "medium",
                code=f"job_run_{run_summary['operator_status']}",
                message=operator_note,
            )
        )

    job_key = str(run.get("job_key") or "")
    definition = get_declared_job_row(job_key)
    definition_summary = None
    if definition is None:
        if not _job_key_is_adhoc(job_key):
            attention.append(
                _attention(
                    severity="medium",
                    code="declared_job_missing",
                    message=f"No declared config exists for job key {job_key}.",
                )
            )
            statuses.append("degraded")
    else:
        latest_definition_runs = job_store.list_latest_runs_by_job_keys(
            job_keys=[job_key],
            statuses=None,
        )
        latest_definition_run = latest_definition_runs[0] if latest_definition_runs else None
        definition_summary = _summarize_job_definition(
            definition,
            latest_definition_run,
            now=now,
        )
        if latest_definition_run is not None and latest_definition_run.get("job_run_id") != run_summary["job_run_id"]:
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

    singleton_scope = as_text((run.get("payload") or {}).get("singleton_scope"))
    singleton_lease = None
    if singleton_scope is not None and as_text(run.get("job_type")) is not None:
        singleton_lease = job_store.get_lease(singleton_lease_key(str(run["job_type"]), singleton_scope))
        if singleton_lease is not None and singleton_lease.get("job_run_id") != run_summary["job_run_id"]:
            statuses.append("degraded")
            attention.append(
                _attention(
                    severity="medium",
                    code="singleton_lease_held_elsewhere",
                    message=(
                        f"Singleton lease for {run.get('job_type')}:{singleton_scope} is currently held by {singleton_lease.get('job_run_id')}."
                    ),
                )
            )

    result = run.get("result") if isinstance(run.get("result"), Mapping) else {}
    if str(run.get("status") or "") == "failed" and as_text(run.get("error_text")) is None:
        result_reason = as_text(result.get("reason"))
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
            "result_status": run_summary.get("result_status"),
            "result_reason": run_summary.get("result_reason"),
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
            "singleton_lease": None if singleton_lease is None else dict(singleton_lease),
        },
    }


__all__ = ["build_job_run_view"]
