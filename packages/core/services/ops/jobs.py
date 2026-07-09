from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from core.db.decorators import with_storage
from core.jobs.orchestration import (
    SINGLETON_LEASE_PREFIX,
    resolve_scheduled_for,
    singleton_lease_key,
)
from core.jobs.registry import JOB_SPECS, TEMPORAL_TASK_QUEUES, get_task_queue_name_for_job_type
from core.jobs.specs import (
    excluded_declared_job_types,
    get_declared_job_row,
    list_declared_job_rows,
)
from core.services.broker_sync import BROKER_SYNC_KEY
from core.storage.serializers import parse_datetime
from core.value_coercion import (
    as_text,
    coerce_int,
    utc_iso,
    utc_now,
    utc_now_iso,
)

from .broker_sync import broker_sync_payload as _broker_sync_payload
from .market_session import market_session_context as _market_session_context
from .shared import (
    JOB_RUN_HEARTBEAT_STALE_AFTER_SECONDS,
    JOB_RUN_QUEUE_STALE_AFTER_SECONDS,
    OpsLookupError,
    _activity_at,
    _attention,
    _combine_statuses,
    _is_recent,
    _run_duration_seconds,
    _seconds_since,
    _sorted_by_activity,
    _stream_quote_ticks_saved,
    _stream_trade_ticks_saved,
)

_JOB_TYPE_BY_TASK_NAME = {spec.task_name: job_type for job_type, spec in JOB_SPECS.items()}
RETIRED_LIFECYCLE_JOB_TYPES = frozenset(
    {
        "execution_intent" + "_dispatch",
        "execution" + "_submit",
    }
)
TRADING_STRATEGY_MANAGE_BROKER_SYNC_SKIP_REASONS = {
    "broker_sync_in_flight",
    "broker_sync_missing",
    "broker_sync_schema_unavailable",
    "broker_sync_stale",
    "broker_sync_unhealthy",
    "broker_sync_updated_at_missing",
}


@dataclass(frozen=True)
class _JobRunHealthProjection:
    status_counts: Counter[str]
    operator_status_counts: Counter[str]
    job_type_counts: Counter[str]
    stale_running_count: int
    actionable_failed_count: int
    historical_failed_count: int
    actionable_skipped_count: int
    stale_queued_job_count: int


@dataclass(frozen=True)
class _WorkerRuntimeProjection:
    schedules_payload: dict[str, Any]
    task_queue_rows: list[dict[str, Any]]
    singleton_leases: list[dict[str, Any]]
    stale_singleton_leases: list[dict[str, Any]]
    blocked_task_queue_count: int
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


def _temporal_schedules_payload(definitions: list[dict[str, Any]]) -> dict[str, Any]:
    enabled_count = sum(1 for row in definitions if bool(row.get("enabled")))
    declared_count = len(definitions)
    return {
        "status": "healthy" if enabled_count else "idle",
        "kind": "temporal_schedules",
        "declared_schedule_count": declared_count,
        "enabled_schedule_count": enabled_count,
        "disabled_schedule_count": max(declared_count - enabled_count, 0),
        "reconcile_command": "spreads runtime temporal-schedules",
        "note": "Temporal schedules are reconciled from declared job definitions.",
    }


def _broker_sync_state_supersedes_run(
    broker_sync: Mapping[str, Any] | None,
    run: Mapping[str, Any] | None,
) -> bool:
    if not broker_sync or not run:
        return False
    if str(run.get("job_key") or "") != BROKER_SYNC_KEY:
        return False
    if str(run.get("status") or "").strip().lower() != "failed":
        return False
    if str(broker_sync.get("status") or "") not in {"healthy", "idle"}:
        return False
    state_updated_at = parse_datetime(broker_sync.get("updated_at"))
    run_activity_at = parse_datetime(run.get("activity_at") or _activity_at(run))
    return state_updated_at is not None and run_activity_at is not None and state_updated_at > run_activity_at


def _broker_sync_recovered_note(broker_sync: Mapping[str, Any]) -> str:
    updated_at = as_text(broker_sync.get("updated_at")) or "a later sync"
    return f"Broker sync state recovered at {updated_at}; this failed scheduled run " "is historical."


def _apply_broker_sync_run_overrides(
    run_rows: list[dict[str, Any]],
    *,
    broker_sync: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    if not broker_sync:
        return run_rows
    rows: list[dict[str, Any]] = []
    for row in run_rows:
        if _broker_sync_state_supersedes_run(broker_sync, row):
            rows.append(
                {
                    **row,
                    "operator_status": "healthy",
                    "operator_note": _broker_sync_recovered_note(broker_sync),
                    "broker_sync_status": broker_sync.get("status"),
                    "broker_sync_updated_at": broker_sync.get("updated_at"),
                }
            )
        else:
            rows.append(row)
    return rows


def _apply_broker_sync_definition_overrides(
    definition_rows: list[dict[str, Any]],
    *,
    latest_run_by_key: Mapping[str, Mapping[str, Any]],
    broker_sync: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    if not broker_sync:
        return definition_rows
    rows: list[dict[str, Any]] = []
    for row in definition_rows:
        if _broker_sync_state_supersedes_run(
            broker_sync,
            latest_run_by_key.get(str(row.get("job_key") or "")),
        ):
            rows.append(
                {
                    **row,
                    "operator_status": broker_sync.get("status"),
                    "latest_run_operator_status": "healthy",
                    "schedule_note": _broker_sync_recovered_note(broker_sync),
                    "broker_sync_status": broker_sync.get("status"),
                    "broker_sync_updated_at": broker_sync.get("updated_at"),
                }
            )
        else:
            rows.append(row)
    return rows


def _run_is_superseded_by_later_success(
    run: Mapping[str, Any],
    latest_run: Mapping[str, Any] | None,
) -> bool:
    if not latest_run:
        return False
    if str(run.get("status") or "").strip().lower() != "failed":
        return False
    if str(latest_run.get("status") or "").strip().lower() != "succeeded":
        return False
    if str(run.get("job_key") or "") != str(latest_run.get("job_key") or ""):
        return False
    if str(run.get("job_run_id") or "") == str(latest_run.get("job_run_id") or ""):
        return False
    run_activity_at = parse_datetime(run.get("activity_at") or _activity_at(run))
    latest_activity_at = parse_datetime(latest_run.get("activity_at") or _activity_at(latest_run))
    return run_activity_at is not None and latest_activity_at is not None and latest_activity_at > run_activity_at


def _superseded_run_note(latest_run: Mapping[str, Any]) -> str:
    latest_at = as_text(latest_run.get("finished_at") or _activity_at(latest_run))
    latest_id = as_text(latest_run.get("job_run_id")) or "a later run"
    return f"Later successful run {latest_id} completed at {latest_at or 'a later time'}; " "this failed run is historical."


def _apply_superseded_run_overrides(
    run_rows: list[dict[str, Any]],
    *,
    latest_run_by_key: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in run_rows:
        latest_run = latest_run_by_key.get(str(row.get("job_key") or ""))
        if _run_is_superseded_by_later_success(row, latest_run):
            rows.append(
                {
                    **row,
                    "operator_status": "healthy",
                    "operator_note": _superseded_run_note(latest_run or {}),
                    "superseded_by_job_run_id": None if latest_run is None else latest_run.get("job_run_id"),
                }
            )
        else:
            rows.append(row)
    return rows


def _job_run_is_actionable_failure(row: Mapping[str, Any]) -> bool:
    if str(row.get("status") or "") != "failed":
        return False
    return str(row.get("operator_status") or "") not in {"healthy", "idle"}


def _skip_reason_text(run: Mapping[str, Any]) -> str | None:
    result = run.get("result") if isinstance(run.get("result"), Mapping) else {}
    return as_text(result.get("reason"))


def _skip_is_benign(run: Mapping[str, Any]) -> bool:
    reason = str(_skip_reason_text(run) or "").strip().lower()
    if str(run.get("job_type") or "") == "trading_strategy_manage" and reason in TRADING_STRATEGY_MANAGE_BROKER_SYNC_SKIP_REASONS:
        return True
    if reason == "singleton_lease_unavailable":
        return True
    if reason == "outside_schedule_window":
        return True
    if reason == "superseded_by_newer_scheduled_run":
        return True
    error_text = str(as_text(run.get("error_text")) or "").strip().lower()
    return error_text in {
        "superseded during queue consolidation",
        "superseded by a newer live slot under Temporal schedule coalescing.",
        "superseded by a newer scheduled run.",
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


def _filter_excluded_job_runs(
    rows: list[dict[str, Any]],
    *,
    excluded_job_types: set[str],
) -> list[dict[str, Any]]:
    excluded = set(excluded_job_types).union(RETIRED_LIFECYCLE_JOB_TYPES)
    return [dict(row) for row in rows if str(row.get("job_type") or "").strip() not in excluded]


def _job_run_operator_status(
    run: Mapping[str, Any],
    *,
    now: datetime,
) -> tuple[str, str | None]:
    status = str(run.get("status") or "unknown").strip().lower()
    if status == "failed":
        error_text = as_text(run.get("error_text"))
        return "blocked", error_text or "Job run failed."
    if status == "skipped":
        result = run.get("result") if isinstance(run.get("result"), Mapping) else {}
        reason = as_text(result.get("reason"))
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
            if reason == "superseded_by_newer_scheduled_run":
                return (
                    "healthy",
                    "Older scheduled job run was superseded by a newer run for the same job key.",
                )
            if str(run.get("job_type") or "") == "trading_strategy_manage" and reason in TRADING_STRATEGY_MANAGE_BROKER_SYNC_SKIP_REASONS:
                return (
                    "healthy",
                    "Strategy manage skipped until broker sync is current.",
                )
            return "healthy", "Job run was superseded during queue consolidation."
        return "degraded", reason or "Job run was skipped."
    if status == "queued":
        queue_age_seconds = _seconds_since(run.get("scheduled_for"), now=now)
        if queue_age_seconds is not None and queue_age_seconds > JOB_RUN_QUEUE_STALE_AFTER_SECONDS:
            return "degraded", "Queued job run is older than 15 minutes."
        return "healthy", None
    if status == "running":
        payload = run.get("payload") if isinstance(run.get("payload"), Mapping) else {}
        interval_seconds = coerce_int(payload.get("interval_seconds")) or 0
        stale_after_seconds = max(
            interval_seconds * 2,
            JOB_RUN_HEARTBEAT_STALE_AFTER_SECONDS,
        )
        heartbeat_age_seconds = _seconds_since(
            run.get("heartbeat_at") or run.get("started_at") or run.get("scheduled_for"),
            now=now,
        )
        if heartbeat_age_seconds is None or heartbeat_age_seconds > stale_after_seconds:
            return "degraded", "Running job heartbeat is stale."
        return "healthy", None
    if status == "succeeded":
        return "healthy", None
    return "unknown", None


def _definition_is_currently_schedulable(
    definition: Mapping[str, Any],
    *,
    now: datetime,
) -> bool:
    if not bool(definition.get("enabled")):
        return False
    try:
        return resolve_scheduled_for(definition, now=now) is not None
    except (TypeError, ValueError):
        return True


def _summarize_job_run(
    run: Mapping[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    enriched = dict(run)
    operator_status, operator_note = _job_run_operator_status(enriched, now=now)
    quote_capture = enriched.get("quote_capture") if isinstance(enriched.get("quote_capture"), Mapping) else {}
    trade_capture = enriched.get("trade_capture") if isinstance(enriched.get("trade_capture"), Mapping) else {}
    payload = enriched.get("payload") if isinstance(enriched.get("payload"), Mapping) else {}
    result = enriched.get("result") if isinstance(enriched.get("result"), Mapping) else {}
    stream_quote_ticks_saved = _stream_quote_ticks_saved(quote_capture)
    stream_trade_ticks_saved = _stream_trade_ticks_saved(trade_capture)
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
        "retry_count": coerce_int(enriched.get("retry_count")) or 0,
        "worker_name": enriched.get("worker_name"),
        "orchestration_id": enriched.get("orchestration_id"),
        "error_text": enriched.get("error_text"),
        "capture_status": enriched.get("capture_status"),
        "singleton_scope": payload.get("singleton_scope"),
        "result_status": result.get("status"),
        "result_reason": result.get("reason"),
        "stream_quote_ticks_saved": stream_quote_ticks_saved,
        "websocket_quote_ticks_saved": stream_quote_ticks_saved,
        "baseline_quote_ticks_saved": coerce_int(quote_capture.get("baseline_quote_ticks_saved")) or 0,
        "recovery_quote_ticks_saved": coerce_int(quote_capture.get("recovery_quote_ticks_saved")) or 0,
        "total_trade_ticks_saved": coerce_int(trade_capture.get("total_trade_ticks_saved")) or 0,
        "stream_trade_ticks_saved": stream_trade_ticks_saved,
        "websocket_trade_ticks_saved": stream_trade_ticks_saved,
    }


def _job_definition_status(
    definition: Mapping[str, Any],
    latest_run: Mapping[str, Any] | None,
    *,
    now: datetime,
) -> str:
    if not bool(definition.get("enabled")):
        return "idle"
    currently_schedulable = _definition_is_currently_schedulable(
        definition,
        now=now,
    )
    if latest_run is None:
        return "unknown" if currently_schedulable else "idle"
    latest_status, _ = _job_run_operator_status(latest_run, now=now)
    if not currently_schedulable and str(latest_run.get("status") or "") in {
        "failed",
        "skipped",
        "succeeded",
    }:
        return "idle"
    return _combine_statuses("healthy", latest_status)


def _summarize_job_definition(
    definition: Mapping[str, Any],
    latest_run: Mapping[str, Any] | None,
    *,
    now: datetime,
) -> dict[str, Any]:
    enriched_latest_run = None if latest_run is None else dict(latest_run)
    latest_summary = None if enriched_latest_run is None else _summarize_job_run(enriched_latest_run, now=now)
    session_schedule: dict[str, Any] = {}
    return {
        "job_key": definition.get("job_key"),
        "job_type": definition.get("job_type"),
        "enabled": bool(definition.get("enabled")),
        "schedule_type": definition.get("schedule_type"),
        "schedule": dict(definition.get("schedule") or {}),
        "session_schedule": session_schedule,
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
        "latest_run_operator_status": None if latest_summary is None else latest_summary.get("operator_status"),
        "latest_run_at": None if latest_summary is None else latest_summary.get("activity_at"),
        "latest_capture_status": None if latest_summary is None else latest_summary.get("capture_status"),
        "schedule_state": session_schedule.get("state"),
        "expected_slot_at": session_schedule.get("expected_current_slot_at"),
        "schedule_note": None,
    }


def _job_key_is_adhoc(job_key: Any) -> bool:
    return str(job_key or "").strip().endswith(":adhoc")


def _task_queue_rows(
    *,
    queued_jobs: list[dict[str, Any]],
    running_jobs: list[dict[str, Any]],
    definitions: list[dict[str, Any]],
    excluded_job_types: set[str] | None = None,
) -> list[dict[str, Any]]:
    excluded = {str(value).strip() for value in (excluded_job_types or set()) if str(value).strip()}
    enabled_job_types = {
        str(row.get("job_type") or "").strip() for row in definitions if bool(row.get("enabled")) and str(row.get("job_type") or "").strip()
    }
    queued_by_task_queue = Counter(get_task_queue_name_for_job_type(str(row.get("job_type") or "unknown")) or "unknown" for row in queued_jobs)
    running_by_task_queue = Counter(get_task_queue_name_for_job_type(str(row.get("job_type") or "unknown")) or "unknown" for row in running_jobs)

    rows: list[dict[str, Any]] = []
    for lane in TEMPORAL_TASK_QUEUES:
        task_names = [task_name for task_name in lane.task_names if _JOB_TYPE_BY_TASK_NAME.get(task_name, "") not in excluded]
        if not task_names:
            continue
        enabled_task_names = [task_name for task_name in task_names if _JOB_TYPE_BY_TASK_NAME.get(task_name, "") in enabled_job_types]
        queued_job_count = int(queued_by_task_queue.get(str(lane.task_queue_name), 0))
        running_job_count = int(running_by_task_queue.get(str(lane.task_queue_name), 0))
        if not enabled_task_names:
            status = "degraded" if queued_job_count > 0 or running_job_count > 0 else "idle"
        else:
            status = "healthy"
        rows.append(
            {
                "lane": lane.lane_name,
                "task_queue": lane.task_queue_name,
                "task_names": task_names,
                "task_count": len(task_names),
                "enabled_task_names": enabled_task_names,
                "enabled_task_count": len(enabled_task_names),
                "max_jobs": lane.max_jobs,
                "queued_job_count": queued_job_count,
                "running_job_count": running_job_count,
                "status": status,
            }
        )
    return rows


def _disabled_task_queue_rows(
    *,
    excluded_job_types: set[str],
) -> list[dict[str, Any]]:
    excluded = {str(value).strip() for value in excluded_job_types if str(value).strip()}
    rows: list[dict[str, Any]] = []
    for lane in TEMPORAL_TASK_QUEUES:
        disabled_task_names = []
        disabled_job_types = []
        active_task_names = []
        for task_name in lane.task_names:
            job_type = _JOB_TYPE_BY_TASK_NAME.get(task_name, "")
            if job_type in excluded:
                disabled_task_names.append(task_name)
                disabled_job_types.append(job_type)
            else:
                active_task_names.append(task_name)
        if not disabled_task_names or active_task_names:
            continue
        rows.append(
            {
                "lane": lane.lane_name,
                "task_queue": lane.task_queue_name,
                "task_names": disabled_task_names,
                "task_count": len(disabled_task_names),
                "enabled_task_names": [],
                "enabled_task_count": 0,
                "disabled_job_types": disabled_job_types,
                "disabled_job_type_count": len(disabled_job_types),
                "queued_job_count": 0,
                "running_job_count": 0,
                "status": "disabled",
                "operator_note": "Task queue disabled by deploy target excluded_job_types.",
            }
        )
    return rows


def _split_active_queued_jobs(
    queued_jobs: list[dict[str, Any]],
    *,
    now: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    active: list[dict[str, Any]] = []
    stale: list[dict[str, Any]] = []
    for row in queued_jobs:
        operator_status, _ = _job_run_operator_status(row, now=now)
        if operator_status == "healthy":
            active.append(dict(row))
        else:
            stale.append(dict(row))
    return active, stale


def _project_job_run_health(
    *,
    counted_runs: list[dict[str, Any]],
    running_health_rows: list[dict[str, Any]],
    stale_queued_rows: list[dict[str, Any]],
) -> _JobRunHealthProjection:
    return _JobRunHealthProjection(
        status_counts=Counter(str(row.get("status") or "unknown") for row in counted_runs),
        operator_status_counts=Counter(str(row.get("operator_status") or "unknown") for row in counted_runs),
        job_type_counts=Counter(str(row.get("job_type") or "unknown") for row in counted_runs),
        stale_running_count=sum(
            1 for row in running_health_rows if str(row.get("status") or "") == "running" and str(row.get("operator_status") or "") != "healthy"
        ),
        actionable_failed_count=sum(1 for row in counted_runs if _job_run_is_actionable_failure(row)),
        historical_failed_count=sum(
            1 for row in counted_runs if str(row.get("status") or "") == "failed" and str(row.get("operator_status") or "") in {"healthy", "idle"}
        ),
        actionable_skipped_count=sum(
            1 for row in counted_runs if str(row.get("status") or "") == "skipped" and str(row.get("operator_status") or "") != "healthy"
        ),
        stale_queued_job_count=len(stale_queued_rows),
    )


def _job_run_health_attention(health: _JobRunHealthProjection) -> list[dict[str, str]]:
    attention: list[dict[str, str]] = []
    if health.actionable_failed_count:
        attention.append(
            _attention(
                severity="high",
                code="failed_job_runs_present",
                message=f"{health.actionable_failed_count} recent job run(s) failed.",
            )
        )
    if health.actionable_skipped_count:
        attention.append(
            _attention(
                severity="medium",
                code="skipped_job_runs_present",
                message=f"{health.actionable_skipped_count} recent job run(s) were skipped.",
            )
        )
    if health.stale_running_count:
        attention.append(
            _attention(
                severity="medium",
                code="stale_running_jobs",
                message=f"{health.stale_running_count} running job run(s) have stale heartbeats.",
            )
        )
    if health.stale_queued_job_count:
        attention.append(
            _attention(
                severity="low",
                code="stale_queued_jobs_present",
                message=f"{health.stale_queued_job_count} queued job run(s) are stale and no longer count as active backlog.",
            )
        )
    return attention


def _project_worker_runtime(
    *,
    job_store: Any,
    definitions: list[dict[str, Any]],
    queued_jobs: list[dict[str, Any]],
    running_jobs: list[dict[str, Any]],
    excluded_job_types: set[str],
    now: datetime,
    include_singletons: bool,
    include_blocked_task_queue_status: bool,
) -> _WorkerRuntimeProjection:
    attention: list[dict[str, str]] = []
    statuses: list[str] = []
    schedules_payload = _temporal_schedules_payload(definitions)
    task_queue_rows = _task_queue_rows(
        queued_jobs=queued_jobs,
        running_jobs=running_jobs,
        definitions=definitions,
        excluded_job_types=excluded_job_types,
    )
    blocked_task_queue_count = sum(1 for row in task_queue_rows if str(row.get("status") or "") == "blocked")

    statuses.append(str(schedules_payload.get("status") or "unknown"))

    if blocked_task_queue_count:
        if include_blocked_task_queue_status:
            statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="temporal_task_queues_blocked",
                message=f"{blocked_task_queue_count} Temporal task queue(s) are blocked.",
            )
        )

    singleton_leases: list[dict[str, Any]] = []
    stale_singleton_leases: list[dict[str, Any]] = []
    if include_singletons:
        singleton_leases = [dict(row) for row in job_store.list_active_leases(prefix=SINGLETON_LEASE_PREFIX)]
        for lease in singleton_leases:
            lease_run_id = as_text(lease.get("job_run_id"))
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

    return _WorkerRuntimeProjection(
        schedules_payload=schedules_payload,
        task_queue_rows=task_queue_rows,
        singleton_leases=singleton_leases,
        stale_singleton_leases=stale_singleton_leases,
        blocked_task_queue_count=blocked_task_queue_count,
        statuses=tuple(statuses),
        attention=attention,
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


@with_storage()
def build_job_task_queues_overview(
    *,
    db_target: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    payload = build_jobs_overview(db_target=db_target, storage=storage)
    details = dict(payload.get("details") or {})
    task_queue_rows = list(details.get("task_queues") or [])
    disabled_task_queue_rows = list(details.get("disabled_task_queues") or [])
    summary = dict(payload.get("summary") or {})
    return {
        "status": payload.get("status"),
        "generated_at": payload.get("generated_at"),
        "summary": {
            "view": "task_queues",
            "task_queue_count": len(task_queue_rows),
            "disabled_task_queue_count": len(disabled_task_queue_rows),
            "running_job_count": sum(int(row.get("running_job_count") or 0) for row in task_queue_rows),
            "queued_job_count": sum(int(row.get("queued_job_count") or 0) for row in task_queue_rows),
            "singleton_lease_count": summary.get("singleton_lease_count"),
        },
        "attention": list(payload.get("attention") or []),
        "details": {
            "view": "task_queues",
            "schedules": details.get("schedules"),
            "task_queues": task_queue_rows,
            "disabled_task_queues": disabled_task_queue_rows,
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
                        f"Singleton lease for {run.get('job_type')}:{singleton_scope} is currently held by " f"{singleton_lease.get('job_run_id')}."
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
