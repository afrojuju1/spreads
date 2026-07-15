from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from core.db.decorators import with_storage
from core.jobs.orchestration import expected_routine_slots, resolve_scheduled_for
from core.jobs.registry import (
    ROUTINE_SCHEDULE_RECONCILE_JOB_TYPE,
    WORKFLOW_LANES,
    get_workflow_lane_for_job_type,
)
from core.jobs.specs import (
    disabled_workflow_lanes,
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

from core.services.ops.shared import (
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
from core.services.ops.trading.broker import broker_sync_payload as _broker_sync_payload
from core.services.ops.trading.market import market_session_context as _market_session_context
from core.workflow_runtime.diagnostics import get_workflow_runtime_diagnostics
from core.workflow_runtime.routine_schedules import routine_config_hash

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
class _WorkflowRuntimeProjection:
    routine_schedules_payload: dict[str, Any]
    workflow_lane_rows: list[dict[str, Any]]
    execution_health_payload: dict[str, Any]
    blocked_workflow_lane_count: int
    due_routines_missing: list[dict[str, Any]]
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


def _routine_schedules_payload(
    *,
    job_store: Any,
    definitions: list[dict[str, Any]],
    now: datetime,
) -> dict[str, Any]:
    scheduled_definitions = [row for row in definitions if str(row.get("schedule_type") or "manual") != "manual"]
    enabled_count = sum(1 for row in scheduled_definitions if bool(row.get("enabled")))
    declared_count = len(scheduled_definitions)
    latest_runs = job_store.list_job_runs(job_type=ROUTINE_SCHEDULE_RECONCILE_JOB_TYPE, limit=1)
    latest = None if not latest_runs else dict(latest_runs[0])
    result = latest.get("result") if isinstance(latest, Mapping) and isinstance(latest.get("result"), Mapping) else {}
    expected_hash = routine_config_hash(definitions)
    observed_hash = as_text(result.get("config_hash"))
    age_seconds = None if latest is None else _seconds_since(_activity_at(latest), now=now)
    hash_matches = observed_hash == expected_hash
    if enabled_count == 0:
        status = "idle"
    elif latest is None or not hash_matches:
        status = "blocked"
    elif age_seconds is None or age_seconds > 86400:
        status = "degraded"
    else:
        status = "healthy"
    return {
        "status": status,
        "kind": "routine_schedules",
        "declared_schedule_count": declared_count,
        "enabled_schedule_count": enabled_count,
        "disabled_schedule_count": max(declared_count - enabled_count, 0),
        "reconcile_command": "spreads runtime routine-schedules",
        "last_reconciled_at": None if latest is None else _activity_at(latest),
        "reconciliation_age_seconds": age_seconds,
        "config_hash": expected_hash,
        "reconciled_config_hash": observed_hash,
        "config_hash_matches": hash_matches,
        "note": "Routine schedules are reconciled from declared routine definitions.",
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
    return f"Broker sync state recovered at {updated_at}; this failed scheduled run is historical."


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
    return f"Later successful run {latest_id} completed at {latest_at or 'a later time'}; this failed run is historical."


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
    if reason == "outside_schedule_window":
        return True
    if reason == "superseded_by_newer_scheduled_run":
        return True
    error_text = str(as_text(run.get("error_text")) or "").strip().lower()
    return error_text in {
        "superseded during queue consolidation",
        "superseded by a newer live slot under routine schedule coalescing.",
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
    slots = expected_routine_slots(definition, now=now)
    session_schedule: dict[str, Any] = {
        "previous_expected_slot_at": None if slots["previous"] is None else utc_iso(slots["previous"]),
        "next_expected_slot_at": None if slots["next"] is None else utc_iso(slots["next"]),
    }
    return {
        "job_key": definition.get("job_key"),
        "job_type": definition.get("job_type"),
        "enabled": bool(definition.get("enabled")),
        "schedule_type": definition.get("schedule_type"),
        "schedule": dict(definition.get("schedule") or {}),
        "session_schedule": session_schedule,
        "market_calendar": definition.get("market_calendar"),
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
        "expected_slot_at": session_schedule.get("previous_expected_slot_at"),
        "next_expected_slot_at": session_schedule.get("next_expected_slot_at"),
        "schedule_note": None,
    }


def _job_key_is_adhoc(job_key: Any) -> bool:
    return str(job_key or "").strip().endswith(":adhoc")


def _workflow_lane_rows(
    *,
    queued_jobs: list[dict[str, Any]],
    running_jobs: list[dict[str, Any]],
    definitions: list[dict[str, Any]],
    diagnostics: Mapping[str, Any],
    disabled_lanes: set[str] | None = None,
) -> list[dict[str, Any]]:
    disabled = {str(value).strip() for value in (disabled_lanes or set()) if str(value).strip()}
    enabled_job_types = {
        str(row.get("job_type") or "").strip() for row in definitions if bool(row.get("enabled")) and str(row.get("job_type") or "").strip()
    }
    queued_by_lane = Counter(get_workflow_lane_for_job_type(str(row.get("job_type") or "unknown")) or "unknown" for row in queued_jobs)
    running_by_lane = Counter(get_workflow_lane_for_job_type(str(row.get("job_type") or "unknown")) or "unknown" for row in running_jobs)
    diagnostics_by_lane = {str(row.get("lane")): row for row in diagnostics.get("lanes") or [] if isinstance(row, Mapping)}

    rows: list[dict[str, Any]] = []
    for spec in WORKFLOW_LANES:
        diagnostic = diagnostics_by_lane.get(spec.lane, {})
        job_types = list(spec.job_types)
        enabled_types = [job_type for job_type in job_types if job_type in enabled_job_types]
        queued_job_count = int(queued_by_lane.get(spec.lane, 0))
        running_job_count = int(running_by_lane.get(spec.lane, 0))
        lane_disabled = spec.lane in disabled or not bool(diagnostic.get("enabled", True))
        if lane_disabled:
            status = "disabled"
        elif spec.optional and not enabled_types:
            status = "idle"
        elif int(diagnostic.get("poller_count") or 0) == 0:
            status = "blocked"
        else:
            status = "healthy"
        rows.append(
            {
                "lane": spec.lane,
                "required_for_trading": spec.required_for_trading,
                "required_for_deploy": spec.required_for_deploy,
                "optional": spec.optional,
                "routine_types": job_types,
                "routine_type_count": len(job_types),
                "enabled_routine_types": enabled_types,
                "enabled_routine_type_count": len(enabled_types),
                "max_concurrency": spec.max_concurrency,
                "queued_job_count": queued_job_count,
                "running_job_count": running_job_count,
                "poller_count": int(diagnostic.get("poller_count") or 0),
                "pollers": list(diagnostic.get("pollers") or []),
                **({} if not isinstance(diagnostic.get("provider"), Mapping) else {"provider": dict(diagnostic["provider"])}),
                "status": status,
            }
        )
    return rows


def _workflow_runtime_targets(
    *,
    execution_store: Any,
    queued_jobs: list[dict[str, Any]],
    running_jobs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    if execution_store is not None and execution_store.intent_schema_ready():
        for intent in execution_store.list_execution_intents(states=["claimed"], limit=200):
            row = dict(intent)
            payload = row.get("payload") if isinstance(row.get("payload"), Mapping) else {}
            targets.append(
                {
                    "kind": "execution_intent",
                    "correlation_id": row.get("execution_intent_id"),
                    "status": row.get("state"),
                    "workflow_id": payload.get("workflow_id"),
                    "workflow_run_id": payload.get("workflow_run_id"),
                    "projected_at": parse_datetime(row.get("updated_at") or row.get("created_at")),
                }
            )
    for row in [*queued_jobs, *running_jobs]:
        targets.append(
            {
                "kind": "job_run",
                "correlation_id": row.get("job_run_id"),
                "status": row.get("status"),
                "workflow_run_id": row.get("orchestration_id"),
                "projected_at": parse_datetime(_activity_at(row)),
            }
        )
    return targets


def _disabled_workflow_lane_rows(
    *,
    disabled_lanes: set[str],
) -> list[dict[str, Any]]:
    disabled = {str(value).strip() for value in disabled_lanes if str(value).strip()}
    return [
            {
                "lane": spec.lane,
                "required_for_trading": spec.required_for_trading,
                "required_for_deploy": spec.required_for_deploy,
                "optional": spec.optional,
                "routine_types": list(spec.job_types),
                "routine_type_count": len(spec.job_types),
                "queued_job_count": 0,
                "running_job_count": 0,
                "poller_count": 0,
                "status": "disabled",
                "operator_note": "Workflow lane is explicitly disabled by deploy policy.",
            }
            for spec in WORKFLOW_LANES
            if spec.lane in disabled
        ]


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


def _project_workflow_runtime(
    *,
    job_store: Any,
    execution_store: Any,
    definitions: list[dict[str, Any]],
    routine_schedule_definitions: list[dict[str, Any]],
    queued_jobs: list[dict[str, Any]],
    correlated_queued_jobs: list[dict[str, Any]],
    running_jobs: list[dict[str, Any]],
    disabled_lanes: set[str],
    now: datetime,
    include_blocked_workflow_lane_status: bool,
) -> _WorkflowRuntimeProjection:
    attention: list[dict[str, str]] = []
    statuses: list[str] = []
    schedules_payload = _routine_schedules_payload(
        job_store=job_store,
        definitions=routine_schedule_definitions,
        now=now,
    )
    diagnostics = get_workflow_runtime_diagnostics(
        targets=_workflow_runtime_targets(
            execution_store=execution_store,
            queued_jobs=correlated_queued_jobs,
            running_jobs=running_jobs,
        )
    )
    execution_health_payload = dict(diagnostics.get("executions") or {})
    workflow_lane_rows = _workflow_lane_rows(
        queued_jobs=queued_jobs,
        running_jobs=running_jobs,
        definitions=definitions,
        diagnostics=diagnostics,
        disabled_lanes=disabled_lanes,
    )
    blocked_workflow_lane_count = sum(1 for row in workflow_lane_rows if str(row.get("status") or "") == "blocked")

    enabled_definitions = [row for row in definitions if bool(row.get("enabled"))]
    latest_by_key = {
        str(row["job_key"]): dict(row)
        for row in job_store.list_latest_runs_by_job_keys(
            job_keys=[str(row["job_key"]) for row in enabled_definitions],
            statuses=None,
        )
    }
    due_routines_missing: list[dict[str, Any]] = []
    for definition in enabled_definitions:
        previous_slot = expected_routine_slots(definition, now=now)["previous"]
        if previous_slot is None or now < previous_slot + timedelta(minutes=2):
            continue
        latest = latest_by_key.get(str(definition["job_key"]))
        latest_at = None if latest is None else parse_datetime(_activity_at(latest))
        if latest_at is None or latest_at < previous_slot:
            due_routines_missing.append(
                {
                    "job_key": definition["job_key"],
                    "job_type": definition["job_type"],
                    "expected_slot_at": utc_iso(previous_slot),
                    "latest_activity_at": None if latest_at is None else utc_iso(latest_at),
                }
            )

    statuses.append(str(schedules_payload.get("status") or "unknown"))
    execution_health_status = str(execution_health_payload.get("status") or "unknown")
    statuses.append(execution_health_status)

    if execution_health_status in {"blocked", "degraded"}:
        attention.append(
            _attention(
                severity="high" if execution_health_status == "blocked" else "medium",
                code="workflow_executions_unhealthy",
                message=(
                    f"{len(execution_health_payload.get('issues') or [])} Temporal workflow execution issue(s) need attention."
                ),
            )
        )

    if blocked_workflow_lane_count:
        if include_blocked_workflow_lane_status:
            statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="workflow_lanes_blocked",
                message=f"{blocked_workflow_lane_count} enabled workflow lane(s) have no live poller.",
            )
        )

    if due_routines_missing:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="high",
                code="due_routines_missing",
                message=f"{len(due_routines_missing)} enabled routine(s) have no activity for their latest expected slot.",
            )
        )

    return _WorkflowRuntimeProjection(
        routine_schedules_payload=schedules_payload,
        workflow_lane_rows=workflow_lane_rows,
        execution_health_payload=execution_health_payload,
        blocked_workflow_lane_count=blocked_workflow_lane_count,
        due_routines_missing=due_routines_missing,
        statuses=tuple(statuses),
        attention=attention,
    )


__all__ = [
    "Any",
    "BROKER_SYNC_KEY",
    "Counter",
    "Mapping",
    "OpsLookupError",
    "RETIRED_LIFECYCLE_JOB_TYPES",
    "TRADING_STRATEGY_MANAGE_BROKER_SYNC_SKIP_REASONS",
    "_activity_at",
    "_apply_broker_sync_definition_overrides",
    "_apply_broker_sync_run_overrides",
    "_apply_superseded_run_overrides",
    "_attention",
    "_broker_sync_payload",
    "_combine_statuses",
    "_definition_requires_attention",
    "_disabled_workflow_lane_rows",
    "_filter_excluded_job_runs",
    "_job_key_is_adhoc",
    "_job_run_health_attention",
    "_market_session_context",
    "_project_job_run_health",
    "_project_workflow_runtime",
    "_sorted_by_activity",
    "_split_active_queued_jobs",
    "_summarize_job_definition",
    "_summarize_job_run",
    "as_text",
    "disabled_workflow_lanes",
    "get_declared_job_row",
    "list_declared_job_rows",
    "utc_iso",
    "utc_now",
    "utc_now_iso",
    "with_storage",
]
