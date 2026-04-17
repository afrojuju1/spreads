from __future__ import annotations

import asyncio
import json
import os
import socket
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import redis.asyncio as redis_async
from arq import create_pool

from core.events.bus import publish_global_event_async
from core.jobs.live_collector import (
    LiveTickContext,
    build_collection_args,
    run_collection_tick,
)
from core.jobs.registry import (
    ALERT_DELIVERY_JOB_TYPE,
    ALERT_RECONCILE_JOB_TYPE,
    BROKER_SYNC_JOB_TYPE,
    COLLECTOR_RECOVERY_JOB_KEY,
    COLLECTOR_RECOVERY_JOB_TYPE,
    DISCOVERY_QUEUE_NAME,
    EXECUTION_SUBMIT_JOB_TYPE,
    LIVE_COLLECTOR_JOB_TYPE,
    OPTIONS_AUTOMATION_EXECUTE_JOB_TYPE,
    OPTIONS_AUTOMATION_ENTRY_JOB_TYPE,
    OPTIONS_AUTOMATION_MANAGEMENT_JOB_TYPE,
    POSITION_EXIT_MANAGER_JOB_TYPE,
    POST_CLOSE_ANALYSIS_JOB_TYPE,
    POST_MARKET_ANALYSIS_JOB_TYPE,
    RUNTIME_QUEUE_NAME,
    get_job_spec,
)
from core.jobs.orchestration import (
    singleton_lease_key,
    worker_runtime_lease_key,
)
from core.runtime.config import default_database_url, default_redis_url
from core.runtime.redis import build_redis_settings
from core.services.analysis import (
    build_analysis_args,
    run_post_close_analysis,
)
from core.services.alert_delivery import (
    ALERT_DELIVERY_STALE_SECONDS,
    reconcile_alert_delivery,
    run_alert_delivery,
)
from core.services.broker_sync import run_broker_sync
from core.services.decision_engine import run_entry_automation_decision
from core.services.exit_manager import run_position_exit_manager
from core.services.execution import run_execution_submit
from core.services.execution_intents import dispatch_pending_execution_intents
from core.services.live_collector_health import enrich_live_collector_job_run_payload
from core.services.market_dates import resolve_market_date
from core.services.live_recovery import (
    LIVE_SLOT_STATUS_MISSED,
    LIVE_SLOT_STATUS_RUNNING,
    LIVE_SLOT_STATUS_SUCCEEDED,
    build_slot_details_from_cycle_result,
    run_collector_recovery,
)
from core.services.live_pipelines import (
    build_live_session_catalog,
    build_live_run_scope_id,
)
from core.services.post_market_analysis import parse_args as parse_post_market_args
from core.services.post_market_analysis import run_post_market_analysis
from core.services.strategy_positions import run_management_automation_decision
from core.storage.factory import (
    build_job_repository,
    build_post_market_repository,
    build_storage_context,
)

WORKER_HEARTBEAT_SECONDS = 30
WORKER_LEASE_TTL_SECONDS = 90
JOB_LEASE_TTL_SECONDS = 600
LIVE_COLLECTOR_STREAM_STALL_THRESHOLD = 2
LIVE_COLLECTOR_SLOT_LAG_THRESHOLD = 2


def worker_name() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


class ManagedJobFailure(RuntimeError):
    def __init__(self, message: str, *, result: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.result = result


class SupersededJobRun(RuntimeError):
    pass


def _compact_single_analysis_result(
    result: dict[str, Any],
    *,
    include_report: bool = False,
) -> dict[str, Any]:
    summary = result["summary"]
    outcomes = summary["outcomes"]
    payload = {
        "session_date": result["session_date"],
        "label": result["label"],
        "cycle_count": summary["cycle_count"],
        "idea_count": outcomes["idea_count"],
        "counts_by_selection_state": outcomes["counts_by_selection_state"],
        "run_count": summary["run_overview"]["run_count"],
        "quote_event_count": summary["quote_overview"]["quote_event_count"],
        "event_count": summary["event_overview"]["event_count"],
    }
    if include_report:
        payload["report"] = result.get("report")
    return payload


def compact_analysis_result(
    result: dict[str, Any],
    *,
    include_report: bool = False,
) -> dict[str, Any]:
    if result.get("mode") == "planner":
        return {
            "mode": "planner",
            "session_date": result["session_date"],
            "expected_labels": list(result.get("expected_labels") or []),
            "realized_labels": list(result.get("realized_labels") or []),
            "runs": [
                _compact_single_analysis_result(item, include_report=include_report)
                for item in result.get("runs", [])
            ],
            "skipped_labels": [dict(item) for item in result.get("skipped_labels", [])],
            "failed_labels": [dict(item) for item in result.get("failed_labels", [])],
        }
    return _compact_single_analysis_result(result, include_report=include_report)


def _compact_single_post_market_result(result: dict[str, Any]) -> dict[str, Any]:
    diagnostics = result["diagnostics"]
    selection_state_performance = diagnostics["selection_state_performance"]
    return {
        "analysis_run_id": result["analysis_run_id"],
        "session_date": result["session_date"],
        "label": result["label"],
        "status": result["status"],
        "overall_verdict": diagnostics["overall_verdict"],
        "strength_count": len(diagnostics["strengths"]),
        "problem_count": len(diagnostics["problems"]),
        "recommendation_count": len(result["recommendations"]),
        "promotable_count": selection_state_performance["promotable"]["count"],
        "monitor_count": selection_state_performance["monitor"]["count"],
    }


def compact_post_market_result(result: dict[str, Any]) -> dict[str, Any]:
    if result.get("mode") == "planner":
        return {
            "mode": "planner",
            "session_date": result["session_date"],
            "expected_labels": list(result.get("expected_labels") or []),
            "realized_labels": list(result.get("realized_labels") or []),
            "runs": [
                _compact_single_post_market_result(item)
                for item in result.get("runs", [])
            ],
            "skipped_labels": [dict(item) for item in result.get("skipped_labels", [])],
            "failed_labels": [dict(item) for item in result.get("failed_labels", [])],
        }
    return _compact_single_post_market_result(result)


async def _heartbeat_runtime(job_store: Any, runtime_owner: str) -> None:
    state = {
        "kind": "worker",
        "lane": str(getattr(job_store, "_worker_lane", "") or "unknown"),
        "settings_name": str(
            getattr(job_store, "_worker_settings_name", "") or "unknown"
        ),
        "queue_name": str(getattr(job_store, "_worker_queue_name", "") or "unknown"),
    }
    while True:
        await asyncio.to_thread(
            job_store.acquire_lease,
            lease_key=worker_runtime_lease_key(runtime_owner),
            owner=runtime_owner,
            expires_in_seconds=WORKER_LEASE_TTL_SECONDS,
            state=state,
        )
        await asyncio.sleep(WORKER_HEARTBEAT_SECONDS)


async def startup(ctx: dict[str, Any]) -> None:
    ctx["database_url"] = default_database_url()
    ctx["redis_url"] = default_redis_url()
    ctx["worker_name"] = worker_name()
    ctx["storage"] = build_storage_context(ctx["database_url"])
    ctx["job_store"] = build_job_repository(context=ctx["storage"])
    setattr(ctx["job_store"], "_worker_lane", ctx.get("worker_lane", "unknown"))
    setattr(
        ctx["job_store"],
        "_worker_settings_name",
        ctx.get("worker_settings_name", "unknown"),
    )
    setattr(
        ctx["job_store"],
        "_worker_queue_name",
        ctx.get("worker_queue_name", "unknown"),
    )
    ctx["event_bus"] = redis_async.from_url(ctx["redis_url"], decode_responses=True)
    ctx["runtime_heartbeat_task"] = asyncio.create_task(
        _heartbeat_runtime(ctx["job_store"], ctx["worker_name"])
    )


async def _enqueue_startup_collector_recovery(ctx: dict[str, Any]) -> None:
    job_store = ctx["job_store"]
    definition = await asyncio.to_thread(
        job_store.get_job_definition,
        COLLECTOR_RECOVERY_JOB_KEY,
    )
    if definition is None or not bool(definition.get("enabled")):
        return
    latest_runs = await asyncio.to_thread(
        job_store.list_job_runs,
        job_key=COLLECTOR_RECOVERY_JOB_KEY,
        limit=1,
    )
    latest_run = latest_runs[0] if latest_runs else None
    if latest_run is not None and str(latest_run.get("status") or "") in {
        "queued",
        "running",
    }:
        return
    spec = get_job_spec(COLLECTOR_RECOVERY_JOB_TYPE)
    if spec is None:
        return
    redis = await create_pool(build_redis_settings(ctx["redis_url"]))
    try:
        scheduled_for = datetime.now(UTC)
        job_run_id = f"collector_recovery:start:{uuid4().hex}"
        payload = dict(definition.get("payload") or {})
        payload.update(
            {
                "job_key": COLLECTOR_RECOVERY_JOB_KEY,
                "job_type": COLLECTOR_RECOVERY_JOB_TYPE,
                "scheduled_for": scheduled_for.isoformat().replace("+00:00", "Z"),
                "singleton_scope": definition.get("singleton_scope"),
            }
        )
        run_record, created = await asyncio.to_thread(
            job_store.create_job_run,
            job_run_id=job_run_id,
            job_key=COLLECTOR_RECOVERY_JOB_KEY,
            arq_job_id=job_run_id,
            job_type=COLLECTOR_RECOVERY_JOB_TYPE,
            status="queued",
            scheduled_for=scheduled_for,
            payload=payload,
        )
        if not created:
            return
        await redis.enqueue_job(
            spec.task_name,
            COLLECTOR_RECOVERY_JOB_KEY,
            run_record["job_run_id"],
            run_record["payload"],
            run_record["arq_job_id"],
            _job_id=run_record["arq_job_id"],
            _queue_name=spec.queue_name,
        )
    finally:
        await redis.close()


async def runtime_startup(ctx: dict[str, Any]) -> None:
    ctx["worker_lane"] = "runtime"
    ctx["worker_settings_name"] = "RuntimeWorkerSettings"
    ctx["worker_queue_name"] = RUNTIME_QUEUE_NAME
    await startup(ctx)
    await _enqueue_startup_collector_recovery(ctx)


async def discovery_startup(ctx: dict[str, Any]) -> None:
    ctx["worker_lane"] = "discovery"
    ctx["worker_settings_name"] = "DiscoveryWorkerSettings"
    ctx["worker_queue_name"] = DISCOVERY_QUEUE_NAME
    await startup(ctx)


async def shutdown(ctx: dict[str, Any]) -> None:
    task = ctx.get("runtime_heartbeat_task")
    if task is not None:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    job_store = ctx.get("job_store")
    if job_store is not None:
        await asyncio.to_thread(
            job_store.release_lease,
            worker_runtime_lease_key(ctx["worker_name"]),
            owner=ctx["worker_name"],
        )
        await asyncio.to_thread(job_store.close)
    storage = ctx.get("storage")
    if storage is not None:
        await asyncio.to_thread(storage.close)
    event_bus = ctx.get("event_bus")
    if event_bus is not None:
        await event_bus.aclose()


async def _publish_job_run_event(ctx: dict[str, Any], run_record: Any) -> None:
    if run_record is None:
        return
    event_bus = ctx.get("event_bus")
    if event_bus is None:
        return
    try:
        payload = enrich_live_collector_job_run_payload(run_record)
        await publish_global_event_async(
            event_bus,
            topic="job.run.updated",
            event_class="control_event",
            entity_type="job_run",
            entity_id=run_record["job_run_id"],
            payload=payload,
            timestamp=run_record.get("finished_at")
            or run_record.get("heartbeat_at")
            or run_record["scheduled_for"],
            source="worker",
            session_date=payload.get("session_date")
            if isinstance(payload.get("session_date"), str)
            else None,
            correlation_id=str(run_record["job_key"]),
        )
    except Exception:
        pass


async def _publish_post_market_event(
    ctx: dict[str, Any],
    *,
    analysis_run_id: str,
    payload: dict[str, Any],
    timestamp: str | datetime | None = None,
) -> None:
    event_bus = ctx.get("event_bus")
    if event_bus is None:
        return
    session_id = None
    label = payload.get("label")
    session_date = payload.get("session_date")
    if (
        isinstance(label, str)
        and label
        and isinstance(session_date, str)
        and session_date
    ):
        session_id = build_live_run_scope_id(label, session_date)
    try:
        await publish_global_event_async(
            event_bus,
            topic="post_market.analysis.updated",
            event_class="analytics_event",
            entity_type="post_market_analysis",
            entity_id=analysis_run_id,
            payload={
                **payload,
                **({} if session_id is None else {"session_id": session_id}),
            },
            timestamp=timestamp,
            source="worker",
            session_date=session_date if isinstance(session_date, str) else None,
            correlation_id=session_id,
        )
    except Exception:
        pass


async def _publish_post_market_planner_events(
    ctx: dict[str, Any], result: dict[str, Any]
) -> None:
    for run in result.get("runs", []):
        await _publish_post_market_event(
            ctx,
            analysis_run_id=str(run["analysis_run_id"]),
            payload=run,
            timestamp=datetime.now(UTC),
        )
    for skipped in result.get("skipped_labels", []):
        await _publish_post_market_event(
            ctx,
            analysis_run_id=str(skipped["analysis_run_id"]),
            payload=skipped,
            timestamp=datetime.now(UTC),
        )


def _parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00") if value.endswith("Z") else value
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _run_duration_seconds(run_payload: dict[str, Any]) -> float | None:
    started_at = _parse_utc(run_payload.get("started_at"))
    finished_at = _parse_utc(run_payload.get("finished_at"))
    if started_at is None or finished_at is None:
        return None
    duration = (finished_at - started_at).total_seconds()
    return round(duration, 3) if duration >= 0 else None


def _slot_lag_slots(run_payload: dict[str, Any]) -> int:
    slot_at = _parse_utc(run_payload.get("slot_at"))
    finished_at = _parse_utc(run_payload.get("finished_at"))
    interval_seconds = int(
        (run_payload.get("payload") or {}).get("interval_seconds") or 0
    )
    if slot_at is None or finished_at is None or interval_seconds <= 0:
        return 0
    elapsed_seconds = max((finished_at - slot_at).total_seconds(), 0.0)
    return max(int(elapsed_seconds // interval_seconds) - 1, 0)


def _count_consecutive_stream_zero_slots(
    job_store: Any,
    *,
    job_key: str,
    session_id: str,
) -> int:
    rows = job_store.list_job_runs(
        job_key=job_key,
        status="succeeded",
        session_id=session_id,
        limit=8,
    )
    consecutive = 0
    for row in rows:
        payload = enrich_live_collector_job_run_payload(row)
        quote_capture = payload.get("quote_capture") or {}
        if int(quote_capture.get("expected_quote_symbol_count", 0)) <= 0:
            continue
        if int(quote_capture.get("stream_quote_events_saved", 0)) > 0:
            break
        consecutive += 1
    return consecutive


def _build_live_collector_log_payload(
    run_payload: dict[str, Any],
    *,
    consecutive_stream_zero_slots: int,
    slot_lag_slots: int,
) -> dict[str, Any]:
    result = run_payload.get("result") or {}
    quote_capture = run_payload.get("quote_capture") or {}
    trade_capture = run_payload.get("trade_capture") or {}
    uoa_summary = run_payload.get("uoa_summary") or {}
    uoa_quote_summary = run_payload.get("uoa_quote_summary") or {}
    uoa_decisions = run_payload.get("uoa_decisions") or {}
    cycle_ids = result.get("cycle_ids") or []
    return {
        "event": "live_collector_slot_completed",
        "job_run_id": run_payload["job_run_id"],
        "job_key": run_payload["job_key"],
        "label": result.get("label") or (run_payload.get("payload") or {}).get("label"),
        "session_id": run_payload.get("session_id"),
        "slot_at": run_payload.get("slot_at"),
        "cycle_id": None if not cycle_ids else cycle_ids[0],
        "worker_name": run_payload.get("worker_name"),
        "duration_seconds": _run_duration_seconds(run_payload),
        "promotable_opportunity_count": int(
            result.get("promotable_opportunity_count") or 0
        ),
        "monitor_opportunity_count": int(result.get("monitor_opportunity_count") or 0),
        "quote_capture": quote_capture,
        "trade_capture": trade_capture,
        "uoa_overview": dict(uoa_summary.get("overview") or {}),
        "uoa_quote_overview": dict(uoa_quote_summary.get("overview") or {}),
        "uoa_top_roots": [
            dict(item) for item in (uoa_summary.get("top_roots") or [])[:3]
        ],
        "uoa_top_contracts": [
            dict(item) for item in (uoa_summary.get("top_contracts") or [])[:3]
        ],
        "uoa_decision_overview": dict(uoa_decisions.get("overview") or {}),
        "uoa_promotable_roots": [
            dict(item) for item in (uoa_decisions.get("top_promotable_roots") or [])[:3]
        ],
        "uoa_high_roots": [
            dict(item) for item in (uoa_decisions.get("top_high_roots") or [])[:3]
        ],
        "consecutive_stream_zero_slots": consecutive_stream_zero_slots,
        "slot_lag_slots": slot_lag_slots,
    }


def _build_live_collector_degradation(
    run_payload: dict[str, Any],
    *,
    consecutive_stream_zero_slots: int,
    slot_lag_slots: int,
) -> dict[str, Any] | None:
    quote_capture = run_payload.get("quote_capture") or {}
    expected_quote_symbol_count = int(
        quote_capture.get("expected_quote_symbol_count", 0) or 0
    )
    reasons: list[str] = []
    if (
        expected_quote_symbol_count > 0
        and int(quote_capture.get("total_quote_events_saved", 0)) == 0
    ):
        reasons.append("quote_capture_empty")
    if (
        expected_quote_symbol_count > 0
        and int(quote_capture.get("stream_quote_events_saved", 0)) == 0
        and consecutive_stream_zero_slots >= LIVE_COLLECTOR_STREAM_STALL_THRESHOLD
    ):
        reasons.append("stream_capture_stalled")
    if slot_lag_slots >= LIVE_COLLECTOR_SLOT_LAG_THRESHOLD:
        reasons.append("slot_lagging")
    if not reasons:
        return None
    result = run_payload.get("result") or {}
    cycle_ids = result.get("cycle_ids") or []
    return {
        "reasons": reasons,
        "job_run_id": run_payload["job_run_id"],
        "job_key": run_payload["job_key"],
        "label": result.get("label") or (run_payload.get("payload") or {}).get("label"),
        "session_id": run_payload.get("session_id"),
        "slot_at": run_payload.get("slot_at"),
        "cycle_id": None if not cycle_ids else cycle_ids[0],
        "capture_status": quote_capture.get("capture_status"),
        "quote_capture": quote_capture,
        "consecutive_stream_zero_slots": consecutive_stream_zero_slots,
        "slot_lag_slots": slot_lag_slots,
    }


async def _emit_live_collector_observability(
    ctx: dict[str, Any], run_record: Any
) -> None:
    run_payload = enrich_live_collector_job_run_payload(run_record)
    session_id = run_payload.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        return
    job_store = ctx["job_store"]
    consecutive_stream_zero_slots = await asyncio.to_thread(
        _count_consecutive_stream_zero_slots,
        job_store,
        job_key=str(run_payload["job_key"]),
        session_id=session_id,
    )
    slot_lag_slots = _slot_lag_slots(run_payload)
    log_payload = _build_live_collector_log_payload(
        run_payload,
        consecutive_stream_zero_slots=consecutive_stream_zero_slots,
        slot_lag_slots=slot_lag_slots,
    )
    print(json.dumps(log_payload, separators=(",", ":"), sort_keys=True), flush=True)
    degradation = _build_live_collector_degradation(
        run_payload,
        consecutive_stream_zero_slots=consecutive_stream_zero_slots,
        slot_lag_slots=slot_lag_slots,
    )
    if degradation is None:
        return
    print(
        json.dumps(
            {"event": "live_collector_slot_degraded", **degradation},
            separators=(",", ":"),
            sort_keys=True,
        ),
        flush=True,
    )
    event_bus = ctx.get("event_bus")
    if event_bus is None:
        return
    try:
        await publish_global_event_async(
            event_bus,
            topic="live.collector.degraded",
            event_class="control_event",
            entity_type="job_run",
            entity_id=run_payload["job_run_id"],
            payload=degradation,
            event_type="alert",
            timestamp=run_payload.get("finished_at") or run_payload.get("slot_at"),
            source="worker",
            session_date=run_payload.get("session_date")
            if isinstance(run_payload.get("session_date"), str)
            else None,
            correlation_id=session_id,
        )
    except Exception:
        pass


async def _mark_running(
    job_store: Any, job_run_id: str, runtime_owner: str, arq_job_id: str
) -> Any:
    now = datetime.now(UTC)
    run_record = await asyncio.to_thread(
        job_store.update_job_run_status,
        job_run_id=job_run_id,
        status="running",
        expected_arq_job_id=arq_job_id,
        worker_name=runtime_owner,
        started_at=now,
        heartbeat_at=now,
    )
    if run_record is None:
        raise SupersededJobRun(f"Job run {job_run_id} was superseded before start.")
    return run_record


async def _update_live_slot_status(
    ctx: dict[str, Any],
    *,
    payload: Mapping[str, Any],
    job_run_id: str,
    status: str,
    capture_status: str | None = None,
    recovery_note: str | None = None,
    slot_details: dict[str, Any] | None = None,
    queued_at: str | datetime | None = None,
    started_at: str | datetime | None = None,
    finished_at: str | datetime | None = None,
) -> None:
    recovery_store = ctx["storage"].recovery
    if not recovery_store.schema_ready():
        return
    session_id = payload.get("session_id")
    session_date = payload.get("session_date")
    label = payload.get("label")
    slot_at = payload.get("slot_at")
    job_key = payload.get("job_key")
    if not all(
        isinstance(value, str) and value
        for value in (session_id, session_date, label, slot_at, job_key)
    ):
        return
    await asyncio.to_thread(
        recovery_store.upsert_live_session_slot,
        job_key=str(job_key),
        session_id=str(session_id),
        session_date=str(session_date),
        label=str(label),
        slot_at=str(slot_at),
        scheduled_for=str(payload.get("scheduled_for") or slot_at),
        status=status,
        job_run_id=job_run_id,
        capture_status=capture_status,
        recovery_note=recovery_note,
        slot_details={} if slot_details is None else dict(slot_details),
        queued_at=None if queued_at is None else str(queued_at),
        started_at=None if started_at is None else str(started_at),
        finished_at=None if finished_at is None else str(finished_at),
        updated_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    )


def _sync_job_heartbeat(
    job_store: Any,
    *,
    job_run_id: str,
    arq_job_id: str,
    runtime_owner: str,
    lease_key: str | None,
) -> None:
    now = datetime.now(UTC)
    run_record = job_store.heartbeat_job_run(
        job_run_id=job_run_id,
        expected_arq_job_id=arq_job_id,
        heartbeat_at=now,
        worker_name=runtime_owner,
    )
    if run_record is None:
        raise SupersededJobRun(f"Job run {job_run_id} was superseded during execution.")
    if lease_key is not None:
        job_store.renew_lease(
            lease_key=lease_key,
            owner=job_run_id,
            expires_in_seconds=JOB_LEASE_TTL_SECONDS,
            state={"kind": "singleton_job"},
        )


def build_live_session_catalog_for_date(
    *,
    job_store: Any,
    session_date: str,
) -> dict[str, Any]:
    definitions = job_store.list_job_definitions(
        enabled_only=True, job_type="live_collector"
    )
    base_catalog = build_live_session_catalog(definitions, realized_labels=[])
    realized_labels = [
        str(pipeline["label"])
        for pipeline in base_catalog["pipelines"]
        if job_store.list_job_runs(
            job_key=str(pipeline["job_key"]),
            job_type="live_collector",
            status="succeeded",
            session_id=build_live_run_scope_id(str(pipeline["label"]), session_date),
            limit=1,
        )
    ]
    return build_live_session_catalog(definitions, realized_labels=realized_labels)


def _planner_analysis_payload(
    base_payload: dict[str, Any],
    *,
    db_target: str,
    label: str,
) -> dict[str, Any]:
    return {
        "db": db_target,
        "date": str(base_payload.get("date", "today")),
        "label": label,
        "replay_profit_target": base_payload.get("replay_profit_target", 0.5),
        "replay_stop_multiple": base_payload.get("replay_stop_multiple", 2.0),
    }


def run_post_close_analysis_targets(
    *,
    db_target: str,
    job_store: Any,
    payload: dict[str, Any],
    heartbeat: Any,
) -> dict[str, Any]:
    session_date = resolve_market_date(str(payload.get("date", "today")))
    catalog = build_live_session_catalog_for_date(
        job_store=job_store,
        session_date=session_date,
    )
    runs: list[dict[str, Any]] = []
    skipped_labels: list[dict[str, Any]] = []
    failed_labels: list[dict[str, Any]] = []

    for pipeline in catalog["pipelines"]:
        heartbeat()
        label = str(pipeline["label"])
        if not pipeline["has_session"]:
            skipped_labels.append({"label": label, "reason": "missing_session"})
            continue
        try:
            args = build_analysis_args(
                _planner_analysis_payload(payload, db_target=db_target, label=label)
            )
            runs.append(run_post_close_analysis(args, emit_output=False))
        except Exception as exc:
            failed_labels.append({"label": label, "error": str(exc)})

    result = {
        "mode": "planner",
        "session_date": session_date,
        "expected_labels": list(catalog["expected_labels"]),
        "realized_labels": list(catalog["realized_labels"]),
        "runs": runs,
        "skipped_labels": skipped_labels,
        "failed_labels": failed_labels,
    }
    if failed_labels:
        labels = ", ".join(item["label"] for item in failed_labels)
        raise ManagedJobFailure(
            f"Post-close analysis failed for labels: {labels}",
            result=result,
        )
    return result


def run_post_market_analysis_targets(
    *,
    db_target: str,
    job_store: Any,
    parent_job_run_id: str,
    payload: dict[str, Any],
    heartbeat: Any,
) -> dict[str, Any]:
    session_date = resolve_market_date(str(payload.get("date", "today")))
    catalog = build_live_session_catalog_for_date(
        job_store=job_store,
        session_date=session_date,
    )
    repository = build_post_market_repository(db_target)
    runs: list[dict[str, Any]] = []
    skipped_labels: list[dict[str, Any]] = []
    failed_labels: list[dict[str, Any]] = []
    try:
        for pipeline in catalog["pipelines"]:
            heartbeat()
            label = str(pipeline["label"])
            analysis_run_id = f"{parent_job_run_id}:{label}:{uuid4().hex[:8]}"
            if not pipeline["has_session"]:
                created_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
                repository.begin_run(
                    analysis_run_id=analysis_run_id,
                    job_run_id=None,
                    session_date=session_date,
                    label=label,
                    created_at=created_at,
                )
                repository.skip_run(
                    analysis_run_id=analysis_run_id,
                    completed_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                    error_text="No persisted collector cycles were available for this session label.",
                )
                skipped_labels.append(
                    {
                        "analysis_run_id": analysis_run_id,
                        "label": label,
                        "session_date": session_date,
                        "status": "skipped",
                        "reason": "missing_session",
                    }
                )
                continue
            try:
                args = parse_post_market_args(
                    [
                        "--db",
                        db_target,
                        "--date",
                        session_date,
                        "--label",
                        label,
                        "--replay-profit-target",
                        str(payload.get("replay_profit_target", 0.5)),
                        "--replay-stop-multiple",
                        str(payload.get("replay_stop_multiple", 2.0)),
                    ]
                )
                runs.append(
                    run_post_market_analysis(
                        args,
                        emit_output=False,
                        analysis_run_id=analysis_run_id,
                        job_run_id=None,
                    )
                )
            except Exception as exc:
                failed_labels.append({"label": label, "error": str(exc)})
    finally:
        repository.close()

    result = {
        "mode": "planner",
        "session_date": session_date,
        "expected_labels": list(catalog["expected_labels"]),
        "realized_labels": list(catalog["realized_labels"]),
        "runs": runs,
        "skipped_labels": skipped_labels,
        "failed_labels": failed_labels,
    }
    if failed_labels:
        labels = ", ".join(item["label"] for item in failed_labels)
        raise ManagedJobFailure(
            f"Post-market analysis failed for labels: {labels}",
            result=result,
        )
    return result


async def _execute_managed_job(
    ctx: dict[str, Any],
    *,
    job_key: str,
    job_run_id: str,
    arq_job_id: str,
    payload: dict[str, Any],
    runner: Any,
    compact_result: Any,
    on_running: Any | None = None,
    on_completed: Any | None = None,
    on_failed: Any | None = None,
) -> dict[str, Any]:
    job_store = ctx["job_store"]
    runtime_owner = ctx["worker_name"]
    lease_key = None
    scope = payload.get("singleton_scope")
    job_type = payload.get("job_type")
    if scope and job_type:
        lease_key = singleton_lease_key(str(job_type), str(scope))
        acquired = await asyncio.to_thread(
            job_store.acquire_lease,
            lease_key=lease_key,
            owner=job_run_id,
            job_run_id=job_run_id,
            expires_in_seconds=JOB_LEASE_TTL_SECONDS,
            state={"kind": "singleton_job", "job_key": job_key},
        )
        if not acquired:
            result = {"status": "skipped", "reason": "singleton_lease_unavailable"}
            skipped_record = await asyncio.to_thread(
                job_store.update_job_run_status,
                job_run_id=job_run_id,
                status="skipped",
                expected_arq_job_id=arq_job_id,
                worker_name=runtime_owner,
                finished_at=datetime.now(UTC),
                heartbeat_at=datetime.now(UTC),
                result=result,
            )
            await _publish_job_run_event(ctx, skipped_record)
            return result

    running_record = await _mark_running(
        job_store, job_run_id, runtime_owner, arq_job_id
    )
    await _publish_job_run_event(ctx, running_record)
    if on_running is not None:
        await on_running(running_record)
    try:
        result = await asyncio.to_thread(
            runner,
            lambda: _sync_job_heartbeat(
                job_store,
                job_run_id=job_run_id,
                arq_job_id=arq_job_id,
                runtime_owner=runtime_owner,
                lease_key=lease_key,
            ),
        )
        compact = compact_result(result)
        final_status = (
            "skipped"
            if isinstance(result, dict) and result.get("status") == "skipped"
            else "succeeded"
        )
        completed_record = await asyncio.to_thread(
            job_store.update_job_run_status,
            job_run_id=job_run_id,
            status=final_status,
            expected_arq_job_id=arq_job_id,
            worker_name=runtime_owner,
            finished_at=datetime.now(UTC),
            heartbeat_at=datetime.now(UTC),
            result=compact,
        )
        if completed_record is None:
            raise SupersededJobRun(
                f"Job run {job_run_id} was superseded before completion."
            )
        await _publish_job_run_event(ctx, completed_record)
        if on_completed is not None:
            await on_completed(completed_record, result)
        if payload.get("job_type") == "live_collector" and final_status == "succeeded":
            await _emit_live_collector_observability(ctx, completed_record)
        return compact
    except SupersededJobRun:
        return {"status": "superseded", "job_run_id": job_run_id}
    except Exception as exc:
        partial_result = exc.result if isinstance(exc, ManagedJobFailure) else None
        failed_record = await asyncio.to_thread(
            job_store.update_job_run_status,
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=arq_job_id,
            worker_name=runtime_owner,
            finished_at=datetime.now(UTC),
            heartbeat_at=datetime.now(UTC),
            result=None if partial_result is None else compact_result(partial_result),
            error_text=str(exc),
        )
        await _publish_job_run_event(ctx, failed_record)
        if on_failed is not None and failed_record is not None:
            await on_failed(failed_record, partial_result)
        raise
    finally:
        if lease_key is not None:
            await asyncio.to_thread(
                job_store.release_lease, lease_key, owner=job_run_id
            )


async def run_broker_sync_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = str(payload.get("db") or ctx["database_url"])

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_broker_sync(
            db_target=database_url,
            history_range=str(payload.get("history_range", "1D")),
            activity_lookback_days=int(payload.get("activity_lookback_days", 1)),
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = BROKER_SYNC_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_collector_recovery_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = str(payload.get("db") or ctx["database_url"])

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_collector_recovery(
            db_target=database_url,
            storage=ctx["storage"],
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = COLLECTOR_RECOVERY_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_execution_submit_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = ctx["database_url"]

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_execution_submit(
            db_target=database_url,
            execution_attempt_id=str(payload["execution_attempt_id"]),
            heartbeat=heartbeat,
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = EXECUTION_SUBMIT_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_position_exit_manager_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = ctx["database_url"]

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_position_exit_manager(
            db_target=database_url,
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = POSITION_EXIT_MANAGER_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_options_automation_entry_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = ctx["database_url"]

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_entry_automation_decision(
            db_target=database_url,
            bot_id=str(payload["bot_id"]),
            automation_id=str(payload["automation_id"]),
            market_date=payload.get("market_date"),
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = OPTIONS_AUTOMATION_ENTRY_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_options_automation_management_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = ctx["database_url"]

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_management_automation_decision(
            db_target=database_url,
            bot_id=str(payload["bot_id"]),
            automation_id=str(payload["automation_id"]),
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = OPTIONS_AUTOMATION_MANAGEMENT_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_options_automation_execute_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = ctx["database_url"]

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return dispatch_pending_execution_intents(
            db_target=database_url,
            limit=int(payload.get("limit", 25) or 25),
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = OPTIONS_AUTOMATION_EXECUTE_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_alert_delivery_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_alert_delivery(
            alert_store=ctx["storage"].alerts,
            alert_id=int(payload["alert_id"]),
            delivery_job_run_id=job_run_id,
            worker_name=ctx["worker_name"],
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = ALERT_DELIVERY_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_alert_reconcile_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return reconcile_alert_delivery(
            alert_store=ctx["storage"].alerts,
            job_store=ctx["job_store"],
            limit=int(payload.get("limit", 200)),
            stale_after_seconds=int(
                payload.get("stale_after_seconds", ALERT_DELIVERY_STALE_SECONDS)
            ),
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = ALERT_RECONCILE_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_live_collector_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    async def on_running(run_record: Mapping[str, Any]) -> None:
        await _update_live_slot_status(
            ctx,
            payload=payload,
            job_run_id=str(run_record["job_run_id"]),
            status=LIVE_SLOT_STATUS_RUNNING,
            queued_at=run_record.get("scheduled_for"),
            started_at=run_record.get("started_at"),
        )

    async def on_completed(
        run_record: Mapping[str, Any], result: Mapping[str, Any]
    ) -> None:
        slot_status = LIVE_SLOT_STATUS_SUCCEEDED
        recovery_note = None
        capture_status = None
        slot_details = None
        if str(result.get("status") or "") == "completed":
            capture_status = str(
                (result.get("quote_capture") or {}).get("capture_status") or ""
            )
            slot_details = build_slot_details_from_cycle_result(result)
        elif str(result.get("slot_status") or "") == LIVE_SLOT_STATUS_MISSED:
            slot_status = LIVE_SLOT_STATUS_MISSED
            recovery_note = str(
                result.get("message")
                or result.get("reason")
                or "Live slot was skipped as stale."
            )
        else:
            slot_status = LIVE_SLOT_STATUS_MISSED
            recovery_note = str(
                result.get("reason") or "Live slot did not complete successfully."
            )
        await _update_live_slot_status(
            ctx,
            payload=payload,
            job_run_id=str(run_record["job_run_id"]),
            status=slot_status,
            capture_status=capture_status,
            recovery_note=recovery_note,
            slot_details=slot_details,
            started_at=run_record.get("started_at"),
            finished_at=run_record.get("finished_at"),
        )

    async def on_failed(
        run_record: Mapping[str, Any], partial_result: Mapping[str, Any] | None
    ) -> None:
        await _update_live_slot_status(
            ctx,
            payload=payload,
            job_run_id=str(run_record["job_run_id"]),
            status=LIVE_SLOT_STATUS_MISSED,
            recovery_note=(
                None
                if partial_result is None
                else str(
                    partial_result.get("reason")
                    or partial_result.get("message")
                    or "Live slot failed."
                )
            )
            or "Live slot failed before it could complete.",
            slot_details=None
            if partial_result is None
            else build_slot_details_from_cycle_result(partial_result),
            started_at=run_record.get("started_at"),
            finished_at=run_record.get("finished_at"),
        )

    def runner(heartbeat: Any) -> dict[str, Any]:
        args = build_collection_args(payload)
        session_id = payload.get("session_id")
        slot_at = payload.get("slot_at")
        if not isinstance(session_id, str) or not session_id:
            raise ValueError("live_collector payload is missing session_id")
        if not isinstance(slot_at, str) or not slot_at:
            raise ValueError("live_collector payload is missing slot_at")
        tick_context = LiveTickContext(
            job_run_id=job_run_id,
            session_id=session_id,
            slot_at=slot_at,
        )
        return run_collection_tick(
            args,
            tick_context=tick_context,
            heartbeat=heartbeat,
            emit_output=False,
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = LIVE_COLLECTOR_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
        on_running=on_running,
        on_completed=on_completed,
        on_failed=on_failed,
    )


async def run_post_close_analysis_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = ctx["database_url"]
    job_store = ctx["job_store"]

    def runner(heartbeat: Any) -> dict[str, Any]:
        if payload.get("label"):
            heartbeat()
            args = build_analysis_args(
                {
                    "db": database_url,
                    "date": payload.get("date", "today"),
                    "label": payload["label"],
                    "replay_profit_target": payload.get("replay_profit_target", 0.5),
                    "replay_stop_multiple": payload.get("replay_stop_multiple", 2.0),
                }
            )
            return run_post_close_analysis(args, emit_output=False)
        return run_post_close_analysis_targets(
            db_target=database_url,
            job_store=job_store,
            payload=payload,
            heartbeat=heartbeat,
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = POST_CLOSE_ANALYSIS_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: compact_analysis_result(
            result,
            include_report=bool(payload.get("include_report")),
        ),
    )


async def run_post_market_analysis_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = str(payload.get("db") or ctx["database_url"])
    job_store = ctx["job_store"]

    def runner(heartbeat: Any) -> dict[str, Any]:
        if payload.get("label"):
            heartbeat()
            args = parse_post_market_args(
                [
                    "--db",
                    database_url,
                    "--date",
                    str(payload.get("date", "today")),
                    "--label",
                    str(payload["label"]),
                    "--replay-profit-target",
                    str(payload.get("replay_profit_target", 0.5)),
                    "--replay-stop-multiple",
                    str(payload.get("replay_stop_multiple", 2.0)),
                ]
            )
            return run_post_market_analysis(
                args,
                emit_output=False,
                analysis_run_id=job_run_id,
                job_run_id=job_run_id,
            )
        return run_post_market_analysis_targets(
            db_target=database_url,
            job_store=job_store,
            parent_job_run_id=job_run_id,
            payload=payload,
            heartbeat=heartbeat,
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = POST_MARKET_ANALYSIS_JOB_TYPE
    try:
        result = await _execute_managed_job(
            ctx,
            job_key=job_key,
            job_run_id=job_run_id,
            arq_job_id=arq_job_id,
            payload=enriched_payload,
            runner=runner,
            compact_result=compact_post_market_result,
        )
        if result.get("mode") == "planner":
            await _publish_post_market_planner_events(ctx, result)
            return result
        await _publish_post_market_event(
            ctx,
            analysis_run_id=str(result["analysis_run_id"]),
            payload=result,
            timestamp=datetime.now(UTC),
        )
        return result
    except ManagedJobFailure as exc:
        partial_result = (
            compact_post_market_result(exc.result) if exc.result is not None else None
        )
        if partial_result is not None and partial_result.get("mode") == "planner":
            await _publish_post_market_planner_events(ctx, partial_result)
        await _publish_post_market_event(
            ctx,
            analysis_run_id=job_run_id,
            payload={
                "analysis_run_id": job_run_id,
                "session_date": payload.get("date", "today"),
                "status": "failed",
                "failed_labels": []
                if partial_result is None
                else partial_result.get("failed_labels", []),
            },
            timestamp=datetime.now(UTC),
        )
        raise
    except Exception:
        await _publish_post_market_event(
            ctx,
            analysis_run_id=job_run_id,
            payload={
                "analysis_run_id": job_run_id,
                "label": payload.get("label"),
                "session_date": payload.get("date", "today"),
                "status": "failed",
            },
            timestamp=datetime.now(UTC),
        )
        raise


class RuntimeWorkerSettings:
    functions = [
        run_broker_sync_job,
        run_collector_recovery_job,
        run_execution_submit_job,
        run_options_automation_entry_job,
        run_options_automation_management_job,
        run_options_automation_execute_job,
        run_alert_delivery_job,
        run_alert_reconcile_job,
        run_position_exit_manager_job,
        run_post_close_analysis_job,
        run_post_market_analysis_job,
    ]
    queue_name = RUNTIME_QUEUE_NAME
    redis_settings = build_redis_settings(default_redis_url())
    on_startup = runtime_startup
    on_shutdown = shutdown
    keep_result = 0
    job_timeout = 8 * 60 * 60
    max_jobs = 4


class DiscoveryWorkerSettings:
    functions = [
        run_live_collector_job,
    ]
    queue_name = DISCOVERY_QUEUE_NAME
    redis_settings = build_redis_settings(default_redis_url())
    on_startup = discovery_startup
    on_shutdown = shutdown
    keep_result = 0
    job_timeout = 8 * 60 * 60
    max_jobs = 1


WorkerSettings = RuntimeWorkerSettings
