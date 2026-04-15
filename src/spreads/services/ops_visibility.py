from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from spreads.db.decorators import with_storage
from spreads.jobs.orchestration import (
    NEW_YORK,
    SCHEDULER_RUNTIME_LEASE_KEY,
    SINGLETON_LEASE_PREFIX,
    WORKER_RUNTIME_LEASE_PREFIX,
    _market_schedule,
    singleton_lease_key,
)
from spreads.services.account_state import get_account_overview
from spreads.services.audit_replay import DEFAULT_EVENT_SCAN_LIMIT, build_audit_replay
from spreads.services.broker_sync import BROKER_SYNC_KEY
from spreads.services.control_plane import (
    get_control_state_snapshot,
    resolve_execution_kill_switch_reason,
)
from spreads.services.execution import OPEN_STATUSES
from spreads.services.execution_lifecycle import (
    classify_open_execution_attempt,
    is_open_execution_attempt_status,
    resolve_execution_attempt_source_job,
    resolve_execution_submit_job_run_id,
)
from spreads.services.live_collector_health import enrich_live_collector_job_run_payload
from spreads.services.live_pipelines import list_enabled_live_collector_pipelines
from spreads.services.live_runtime import list_latest_live_sessions
from spreads.services.positions import enrich_position_row
from spreads.services.risk_manager import assess_position_risk
from spreads.services.selection_summary import (
    aggregate_selection_summaries as _aggregate_selection_summaries,
    selection_summary_payload as _selection_summary_payload,
)
from spreads.services.selection_terms import (
    MONITOR_SELECTION_STATE,
    PROMOTABLE_SELECTION_STATE,
    normalize_selection_state,
    promotable_monitor_pnl_spread,
)
from spreads.services.uoa_state import get_latest_uoa_state, get_uoa_state_for_cycle
from spreads.services.value_coercion import (
    as_text as _as_text,
    coerce_float as _coerce_float,
    coerce_int as _coerce_int,
    utc_now_iso as _utc_now,
)
from spreads.storage.serializers import parse_datetime

OPEN_POSITION_STATUSES = ["open", "partial_close"]
BROKER_SYNC_STALE_AFTER_SECONDS = 15 * 60
MARK_STALE_AFTER_SECONDS = 15 * 60
JOB_RUN_QUEUE_STALE_AFTER_SECONDS = 15 * 60
JOB_RUN_HEARTBEAT_STALE_AFTER_SECONDS = 10 * 60
OPS_INCIDENT_WINDOW_SECONDS = 24 * 60 * 60
RECENT_FAILURE_LIMIT = 10
RECENT_ALERT_LIMIT = 200
TOP_POSITION_LIMIT = 5
MODELED_IDEA_LIMIT = 5
UOA_ROOT_LIMIT = 5
UOA_CONTRACT_LIMIT = 8
UOA_CANDIDATE_LIMIT = 8
UOA_EVENT_LIMIT = 20

STATUS_RANK = {
    "healthy": 0,
    "idle": 0,
    "unknown": 1,
    "degraded": 2,
    "blocked": 3,
    "halted": 4,
}


class OpsLookupError(LookupError):
    pass

def _stream_quote_events_saved(capture: Mapping[str, Any] | None) -> int:
    if not isinstance(capture, Mapping):
        return 0
    return (
        _coerce_int(capture.get("stream_quote_events_saved"))
        or _coerce_int(capture.get("websocket_quote_events_saved"))
        or 0
    )


def _stream_trade_events_saved(capture: Mapping[str, Any] | None) -> int:
    if not isinstance(capture, Mapping):
        return 0
    return (
        _coerce_int(capture.get("stream_trade_events_saved"))
        or _coerce_int(capture.get("websocket_trade_events_saved"))
        or 0
    )


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    text = _as_text(value)
    if text is None:
        return None
    try:
        parsed = parse_datetime(text)
    except ValueError:
        return None
    if parsed is None:
        return None
    return parsed.astimezone(UTC)


def _seconds_since(value: Any, *, now: datetime) -> float | None:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return None
    return max((now - parsed).total_seconds(), 0.0)


def _seconds_until(value: Any, *, now: datetime) -> float | None:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return None
    return (parsed - now).total_seconds()


def _is_recent(
    value: Any,
    *,
    now: datetime,
    within_seconds: int = OPS_INCIDENT_WINDOW_SECONDS,
) -> bool:
    age_seconds = _seconds_since(value, now=now)
    return age_seconds is not None and age_seconds <= within_seconds


def _combine_statuses(*statuses: str | None) -> str:
    normalized = [
        str(status or "unknown").strip().lower()
        for status in statuses
        if status is not None
    ]
    if not normalized:
        return "unknown"
    return max(normalized, key=lambda status: STATUS_RANK.get(status, 1))


def _attention(*, severity: str, code: str, message: str) -> dict[str, str]:
    return {
        "severity": severity,
        "code": code,
        "message": message,
    }

def _control_status(control: Mapping[str, Any]) -> str:
    mode = str(control.get("mode") or "unknown")
    if mode == "halted":
        return "halted"
    if mode == "degraded":
        return "degraded"
    if mode == "normal":
        return "healthy"
    return "unknown"


def _session_status(status: Any) -> str:
    normalized = str(status or "unknown").strip().lower()
    if normalized == "failed":
        return "blocked"
    if normalized in {"healthy", "idle", "degraded"}:
        return normalized
    return "unknown"


def _lease_status(lease: Mapping[str, Any] | None, *, now: datetime) -> str:
    if lease is None:
        return "blocked"
    remaining = _seconds_until(lease.get("expires_at"), now=now)
    if remaining is None or remaining <= 0:
        return "blocked"
    if remaining <= 30:
        return "degraded"
    return "healthy"


def _activity_at(row: Mapping[str, Any]) -> str | None:
    for key in (
        "finished_at",
        "heartbeat_at",
        "started_at",
        "slot_at",
        "scheduled_for",
        "requested_at",
        "updated_at",
    ):
        value = _as_text(row.get(key))
        if value:
            return value
    return None


def _sorted_by_activity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: _parse_timestamp(_activity_at(row))
        or datetime.fromtimestamp(0, UTC),
        reverse=True,
    )


def _collector_status(run: Mapping[str, Any] | None) -> str:
    if run is None:
        return "unknown"
    live_action_gate = (
        run.get("live_action_gate")
        if isinstance(run.get("live_action_gate"), Mapping)
        else {}
    )
    if str(live_action_gate.get("status") or "") == "blocked":
        return "blocked"
    if str(run.get("status") or "") != "succeeded":
        return "degraded"
    capture_status = str(run.get("capture_status") or "")
    if capture_status == "healthy":
        return "healthy"
    if capture_status == "idle":
        return "idle"
    return "degraded"


def _collector_requires_attention(
    run: Mapping[str, Any] | None,
    *,
    now: datetime,
) -> bool:
    if run is None:
        return True
    collector_status = _collector_status(run)
    if collector_status in {"healthy", "idle"}:
        return False
    return _is_recent(
        run.get("slot_at")
        or run.get("finished_at")
        or run.get("scheduled_for")
        or run.get("started_at"),
        now=now,
    )


def _latest_live_collectors(
    *,
    storage: Any,
    now: datetime,
) -> list[dict[str, Any]]:
    job_store = storage.jobs
    collector_definitions = list_enabled_live_collector_pipelines(
        job_store.list_job_definitions(enabled_only=True, job_type="live_collector")
    )
    if not collector_definitions:
        return []
    latest_session_by_pipeline_id = {
        str(session["pipeline"]["pipeline_id"]): session
        for session in list_latest_live_sessions(
            storage=storage,
            limit=max(len(collector_definitions), 1),
        )
        if isinstance(session.get("pipeline"), Mapping)
        and session["pipeline"].get("pipeline_id")
    }
    latest_collectors: list[dict[str, Any]] = []
    for definition in collector_definitions:
        session = latest_session_by_pipeline_id.get(str(definition["pipeline_id"]))
        run = (
            None
            if session is None or not isinstance(session.get("latest_run"), Mapping)
            else dict(session["latest_run"])
        )
        job_run = (
            {}
            if session is None or not isinstance(session.get("job_run"), Mapping)
            else dict(session.get("job_run") or {})
        )
        quote_capture = (
            {}
            if session is None or not isinstance(session.get("quote_capture"), Mapping)
            else dict(session.get("quote_capture") or {})
        )
        capture_status = (
            None
            if run is None
            else run.get("capture_status")
        )
        if capture_status is None:
            capture_status = quote_capture.get("capture_status")
        collector_status = _collector_status(run)
        needs_attention = _collector_requires_attention(run, now=now)
        stream_quote_events_saved = _stream_quote_events_saved(quote_capture)
        latest_collectors.append(
            {
                "job_key": str(definition["job_key"]),
                "status": collector_status,
                "needs_attention": needs_attention,
                "capture_status": capture_status,
                "live_action_gate": None
                if run is None
                else dict(run.get("live_action_gate") or {}),
                "auto_execution_summary": None
                if run is None
                else run.get("auto_execution_summary"),
                "selection_summary": None
                if session is None
                else _selection_summary_payload(session.get("selection_summary")),
                "last_slot_at": (
                    None
                    if run is None
                    else run.get("slot_at") or run.get("scheduled_for")
                )
                or job_run.get("slot_at")
                or job_run.get("scheduled_for"),
                "stream_quote_events_saved": stream_quote_events_saved,
                "websocket_quote_events_saved": stream_quote_events_saved,
                "baseline_quote_events_saved": _coerce_int(
                    quote_capture.get("baseline_quote_events_saved")
                )
                or 0,
                "session_id": None
                if session is None
                else session.get("session_id") or job_run.get("session_id"),
            }
        )
    return latest_collectors


def _market_session_context(
    *,
    now: datetime,
    calendar_name: str = "NYSE",
) -> dict[str, Any]:
    local_now = now.astimezone(NEW_YORK)
    market_window = _market_schedule(calendar_name, local_now.date())
    if market_window is None:
        return {
            "calendar": calendar_name,
            "status": "closed",
            "is_open": False,
            "market_open_at": None,
            "market_close_at": None,
        }
    market_open, market_close = market_window
    return {
        "calendar": calendar_name,
        "status": "open" if market_open <= local_now < market_close else "closed",
        "is_open": market_open <= local_now < market_close,
        "market_open_at": market_open.astimezone(UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "market_close_at": market_close.astimezone(UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
    }


def _skip_reason_text(run: Mapping[str, Any]) -> str | None:
    result = run.get("result") if isinstance(run.get("result"), Mapping) else {}
    return _as_text(result.get("reason"))


def _skip_is_benign(run: Mapping[str, Any]) -> bool:
    reason = str(_skip_reason_text(run) or "").strip().lower()
    if reason == "singleton_lease_unavailable":
        return True
    if reason == "stale_slot" and str(run.get("job_type") or "") == "live_collector":
        return True
    error_text = str(_as_text(run.get("error_text")) or "").strip().lower()
    return error_text == "superseded during queue consolidation"


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
    operator_status = (
        str(definition.get("operator_status") or "unknown").strip().lower()
    )
    if operator_status not in {"degraded", "blocked"}:
        return False
    latest_run_at = definition.get("latest_run_at")
    if latest_run_at is None:
        return True
    return _is_recent(latest_run_at, now=now)


def _broker_sync_payload(
    state: Mapping[str, Any] | None,
    *,
    now: datetime,
    market_session: Mapping[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    if state is None:
        return (
            "blocked",
            {
                "status": "missing",
                "raw_status": None,
                "updated_at": None,
                "summary": {},
                "error_text": None,
                "age_seconds": None,
            },
        )
    payload = dict(state)
    summary = (
        payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
    )
    age_seconds = _seconds_since(payload.get("updated_at"), now=now)
    status = str(payload.get("status") or "unknown")
    normalized = "unknown"
    if status == "healthy":
        normalized = "healthy"
    elif status == "degraded":
        normalized = "degraded"
    elif status == "failed":
        normalized = "blocked"
    open_position_count = _coerce_int(summary.get("open_position_count")) or 0
    queued_attempt_count = _coerce_int(summary.get("queued_attempt_count")) or 0
    requires_freshness = bool((market_session or {}).get("is_open")) or bool(
        open_position_count or queued_attempt_count
    )
    freshness = "current"
    if (
        age_seconds is not None
        and age_seconds > BROKER_SYNC_STALE_AFTER_SECONDS
        and normalized == "healthy"
    ):
        freshness = "stale"
        normalized = "degraded" if requires_freshness else "idle"
    payload["raw_status"] = status
    payload["status"] = normalized
    payload["age_seconds"] = age_seconds
    payload["freshness"] = freshness
    payload["requires_freshness"] = requires_freshness
    payload["market_session"] = dict(market_session or {})
    return normalized, payload


def _alert_delivery_payload(
    rows: list[dict[str, Any]],
    *,
    now: datetime,
) -> dict[str, Any]:
    recent_rows = [
        row
        for row in rows
        if _is_recent(row.get("updated_at") or row.get("created_at"), now=now)
    ]
    counts = Counter(str(row.get("status") or "unknown") for row in recent_rows)
    status = "healthy"
    if counts.get("dead_letter", 0) or counts.get("retry_wait", 0):
        status = "degraded"
    return {
        "status": status,
        "count": len(recent_rows),
        "recent_count": len(recent_rows),
        "status_counts": dict(counts),
        "dead_letter_count": counts.get("dead_letter", 0),
        "retry_wait_count": counts.get("retry_wait", 0),
        "dispatching_count": counts.get("dispatching", 0),
        "pending_count": counts.get("pending", 0),
        "historical_status_counts": dict(
            Counter(str(row.get("status") or "unknown") for row in rows)
        ),
    }


def _promotable_monitor_pnl_spread(summary: Mapping[str, Any] | None) -> float | None:
    if not isinstance(summary, Mapping):
        return None
    outcomes = summary.get("outcomes")
    if not isinstance(outcomes, Mapping):
        return None
    averages = outcomes.get(
        "average_estimated_pnl_by_selection_state",
        outcomes.get("average_estimated_pnl_by_bucket"),
    )
    return (
        None
        if not isinstance(averages, Mapping)
        else promotable_monitor_pnl_spread(averages)
    )


def _post_market_view(run: Mapping[str, Any] | None) -> dict[str, Any]:
    if run is None:
        return {
            "overall_verdict": None,
            "promotable_monitor_pnl_spread": None,
            "recommendations": [],
            "completed_at": None,
        }
    diagnostics = (
        run.get("diagnostics") if isinstance(run.get("diagnostics"), Mapping) else {}
    )
    summary = run.get("summary") if isinstance(run.get("summary"), Mapping) else {}
    return {
        "overall_verdict": _as_text(diagnostics.get("overall_verdict")),
        "promotable_monitor_pnl_spread": _promotable_monitor_pnl_spread(summary),
        "recommendations": list(run.get("recommendations") or []),
        "completed_at": _as_text(run.get("completed_at")),
    }


def _modeled_pnl(idea: Mapping[str, Any]) -> float | None:
    expiry_pnl = _coerce_float(idea.get("estimated_expiry_pnl"))
    if expiry_pnl is not None:
        return expiry_pnl
    return _coerce_float(idea.get("estimated_close_pnl"))


def _rank_modeled_ideas(
    ideas: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ranked = []
    for idea in ideas:
        modeled_pnl = _modeled_pnl(idea)
        if modeled_pnl is None:
            continue
        ranked.append(
            {
                "underlying_symbol": idea.get("underlying_symbol"),
                "selection_state": normalize_selection_state(
                    idea.get("selection_state", idea.get("classification"))
                ),
                "strategy": idea.get("strategy"),
                "short_symbol": idea.get("short_symbol"),
                "long_symbol": idea.get("long_symbol"),
                "replay_verdict": idea.get("replay_verdict"),
                "modeled_pnl": round(modeled_pnl, 2),
            }
        )
    descending = sorted(ranked, key=lambda row: float(row["modeled_pnl"]), reverse=True)
    ascending = sorted(ranked, key=lambda row: float(row["modeled_pnl"]))
    return descending[:MODELED_IDEA_LIMIT], ascending[:MODELED_IDEA_LIMIT]


def _top_positions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for row in rows:
        exposure = _coerce_float(row.get("max_loss"))
        if exposure is None:
            exposure = _coerce_float(row.get("entry_notional"))
        net_pnl = _coerce_float(row.get("net_pnl"))
        ranked.append(
            {
                "session_id": row.get("session_id"),
                "underlying_symbol": row.get("underlying_symbol"),
                "status": row.get("status"),
                "exposure": 0.0 if exposure is None else round(abs(exposure), 2),
                "net_pnl": None if net_pnl is None else round(net_pnl, 2),
                "risk_status": row.get("risk_status"),
            }
        )
    ranked.sort(key=lambda row: float(row.get("exposure") or 0.0), reverse=True)
    return ranked[:TOP_POSITION_LIMIT]


def _load_execution_attempt_job_context(
    *,
    job_store: Any,
    attempts: list[Mapping[str, Any]],
) -> tuple[dict[str, Mapping[str, Any] | None], dict[str, Mapping[str, Any] | None]]:
    submit_jobs: dict[str, Mapping[str, Any] | None] = {}
    source_definitions: dict[str, Mapping[str, Any] | None] = {}
    if job_store is None or (
        hasattr(job_store, "schema_ready") and not job_store.schema_ready()
    ):
        return submit_jobs, source_definitions

    for attempt in attempts:
        execution_attempt_id = _as_text(attempt.get("execution_attempt_id"))
        if execution_attempt_id is None:
            continue
        try:
            submit_jobs[execution_attempt_id] = job_store.get_job_run(
                resolve_execution_submit_job_run_id(execution_attempt_id)
            )
        except Exception:
            submit_jobs[execution_attempt_id] = None

        source_job = resolve_execution_attempt_source_job(attempt)
        source_job_key = _as_text(source_job.get("job_key"))
        if source_job_key is None or source_job_key in source_definitions:
            continue
        try:
            source_definitions[source_job_key] = job_store.get_job_definition(
                source_job_key
            )
        except Exception:
            source_definitions[source_job_key] = None
    return submit_jobs, source_definitions


def _execution_attempt_lifecycle(
    *,
    attempt: Mapping[str, Any],
    now: datetime,
    submit_jobs: Mapping[str, Mapping[str, Any] | None],
    source_definitions: Mapping[str, Mapping[str, Any] | None],
) -> dict[str, Any]:
    if not is_open_execution_attempt_status(attempt.get("status")):
        return {}
    execution_attempt_id = _as_text(attempt.get("execution_attempt_id")) or ""
    source_job = resolve_execution_attempt_source_job(attempt)
    source_job_key = _as_text(source_job.get("job_key"))
    submit_job = submit_jobs.get(execution_attempt_id)
    source_definition = (
        None if source_job_key is None else source_definitions.get(source_job_key)
    )
    return classify_open_execution_attempt(
        attempt,
        now=now,
        submit_job=submit_job,
        source_job_definition=source_definition,
    )


def _summarize_execution_attempt(
    attempt: Mapping[str, Any],
    *,
    lifecycle: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    lifecycle_payload = dict(lifecycle or {})
    return {
        "execution_attempt_id": attempt.get("execution_attempt_id"),
        "session_id": attempt.get("session_id"),
        "label": attempt.get("label"),
        "underlying_symbol": attempt.get("underlying_symbol"),
        "strategy": attempt.get("strategy"),
        "trade_intent": attempt.get("trade_intent"),
        "status": attempt.get("status"),
        "requested_at": attempt.get("requested_at"),
        "submitted_at": attempt.get("submitted_at"),
        "completed_at": attempt.get("completed_at"),
        "broker_order_id": attempt.get("broker_order_id"),
        "candidate_id": attempt.get("candidate_id"),
        "source_kind": lifecycle_payload.get("source_kind"),
        "lifecycle_phase": lifecycle_payload.get("phase"),
        "lifecycle_note": lifecycle_payload.get("note"),
        "age_seconds": lifecycle_payload.get("age_seconds"),
        "queue_age_seconds": lifecycle_payload.get("queue_age_seconds"),
        "stale_after_seconds": lifecycle_payload.get("working_stale_after_seconds"),
        "submission_grace_seconds": lifecycle_payload.get("submission_grace_seconds"),
        "submit_job_status": lifecycle_payload.get("submit_job_status"),
        "submit_job_age_seconds": lifecycle_payload.get("submit_job_age_seconds"),
        "submit_job_heartbeat_age_seconds": lifecycle_payload.get(
            "submit_job_heartbeat_age_seconds"
        ),
        "stale": bool(lifecycle_payload.get("stale")),
        "next_action": lifecycle_payload.get("next_action"),
        "blocks_capacity": bool(lifecycle_payload.get("blocks_capacity")),
        "occupies_position_slot": bool(lifecycle_payload.get("occupies_position_slot")),
    }


def _summarize_slot_run(run: Mapping[str, Any]) -> dict[str, Any]:
    quote_capture = (
        run.get("quote_capture")
        if isinstance(run.get("quote_capture"), Mapping)
        else {}
    )
    trade_capture = (
        run.get("trade_capture")
        if isinstance(run.get("trade_capture"), Mapping)
        else {}
    )
    stream_quote_events_saved = _stream_quote_events_saved(quote_capture)
    stream_trade_events_saved = _stream_trade_events_saved(trade_capture)
    return {
        "job_run_id": run.get("job_run_id"),
        "slot_at": run.get("slot_at"),
        "scheduled_for": run.get("scheduled_for"),
        "status": run.get("status"),
        "capture_status": run.get("capture_status"),
        "started_at": run.get("started_at"),
        "finished_at": run.get("finished_at"),
        "quote_capture": {
            "capture_status": quote_capture.get("capture_status"),
            "stream_quote_events_saved": stream_quote_events_saved,
            "websocket_quote_events_saved": stream_quote_events_saved,
            "baseline_quote_events_saved": quote_capture.get(
                "baseline_quote_events_saved"
            ),
            "recovery_quote_events_saved": quote_capture.get(
                "recovery_quote_events_saved"
            ),
        },
        "trade_capture": {
            "capture_status": trade_capture.get("capture_status"),
            "total_trade_events_saved": trade_capture.get("total_trade_events_saved"),
            "stream_trade_events_saved": stream_trade_events_saved,
            "websocket_trade_events_saved": stream_trade_events_saved,
        },
    }


def _summarize_recovery_slot(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "slot_at": row.get("slot_at"),
        "status": row.get("status"),
        "capture_status": row.get("capture_status"),
        "recovery_note": row.get("recovery_note"),
        "job_run_id": row.get("job_run_id"),
        "updated_at": row.get("updated_at"),
    }


def _summarize_alert(alert: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "alert_id": alert.get("alert_id"),
        "created_at": alert.get("created_at"),
        "label": alert.get("label"),
        "symbol": alert.get("symbol"),
        "alert_type": alert.get("alert_type"),
        "delivery_target": alert.get("delivery_target"),
        "status": alert.get("status"),
    }


def _run_duration_seconds(run: Mapping[str, Any]) -> float | None:
    started_at = _parse_timestamp(run.get("started_at"))
    finished_at = _parse_timestamp(run.get("finished_at"))
    if started_at is None or finished_at is None:
        return None
    duration_seconds = (finished_at - started_at).total_seconds()
    if duration_seconds < 0:
        return None
    return round(duration_seconds, 3)


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
            if (
                reason == "stale_slot"
                and str(run.get("job_type") or "") == "live_collector"
            ):
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
            interval_seconds * 2, JOB_RUN_HEARTBEAT_STALE_AFTER_SECONDS
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
        None
        if latest_run is None
        else enrich_live_collector_job_run_payload(latest_run)
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
            definition, enriched_latest_run, now=now
        ),
        "latest_run_id": None
        if latest_summary is None
        else latest_summary.get("job_run_id"),
        "latest_run_status": None
        if latest_summary is None
        else latest_summary.get("status"),
        "latest_run_operator_status": None
        if latest_summary is None
        else latest_summary.get("operator_status"),
        "latest_run_at": None
        if latest_summary is None
        else latest_summary.get("activity_at"),
        "latest_capture_status": None
        if latest_summary is None
        else latest_summary.get("capture_status"),
    }


@with_storage()
def build_system_status(
    *,
    db_target: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    generated_at = _utc_now()
    now = _parse_timestamp(generated_at) or datetime.now(UTC)
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
    scheduler_payload: dict[str, Any]
    workers: list[dict[str, Any]]
    running_jobs: list[dict[str, Any]]
    queued_jobs: list[dict[str, Any]]
    recent_failures: list[dict[str, Any]]
    actionable_recent_failures: list[dict[str, Any]]
    latest_collectors: list[dict[str, Any]]
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
        latest_collectors = []
        statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="job_schema_unavailable",
                message="Job storage is not available yet.",
            )
        )
    else:
        scheduler_lease = job_store.get_lease(SCHEDULER_RUNTIME_LEASE_KEY)
        scheduler_status = _lease_status(scheduler_lease, now=now)
        scheduler_payload = {
            "status": scheduler_status,
            "expires_at": None
            if scheduler_lease is None
            else scheduler_lease.get("expires_at"),
            "owner": None if scheduler_lease is None else scheduler_lease.get("owner"),
            "job_run_id": None
            if scheduler_lease is None
            else scheduler_lease.get("job_run_id"),
        }
        if scheduler_status != "healthy":
            attention.append(
                _attention(
                    severity="high" if scheduler_status == "blocked" else "medium",
                    code="scheduler_unhealthy",
                    message="Scheduler lease is missing, expired, or close to expiring.",
                )
            )

        workers = [
            dict(row)
            for row in job_store.list_active_leases(prefix=WORKER_RUNTIME_LEASE_PREFIX)
        ]
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
            dict(row) for row in job_store.list_job_runs(status="running", limit=100)
        ]
        queued_jobs = [
            dict(row) for row in job_store.list_job_runs(status="queued", limit=100)
        ]
        failed_jobs = [
            dict(row)
            for row in job_store.list_job_runs(
                status="failed", limit=RECENT_FAILURE_LIMIT
            )
        ]
        skipped_jobs = [
            dict(row)
            for row in job_store.list_job_runs(
                status="skipped", limit=RECENT_FAILURE_LIMIT
            )
        ]
        recent_failures = _sorted_by_activity(failed_jobs + skipped_jobs)[
            :RECENT_FAILURE_LIMIT
        ]
        actionable_recent_failures = [
            row for row in recent_failures if _job_run_requires_attention(row, now=now)
        ]
        if actionable_recent_failures:
            attention.append(
                _attention(
                    severity="medium",
                    code="recent_job_failures",
                    message=(
                        f"{len(actionable_recent_failures)} recent failed or skipped "
                        "job runs need attention."
                    ),
                )
            )

        latest_collectors = _latest_live_collectors(storage=storage, now=now)
        for row in latest_collectors:
            job_key = str(row.get("job_key") or "")
            if bool(row.get("needs_attention")):
                attention.append(
                    _attention(
                        severity="medium",
                        code="collector_unhealthy",
                        message=(
                            f"Collector {job_key} is {str(row.get('status') or 'unknown')}."
                        ),
                    )
                )

        statuses.append(
            _combine_statuses(
                scheduler_payload["status"],
                worker_status,
                "degraded" if actionable_recent_failures else "healthy",
                "degraded"
                if any(row["needs_attention"] for row in latest_collectors)
                else "healthy",
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
        recent_alerts = [
            dict(row) for row in alert_store.list_alert_events(limit=RECENT_ALERT_LIMIT)
        ]
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
            "latest_collectors": latest_collectors,
            "collector_selection": _aggregate_selection_summaries(
                [row.get("selection_summary") for row in latest_collectors]
            ),
            "broker_sync": broker_sync,
            "alert_delivery": alert_delivery,
        }
    )

    collector_selection = dict(details.get("collector_selection") or {})
    summary = {
        "control_mode": control.get("mode"),
        "worker_count": len(workers),
        "running_job_count": len(running_jobs),
        "queued_job_count": len(queued_jobs),
        "running_jobs_by_type": dict(
            Counter(str(row.get("job_type") or "unknown") for row in running_jobs)
        ),
        "queued_jobs_by_type": dict(
            Counter(str(row.get("job_type") or "unknown") for row in queued_jobs)
        ),
        "recent_failure_count": len(actionable_recent_failures),
        "collector_count": len(latest_collectors),
        "collector_degraded_count": sum(
            1 for row in latest_collectors if row["needs_attention"]
        ),
        "collector_opportunity_count": _coerce_int(
            collector_selection.get("opportunity_count")
        )
        or 0,
        "collector_shadow_only_count": _coerce_int(
            collector_selection.get("shadow_only_count")
        )
        or 0,
        "collector_auto_live_eligible_count": _coerce_int(
            collector_selection.get("auto_live_eligible_count")
        )
        or 0,
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


@with_storage()
def build_trading_health(
    *,
    db_target: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    generated_at = _utc_now()
    now = _parse_timestamp(generated_at) or datetime.now(UTC)
    market_session = _market_session_context(now=now)
    attention: list[dict[str, str]] = []
    statuses: list[str] = []
    details: dict[str, Any] = {"market_session": market_session}

    control = get_control_state_snapshot(storage=storage)
    details["control"] = control
    control_status = _control_status(control)
    statuses.append(control_status)
    if control_status in {"degraded", "halted"}:
        attention.append(
            _attention(
                severity="high" if control_status == "halted" else "medium",
                code=f"control_mode_{control.get('mode')}",
                message=_as_text(control.get("note"))
                or f"Control mode is {control.get('mode')}.",
            )
        )

    kill_switch_reason = resolve_execution_kill_switch_reason()
    if kill_switch_reason is not None:
        statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="kill_switch_enabled",
                message=kill_switch_reason,
            )
        )

    broker_store = storage.broker
    if broker_store.schema_ready():
        broker_sync_status, broker_sync = _broker_sync_payload(
            broker_store.get_sync_state(BROKER_SYNC_KEY),
            now=now,
            market_session=market_session,
        )
    else:
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
    statuses.append(broker_sync_status)
    details["broker_sync"] = broker_sync
    if broker_sync_status not in {"healthy", "idle"}:
        attention.append(
            _attention(
                severity="high" if broker_sync_status == "blocked" else "medium",
                code="broker_sync_unhealthy",
                message="Broker sync is missing, stale, or degraded.",
            )
        )

    account_overview: dict[str, Any] | None = None
    account_error: str | None = None
    try:
        account_overview = get_account_overview(history_range="1D", db_target=db_target)
    except Exception as exc:
        account_error = str(exc)
        statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="account_unavailable",
                message=f"Account overview is unavailable: {exc}",
            )
        )

    if account_overview is None:
        details["account"] = {}
        details["pnl"] = {}
    else:
        details["account"] = dict(account_overview.get("account") or {})
        details["pnl"] = dict(account_overview.get("pnl") or {})
        details["account_sync"] = dict(account_overview.get("sync") or {})
        if str(account_overview.get("source") or "snapshot") != "live":
            statuses.append("degraded")
            attention.append(
                _attention(
                    severity="medium",
                    code="account_snapshot_fallback",
                    message="Account overview fell back to stored broker sync snapshot data.",
                )
            )
        account = details["account"]
        if account.get("trading_blocked") or account.get("account_blocked"):
            statuses.append("blocked")
            attention.append(
                _attention(
                    severity="high",
                    code="broker_account_blocked",
                    message="The broker account is currently blocked for trading.",
                )
            )

    execution_store = storage.execution
    job_store = getattr(storage, "jobs", None)
    if (
        job_store is not None
        and hasattr(job_store, "schema_ready")
        and job_store.schema_ready()
    ):
        latest_collectors = _latest_live_collectors(storage=storage, now=now)
    else:
        latest_collectors = []
    collector_selection = _aggregate_selection_summaries(
        [row.get("selection_summary") for row in latest_collectors]
    )
    details["latest_collectors"] = latest_collectors
    details["collector_selection"] = collector_selection
    open_execution_attempts: list[dict[str, Any]]
    if execution_store.schema_ready():
        open_execution_attempts = [
            dict(row)
            for row in execution_store.list_attempts_by_status(
                statuses=sorted(OPEN_STATUSES),
                limit=200,
            )
        ]
    else:
        open_execution_attempts = []
        statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="execution_schema_unavailable",
                message="Execution attempts storage is not available yet.",
            )
        )

    submit_jobs, source_definitions = _load_execution_attempt_job_context(
        job_store=job_store,
        attempts=open_execution_attempts,
    )
    summarized_open_execution_attempts = [
        _summarize_execution_attempt(
            row,
            lifecycle=_execution_attempt_lifecycle(
                attempt=row,
                now=now,
                submit_jobs=submit_jobs,
                source_definitions=source_definitions,
            ),
        )
        for row in _sorted_by_activity(open_execution_attempts)
    ]
    stale_open_execution_count = sum(
        1 for row in summarized_open_execution_attempts if bool(row.get("stale"))
    )
    submit_unknown_execution_count = sum(
        1
        for row in summarized_open_execution_attempts
        if str(row.get("lifecycle_phase") or "") == "submit_unknown"
    )
    capacity_blocked_underlyings = sorted(
        {
            str(row.get("underlying_symbol") or "")
            for row in summarized_open_execution_attempts
            if bool(row.get("blocks_capacity"))
            and _as_text(row.get("underlying_symbol"))
        }
    )
    capacity_blocked_underlying_count = len(capacity_blocked_underlyings)
    execution_health_status = (
        "degraded"
        if stale_open_execution_count or submit_unknown_execution_count
        else "healthy"
    )
    if submit_unknown_execution_count:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="high",
                code="execution_submit_unknown",
                message=(
                    f"{submit_unknown_execution_count} open execution attempt(s) have uncertain submit "
                    "outcomes and still block capacity."
                ),
            )
        )
    elif stale_open_execution_count:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="stale_open_executions_present",
                message=(
                    f"{stale_open_execution_count} open execution attempt(s) are stale and need "
                    "reconciliation, cancellation, or operator review."
                ),
            )
        )
    elif capacity_blocked_underlyings:
        attention.append(
            _attention(
                severity="low",
                code="open_execution_capacity_reserved",
                message=(
                    "Open execution attempts currently reserve capacity for "
                    f"{', '.join(capacity_blocked_underlyings[:5])}."
                ),
            )
        )

    open_positions: list[dict[str, Any]] = []
    top_positions: list[dict[str, Any]] = []
    risk_breach_count = 0
    reconciliation_mismatch_count = 0
    missing_mark_count = 0
    stale_mark_count = 0
    if execution_store.portfolio_schema_ready():
        persisted_positions = [
            enrich_position_row(dict(row))
            for row in execution_store.list_positions(
                statuses=OPEN_POSITION_STATUSES,
                limit=200,
            )
        ]
        for position in persisted_positions:
            risk = assess_position_risk(position=position)
            close_mark = _coerce_float(position.get("close_mark"))
            mark_age_seconds = _seconds_since(position.get("close_marked_at"), now=now)
            if close_mark is None:
                missing_mark_count += 1
            elif (
                mark_age_seconds is not None
                and mark_age_seconds > MARK_STALE_AFTER_SECONDS
            ):
                stale_mark_count += 1
            if str(position.get("reconciliation_status") or "") == "mismatch":
                reconciliation_mismatch_count += 1
            if str(risk.get("status") or "") == "breach":
                risk_breach_count += 1
            realized_pnl = _coerce_float(position.get("realized_pnl")) or 0.0
            unrealized_pnl = _coerce_float(position.get("unrealized_pnl")) or 0.0
            open_positions.append(
                {
                    **position,
                    "status": position.get("status"),
                    "session_id": position.get("session_id"),
                    "risk_status": risk.get("status"),
                    "risk_note": risk.get("note"),
                    "mark_age_seconds": None
                    if mark_age_seconds is None
                    else round(mark_age_seconds, 2),
                    "net_pnl": round(realized_pnl + unrealized_pnl, 2),
                }
            )
        top_positions = _top_positions(open_positions)
    else:
        statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="position_schema_unavailable",
                message="Position storage is not available yet.",
            )
        )

    mark_error = _as_text((broker_sync.get("summary") or {}).get("mark_error"))
    broker_unquoted_positions = (
        _coerce_int((broker_sync.get("summary") or {}).get("unquoted_position_count"))
        or 0
    )
    mark_health_status = "healthy"
    if (
        missing_mark_count
        or stale_mark_count
        or broker_unquoted_positions
        or mark_error
    ):
        mark_health_status = "degraded"
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="mark_health_degraded",
                message="One or more open positions are missing or stale quote marks.",
            )
        )

    if risk_breach_count:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="risk_breaches_present",
                message=f"{risk_breach_count} open position(s) are outside snapshotted risk limits.",
            )
        )

    if reconciliation_mismatch_count:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="reconciliation_mismatches_present",
                message=f"{reconciliation_mismatch_count} open position(s) have reconciliation mismatches.",
            )
        )

    account = details.get("account") or {}
    trading_allowed = True
    if account_error is not None:
        trading_allowed = False
    elif kill_switch_reason is not None:
        trading_allowed = False
    elif str(control.get("mode") or "") != "normal":
        trading_allowed = False
    elif not bool(market_session.get("is_open")):
        trading_allowed = False
    elif broker_sync_status != "healthy":
        trading_allowed = False
    elif str((account_overview or {}).get("source") or "snapshot") != "live":
        trading_allowed = False
    elif account.get("trading_blocked") or account.get("account_blocked"):
        trading_allowed = False
    elif stale_open_execution_count or submit_unknown_execution_count:
        trading_allowed = False

    summary = {
        "trading_allowed": trading_allowed,
        "market_session_status": market_session.get("status"),
        "account_source": None
        if account_overview is None
        else account_overview.get("source"),
        "environment": None
        if account_overview is None
        else account_overview.get("environment"),
        "open_position_count": len(open_positions),
        "open_execution_count": len(open_execution_attempts),
        "stale_open_execution_count": stale_open_execution_count,
        "submit_unknown_execution_count": submit_unknown_execution_count,
        "capacity_blocked_underlying_count": capacity_blocked_underlying_count,
        "execution_health_status": execution_health_status,
        "risk_breach_count": risk_breach_count,
        "reconciliation_mismatch_count": reconciliation_mismatch_count,
        "mark_health_status": mark_health_status,
        "collector_count": len(latest_collectors),
        "collector_opportunity_count": _coerce_int(
            collector_selection.get("opportunity_count")
        )
        or 0,
        "collector_shadow_only_count": _coerce_int(
            collector_selection.get("shadow_only_count")
        )
        or 0,
        "collector_auto_live_eligible_count": _coerce_int(
            collector_selection.get("auto_live_eligible_count")
        )
        or 0,
        "account_error": account_error,
    }

    details.update(
        {
            "open_execution_attempts": summarized_open_execution_attempts,
            "open_positions": open_positions,
            "top_positions": top_positions,
            "mark_health": {
                "status": mark_health_status,
                "missing_mark_count": missing_mark_count,
                "stale_mark_count": stale_mark_count,
                "broker_unquoted_position_count": broker_unquoted_positions,
                "mark_error": mark_error,
            },
            "execution_health": {
                "status": execution_health_status,
                "stale_open_execution_count": stale_open_execution_count,
                "submit_unknown_execution_count": submit_unknown_execution_count,
                "capacity_blocked_underlying_count": capacity_blocked_underlying_count,
                "capacity_blocked_underlyings": capacity_blocked_underlyings,
            },
        }
    )
    return {
        "status": _combine_statuses(*statuses),
        "generated_at": generated_at,
        "summary": summary,
        "attention": attention,
        "details": details,
    }


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
    now = _parse_timestamp(generated_at) or datetime.now(UTC)
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
            definition, latest_run_by_key.get(str(definition["job_key"])), now=now
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
        "expires_at": None
        if scheduler_lease is None
        else scheduler_lease.get("expires_at"),
        "owner": None if scheduler_lease is None else scheduler_lease.get("owner"),
        "job_run_id": None
        if scheduler_lease is None
        else scheduler_lease.get("job_run_id"),
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
                severity="high"
                if scheduler_payload["status"] == "blocked"
                else "medium",
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
    if status_counts.get("skipped", 0):
        attention.append(
            _attention(
                severity="medium",
                code="skipped_job_runs_present",
                message=f"{status_counts['skipped']} recent job run(s) were skipped.",
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
    if actionable_definition_status_counts.get(
        "degraded", 0
    ) or actionable_definition_status_counts.get("blocked", 0):
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
            "stale_running_count": stale_running_count,
            "degraded_capture_count": degraded_capture_count,
        },
        "attention": attention,
        "details": {
            "view": "list",
            "scheduler": scheduler_payload,
            "workers": workers,
            "singleton_leases": singleton_leases,
            "stale_singleton_leases": stale_singleton_leases,
            "job_definitions": definition_rows,
            "job_runs": run_rows,
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
    now = _parse_timestamp(generated_at) or datetime.now(UTC)
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
                severity="high"
                if run_summary["operator_status"] == "blocked"
                else "medium",
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
        latest_definition_run = (
            latest_definition_runs[0] if latest_definition_runs else None
        )
        definition_summary = _summarize_job_definition(
            definition, latest_definition_run, now=now
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
            "selection_summary": _selection_summary_payload(
                run.get("selection_summary")
            ),
            "singleton_lease": None
            if singleton_lease is None
            else dict(singleton_lease),
        },
    }


@with_storage()
def build_audit_view(
    *,
    pipeline_id: str,
    market_date: str,
    db_target: str | None = None,
    timeline_limit: int = 120,
    event_scan_limit: int = DEFAULT_EVENT_SCAN_LIMIT,
    storage: Any | None = None,
) -> dict[str, Any]:
    generated_at = _utc_now()
    try:
        replay = build_audit_replay(
            db_target=db_target or "",
            pipeline_id=pipeline_id,
            market_date=market_date,
            timeline_limit=timeline_limit,
            event_scan_limit=event_scan_limit,
            storage=storage,
        )
    except ValueError as exc:
        raise OpsLookupError(str(exc)) from exc

    target = replay.get("target") if isinstance(replay.get("target"), Mapping) else {}
    timeline_stats = (
        replay.get("timeline_stats")
        if isinstance(replay.get("timeline_stats"), Mapping)
        else {}
    )
    state_summary = (
        replay.get("state_summary")
        if isinstance(replay.get("state_summary"), Mapping)
        else {}
    )
    explanations = (
        replay.get("explanations")
        if isinstance(replay.get("explanations"), Mapping)
        else {}
    )
    control = (
        state_summary.get("control_snapshot")
        if isinstance(state_summary.get("control_snapshot"), Mapping)
        else {}
    )
    current_cycle = (
        state_summary.get("current_cycle")
        if isinstance(state_summary.get("current_cycle"), Mapping)
        else {}
    )
    counts = (
        state_summary.get("counts")
        if isinstance(state_summary.get("counts"), Mapping)
        else {}
    )
    portfolio = (
        state_summary.get("portfolio")
        if isinstance(state_summary.get("portfolio"), Mapping)
        else {}
    )
    portfolio_summary = (
        portfolio.get("summary")
        if isinstance(portfolio.get("summary"), Mapping)
        else {}
    )
    post_market = (
        replay.get("post_market")
        if isinstance(replay.get("post_market"), Mapping)
        else {}
    )
    selected_opportunities = [
        dict(row)
        for row in list(explanations.get("selected_opportunities") or [])
        if isinstance(row, Mapping)
    ]
    risk_decisions = [
        dict(row)
        for row in list(explanations.get("risk_decisions") or [])
        if isinstance(row, Mapping)
    ]
    execution_outcomes = [
        dict(row)
        for row in list(explanations.get("execution_outcomes") or [])
        if isinstance(row, Mapping)
    ]
    control_actions = [
        dict(row)
        for row in list(explanations.get("control_actions") or [])
        if isinstance(row, Mapping)
    ]

    attention: list[dict[str, str]] = []
    statuses = [
        _session_status(target.get("status") or state_summary.get("status")),
        _control_status(control),
    ]

    target_status = _session_status(target.get("status") or state_summary.get("status"))
    if target_status == "blocked":
        attention.append(
            _attention(
                severity="high",
                code="audit_pipeline_run_failed",
                message=(
                    f"Pipeline {target.get('pipeline_id') or pipeline_id} on "
                    f"{target.get('market_date') or market_date} is recorded as failed."
                ),
            )
        )
    elif target_status == "degraded":
        attention.append(
            _attention(
                severity="medium",
                code="audit_pipeline_run_degraded",
                message=(
                    f"Pipeline {target.get('pipeline_id') or pipeline_id} on "
                    f"{target.get('market_date') or market_date} is degraded."
                ),
            )
        )

    control_mode = _as_text(control.get("mode"))
    if control_mode == "halted":
        attention.append(
            _attention(
                severity="high",
                code="audit_control_halted",
                message="Control mode was halted during the session.",
            )
        )
    elif control_mode == "degraded":
        attention.append(
            _attention(
                severity="medium",
                code="audit_control_degraded",
                message="Control mode was degraded during the session.",
            )
        )

    risk_status = str(target.get("risk_status") or "").strip().lower()
    if risk_status == "blocked":
        statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="audit_risk_blocked",
                message=_as_text(target.get("risk_note"))
                or "Pipeline run risk state was blocked.",
            )
        )
    elif risk_status not in {"", "ok", "disabled"}:
        statuses.append("degraded")

    reconciliation_status = (
        str(target.get("reconciliation_status") or "").strip().lower()
    )
    if reconciliation_status == "mismatch":
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="audit_reconciliation_mismatch",
                message=_as_text(target.get("reconciliation_note"))
                or "Pipeline run reconciliation had mismatches.",
            )
        )

    weak_verdict = (
        str(post_market.get("overall_verdict") or "").strip().lower() == "weak"
    )
    if weak_verdict:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="audit_post_market_weak",
                message="Post-market verdict is weak.",
            )
        )

    blocked_risk_count = sum(
        1
        for row in risk_decisions
        if str(row.get("status") or "").strip().lower()
        in {"blocked", "rejected", "denied"}
    )
    if blocked_risk_count:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="audit_risk_decisions_blocked",
                message=f"{blocked_risk_count} risk decision(s) were blocked by policy.",
            )
        )

    failed_execution_count = sum(
        1
        for row in execution_outcomes
        if _as_text(row.get("error_text")) is not None
        or str(row.get("status") or "").strip().lower() in {"failed", "rejected"}
    )
    if failed_execution_count:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="high",
                code="audit_execution_failed",
                message=f"{failed_execution_count} execution attempt(s) failed or were rejected.",
            )
        )

    open_execution_count = sum(
        1
        for row in execution_outcomes
        if str(row.get("status") or "").strip().lower() in OPEN_STATUSES
    )
    if open_execution_count:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="audit_execution_open",
                message=f"{open_execution_count} execution attempt(s) are still open.",
            )
        )

    if bool(timeline_stats.get("timeline_truncated")):
        attention.append(
            _attention(
                severity="low",
                code="audit_timeline_truncated",
                message=(
                    f"Timeline output was truncated to {timeline_stats.get('returned_timeline_item_count')} "
                    f"items; {timeline_stats.get('omitted_timeline_item_count')} item(s) were omitted."
                ),
            )
        )

    if bool(timeline_stats.get("event_scan_limit_hit")):
        attention.append(
            _attention(
                severity="low",
                code="audit_event_scan_limited",
                message=(
                    f"Replay hit the event scan limit of {timeline_stats.get('event_scan_limit')}; "
                    "older events may be omitted."
                ),
            )
        )

    return {
        "status": _combine_statuses(*statuses),
        "generated_at": generated_at,
        "summary": {
            "view": "audit",
            "pipeline_id": target.get("pipeline_id") or pipeline_id,
            "label": target.get("label"),
            "market_date": target.get("market_date") or market_date,
            "run_status": target.get("status") or state_summary.get("status"),
            "control_mode": control.get("mode"),
            "risk_status": target.get("risk_status"),
            "reconciliation_status": target.get("reconciliation_status"),
            "alert_count": counts.get("alerts"),
            "opportunity_count": counts.get("opportunities"),
            "risk_decision_count": counts.get("risk_decisions"),
            "execution_count": counts.get("executions"),
            "timeline_item_count": timeline_stats.get("timeline_item_count"),
            "returned_timeline_item_count": timeline_stats.get(
                "returned_timeline_item_count"
            ),
            "post_market_verdict": post_market.get("overall_verdict"),
            "net_pnl_total": portfolio_summary.get("net_pnl_total"),
        },
        "attention": attention[:10],
        "details": {
            "view": "audit",
            "target": dict(target),
            "control": dict(control),
            "current_cycle": dict(current_cycle),
            "counts": dict(counts),
            "portfolio_summary": dict(portfolio_summary),
            "post_market": dict(post_market),
            "slot_runs": [
                dict(row)
                for row in list(replay.get("slot_runs") or [])
                if isinstance(row, Mapping)
            ],
            "alerts": [
                dict(row)
                for row in list(replay.get("alerts") or [])
                if isinstance(row, Mapping)
            ],
            "selected_opportunities": selected_opportunities,
            "risk_decisions": risk_decisions,
            "execution_outcomes": execution_outcomes,
            "control_actions": control_actions,
            "timeline_stats": dict(timeline_stats),
            "timeline": [
                dict(row)
                for row in list(replay.get("timeline") or [])
                if isinstance(row, Mapping)
            ],
        },
    }


def _count_rows_from_mapping(
    mapping: Mapping[str, Any] | None,
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    if not isinstance(mapping, Mapping):
        return []
    rows: list[dict[str, Any]] = []
    for key, value in mapping.items():
        count = _coerce_int(value)
        if count is None or count <= 0:
            continue
        rows.append({"name": str(key), "count": count})
    rows.sort(key=lambda row: (-int(row["count"]), str(row["name"])))
    return rows[:limit]


def _summarize_uoa_root(root: Mapping[str, Any]) -> dict[str, Any]:
    quote_context = (
        root.get("quote_context")
        if isinstance(root.get("quote_context"), Mapping)
        else {}
    )
    current = root.get("current") if isinstance(root.get("current"), Mapping) else {}
    return {
        "underlying_symbol": root.get("underlying_symbol"),
        "root_score": _coerce_float(root.get("root_score")),
        "scoreable_premium": _coerce_float(root.get("scoreable_premium")),
        "scoreable_trade_count": _coerce_int(root.get("scoreable_trade_count")),
        "scoreable_contract_count": _coerce_int(root.get("scoreable_contract_count")),
        "dominant_flow": root.get("dominant_flow") or current.get("dominant_flow"),
        "dominant_flow_ratio": _coerce_float(
            root.get("dominant_flow_ratio") or current.get("dominant_flow_ratio")
        ),
        "supporting_volume_oi_ratio": _coerce_float(
            root.get("supporting_volume_oi_ratio")
            or current.get("supporting_volume_oi_ratio")
        ),
        "max_volume_oi_ratio": _coerce_float(
            root.get("max_volume_oi_ratio") or current.get("max_volume_oi_ratio")
        ),
        "decision_state": root.get("decision_state"),
        "decision_score": _coerce_float(root.get("decision_score")),
        "quality_state": quote_context.get("quality_state"),
        "liquid_contract_count": _coerce_int(
            quote_context.get("liquid_contract_count")
        ),
        "reason_codes": list(root.get("reason_codes") or []),
        "explanation": _as_text(root.get("explanation")),
    }


def _summarize_uoa_contract(contract: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "option_symbol": contract.get("option_symbol"),
        "underlying_symbol": contract.get("underlying_symbol"),
        "strategy": contract.get("strategy"),
        "option_type": contract.get("option_type"),
        "dte": _coerce_int(contract.get("dte")),
        "percent_otm": _coerce_float(contract.get("percent_otm")),
        "scoreable_premium": _coerce_float(contract.get("scoreable_premium")),
        "scoreable_trade_count": _coerce_int(contract.get("scoreable_trade_count")),
        "contract_score": _coerce_float(contract.get("contract_score")),
        "quality_score": _coerce_float(contract.get("quality_score")),
        "quality_state": contract.get("quality_state"),
        "midpoint": _coerce_float(contract.get("midpoint")),
        "spread_pct": _coerce_float(contract.get("spread_pct")),
        "volume": _coerce_int(contract.get("volume")),
        "open_interest": _coerce_int(contract.get("open_interest")),
        "volume_oi_ratio": _coerce_float(contract.get("volume_oi_ratio")),
        "implied_volatility": _coerce_float(contract.get("implied_volatility")),
        "delta": _coerce_float(contract.get("delta")),
        "quote_age_seconds": _coerce_float(contract.get("quote_age_seconds")),
    }


def _summarize_uoa_candidate(candidate: Mapping[str, Any]) -> dict[str, Any]:
    payload = (
        candidate.get("candidate")
        if isinstance(candidate.get("candidate"), Mapping)
        else {}
    )
    return {
        "candidate_id": candidate.get("candidate_id"),
        "selection_state": candidate.get("selection_state"),
        "selection_rank": _coerce_int(candidate.get("selection_rank")),
        "underlying_symbol": candidate.get("underlying_symbol"),
        "strategy": candidate.get("strategy") or payload.get("strategy"),
        "short_symbol": candidate.get("short_symbol") or payload.get("short_symbol"),
        "long_symbol": candidate.get("long_symbol") or payload.get("long_symbol"),
        "expiration_date": candidate.get("expiration_date")
        or payload.get("expiration_date"),
        "dte": _coerce_int(payload.get("days_to_expiration")),
        "quality_score": _coerce_float(
            candidate.get("quality_score") or payload.get("quality_score")
        ),
        "midpoint_credit": _coerce_float(
            candidate.get("midpoint_credit") or payload.get("midpoint_credit")
        ),
        "max_loss": _coerce_float(payload.get("max_loss")),
        "return_on_risk": _coerce_float(payload.get("return_on_risk")),
        "underlying_price": _coerce_float(payload.get("underlying_price")),
        "setup_status": payload.get("setup_status"),
    }


def _summarize_uoa_event(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": event.get("generated_at"),
        "symbol": event.get("symbol"),
        "event_type": event.get("event_type"),
        "message": event.get("message"),
    }


def _uoa_quote_status(
    quote_capture: Mapping[str, Any],
    quote_overview: Mapping[str, Any],
) -> tuple[str, str | None]:
    capture_status = str(quote_capture.get("capture_status") or "").strip().lower()
    expected_count = (
        _coerce_int(
            quote_capture.get("expected_quote_symbol_count")
            or quote_overview.get("expected_contract_count")
        )
        or 0
    )
    total_saved = _coerce_int(quote_capture.get("total_quote_events_saved")) or 0
    fresh_count = _coerce_int(quote_overview.get("fresh_contract_count")) or 0
    liquid_count = _coerce_int(quote_overview.get("liquid_contract_count")) or 0

    if capture_status == "healthy":
        if expected_count > 0 and fresh_count <= 0:
            return "degraded", "Expected UOA contracts have no fresh quotes."
        if expected_count > 0 and liquid_count <= 0:
            return "degraded", "Expected UOA contracts have no liquid quotes."
        return "healthy", None
    if capture_status == "idle":
        return "idle", None
    if capture_status == "baseline_only":
        return "degraded", "UOA quote capture fell back to baseline-only quotes."
    if capture_status == "recovery_only":
        return "degraded", "UOA quote capture relied only on recovery quotes."
    if capture_status == "empty":
        if expected_count > 0 or total_saved > 0:
            return "degraded", "Expected UOA contracts produced no quote capture."
        return "idle", None
    return "unknown", None


def _uoa_trade_status(
    trade_capture: Mapping[str, Any],
    uoa_overview: Mapping[str, Any],
) -> tuple[str, str | None]:
    capture_status = str(trade_capture.get("capture_status") or "").strip().lower()
    expected_count = _coerce_int(trade_capture.get("expected_trade_symbol_count")) or 0
    raw_trade_count = _coerce_int(uoa_overview.get("raw_trade_count")) or 0
    scoreable_trade_count = _coerce_int(uoa_overview.get("scoreable_trade_count")) or 0
    excluded_trade_count = _coerce_int(uoa_overview.get("excluded_trade_count")) or 0

    if capture_status == "healthy":
        return "healthy", None
    if capture_status == "idle":
        return "idle", None
    if capture_status == "baseline_only" and raw_trade_count > 0:
        return (
            "degraded",
            "UOA trade capture missed websocket trades and used baseline-only data.",
        )
    if capture_status == "empty":
        if raw_trade_count <= 0 and scoreable_trade_count <= 0 and expected_count > 0:
            return "idle", None
        if excluded_trade_count > 0 and scoreable_trade_count <= 0:
            return "idle", None
    return "unknown", None


def _uoa_activity_status(
    uoa_overview: Mapping[str, Any],
    decision_overview: Mapping[str, Any],
) -> str:
    summary_status = str(uoa_overview.get("summary_status") or "").strip().lower()
    decision_status = (
        str(decision_overview.get("decision_status") or "").strip().lower()
    )
    if summary_status == "active" or decision_status == "active":
        return "healthy"
    if summary_status in {
        "empty",
        "captured_no_scoreable_trades",
    } and decision_status in {"", "empty"}:
        return "idle"
    return "unknown"


def _uoa_overall_status(
    *,
    cycle_present: bool,
    quote_status: str,
    trade_status: str,
    activity_status: str,
) -> str:
    if not cycle_present:
        return "degraded"
    if "degraded" in {quote_status, trade_status, activity_status}:
        return "degraded"
    if activity_status == "healthy":
        return "healthy"
    if activity_status == "idle":
        return "idle"
    if quote_status == "healthy" or trade_status == "healthy":
        return "healthy"
    return "unknown"


def _build_uoa_payload(
    *,
    state: Mapping[str, Any],
    generated_at: str,
    view: str,
    requested_label: str | None = None,
) -> dict[str, Any]:
    cycle = state.get("cycle") if isinstance(state.get("cycle"), Mapping) else {}
    job_run = state.get("job_run") if isinstance(state.get("job_run"), Mapping) else {}
    quote_capture = dict(state.get("quote_capture") or {})
    trade_capture = dict(state.get("trade_capture") or {})
    uoa_summary = (
        state.get("uoa_summary")
        if isinstance(state.get("uoa_summary"), Mapping)
        else {}
    )
    uoa_overview = (
        uoa_summary.get("overview")
        if isinstance(uoa_summary.get("overview"), Mapping)
        else {}
    )
    uoa_quote_summary = (
        state.get("uoa_quote_summary")
        if isinstance(state.get("uoa_quote_summary"), Mapping)
        else {}
    )
    uoa_quote_overview = (
        uoa_quote_summary.get("overview")
        if isinstance(uoa_quote_summary.get("overview"), Mapping)
        else {}
    )
    uoa_decisions = (
        state.get("uoa_decisions")
        if isinstance(state.get("uoa_decisions"), Mapping)
        else {}
    )
    uoa_decision_overview = (
        uoa_decisions.get("overview")
        if isinstance(uoa_decisions.get("overview"), Mapping)
        else {}
    )
    opportunities = list(state.get("opportunities") or [])
    promotable_candidates = [
        _summarize_uoa_candidate(row)
        for row in opportunities[:UOA_CANDIDATE_LIMIT]
        if isinstance(row, Mapping)
        and str(row.get("selection_state") or "") == "promotable"
    ]
    monitor_candidates = [
        _summarize_uoa_candidate(row)
        for row in opportunities[:UOA_CANDIDATE_LIMIT]
        if isinstance(row, Mapping)
        and str(row.get("selection_state") or "") == "monitor"
    ]
    cycle_events = [
        _summarize_uoa_event(row)
        for row in list(state.get("cycle_events") or [])[:UOA_EVENT_LIMIT]
        if isinstance(row, Mapping)
    ]
    top_roots = [
        _summarize_uoa_root(row)
        for row in list(uoa_summary.get("top_roots") or [])[:UOA_ROOT_LIMIT]
        if isinstance(row, Mapping)
    ]
    top_contracts = [
        _summarize_uoa_contract(row)
        for row in list(uoa_summary.get("top_contracts") or [])[:UOA_CONTRACT_LIMIT]
        if isinstance(row, Mapping)
    ]
    top_monitor_roots = [
        _summarize_uoa_root(row)
        for row in list(uoa_decisions.get("top_monitor_roots") or [])[:UOA_ROOT_LIMIT]
        if isinstance(row, Mapping)
    ]
    top_promotable_roots = [
        _summarize_uoa_root(row)
        for row in list(uoa_decisions.get("top_promotable_roots") or [])[
            :UOA_ROOT_LIMIT
        ]
        if isinstance(row, Mapping)
    ]
    top_high_roots = [
        _summarize_uoa_root(row)
        for row in list(uoa_decisions.get("top_high_roots") or [])[:UOA_ROOT_LIMIT]
        if isinstance(row, Mapping)
    ]
    top_exclusion_reasons = _count_rows_from_mapping(
        uoa_overview.get("excluded_reason_counts")
        if isinstance(uoa_overview, Mapping)
        else None
    )
    top_conditions = _count_rows_from_mapping(
        uoa_overview.get("condition_counts")
        if isinstance(uoa_overview, Mapping)
        else None
    )

    attention: list[dict[str, str]] = []
    cycle_present = bool(cycle)
    if not cycle_present:
        attention.append(
            _attention(
                severity="high",
                code="uoa_cycle_missing",
                message="Collector cycle payload is missing for the selected UOA state.",
            )
        )

    quote_status, quote_message = _uoa_quote_status(quote_capture, uoa_quote_overview)
    if quote_message is not None:
        attention.append(
            _attention(
                severity="medium",
                code="uoa_quote_capture_unhealthy",
                message=quote_message,
            )
        )

    trade_status, trade_message = _uoa_trade_status(trade_capture, uoa_overview)
    if trade_message is not None:
        attention.append(
            _attention(
                severity="medium",
                code="uoa_trade_capture_unhealthy",
                message=trade_message,
            )
        )

    raw_trade_count = _coerce_int(uoa_overview.get("raw_trade_count")) or 0
    scoreable_trade_count = _coerce_int(uoa_overview.get("scoreable_trade_count")) or 0
    excluded_trade_count = _coerce_int(uoa_overview.get("excluded_trade_count")) or 0
    if excluded_trade_count >= 3 and excluded_trade_count > scoreable_trade_count:
        attention.append(
            _attention(
                severity="medium",
                code="uoa_exclusions_dominate",
                message="Most captured UOA trades were excluded from scoring.",
            )
        )

    activity_status = _uoa_activity_status(uoa_overview, uoa_decision_overview)
    overall_status = _uoa_overall_status(
        cycle_present=cycle_present,
        quote_status=quote_status,
        trade_status=trade_status,
        activity_status=activity_status,
    )

    return {
        "status": overall_status,
        "generated_at": generated_at,
        "summary": {
            "view": view,
            "label": cycle.get("label") or requested_label,
            "cycle_id": cycle.get("cycle_id"),
            "session_id": cycle.get("session_id") or job_run.get("session_id"),
            "session_date": cycle.get("session_date"),
            "job_run_id": job_run.get("job_run_id"),
            "job_key": job_run.get("job_key"),
            "slot_at": job_run.get("slot_at"),
            "quote_capture_status": quote_capture.get("capture_status"),
            "trade_capture_status": trade_capture.get("capture_status"),
            "uoa_summary_status": uoa_overview.get("summary_status"),
            "decision_status": uoa_decision_overview.get("decision_status"),
            "observed_contract_count": uoa_overview.get("observed_contract_count"),
            "scoreable_root_count": uoa_overview.get("scoreable_root_count"),
            "raw_trade_count": raw_trade_count,
            "scoreable_trade_count": scoreable_trade_count,
            "excluded_trade_count": excluded_trade_count,
            "root_count": uoa_decision_overview.get("root_count"),
            "monitor_count": uoa_decision_overview.get("monitor_count"),
            "promotable_count": uoa_decision_overview.get("promotable_count"),
            "high_count": uoa_decision_overview.get("high_count"),
            "top_decision_symbol": uoa_decision_overview.get("top_decision_symbol"),
            "top_decision_state": uoa_decision_overview.get("top_decision_state"),
            "top_decision_score": uoa_decision_overview.get("top_decision_score"),
            "selected_promotable_count": int(
                dict(state.get("selection_counts") or {}).get(
                    PROMOTABLE_SELECTION_STATE
                )
                or 0
            ),
            "selected_monitor_count": int(
                dict(state.get("selection_counts") or {}).get(MONITOR_SELECTION_STATE)
                or 0
            ),
            "event_count": len(list(state.get("cycle_events") or [])),
        },
        "attention": attention,
        "details": {
            "view": view,
            "job_run": dict(job_run),
            "cycle": dict(cycle),
            "quote_capture": quote_capture,
            "trade_capture": trade_capture,
            "uoa_overview": dict(uoa_overview),
            "uoa_quote_overview": dict(uoa_quote_overview),
            "uoa_decision_overview": dict(uoa_decision_overview),
            "top_roots": top_roots,
            "top_contracts": top_contracts,
            "top_monitor_roots": top_monitor_roots,
            "top_promotable_roots": top_promotable_roots,
            "top_high_roots": top_high_roots,
            "promotable_candidates": promotable_candidates,
            "monitor_candidates": monitor_candidates,
            "cycle_events": cycle_events,
            "selection_memory": dict(cycle.get("selection_memory") or {}),
            "selection_counts": dict(state.get("selection_counts") or {}),
            "top_exclusion_reasons": top_exclusion_reasons,
            "top_conditions": top_conditions,
        },
    }


@with_storage()
def build_uoa_overview(
    *,
    db_target: str | None = None,
    label: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    generated_at = _utc_now()
    try:
        state = get_latest_uoa_state(
            db_target=db_target,
            label=label,
            storage=storage,
        )
    except ValueError as exc:
        raise OpsLookupError(str(exc)) from exc
    return _build_uoa_payload(
        state=state,
        generated_at=generated_at,
        view="latest",
        requested_label=label,
    )


@with_storage()
def build_uoa_cycle_view(
    *,
    cycle_id: str,
    db_target: str | None = None,
    label: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    generated_at = _utc_now()
    try:
        state = get_uoa_state_for_cycle(
            db_target=db_target,
            cycle_id=cycle_id,
            label=label,
            storage=storage,
        )
    except ValueError as exc:
        raise OpsLookupError(str(exc)) from exc
    return _build_uoa_payload(
        state=state,
        generated_at=generated_at,
        view="cycle",
        requested_label=label,
    )
