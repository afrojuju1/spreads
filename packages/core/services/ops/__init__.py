from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import func, select

from core.db.decorators import with_storage
from core.jobs.registry import WORKER_LANES, get_queue_name_for_job_type
from core.jobs.orchestration import (
    NEW_YORK,
    SCHEDULER_RUNTIME_LEASE_KEY,
    SINGLETON_LEASE_PREFIX,
    WORKER_RUNTIME_LEASE_PREFIX,
    _market_schedule,
    singleton_lease_key,
)
from core.services.account_state import get_account_overview
from core.services.bot_analytics import build_automation_performance_summary
from core.services.bots import load_active_bots
from core.services.broker_sync import BROKER_SYNC_KEY
from core.services.control_plane import (
    get_control_state_snapshot,
    resolve_execution_kill_switch_reason,
)
from core.services.execution import OPEN_STATUSES
from core.services.execution_lifecycle import (
    classify_open_execution_attempt,
    is_open_execution_attempt_status,
    resolve_execution_attempt_source_job,
    resolve_execution_submit_job_run_id,
)
from core.services.live_collector_health import enrich_live_collector_job_run_payload
from core.services.live_pipelines import (
    list_enabled_live_collector_pipelines,
    resolve_live_collector_label,
)
from core.services.live_runtime import list_latest_live_sessions
from core.services.positions import enrich_position_row
from core.services.risk_manager import assess_position_risk
from core.services.selection_summary import (
    aggregate_selection_summaries as _aggregate_selection_summaries,
    selection_summary_payload as _selection_summary_payload,
)
from core.services.selection_terms import (
    normalize_selection_state,
    promotable_monitor_pnl_spread,
)
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_float as _coerce_float,
    coerce_int as _coerce_int,
    utc_now_iso as _utc_now,
)
from core.storage.execution_models import ExecutionIntentModel
from core.storage.signal_models import OpportunityDecisionModel, OpportunityModel

from .audit import build_audit_view
from .shared import (
    OpsLookupError,
    _attention,
    _combine_statuses,
    _control_status,
    _parse_timestamp,
    _session_status,
)
from .uoa import build_uoa_cycle_view, build_uoa_overview

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
    run_status = str(run.get("status") or "")
    capture_status = str(run.get("capture_status") or "")
    if run_status == "running" and capture_status in {"healthy", "idle"}:
        return "healthy"
    if run_status != "succeeded":
        return "degraded"
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
        raw_candidate_summary = (
            {}
            if session is None
            or not isinstance(session.get("raw_candidate_summary"), Mapping)
            else dict(session.get("raw_candidate_summary") or {})
        )
        capture_status = None if run is None else run.get("capture_status")
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
                "raw_candidate_summary": raw_candidate_summary,
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


def _active_options_automation_labels(job_store: Any) -> set[str]:
    if job_store is None or not job_store.schema_ready():
        return set()
    labels: set[str] = set()
    for definition in job_store.list_job_definitions(
        enabled_only=True, job_type="live_collector"
    ):
        payload = dict(definition.get("payload") or {})
        if not bool(payload.get("options_automation_enabled", False)):
            continue
        labels.add(resolve_live_collector_label(payload))
    return labels


def _bot_runtime_summary(*, storage: Any, market_date: str) -> dict[str, Any]:
    bots = load_active_bots()
    bot_ids = sorted(bots)
    summary = {
        "bot_count": len(bot_ids),
        "entry_automation_count": sum(
            1
            for bot in bots.values()
            for automation in bot.automations
            if automation.automation.is_entry
        ),
        "management_automation_count": sum(
            1
            for bot in bots.values()
            for automation in bot.automations
            if automation.automation.is_management
        ),
        "opportunity_count": 0,
        "decision_count": 0,
        "decision_state_counts": {},
        "intent_count": 0,
        "intent_state_counts": {},
        "open_position_count": 0,
        "open_position_symbols": {},
    }
    if not bot_ids:
        return summary

    market_day = date.fromisoformat(market_date)
    window_start = datetime.fromisoformat(market_date).replace(tzinfo=UTC)
    window_end = window_start + timedelta(days=1)
    labels = _active_options_automation_labels(storage.jobs)
    signal_store = storage.signals
    execution_store = storage.execution

    if signal_store.schema_ready():
        with signal_store.session_factory() as session:
            if labels:
                summary["opportunity_count"] = int(
                    session.scalar(
                        select(func.count())
                        .select_from(OpportunityModel)
                        .where(OpportunityModel.market_date == market_day)
                        .where(OpportunityModel.label.in_(sorted(labels)))
                    )
                    or 0
                )
            decision_rows = session.execute(
                select(OpportunityDecisionModel.state, func.count())
                .where(OpportunityDecisionModel.bot_id.in_(bot_ids))
                .where(OpportunityDecisionModel.decided_at >= window_start)
                .where(OpportunityDecisionModel.decided_at < window_end)
                .group_by(OpportunityDecisionModel.state)
                .order_by(OpportunityDecisionModel.state.asc())
            ).all()
            decision_state_counts = {
                str(state): int(count) for state, count in decision_rows
            }
            summary["decision_state_counts"] = decision_state_counts
            summary["decision_count"] = sum(decision_state_counts.values())

    if execution_store.intent_schema_ready():
        with execution_store.session_factory() as session:
            intent_rows = session.execute(
                select(ExecutionIntentModel.state, func.count())
                .where(ExecutionIntentModel.bot_id.in_(bot_ids))
                .where(ExecutionIntentModel.created_at >= window_start)
                .where(ExecutionIntentModel.created_at < window_end)
                .group_by(ExecutionIntentModel.state)
                .order_by(ExecutionIntentModel.state.asc())
            ).all()
            intent_state_counts = {
                str(state): int(count) for state, count in intent_rows
            }
            summary["intent_state_counts"] = intent_state_counts
            summary["intent_count"] = sum(intent_state_counts.values())

    if (
        execution_store.portfolio_schema_ready()
        and execution_store.intent_schema_ready()
    ):
        symbol_counts: Counter[str] = Counter()
        open_positions = [
            enrich_position_row(dict(position))
            for position in execution_store.list_positions(
                statuses=OPEN_POSITION_STATUSES,
                limit=200,
            )
        ]
        for position in open_positions:
            open_execution_attempt_id = _as_text(
                position.get("open_execution_attempt_id")
            )
            if open_execution_attempt_id is None:
                continue
            attempt = execution_store.get_attempt(open_execution_attempt_id)
            if attempt is None:
                continue
            request = (
                attempt.get("request")
                if isinstance(attempt.get("request"), Mapping)
                else {}
            )
            execution_intent_id = _as_text(request.get("execution_intent_id"))
            if execution_intent_id is None:
                continue
            intent = execution_store.get_execution_intent(execution_intent_id)
            if intent is None or str(intent.get("bot_id") or "") not in bot_ids:
                continue
            symbol_counts[
                str(
                    position.get("underlying_symbol")
                    or position.get("root_symbol")
                    or "unknown"
                )
            ] += 1
        summary["open_position_count"] = int(sum(symbol_counts.values()))
        summary["open_position_symbols"] = dict(sorted(symbol_counts.items()))
    return summary


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
            if reason == "outside_schedule_window":
                return (
                    "healthy",
                    "Job run was skipped because it was outside its configured schedule window.",
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
            "automation_runtime": _bot_runtime_summary(
                storage=storage,
                market_date=market_date,
            ),
            "automation_performance": build_automation_performance_summary(
                storage=storage,
                market_date=market_date,
            ),
            "broker_sync": broker_sync,
            "alert_delivery": alert_delivery,
        }
    )

    collector_selection = dict(details.get("collector_selection") or {})
    automation_runtime = dict(details.get("automation_runtime") or {})
    automation_performance = dict(details.get("automation_performance") or {})
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
        "automation_opportunity_count": _coerce_int(
            automation_runtime.get("opportunity_count")
        )
        or 0,
        "automation_selected_count": _coerce_int(
            (automation_runtime.get("decision_state_counts") or {}).get("selected")
        )
        or 0,
        "automation_intent_count": _coerce_int(automation_runtime.get("intent_count"))
        or 0,
        "automation_open_position_count": _coerce_int(
            automation_runtime.get("open_position_count")
        )
        or 0,
        "automation_daily_pnl": _coerce_float(
            automation_performance.get("daily_total_pnl")
        ),
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
    market_date = now.astimezone(NEW_YORK).date().isoformat()
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
    details["automation_runtime"] = _bot_runtime_summary(
        storage=storage,
        market_date=market_date,
    )
    details["automation_performance"] = build_automation_performance_summary(
        storage=storage,
        market_date=market_date,
    )
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
        "automation_opportunity_count": _coerce_int(
            (details.get("automation_runtime") or {}).get("opportunity_count")
        )
        or 0,
        "automation_selected_count": _coerce_int(
            (
                (details.get("automation_runtime") or {}).get("decision_state_counts")
                or {}
            ).get("selected")
        )
        or 0,
        "automation_intent_count": _coerce_int(
            (details.get("automation_runtime") or {}).get("intent_count")
        )
        or 0,
        "automation_open_position_count": _coerce_int(
            (details.get("automation_runtime") or {}).get("open_position_count")
        )
        or 0,
        "automation_daily_pnl": _coerce_float(
            (details.get("automation_performance") or {}).get("daily_total_pnl")
        ),
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

