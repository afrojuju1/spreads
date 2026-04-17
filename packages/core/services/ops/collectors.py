from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import func, select

from core.jobs.orchestration import NEW_YORK, _market_schedule
from core.services.bots import load_active_bots
from core.services.live_pipelines import (
    list_enabled_live_collector_pipelines,
    resolve_live_collector_label,
)
from core.services.live_runtime import list_latest_live_sessions
from core.services.positions import enrich_position_row
from core.services.selection_summary import selection_summary_payload as _selection_summary_payload
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_int as _coerce_int,
)
from core.storage.execution_models import ExecutionIntentModel
from core.storage.signal_models import OpportunityDecisionModel, OpportunityModel

from .shared import (
    _is_recent,
    _stream_quote_events_saved,
)

OPEN_POSITION_STATUSES = ["open", "partial_close"]


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
        automation_summary = (
            {}
            if session is None
            or not isinstance(session.get("automation_summary"), Mapping)
            else dict(session.get("automation_summary") or {})
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
                "automation_summary": automation_summary,
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

    if execution_store.portfolio_schema_ready() and execution_store.intent_schema_ready():
        symbol_counts: Counter[str] = Counter()
        open_positions = [
            enrich_position_row(dict(position))
            for position in execution_store.list_positions(
                statuses=OPEN_POSITION_STATUSES,
                limit=200,
            )
        ]
        for position in open_positions:
            open_execution_attempt_id = _as_text(position.get("open_execution_attempt_id"))
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
