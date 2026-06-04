from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from core.jobs.specs import list_declared_job_rows
from core.services.live_pipelines import (
    list_enabled_discovery_run_pipelines,
)
from core.services.live_runtime import list_latest_live_sessions
from core.services.discovery_run_health.schedule import (
    evaluate_discovery_run_schedule_health,
)
from core.services.positions import enrich_position_row
from core.services.selection_summary import selection_summary_payload as _selection_summary_payload
from core.services.trading_strategies import load_active_trading_strategies
from core.services.value_coercion import coerce_int as _coerce_int
from core.storage.serializers import parse_datetime

from .jobs import _job_run_operator_status
from .shared import (
    _is_recent,
    _stream_quote_events_saved,
)

OPEN_POSITION_STATUSES = ["open", "partial_close"]


def _discovery_run_status(
    run: Mapping[str, Any] | None,
    *,
    now: datetime,
    schedule_summary: Mapping[str, Any] | None = None,
) -> str:
    if run is None:
        schedule_health = evaluate_discovery_run_schedule_health(
            schedule_summary=schedule_summary,
            latest_run=run,
            now=now,
        )
        if bool(schedule_health.get("overdue")):
            return "degraded"
        return "unknown"
    live_action_gate = run.get("live_action_gate") if isinstance(run.get("live_action_gate"), Mapping) else {}
    if str(live_action_gate.get("status") or "") == "blocked":
        return "blocked"
    job_operator_status, _ = _job_run_operator_status(run, now=now)
    if job_operator_status in {"blocked", "degraded"}:
        return job_operator_status
    run_status = str(run.get("status") or "")
    capture_status = str(run.get("capture_status") or "")
    if run_status in {"queued", "running"} and job_operator_status == "healthy":
        return "healthy"
    schedule_health = evaluate_discovery_run_schedule_health(
        schedule_summary=schedule_summary,
        latest_run=run,
        now=now,
    )
    if bool(schedule_health.get("overdue")):
        return "degraded"
    if run_status != "succeeded":
        return job_operator_status if job_operator_status != "unknown" else "degraded"
    if capture_status == "healthy":
        return "healthy"
    if capture_status == "idle":
        return "idle"
    return "degraded"


def _discovery_run_requires_attention(
    run: Mapping[str, Any] | None,
    *,
    now: datetime,
    schedule_summary: Mapping[str, Any] | None = None,
) -> bool:
    schedule_health = evaluate_discovery_run_schedule_health(
        schedule_summary=schedule_summary,
        latest_run=run,
        now=now,
    )
    if run is None:
        return str((schedule_summary or {}).get("state") or "") not in {
            "pending",
            "off_day",
        }
    if bool(schedule_health.get("overdue")):
        return True
    discovery_run_status = _discovery_run_status(
        run,
        now=now,
        schedule_summary=schedule_summary,
    )
    if discovery_run_status in {"healthy", "idle"}:
        return False
    return _is_recent(
        run.get("slot_at") or run.get("finished_at") or run.get("scheduled_for") or run.get("started_at"),
        now=now,
    )


def _latest_discovery_runs(
    *,
    storage: Any,
    now: datetime,
) -> list[dict[str, Any]]:
    discovery_run_definitions = list_enabled_discovery_run_pipelines(
        list_declared_job_rows(enabled_only=True, job_type="discovery_run"),
        now=now,
    )
    if not discovery_run_definitions:
        return []
    latest_session_by_pipeline_id = {
        str(session["pipeline"]["pipeline_id"]): session
        for session in list_latest_live_sessions(
            storage=storage,
            limit=max(len(discovery_run_definitions), 1),
        )
        if isinstance(session.get("pipeline"), Mapping) and session["pipeline"].get("pipeline_id")
    }
    latest_discovery_runs: list[dict[str, Any]] = []
    for definition in discovery_run_definitions:
        payload = definition.get("payload") if isinstance(definition.get("payload"), Mapping) else {}
        if bool(payload.get("uoa_only", False)):
            continue
        session = latest_session_by_pipeline_id.get(str(definition["pipeline_id"]))
        run = None if session is None or not isinstance(session.get("latest_run"), Mapping) else dict(session["latest_run"])
        job_run = {} if session is None or not isinstance(session.get("job_run"), Mapping) else dict(session.get("job_run") or {})
        quote_capture = {} if session is None or not isinstance(session.get("quote_capture"), Mapping) else dict(session.get("quote_capture") or {})
        raw_candidate_summary = (
            {}
            if session is None or not isinstance(session.get("raw_candidate_summary"), Mapping)
            else dict(session.get("raw_candidate_summary") or {})
        )
        strategy_sync_summary = (
            {}
            if session is None or not isinstance(session.get("strategy_sync_summary"), Mapping)
            else dict(session.get("strategy_sync_summary") or {})
        )
        capture_status = None if run is None else run.get("capture_status")
        if capture_status is None:
            capture_status = quote_capture.get("capture_status")
        session_schedule = (
            {} if session is None or not isinstance(session.get("session_schedule"), Mapping) else dict(session.get("session_schedule") or {})
        )
        if not session_schedule:
            session_schedule = dict(definition.get("session_schedule") or {})
        schedule_health = evaluate_discovery_run_schedule_health(
            schedule_summary=session_schedule,
            latest_run=run,
            now=now,
        )
        discovery_run_status = _discovery_run_status(
            run,
            now=now,
            schedule_summary=session_schedule,
        )
        needs_attention = _discovery_run_requires_attention(
            run,
            now=now,
            schedule_summary=session_schedule,
        )
        stream_quote_events_saved = _stream_quote_events_saved(quote_capture)
        latest_discovery_runs.append(
            {
                "job_key": str(definition["job_key"]),
                "status": discovery_run_status,
                "needs_attention": needs_attention,
                "capture_status": capture_status,
                "live_action_gate": None if run is None else dict(run.get("live_action_gate") or {}),
                "auto_execution_summary": None if run is None else run.get("auto_execution_summary"),
                "selection_summary": None if session is None else _selection_summary_payload(session.get("selection_summary")),
                "raw_candidate_summary": raw_candidate_summary,
                "strategy_sync_summary": strategy_sync_summary,
                "last_slot_at": (None if run is None else run.get("slot_at") or run.get("scheduled_for"))
                or job_run.get("slot_at")
                or job_run.get("scheduled_for"),
                "stream_quote_events_saved": stream_quote_events_saved,
                "websocket_quote_events_saved": stream_quote_events_saved,
                "baseline_quote_events_saved": _coerce_int(quote_capture.get("baseline_quote_events_saved")) or 0,
                "session_schedule": session_schedule,
                "schedule_state": session_schedule.get("state"),
                "expected_slot_at": session_schedule.get("expected_current_slot_at"),
                "schedule_note": schedule_health.get("message"),
                "schedule_lag_slot_count": int(schedule_health.get("lag_slot_count") or 0),
                "session_id": None if session is None else session.get("session_id") or job_run.get("session_id"),
            }
        )
    return latest_discovery_runs


def _in_market_date_window(row: Mapping[str, Any], field_name: str, *, start: datetime, end: datetime) -> bool:
    try:
        parsed = parse_datetime(row.get(field_name))
    except ValueError:
        return False
    return parsed is not None and start <= parsed < end


def _summarize_intent_counts(rows: list[tuple[Any, Any, int]]) -> dict[str, Any]:
    intent_state_counts: Counter[str] = Counter()
    entry_intent_state_counts: Counter[str] = Counter()
    management_intent_state_counts: Counter[str] = Counter()
    for action_type, state, count in rows:
        normalized_state = str(state or "unknown")
        normalized_action = str(action_type or "")
        intent_state_counts[normalized_state] += int(count)
        if normalized_action == "open":
            entry_intent_state_counts[normalized_state] += int(count)
        elif normalized_action == "close":
            management_intent_state_counts[normalized_state] += int(count)
    return {
        "intent_count": int(sum(intent_state_counts.values())),
        "intent_state_counts": dict(sorted(intent_state_counts.items())),
        "entry_intent_count": int(sum(entry_intent_state_counts.values())),
        "entry_intent_state_counts": dict(sorted(entry_intent_state_counts.items())),
        "management_intent_count": int(sum(management_intent_state_counts.values())),
        "management_intent_state_counts": dict(sorted(management_intent_state_counts.items())),
    }


def _trading_strategy_runtime_summary(*, storage: Any, market_date: str) -> dict[str, Any]:
    strategies = load_active_trading_strategies()
    strategy_ids = sorted(strategies)
    summary = {
        "strategy_count": len(strategy_ids),
        "entry_strategy_count": sum(1 for strategy in strategies.values() if strategy.entry is not None and strategy.entry.enabled),
        "management_strategy_count": sum(1 for strategy in strategies.values() if strategy.management is not None and strategy.management.enabled),
        "opportunity_count": 0,
        "decision_count": 0,
        "decision_state_counts": {},
        "intent_count": 0,
        "intent_state_counts": {},
        "entry_intent_count": 0,
        "entry_intent_state_counts": {},
        "management_intent_count": 0,
        "management_intent_state_counts": {},
        "open_position_count": 0,
        "open_position_symbols": {},
    }
    if not strategy_ids:
        return summary

    window_start = datetime.fromisoformat(market_date).replace(tzinfo=UTC)
    window_end = window_start + timedelta(days=1)
    signal_store = storage.signals
    execution_store = storage.execution

    if signal_store.schema_ready():
        opportunity_count = 0
        decision_state_counts: Counter[str] = Counter()
        for strategy_id in strategy_ids:
            opportunity_count += len(
                signal_store.list_opportunities(
                    market_date=market_date,
                    trading_strategy_id=strategy_id,
                    runtime_owned=True,
                    limit=1000,
                )
            )
            decisions = [
                dict(row)
                for row in signal_store.list_opportunity_decisions(
                    trading_strategy_id=strategy_id,
                    limit=1000,
                )
                if _in_market_date_window(
                    row,
                    "decided_at",
                    start=window_start,
                    end=window_end,
                )
            ]
            decision_state_counts.update(str(row.get("state") or "unknown") for row in decisions)
        summary["opportunity_count"] = opportunity_count
        summary["decision_state_counts"] = dict(sorted(decision_state_counts.items()))
        summary["decision_count"] = int(sum(decision_state_counts.values()))

    if execution_store.intent_schema_ready():
        intent_counts: Counter[tuple[str, str]] = Counter()
        for strategy_id in strategy_ids:
            intents = [
                dict(row)
                for row in execution_store.list_execution_intents(
                    trading_strategy_id=strategy_id,
                    limit=1000,
                )
                if _in_market_date_window(
                    row,
                    "created_at",
                    start=window_start,
                    end=window_end,
                )
            ]
            intent_counts.update((str(row.get("action_type") or ""), str(row.get("state") or "unknown")) for row in intents)
        summary.update(_summarize_intent_counts([(action_type, state, count) for (action_type, state), count in sorted(intent_counts.items())]))

    if execution_store.portfolio_schema_ready():
        symbol_counts: Counter[str] = Counter()
        for strategy_id in strategy_ids:
            open_positions = [
                enrich_position_row(dict(position))
                for position in execution_store.list_positions(
                    trading_strategy_id=strategy_id,
                    statuses=OPEN_POSITION_STATUSES,
                    limit=200,
                )
            ]
            for position in open_positions:
                symbol_counts[str(position.get("underlying_symbol") or position.get("root_symbol") or "unknown")] += 1
        summary["open_position_count"] = int(sum(symbol_counts.values()))
        summary["open_position_symbols"] = dict(sorted(symbol_counts.items()))
    return summary
