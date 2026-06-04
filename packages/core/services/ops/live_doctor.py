from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from core.db.decorators import with_storage
from core.jobs.orchestration import NEW_YORK
from core.services.execution_intents.shared import ACTIVE_INTENT_STATES, OPEN_POSITION_STATES
from core.services.trading_strategies import resolve_trading_strategy
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_float as _coerce_float,
    coerce_int as _coerce_int,
    utc_now_iso as _utc_now,
)

from .jobs import build_jobs_overview
from .shared import _attention, _combine_statuses, _seconds_since
from .system import build_system_status
from .trading import build_trading_health

DEFAULT_RECENT_LIMIT = 5
DEFAULT_FEED_ID = "finviz_momentum"
DEFAULT_TRADING_STRATEGY_ID = "momentum_long_calls"


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _check(
    name: str,
    *,
    status: str,
    message: str,
    metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "status": status,
        "message": message,
        "metrics": dict(metrics or {}),
    }


def _status_for_job_run(
    row: Mapping[str, Any] | None,
    *,
    market_open: bool,
) -> str:
    if row is None:
        return "blocked" if market_open else "idle"
    status = str(row.get("job_status") or row.get("status") or "unknown").strip().lower()
    if status == "succeeded":
        return "healthy"
    if status in {"queued", "running", "leased"}:
        return "idle"
    return "degraded"


def _latest_completed_run(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    return next(
        (row for row in rows if str(row.get("job_status") or row.get("status") or "").strip().lower() == "succeeded"),
        None,
    )


def _runs_for_job_key(
    jobs_details: Mapping[str, Any],
    job_key: str,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    rows = [dict(item) for item in _list(jobs_details.get("runs")) if isinstance(item, Mapping) and str(item.get("job_key") or "") == job_key]
    return rows[:limit]


def _declared_job(
    jobs_details: Mapping[str, Any],
    job_key: str,
) -> dict[str, Any]:
    for item in _list(jobs_details.get("declared_jobs")):
        if isinstance(item, Mapping) and str(item.get("job_key") or "") == job_key:
            return dict(item)
    return {}


def _compact_feed_run(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "job_run_id": row.get("job_run_id"),
        "job_status": row.get("job_status") or row.get("status"),
        "job_key": row.get("job_key"),
        "scheduled_for": row.get("scheduled_for"),
        "worker_name": row.get("worker_name"),
        "result_status": row.get("result_status"),
        "symbol_count": row.get("symbol_count"),
        "candidate_count": row.get("candidate_count"),
        "retained_count": row.get("retained_count"),
        "excluded_instrument_count": row.get("excluded_instrument_count"),
        "symbols": list(row.get("symbols") or [])[:10],
    }


def _compact_strategy_run(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "job_run_id": row.get("job_run_id"),
        "job_status": row.get("job_status") or row.get("status"),
        "job_key": row.get("job_key"),
        "scheduled_for": row.get("scheduled_for"),
        "worker_name": row.get("worker_name"),
        "result_status": row.get("result_status"),
        "opportunity_count": row.get("opportunity_count"),
        "decision_count": row.get("decision_count"),
        "execution_intent_id": row.get("execution_intent_id"),
        "selected_opportunity_id": row.get("selected_opportunity_id"),
        "reason": row.get("result_reason") or row.get("reason"),
    }


def _age_seconds(value: Any, *, now: datetime) -> float | None:
    age = _seconds_since(value, now=now)
    return None if age is None else round(age, 1)


def _status_from_control_mode(mode: Any) -> str:
    normalized = str(mode or "unknown").strip().lower()
    if normalized == "normal":
        return "healthy"
    if normalized == "halted":
        return "halted"
    if normalized == "degraded":
        return "degraded"
    return "unknown"


def _fresh_job_status(
    *,
    base_status: str,
    scheduled_for: Any,
    now: datetime,
    max_age_seconds: int,
    market_open: bool,
) -> tuple[str, float | None]:
    age_seconds = _age_seconds(scheduled_for, now=now)
    if market_open and base_status == "healthy" and age_seconds is not None and age_seconds > max_age_seconds:
        return "degraded", age_seconds
    return base_status, age_seconds


def _checks_attention(checks: list[dict[str, Any]]) -> list[dict[str, str]]:
    attention: list[dict[str, str]] = []
    for row in checks:
        status = str(row.get("status") or "unknown").strip().lower()
        if status in {"healthy", "idle"}:
            continue
        severity = "high" if status in {"blocked", "halted"} else "medium"
        code = "live_doctor_" + str(row.get("name") or "check").strip().lower().replace("/", "_").replace(" ", "_")
        attention.append(
            _attention(
                severity=severity,
                code=code,
                message=str(row.get("message") or row.get("name") or "Check failed."),
            )
        )
    return attention


def _lane_status_message(lanes: list[dict[str, Any]]) -> str:
    if not lanes:
        return "No worker lanes were reported."
    return ", ".join(f"{row.get('lane') or row.get('settings_name')}: {row.get('status')}" for row in lanes)


def _position_sync_counts(positions: list[Any]) -> dict[str, int]:
    total = 0
    open_count = 0
    mismatch_count = 0
    for item in positions:
        if not isinstance(item, Mapping):
            continue
        total += 1
        status = str(item.get("status") or "").strip().lower()
        if status in {"open", "partial_close"}:
            open_count += 1
        reconciliation = str(item.get("reconciliation_status") or "").strip().lower()
        if reconciliation and reconciliation != "matched":
            mismatch_count += 1
    return {
        "position_count": total,
        "open_position_count": open_count,
        "reconciliation_mismatch_count": mismatch_count,
    }


def _latest_closed_position_reason(positions: list[Any]) -> str | None:
    closed = [item for item in positions if isinstance(item, Mapping) and str(item.get("status") or "").strip().lower() == "closed"]
    if not closed:
        return None
    closed.sort(key=lambda item: str(item.get("closed_at") or ""), reverse=True)
    return _as_text(closed[0].get("last_exit_reason"))


@with_storage()
def build_live_doctor(
    *,
    db_target: str | None = None,
    feed_id: str = DEFAULT_FEED_ID,
    trading_strategy_id: str = DEFAULT_TRADING_STRATEGY_ID,
    market_date: str | None = None,
    limit: int = DEFAULT_RECENT_LIMIT,
    storage: Any | None = None,
) -> dict[str, Any]:
    generated_at = _utc_now()
    now = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    resolved_feed_id = _as_text(feed_id) or DEFAULT_FEED_ID
    resolved_trading_strategy_id = _as_text(trading_strategy_id) or DEFAULT_TRADING_STRATEGY_ID
    resolved_market_date = _as_text(market_date) or datetime.now(NEW_YORK).date().isoformat()

    system = build_system_status(db_target=db_target, storage=storage)
    trading = build_trading_health(db_target=db_target, storage=storage)
    jobs = build_jobs_overview(db_target=db_target, limit=max(limit * 5, 25), storage=storage)

    system_summary = _mapping(system.get("summary"))
    trading_summary = _mapping(trading.get("summary"))
    jobs_summary = _mapping(jobs.get("summary"))
    system_details = _mapping(system.get("details"))
    trading_details = _mapping(trading.get("details"))
    jobs_details = _mapping(jobs.get("details"))
    market_session = _mapping(system_details.get("market_session"))
    market_open = bool(market_session.get("is_open"))

    strategy = resolve_trading_strategy(resolved_trading_strategy_id)
    max_feed_age_seconds = strategy.source.max_age_seconds or 300
    max_open_positions = strategy.max_open_positions or None
    max_daily_entries = strategy.max_new_entries_per_day

    checks: list[dict[str, Any]] = []
    market_status = "healthy" if market_open else "idle"
    checks.append(
        _check(
            "Market Session",
            status=market_status,
            message=(
                f"{market_session.get('calendar') or 'market'} "
                f"{market_session.get('status') or 'unknown'}; "
                f"open {market_session.get('market_open_at') or '-'} to "
                f"{market_session.get('market_close_at') or '-'}"
            ),
            metrics={
                "is_open": market_open,
                "market_open_at": market_session.get("market_open_at"),
                "market_close_at": market_session.get("market_close_at"),
            },
        )
    )

    control_mode = system_summary.get("control_mode")
    checks.append(
        _check(
            "Control Mode",
            status=_status_from_control_mode(control_mode),
            message=f"Control mode is {control_mode or 'unknown'}.",
            metrics={"mode": control_mode},
        )
    )

    scheduler = _mapping(jobs_details.get("scheduler") or system_details.get("scheduler"))
    checks.append(
        _check(
            "Scheduler",
            status=str(scheduler.get("status") or "unknown"),
            message=(
                f"Scheduler {scheduler.get('status') or 'unknown'}; "
                f"active={scheduler.get('active_scheduler_count') or 0}, "
                f"expires={scheduler.get('expires_at') or '-'}"
            ),
            metrics=scheduler,
        )
    )

    lanes = [dict(item) for item in _list(jobs_details.get("worker_lanes")) if isinstance(item, Mapping)]
    lane_status = _combine_statuses(*(str(row.get("status") or "unknown") for row in lanes))
    checks.append(
        _check(
            "Worker Lanes",
            status=lane_status,
            message=_lane_status_message(lanes),
            metrics={
                "lane_count": len(lanes),
                "blocked_lane_count": sum(1 for row in lanes if str(row.get("status") or "") == "blocked"),
                "idle_lane_count": sum(1 for row in lanes if str(row.get("status") or "") == "idle"),
            },
        )
    )

    actionable_failed_count = _coerce_int(jobs_summary.get("actionable_failed_count")) or 0
    stale_running_count = _coerce_int(jobs_summary.get("stale_running_count")) or 0
    stale_queued_job_count = _coerce_int(jobs_summary.get("stale_queued_job_count")) or 0
    jobs_status = "healthy"
    if actionable_failed_count:
        jobs_status = "blocked"
    elif stale_running_count or stale_queued_job_count:
        jobs_status = "degraded"
    checks.append(
        _check(
            "Job Runs",
            status=jobs_status,
            message=(f"failed={actionable_failed_count}, stale_running={stale_running_count}, " f"stale_queued={stale_queued_job_count}"),
            metrics={
                "actionable_failed_count": actionable_failed_count,
                "stale_running_count": stale_running_count,
                "stale_queued_job_count": stale_queued_job_count,
                "operator_status_counts": jobs_summary.get("operator_status_counts"),
            },
        )
    )

    trading_allowed = bool(trading_summary.get("trading_allowed"))
    trading_gate_status = "healthy" if trading_allowed else ("blocked" if market_open else "idle")
    checks.append(
        _check(
            "Trading Gate",
            status=trading_gate_status,
            message=f"trading_allowed={trading_allowed}",
            metrics={
                "trading_allowed": trading_allowed,
                "market_session_status": trading_summary.get("market_session_status"),
                "environment": trading_summary.get("environment"),
            },
        )
    )

    broker_sync = _mapping(trading_details.get("broker_sync"))
    broker_status = str(broker_sync.get("status") or trading_summary.get("broker_sync_status") or "unknown")
    broker_freshness = str(broker_sync.get("freshness") or "unknown")
    if market_open and broker_status in {"healthy", "idle"} and broker_freshness != "current":
        broker_status = "degraded"
    checks.append(
        _check(
            "Broker Sync",
            status=broker_status,
            message=(f"broker_sync={broker_sync.get('status') or 'unknown'}, " f"freshness={broker_freshness}, age={broker_sync.get('age_seconds')}"),
            metrics={
                "updated_at": broker_sync.get("updated_at"),
                "age_seconds": broker_sync.get("age_seconds"),
                "freshness": broker_freshness,
                "mismatch_position_count": _mapping(broker_sync.get("summary")).get("mismatch_position_count"),
                "orphan_broker_position_count": _mapping(broker_sync.get("summary")).get("orphan_broker_position_count"),
            },
        )
    )

    execution_health = _mapping(trading_details.get("execution_health"))
    checks.append(
        _check(
            "Execution Health",
            status=str(execution_health.get("status") or trading_summary.get("execution_health_status") or "unknown"),
            message=(
                f"open={trading_summary.get('open_execution_count') or 0}, "
                f"stale={trading_summary.get('stale_open_execution_count') or 0}, "
                f"submit_unknown={trading_summary.get('submit_unknown_execution_count') or 0}"
            ),
            metrics=execution_health,
        )
    )

    mark_health = _mapping(trading_details.get("mark_health"))
    risk_breach_count = _coerce_int(trading_summary.get("risk_breach_count")) or 0
    mark_status = str(mark_health.get("status") or trading_summary.get("mark_health_status") or "unknown")
    checks.append(
        _check(
            "Risk And Marks",
            status=_combine_statuses(
                mark_status,
                "blocked" if risk_breach_count else "healthy",
            ),
            message=f"risk_breaches={risk_breach_count}, mark_health={mark_status}",
            metrics={
                "risk_breach_count": risk_breach_count,
                **mark_health,
            },
        )
    )

    feed_job_key = f"symbol_feed:{resolved_feed_id}"
    entry_job_key = f"trading_strategy:{resolved_trading_strategy_id}:entry"
    manage_job_key = f"trading_strategy:{resolved_trading_strategy_id}:manage"
    dispatch_job_key = "execution_intent_dispatch:global"

    feed_runs = _runs_for_job_key(jobs_details, feed_job_key, limit=limit)
    feed_definition = _declared_job(jobs_details, feed_job_key)
    newest_feed = feed_runs[0] if feed_runs else None
    latest_feed = newest_feed
    if newest_feed is not None and str(newest_feed.get("job_status") or newest_feed.get("status") or "").strip().lower() != "succeeded":
        latest_feed = _latest_completed_run(feed_runs) or newest_feed
    if latest_feed is None and feed_definition:
        latest_feed = {
            "job_run_id": feed_definition.get("latest_run_id"),
            "job_status": feed_definition.get("latest_run_status"),
            "scheduled_for": feed_definition.get("latest_run_at") or feed_definition.get("expected_slot_at"),
            "operator_status": feed_definition.get("operator_status"),
        }
    feed_status = _status_for_job_run(latest_feed, market_open=market_open)
    feed_scheduled_for = None if latest_feed is None else latest_feed.get("scheduled_for")
    feed_status, feed_age_seconds = _fresh_job_status(
        base_status=feed_status,
        scheduled_for=feed_scheduled_for,
        now=now,
        max_age_seconds=max_feed_age_seconds,
        market_open=market_open,
    )
    feed_symbol_count = _coerce_int(None if latest_feed is None else latest_feed.get("symbol_count")) or 0
    if market_open and feed_status == "healthy" and feed_symbol_count <= 0:
        feed_status = "degraded"
    checks.append(
        _check(
            "Finviz Feed",
            status=feed_status,
            message=(
                f"{feed_symbol_count} symbols; latest={feed_scheduled_for or '-'}; "
                f"age={feed_age_seconds}; newest_status="
                f"{None if newest_feed is None else newest_feed.get('job_status')}"
            ),
            metrics={
                "feed_id": resolved_feed_id,
                "job_key": feed_job_key,
                "job_run_id": None if latest_feed is None else latest_feed.get("job_run_id"),
                "job_status": None if latest_feed is None else latest_feed.get("job_status") or latest_feed.get("status"),
                "symbol_count": feed_symbol_count,
                "candidate_count": None if latest_feed is None else latest_feed.get("candidate_count"),
                "retained_count": None if latest_feed is None else latest_feed.get("retained_count"),
                "age_seconds": feed_age_seconds,
                "max_age_seconds": max_feed_age_seconds,
            },
        )
    )

    entry_runs = _runs_for_job_key(jobs_details, entry_job_key, limit=limit)
    entry_definition = _declared_job(jobs_details, entry_job_key)
    newest_entry = entry_runs[0] if entry_runs else None
    latest_entry = newest_entry
    if newest_entry is not None and str(newest_entry.get("job_status") or newest_entry.get("status") or "").strip().lower() != "succeeded":
        latest_entry = _latest_completed_run(entry_runs) or newest_entry
    if latest_entry is None and entry_definition:
        latest_entry = {
            "job_run_id": entry_definition.get("latest_run_id"),
            "job_status": entry_definition.get("latest_run_status"),
            "scheduled_for": entry_definition.get("latest_run_at") or entry_definition.get("expected_slot_at"),
            "operator_status": entry_definition.get("operator_status"),
        }
    entry_status = _status_for_job_run(latest_entry, market_open=market_open)
    entry_scheduled_for = None if latest_entry is None else latest_entry.get("scheduled_for")
    entry_max_age_seconds = max(
        int((strategy.entry.schedule.cadence_minutes if strategy.entry else 2) * 60 * 2),
        60,
    )
    entry_status, entry_age_seconds = _fresh_job_status(
        base_status=entry_status,
        scheduled_for=entry_scheduled_for,
        now=now,
        max_age_seconds=entry_max_age_seconds,
        market_open=market_open,
    )
    entry_opportunity_count = _coerce_int(None if latest_entry is None else latest_entry.get("opportunity_count"))
    checks.append(
        _check(
            "Strategy Entry",
            status=entry_status,
            message=(
                f"strategy={resolved_trading_strategy_id}; "
                f"opportunities={entry_opportunity_count if entry_opportunity_count is not None else '-'}; "
                f"latest={entry_scheduled_for or '-'}"
            ),
            metrics={
                "trading_strategy_id": resolved_trading_strategy_id,
                "job_key": entry_job_key,
                "job_run_id": None if latest_entry is None else latest_entry.get("job_run_id"),
                "job_status": None if latest_entry is None else latest_entry.get("job_status") or latest_entry.get("status"),
                "result_status": None if latest_entry is None else latest_entry.get("result_status"),
                "opportunity_count": entry_opportunity_count,
                "decision_count": None if latest_entry is None else latest_entry.get("decision_count"),
                "selected_opportunity_id": None if latest_entry is None else latest_entry.get("selected_opportunity_id"),
                "execution_intent_id": None if latest_entry is None else latest_entry.get("execution_intent_id"),
                "age_seconds": entry_age_seconds,
                "max_age_seconds": entry_max_age_seconds,
            },
        )
    )

    manage_runs = _runs_for_job_key(jobs_details, manage_job_key, limit=limit)
    manage_definition = _declared_job(jobs_details, manage_job_key)
    newest_manage = manage_runs[0] if manage_runs else None
    latest_manage = newest_manage
    if newest_manage is not None and str(newest_manage.get("job_status") or newest_manage.get("status") or "").strip().lower() != "succeeded":
        latest_manage = _latest_completed_run(manage_runs) or newest_manage
    if latest_manage is None and manage_definition:
        latest_manage = {
            "job_run_id": manage_definition.get("latest_run_id"),
            "job_status": manage_definition.get("latest_run_status"),
            "scheduled_for": manage_definition.get("latest_run_at") or manage_definition.get("expected_slot_at"),
            "operator_status": manage_definition.get("operator_status"),
        }
    manage_status = _status_for_job_run(latest_manage, market_open=market_open)
    manage_scheduled_for = None if latest_manage is None else latest_manage.get("scheduled_for")
    manage_max_age_seconds = max(
        int((strategy.management.schedule.cadence_minutes if strategy.management else 1) * 60 * 2),
        60,
    )
    manage_status, manage_age_seconds = _fresh_job_status(
        base_status=manage_status,
        scheduled_for=manage_scheduled_for,
        now=now,
        max_age_seconds=manage_max_age_seconds,
        market_open=market_open,
    )
    checks.append(
        _check(
            "Strategy Manage",
            status=manage_status,
            message=(f"strategy={resolved_trading_strategy_id}; " f"latest={manage_scheduled_for or '-'}; age={manage_age_seconds}"),
            metrics={
                "trading_strategy_id": resolved_trading_strategy_id,
                "job_key": manage_job_key,
                "job_run_id": None if latest_manage is None else latest_manage.get("job_run_id"),
                "job_status": None if latest_manage is None else latest_manage.get("job_status") or latest_manage.get("status"),
                "result_status": None if latest_manage is None else latest_manage.get("result_status"),
                "age_seconds": manage_age_seconds,
                "max_age_seconds": manage_max_age_seconds,
            },
        )
    )

    dispatch_runs = _runs_for_job_key(jobs_details, dispatch_job_key, limit=limit)
    latest_dispatch = dispatch_runs[0] if dispatch_runs else _declared_job(jobs_details, dispatch_job_key)
    dispatch_status = _status_for_job_run(latest_dispatch, market_open=market_open)
    checks.append(
        _check(
            "Intent Dispatch",
            status=dispatch_status,
            message=(
                f"latest={latest_dispatch.get('scheduled_for') or latest_dispatch.get('latest_run_at') or '-'}; "
                f"status={latest_dispatch.get('job_status') or latest_dispatch.get('status') or latest_dispatch.get('latest_run_status') or '-'}"
            ),
            metrics={
                "job_key": dispatch_job_key,
                "job_run_id": latest_dispatch.get("job_run_id") or latest_dispatch.get("latest_run_id"),
                "job_status": latest_dispatch.get("job_status") or latest_dispatch.get("status") or latest_dispatch.get("latest_run_status"),
                "result_status": latest_dispatch.get("result_status"),
            },
        )
    )

    execution_store = storage.execution
    intents = (
        [
            dict(row)
            for row in execution_store.list_execution_intents(
                trading_strategy_id=resolved_trading_strategy_id,
                limit=500,
            )
        ]
        if execution_store.intent_schema_ready()
        else []
    )
    active_intent_count = sum(1 for row in intents if str(row.get("state") or "") in ACTIVE_INTENT_STATES)
    checks.append(
        _check(
            "Strategy Intents",
            status="healthy" if active_intent_count == 0 else "degraded",
            message=f"active_intents={active_intent_count}",
            metrics={
                "trading_strategy_id": resolved_trading_strategy_id,
                "intent_count": len(intents),
                "active_intent_count": active_intent_count,
            },
        )
    )

    positions = (
        [
            dict(row)
            for row in execution_store.list_positions(
                trading_strategy_id=resolved_trading_strategy_id,
                limit=500,
            )
        ]
        if execution_store.portfolio_schema_ready()
        else []
    )
    position_sync = _position_sync_counts(positions)
    open_position_count = sum(1 for row in positions if str(row.get("status") or "") in OPEN_POSITION_STATES)
    session_entry_count = sum(1 for row in positions if row.get("market_date_opened") == resolved_market_date)
    active_entry_intent_count = sum(
        1 for row in intents if str(row.get("action_type") or "") == "open" and str(row.get("state") or "") in ACTIVE_INTENT_STATES
    )
    remaining_daily_entries = None
    if max_daily_entries is not None:
        remaining_daily_entries = max(
            max_daily_entries - session_entry_count - active_entry_intent_count,
            0,
        )
    cap_status = "healthy"
    if max_open_positions is not None and open_position_count > max_open_positions:
        cap_status = "blocked"
    if max_daily_entries is not None and session_entry_count > max_daily_entries:
        cap_status = "blocked"
    sync_status = "healthy" if position_sync["reconciliation_mismatch_count"] == 0 else "degraded"
    latest_exit_reason = _latest_closed_position_reason(positions)
    checks.append(
        _check(
            "Strategy Positions",
            status=_combine_statuses(cap_status, sync_status),
            message=(
                f"open={open_position_count}/{max_open_positions if max_open_positions is not None else '-'}, "
                f"entries={session_entry_count}/{max_daily_entries if max_daily_entries is not None else '-'}, "
                f"remaining={remaining_daily_entries if remaining_daily_entries is not None else '-'}, "
                f"matched_mismatches={position_sync['reconciliation_mismatch_count']}, "
                f"latest_exit={latest_exit_reason or '-'}"
            ),
            metrics={
                "trading_strategy_id": resolved_trading_strategy_id,
                **position_sync,
                "max_open_positions": max_open_positions,
                "max_daily_entries": max_daily_entries,
                "session_entry_count": session_entry_count,
                "active_entry_intent_count": active_entry_intent_count,
                "remaining_daily_entries": remaining_daily_entries,
                "closed_position_count": sum(1 for row in positions if str(row.get("status") or "") not in OPEN_POSITION_STATES),
                "latest_exit_reason": latest_exit_reason,
                "realized_pnl": round(
                    sum(_coerce_float(row.get("realized_pnl")) or 0.0 for row in positions),
                    2,
                ),
                "unrealized_pnl": round(
                    sum(_coerce_float(row.get("unrealized_pnl")) or 0.0 for row in positions),
                    2,
                ),
            },
        )
    )

    closed_position_count = sum(1 for row in positions if str(row.get("status") or "") not in OPEN_POSITION_STATES)
    realized_pnl = round(
        sum(_coerce_float(row.get("realized_pnl")) or 0.0 for row in positions),
        2,
    )
    unrealized_pnl = round(
        sum(_coerce_float(row.get("unrealized_pnl")) or 0.0 for row in positions),
        2,
    )
    net_pnl = round(realized_pnl + unrealized_pnl, 2)

    status = _combine_statuses(*(str(row.get("status") or "unknown") for row in checks))
    attention = _checks_attention(checks)

    return {
        "status": status,
        "generated_at": generated_at,
        "summary": {
            "market_date": resolved_market_date,
            "market_session_status": market_session.get("status"),
            "market_open_at": market_session.get("market_open_at"),
            "market_close_at": market_session.get("market_close_at"),
            "trading_allowed": trading_allowed,
            "environment": trading_summary.get("environment"),
            "control_mode": control_mode,
            "scheduler_status": scheduler.get("status"),
            "worker_lane_count": len(lanes),
            "blocked_worker_lane_count": sum(1 for row in lanes if str(row.get("status") or "") == "blocked"),
            "idle_worker_lane_count": sum(1 for row in lanes if str(row.get("status") or "") == "idle"),
            "actionable_failed_job_count": actionable_failed_count,
            "broker_sync_status": broker_sync.get("status"),
            "broker_sync_age_seconds": broker_sync.get("age_seconds"),
            "feed_id": resolved_feed_id,
            "trading_strategy_id": resolved_trading_strategy_id,
            "finviz_feed_status": feed_status,
            "finviz_feed_symbol_count": feed_symbol_count,
            "finviz_feed_age_seconds": feed_age_seconds,
            "strategy_entry_status": entry_status,
            "strategy_entry_opportunity_count": entry_opportunity_count,
            "strategy_entry_age_seconds": entry_age_seconds,
            "strategy_manage_status": manage_status,
            "strategy_manage_age_seconds": manage_age_seconds,
            "intent_dispatch_status": dispatch_status,
            "active_intent_count": active_intent_count,
            "open_position_count": open_position_count,
            "max_open_positions": max_open_positions,
            "max_daily_entries": max_daily_entries,
            "session_entry_count": session_entry_count,
            "remaining_daily_entries": remaining_daily_entries,
            "closed_position_count": closed_position_count,
            "latest_exit_reason": latest_exit_reason,
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
            "net_pnl": net_pnl,
        },
        "attention": attention,
        "details": {
            "checks": checks,
            "system_summary": system_summary,
            "trading_summary": trading_summary,
            "jobs_summary": jobs_summary,
            "worker_lanes": lanes,
            "newest_feed_run": _compact_feed_run(newest_feed),
            "latest_feed_run": _compact_feed_run(latest_feed),
            "newest_entry_run": _compact_strategy_run(newest_entry),
            "latest_entry_run": _compact_strategy_run(latest_entry),
            "newest_manage_run": _compact_strategy_run(newest_manage),
            "latest_manage_run": _compact_strategy_run(latest_manage),
            "latest_dispatch_run": _compact_strategy_run(latest_dispatch),
            "intents": intents[:limit],
            "positions": positions[:limit],
        },
    }


__all__ = ["build_live_doctor"]
