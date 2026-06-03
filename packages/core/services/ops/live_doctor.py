from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from core.db.decorators import with_storage
from core.jobs.orchestration import NEW_YORK
from core.jobs.specs import get_declared_job_row
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_float as _coerce_float,
    coerce_int as _coerce_int,
    utc_now_iso as _utc_now,
)

from .finviz import DEFAULT_FEED_ID, build_finviz_direct_ledger
from .jobs import build_jobs_overview
from .shared import _attention, _combine_statuses, _seconds_since
from .system import build_system_status
from .trading import build_trading_health


DEFAULT_RECENT_LIMIT = 5


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
    status = str(row.get("job_status") or "unknown").strip().lower()
    if status == "succeeded":
        return "healthy"
    if status in {"queued", "running", "leased"}:
        return "idle"
    return "degraded"


def _latest_completed_run(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    return next(
        (
            row
            for row in rows
            if str(row.get("job_status") or "").strip().lower() == "succeeded"
        ),
        None,
    )


def _compact_feed_run(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "job_run_id": row.get("job_run_id"),
        "job_status": row.get("job_status"),
        "scheduled_for": row.get("scheduled_for"),
        "worker_name": row.get("worker_name"),
        "result_status": row.get("result_status"),
        "symbol_count": row.get("symbol_count"),
        "candidate_count": row.get("candidate_count"),
        "retained_count": row.get("retained_count"),
        "excluded_instrument_count": row.get("excluded_instrument_count"),
        "symbols": list(row.get("symbols") or [])[:10],
    }


def _compact_direct_run(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "job_run_id": row.get("job_run_id"),
        "job_status": row.get("job_status"),
        "scheduled_for": row.get("scheduled_for"),
        "worker_name": row.get("worker_name"),
        "result_status": row.get("result_status"),
        "feed_status": row.get("feed_status"),
        "feed_job_run_id": row.get("feed_job_run_id"),
        "entry_candidates": row.get("entry_candidates"),
        "managed_positions": row.get("managed_positions"),
        "active_entry_intents": row.get("active_entry_intents"),
        "max_daily_entries": row.get("max_daily_entries"),
        "daily_entry_budget": row.get("daily_entry_budget"),
        "armed": row.get("armed"),
        "entry_armed": row.get("entry_armed"),
        "decision_count": row.get("decision_count"),
        "created_count": row.get("created_count"),
        "triggered_count": row.get("triggered_count"),
        "reason_counts": row.get("reason_counts"),
        "decisions": list(row.get("decisions") or [])[:8],
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
    if (
        market_open
        and base_status == "healthy"
        and age_seconds is not None
        and age_seconds > max_age_seconds
    ):
        return "degraded", age_seconds
    return base_status, age_seconds


def _checks_attention(checks: list[dict[str, Any]]) -> list[dict[str, str]]:
    attention: list[dict[str, str]] = []
    for row in checks:
        status = str(row.get("status") or "unknown").strip().lower()
        if status in {"healthy", "idle"}:
            continue
        severity = "high" if status in {"blocked", "halted"} else "medium"
        code = (
            "live_doctor_"
            + str(row.get("name") or "check")
            .strip()
            .lower()
            .replace("/", "_")
            .replace(" ", "_")
        )
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
    return ", ".join(
        f"{row.get('lane') or row.get('settings_name')}: {row.get('status')}"
        for row in lanes
    )


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
    closed = [
        item
        for item in positions
        if isinstance(item, Mapping)
        and str(item.get("status") or "").strip().lower() == "closed"
    ]
    if not closed:
        return None
    closed.sort(key=lambda item: str(item.get("closed_at") or ""), reverse=True)
    return _as_text(closed[0].get("last_exit_reason"))


@with_storage()
def build_live_doctor(
    *,
    db_target: str | None = None,
    feed_id: str = DEFAULT_FEED_ID,
    market_date: str | None = None,
    limit: int = DEFAULT_RECENT_LIMIT,
    storage: Any | None = None,
) -> dict[str, Any]:
    generated_at = _utc_now()
    now = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    resolved_feed_id = _as_text(feed_id) or DEFAULT_FEED_ID
    resolved_market_date = (
        _as_text(market_date) or datetime.now(NEW_YORK).date().isoformat()
    )

    system = build_system_status(db_target=db_target, storage=storage)
    trading = build_trading_health(db_target=db_target, storage=storage)
    jobs = build_jobs_overview(db_target=db_target, limit=max(limit * 5, 25), storage=storage)
    finviz = build_finviz_direct_ledger(
        db_target=db_target,
        feed_id=resolved_feed_id,
        market_date=resolved_market_date,
        limit=limit,
        storage=storage,
    )

    system_summary = _mapping(system.get("summary"))
    trading_summary = _mapping(trading.get("summary"))
    jobs_summary = _mapping(jobs.get("summary"))
    finviz_summary = _mapping(finviz.get("summary"))
    system_details = _mapping(system.get("details"))
    trading_details = _mapping(trading.get("details"))
    jobs_details = _mapping(jobs.get("details"))
    finviz_details = _mapping(finviz.get("details"))
    market_session = _mapping(system_details.get("market_session"))
    market_open = bool(market_session.get("is_open"))

    direct_job_key = f"finviz_direct_trading:{resolved_feed_id}"
    direct_definition = get_declared_job_row(direct_job_key) or {}
    direct_payload = _mapping(direct_definition.get("payload"))
    max_feed_age_seconds = _coerce_int(direct_payload.get("max_feed_age_seconds")) or 300
    max_open_positions = _coerce_int(direct_payload.get("max_open_positions"))
    max_new_positions_per_run = _coerce_int(
        direct_payload.get("max_new_positions_per_run")
    )
    max_daily_entries = _coerce_int(
        direct_payload.get("max_daily_entries", direct_payload.get("max_session_entries"))
    )
    if max_daily_entries is not None and max_daily_entries <= 0:
        max_daily_entries = None

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

    lanes = [
        dict(item)
        for item in _list(jobs_details.get("worker_lanes"))
        if isinstance(item, Mapping)
    ]
    lane_status = _combine_statuses(
        *(str(row.get("status") or "unknown") for row in lanes)
    )
    checks.append(
        _check(
            "Worker Lanes",
            status=lane_status,
            message=_lane_status_message(lanes),
            metrics={
                "lane_count": len(lanes),
                "blocked_lane_count": sum(
                    1 for row in lanes if str(row.get("status") or "") == "blocked"
                ),
                "idle_lane_count": sum(
                    1 for row in lanes if str(row.get("status") or "") == "idle"
                ),
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
            message=(
                f"failed={actionable_failed_count}, stale_running={stale_running_count}, "
                f"stale_queued={stale_queued_job_count}"
            ),
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
            message=(
                f"broker_sync={broker_sync.get('status') or 'unknown'}, "
                f"freshness={broker_freshness}, age={broker_sync.get('age_seconds')}"
            ),
            metrics={
                "updated_at": broker_sync.get("updated_at"),
                "age_seconds": broker_sync.get("age_seconds"),
                "freshness": broker_freshness,
                "mismatch_position_count": _mapping(broker_sync.get("summary")).get(
                    "mismatch_position_count"
                ),
                "orphan_broker_position_count": _mapping(broker_sync.get("summary")).get(
                    "orphan_broker_position_count"
                ),
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

    feed_runs = [
        dict(item)
        for item in _list(finviz_details.get("recent_feed_runs"))
        if isinstance(item, Mapping)
    ]
    newest_feed = feed_runs[0] if feed_runs else None
    latest_feed = newest_feed
    if (
        newest_feed is not None
        and str(newest_feed.get("job_status") or "").strip().lower() != "succeeded"
    ):
        latest_feed = _latest_completed_run(feed_runs) or newest_feed
    feed_status = _status_for_job_run(latest_feed, market_open=market_open)
    feed_scheduled_for = (
        None if latest_feed is None else latest_feed.get("scheduled_for")
    )
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
                "job_run_id": None if latest_feed is None else latest_feed.get("job_run_id"),
                "job_status": None if latest_feed is None else latest_feed.get("job_status"),
                "symbol_count": feed_symbol_count,
                "candidate_count": None if latest_feed is None else latest_feed.get("candidate_count"),
                "retained_count": None if latest_feed is None else latest_feed.get("retained_count"),
                "age_seconds": feed_age_seconds,
                "max_age_seconds": max_feed_age_seconds,
            },
        )
    )

    direct_runs = [
        dict(item)
        for item in _list(finviz_details.get("recent_direct_runs"))
        if isinstance(item, Mapping)
    ]
    newest_direct = direct_runs[0] if direct_runs else None
    latest_direct = newest_direct
    if (
        newest_direct is not None
        and str(newest_direct.get("job_status") or "").strip().lower() != "succeeded"
    ):
        latest_direct = _latest_completed_run(direct_runs) or newest_direct
    direct_status = _status_for_job_run(latest_direct, market_open=market_open)
    direct_scheduled_for = (
        None if latest_direct is None else latest_direct.get("scheduled_for")
    )
    direct_status, direct_age_seconds = _fresh_job_status(
        base_status=direct_status,
        scheduled_for=direct_scheduled_for,
        now=now,
        max_age_seconds=max_feed_age_seconds,
        market_open=market_open,
    )
    direct_candidate_count = _coerce_int(
        None if latest_direct is None else latest_direct.get("entry_candidates")
    )
    direct_entry_budget = _mapping(
        None if latest_direct is None else latest_direct.get("daily_entry_budget")
    )
    direct_entry_used = _coerce_int(direct_entry_budget.get("used_entry_count"))
    direct_entry_remaining = _coerce_int(
        direct_entry_budget.get("remaining_entry_count")
    )
    if market_open and direct_status == "healthy" and (direct_candidate_count or 0) <= 0:
        direct_status = "degraded"
    checks.append(
        _check(
            "Finviz Direct",
            status=direct_status,
            message=(
                f"candidates={direct_candidate_count if direct_candidate_count is not None else '-'}; "
                f"created={None if latest_direct is None else latest_direct.get('created_count')}; "
                f"entries={direct_entry_used if direct_entry_used is not None else '-'}"
                f"/{max_daily_entries if max_daily_entries is not None else '-'}; "
                f"latest={direct_scheduled_for or '-'}"
            ),
            metrics={
                "feed_id": resolved_feed_id,
                "job_run_id": None if latest_direct is None else latest_direct.get("job_run_id"),
                "job_status": None if latest_direct is None else latest_direct.get("job_status"),
                "result_status": None if latest_direct is None else latest_direct.get("result_status"),
                "entry_candidates": direct_candidate_count,
                "active_entry_intents": None if latest_direct is None else latest_direct.get("active_entry_intents"),
                "created_count": None if latest_direct is None else latest_direct.get("created_count"),
                "reason_counts": None if latest_direct is None else latest_direct.get("reason_counts"),
                "max_daily_entries": max_daily_entries,
                "daily_entry_budget": direct_entry_budget,
                "entry_budget_remaining": direct_entry_remaining,
                "age_seconds": direct_age_seconds,
                "max_age_seconds": max_feed_age_seconds,
            },
        )
    )

    active_intent_count = _coerce_int(finviz_summary.get("active_intent_count")) or 0
    checks.append(
        _check(
            "Finviz Intents",
            status="healthy" if active_intent_count == 0 else "degraded",
            message=f"active_intents={active_intent_count}",
            metrics={
                "intent_count": finviz_summary.get("intent_count"),
                "active_intent_count": active_intent_count,
            },
        )
    )

    positions = _list(finviz_details.get("positions"))
    position_sync = _position_sync_counts(positions)
    open_position_count = _coerce_int(finviz_summary.get("open_position_count")) or 0
    session_entry_count = _coerce_int(finviz_summary.get("session_entry_count")) or 0
    active_entry_intent_count = (
        _coerce_int(finviz_summary.get("active_entry_intent_count")) or 0
    )
    remaining_daily_entries = direct_entry_remaining
    if remaining_daily_entries is None and max_daily_entries is not None:
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
            "Finviz Positions",
            status=_combine_statuses(cap_status, sync_status),
            message=(
                f"open={open_position_count}/{max_open_positions if max_open_positions is not None else '-'}, "
                f"entries={session_entry_count}/{max_daily_entries if max_daily_entries is not None else '-'}, "
                f"remaining={remaining_daily_entries if remaining_daily_entries is not None else '-'}, "
                f"matched_mismatches={position_sync['reconciliation_mismatch_count']}, "
                f"latest_exit={latest_exit_reason or '-'}"
            ),
            metrics={
                **position_sync,
                "max_open_positions": max_open_positions,
                "max_new_positions_per_run": max_new_positions_per_run,
                "max_daily_entries": max_daily_entries,
                "filled_entry_count": finviz_summary.get("filled_entry_count"),
                "position_entry_count": finviz_summary.get("position_entry_count"),
                "session_entry_count": session_entry_count,
                "active_entry_intent_count": active_entry_intent_count,
                "remaining_daily_entries": remaining_daily_entries,
                "closed_position_count": finviz_summary.get("closed_position_count"),
                "latest_exit_reason": latest_exit_reason,
                "realized_pnl": finviz_summary.get("realized_pnl"),
                "unrealized_pnl": finviz_summary.get("unrealized_pnl"),
                "net_pnl": finviz_summary.get("net_pnl"),
            },
        )
    )

    close_lifecycle = _mapping(finviz_details.get("close_lifecycle"))
    latest_failure = _mapping(close_lifecycle.get("latest_failure"))
    close_status = str(close_lifecycle.get("status") or "unknown")
    position_lifecycle_counts = close_lifecycle.get("position_lifecycle_state_counts")
    close_decision_counts = close_lifecycle.get("close_decision_state_counts")
    checks.append(
        _check(
            "Close Lifecycle",
            status=close_status,
            message=(
                "attempts="
                f"{close_lifecycle.get('recent_close_attempt_count') or 0}, "
                "active="
                f"{close_lifecycle.get('active_close_attempt_count') or 0}, "
                "pending_intents="
                f"{close_lifecycle.get('pending_close_intent_count') or 0}, "
                "failed="
                f"{close_lifecycle.get('failed_close_attempt_count') or 0}, "
                "stale_reconcile="
                f"{close_lifecycle.get('stale_reconciliation_skip_count') or 0}, "
                "intent_mismatch="
                f"{close_lifecycle.get('intent_mismatch_reject_count') or 0}"
            ),
            metrics={
                "recent_close_attempt_count": close_lifecycle.get(
                    "recent_close_attempt_count"
                ),
                "close_attempt_status_counts": close_lifecycle.get(
                    "close_attempt_status_counts"
                ),
                "position_lifecycle_state_counts": position_lifecycle_counts,
                "close_decision_state_counts": close_decision_counts,
                "missing_close_decision_count": close_lifecycle.get(
                    "missing_close_decision_count"
                ),
                "active_close_attempt_count": close_lifecycle.get(
                    "active_close_attempt_count"
                ),
                "pending_close_intent_count": close_lifecycle.get(
                    "pending_close_intent_count"
                ),
                "failed_close_attempt_count": close_lifecycle.get(
                    "failed_close_attempt_count"
                ),
                "stale_reconciliation_skip_count": close_lifecycle.get(
                    "stale_reconciliation_skip_count"
                ),
                "intent_mismatch_reject_count": close_lifecycle.get(
                    "intent_mismatch_reject_count"
                ),
                "latest_failure": latest_failure or None,
                "latest_filled_closes": list(
                    close_lifecycle.get("latest_filled_closes") or []
                )[:3],
            },
        )
    )

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
            "blocked_worker_lane_count": sum(
                1 for row in lanes if str(row.get("status") or "") == "blocked"
            ),
            "idle_worker_lane_count": sum(
                1 for row in lanes if str(row.get("status") or "") == "idle"
            ),
            "actionable_failed_job_count": actionable_failed_count,
            "broker_sync_status": broker_sync.get("status"),
            "broker_sync_age_seconds": broker_sync.get("age_seconds"),
            "feed_id": resolved_feed_id,
            "finviz_feed_status": feed_status,
            "finviz_feed_symbol_count": feed_symbol_count,
            "finviz_feed_age_seconds": feed_age_seconds,
            "finviz_direct_status": direct_status,
            "finviz_direct_candidate_count": direct_candidate_count,
            "finviz_direct_age_seconds": direct_age_seconds,
            "active_intent_count": active_intent_count,
            "open_position_count": open_position_count,
            "max_open_positions": max_open_positions,
            "max_daily_entries": max_daily_entries,
            "filled_entry_count": finviz_summary.get("filled_entry_count"),
            "position_entry_count": finviz_summary.get("position_entry_count"),
            "session_entry_count": session_entry_count,
            "remaining_daily_entries": remaining_daily_entries,
            "close_lifecycle_status": close_status,
            "position_lifecycle_state_counts": position_lifecycle_counts,
            "close_decision_state_counts": close_decision_counts,
            "missing_close_decision_count": close_lifecycle.get(
                "missing_close_decision_count"
            ),
            "active_close_attempt_count": close_lifecycle.get(
                "active_close_attempt_count"
            ),
            "pending_close_intent_count": close_lifecycle.get(
                "pending_close_intent_count"
            ),
            "failed_close_attempt_count": close_lifecycle.get(
                "failed_close_attempt_count"
            ),
            "stale_reconciliation_skip_count": close_lifecycle.get(
                "stale_reconciliation_skip_count"
            ),
            "intent_mismatch_reject_count": close_lifecycle.get(
                "intent_mismatch_reject_count"
            ),
            "realized_pnl": _coerce_float(finviz_summary.get("realized_pnl")),
            "unrealized_pnl": _coerce_float(finviz_summary.get("unrealized_pnl")),
            "net_pnl": _coerce_float(finviz_summary.get("net_pnl")),
        },
        "attention": attention,
        "details": {
            "checks": checks,
            "system_summary": system_summary,
            "trading_summary": trading_summary,
            "jobs_summary": jobs_summary,
            "finviz_summary": finviz_summary,
            "worker_lanes": lanes,
            "newest_feed_run": _compact_feed_run(newest_feed),
            "latest_feed_run": _compact_feed_run(latest_feed),
            "newest_direct_run": _compact_direct_run(newest_direct),
            "latest_direct_run": _compact_direct_run(latest_direct),
            "positions": positions[:limit],
            "close_lifecycle": close_lifecycle,
        },
    }


__all__ = ["build_live_doctor"]
