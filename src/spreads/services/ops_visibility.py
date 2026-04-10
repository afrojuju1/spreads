from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from spreads.db.decorators import with_storage
from spreads.jobs.orchestration import SCHEDULER_RUNTIME_LEASE_KEY, WORKER_RUNTIME_LEASE_PREFIX
from spreads.services.account_state import get_account_overview
from spreads.services.broker_sync import BROKER_SYNC_KEY
from spreads.services.control_plane import get_control_state_snapshot, resolve_execution_kill_switch_reason
from spreads.services.execution import OPEN_STATUSES
from spreads.services.live_collector_health import enrich_live_collector_job_run_payload
from spreads.services.risk_manager import assess_position_risk
from spreads.services.sessions import (
    DEFAULT_ANALYSIS_PROFIT_TARGET,
    DEFAULT_ANALYSIS_STOP_MULTIPLE,
    get_session_detail,
    list_existing_sessions,
)
from spreads.storage.serializers import parse_datetime

OPEN_POSITION_STATUSES = ["open", "partial_close"]
BROKER_SYNC_STALE_AFTER_SECONDS = 15 * 60
MARK_STALE_AFTER_SECONDS = 15 * 60
RECENT_FAILURE_LIMIT = 10
RECENT_ALERT_LIMIT = 200
TOP_POSITION_LIMIT = 5
MODELED_IDEA_LIMIT = 5

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


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


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


def _combine_statuses(*statuses: str | None) -> str:
    normalized = [str(status or "unknown").strip().lower() for status in statuses if status is not None]
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
    for key in ("finished_at", "heartbeat_at", "started_at", "slot_at", "scheduled_for", "requested_at", "updated_at"):
        value = _as_text(row.get(key))
        if value:
            return value
    return None


def _sorted_by_activity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: _parse_timestamp(_activity_at(row)) or datetime.fromtimestamp(0, UTC),
        reverse=True,
    )


def _collector_status(run: Mapping[str, Any] | None) -> str:
    if run is None:
        return "unknown"
    if str(run.get("status") or "") != "succeeded":
        return "degraded"
    if str(run.get("capture_status") or "") == "healthy":
        return "healthy"
    return "degraded"


def _broker_sync_payload(state: Mapping[str, Any] | None, *, now: datetime) -> tuple[str, dict[str, Any]]:
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
    age_seconds = _seconds_since(payload.get("updated_at"), now=now)
    status = str(payload.get("status") or "unknown")
    normalized = "unknown"
    if status == "healthy":
        normalized = "healthy"
    elif status == "degraded":
        normalized = "degraded"
    elif status == "failed":
        normalized = "blocked"
    if age_seconds is not None and age_seconds > BROKER_SYNC_STALE_AFTER_SECONDS and normalized == "healthy":
        normalized = "degraded"
    payload["raw_status"] = status
    payload["status"] = normalized
    payload["age_seconds"] = age_seconds
    return normalized, payload


def _alert_delivery_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(str(row.get("status") or "unknown") for row in rows)
    status = "healthy"
    if counts.get("dead_letter", 0) or counts.get("retry_wait", 0):
        status = "degraded"
    return {
        "status": status,
        "count": len(rows),
        "status_counts": dict(counts),
        "dead_letter_count": counts.get("dead_letter", 0),
        "retry_wait_count": counts.get("retry_wait", 0),
        "dispatching_count": counts.get("dispatching", 0),
        "pending_count": counts.get("pending", 0),
    }


def _board_watchlist_pnl_spread(summary: Mapping[str, Any] | None) -> float | None:
    if not isinstance(summary, Mapping):
        return None
    outcomes = summary.get("outcomes")
    if not isinstance(outcomes, Mapping):
        return None
    averages = outcomes.get("average_estimated_pnl_by_bucket")
    if not isinstance(averages, Mapping):
        return None
    board = _coerce_float(averages.get("board"))
    watchlist = _coerce_float(averages.get("watchlist"))
    if board is None or watchlist is None:
        return None
    return round(board - watchlist, 2)


def _post_market_view(run: Mapping[str, Any] | None) -> dict[str, Any]:
    if run is None:
        return {
            "overall_verdict": None,
            "board_watchlist_pnl_spread": None,
            "recommendations": [],
            "completed_at": None,
        }
    diagnostics = run.get("diagnostics") if isinstance(run.get("diagnostics"), Mapping) else {}
    summary = run.get("summary") if isinstance(run.get("summary"), Mapping) else {}
    return {
        "overall_verdict": _as_text(diagnostics.get("overall_verdict")),
        "board_watchlist_pnl_spread": _board_watchlist_pnl_spread(summary),
        "recommendations": list(run.get("recommendations") or []),
        "completed_at": _as_text(run.get("completed_at")),
    }


def _modeled_pnl(idea: Mapping[str, Any]) -> float | None:
    expiry_pnl = _coerce_float(idea.get("estimated_expiry_pnl"))
    if expiry_pnl is not None:
        return expiry_pnl
    return _coerce_float(idea.get("estimated_close_pnl"))


def _rank_modeled_ideas(ideas: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ranked = []
    for idea in ideas:
        modeled_pnl = _modeled_pnl(idea)
        if modeled_pnl is None:
            continue
        ranked.append(
            {
                "underlying_symbol": idea.get("underlying_symbol"),
                "classification": idea.get("classification"),
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


def _summarize_execution_attempt(attempt: Mapping[str, Any]) -> dict[str, Any]:
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
    }


def _summarize_slot_run(run: Mapping[str, Any]) -> dict[str, Any]:
    quote_capture = run.get("quote_capture") if isinstance(run.get("quote_capture"), Mapping) else {}
    trade_capture = run.get("trade_capture") if isinstance(run.get("trade_capture"), Mapping) else {}
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
            "websocket_quote_events_saved": quote_capture.get("websocket_quote_events_saved"),
            "baseline_quote_events_saved": quote_capture.get("baseline_quote_events_saved"),
            "recovery_quote_events_saved": quote_capture.get("recovery_quote_events_saved"),
        },
        "trade_capture": {
            "capture_status": trade_capture.get("capture_status"),
            "total_trade_events_saved": trade_capture.get("total_trade_events_saved"),
            "websocket_trade_events_saved": trade_capture.get("websocket_trade_events_saved"),
        },
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


@with_storage()
def build_system_status(
    *,
    db_target: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    generated_at = _utc_now()
    now = _parse_timestamp(generated_at) or datetime.now(UTC)
    control = get_control_state_snapshot(storage=storage)
    attention: list[dict[str, str]] = []
    statuses = [_control_status(control)]
    details: dict[str, Any] = {
        "control": control,
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
            "expires_at": None if scheduler_lease is None else scheduler_lease.get("expires_at"),
            "owner": None if scheduler_lease is None else scheduler_lease.get("owner"),
            "job_run_id": None if scheduler_lease is None else scheduler_lease.get("job_run_id"),
        }
        if scheduler_status != "healthy":
            attention.append(
                _attention(
                    severity="high" if scheduler_status == "blocked" else "medium",
                    code="scheduler_unhealthy",
                    message="Scheduler lease is missing, expired, or close to expiring.",
                )
            )

        workers = [dict(row) for row in job_store.list_active_leases(prefix=WORKER_RUNTIME_LEASE_PREFIX)]
        worker_status = "healthy" if workers else "blocked"
        if worker_status != "healthy":
            attention.append(
                _attention(
                    severity="high",
                    code="workers_missing",
                    message="No active worker leases are present.",
                )
            )

        running_jobs = [dict(row) for row in job_store.list_job_runs(status="running", limit=100)]
        queued_jobs = [dict(row) for row in job_store.list_job_runs(status="queued", limit=100)]
        failed_jobs = [dict(row) for row in job_store.list_job_runs(status="failed", limit=RECENT_FAILURE_LIMIT)]
        skipped_jobs = [dict(row) for row in job_store.list_job_runs(status="skipped", limit=RECENT_FAILURE_LIMIT)]
        recent_failures = _sorted_by_activity(failed_jobs + skipped_jobs)[:RECENT_FAILURE_LIMIT]
        if recent_failures:
            attention.append(
                _attention(
                    severity="medium",
                    code="recent_job_failures",
                    message=f"{len(recent_failures)} recent failed or skipped job runs need attention.",
                )
            )

        collector_definitions = [dict(row) for row in job_store.list_job_definitions(enabled_only=True, job_type="live_collector")]
        latest_run_by_key = {
            str(row["job_key"]): enrich_live_collector_job_run_payload(row)
            for row in job_store.list_latest_runs_by_job_keys(
                job_keys=[str(row["job_key"]) for row in collector_definitions],
                statuses=["succeeded"],
            )
        }
        latest_collectors = []
        for definition in collector_definitions:
            job_key = str(definition["job_key"])
            run = latest_run_by_key.get(job_key)
            quote_capture = {} if run is None else dict(run.get("quote_capture") or {})
            collector_status = _collector_status(run)
            latest_collectors.append(
                {
                    "job_key": job_key,
                    "status": collector_status,
                    "capture_status": None if run is None else run.get("capture_status"),
                    "last_slot_at": None if run is None else run.get("slot_at") or run.get("scheduled_for"),
                    "websocket_quote_events_saved": _coerce_int(quote_capture.get("websocket_quote_events_saved")) or 0,
                    "baseline_quote_events_saved": _coerce_int(quote_capture.get("baseline_quote_events_saved")) or 0,
                    "session_id": None if run is None else run.get("session_id"),
                }
            )
            if collector_status != "healthy":
                attention.append(
                    _attention(
                        severity="medium",
                        code="collector_unhealthy",
                        message=f"Collector {job_key} is {collector_status}.",
                    )
                )

        statuses.append(
            _combine_statuses(
                scheduler_payload["status"],
                worker_status,
                "degraded" if recent_failures else "healthy",
                "degraded" if any(row["status"] != "healthy" for row in latest_collectors) else "healthy",
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
        )
        if broker_sync_status != "healthy":
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
        recent_alerts = [dict(row) for row in alert_store.list_alert_events(limit=RECENT_ALERT_LIMIT)]
        alert_delivery = _alert_delivery_payload(recent_alerts)
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
                for row in recent_failures
            ],
            "latest_collectors": latest_collectors,
            "broker_sync": broker_sync,
            "alert_delivery": alert_delivery,
        }
    )

    summary = {
        "control_mode": control.get("mode"),
        "worker_count": len(workers),
        "running_job_count": len(running_jobs),
        "queued_job_count": len(queued_jobs),
        "running_jobs_by_type": dict(Counter(str(row.get("job_type") or "unknown") for row in running_jobs)),
        "queued_jobs_by_type": dict(Counter(str(row.get("job_type") or "unknown") for row in queued_jobs)),
        "recent_failure_count": len(recent_failures),
        "collector_count": len(latest_collectors),
        "collector_degraded_count": sum(1 for row in latest_collectors if row["status"] != "healthy"),
        "broker_sync_status": broker_sync.get("status"),
        "alert_delivery_status": alert_delivery.get("status"),
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
    attention: list[dict[str, str]] = []
    statuses: list[str] = []
    details: dict[str, Any] = {}

    control = get_control_state_snapshot(storage=storage)
    details["control"] = control
    control_status = _control_status(control)
    statuses.append(control_status)
    if control_status in {"degraded", "halted"}:
        attention.append(
            _attention(
                severity="high" if control_status == "halted" else "medium",
                code=f"control_mode_{control.get('mode')}",
                message=_as_text(control.get("note")) or f"Control mode is {control.get('mode')}.",
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
        broker_sync_status, broker_sync = _broker_sync_payload(broker_store.get_sync_state(BROKER_SYNC_KEY), now=now)
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
    if broker_sync_status != "healthy":
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

    open_positions: list[dict[str, Any]] = []
    top_positions: list[dict[str, Any]] = []
    risk_breach_count = 0
    reconciliation_mismatch_count = 0
    missing_mark_count = 0
    stale_mark_count = 0
    if execution_store.positions_schema_ready():
        persisted_positions = [
            dict(row)
            for row in execution_store.list_session_positions(
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
            elif mark_age_seconds is not None and mark_age_seconds > MARK_STALE_AFTER_SECONDS:
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
                    "mark_age_seconds": None if mark_age_seconds is None else round(mark_age_seconds, 2),
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
                message="Session positions storage is not available yet.",
            )
        )

    mark_error = _as_text((broker_sync.get("summary") or {}).get("mark_error"))
    broker_unquoted_positions = _coerce_int((broker_sync.get("summary") or {}).get("unquoted_position_count")) or 0
    mark_health_status = "healthy"
    if missing_mark_count or stale_mark_count or broker_unquoted_positions or mark_error:
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
    elif broker_sync_status != "healthy":
        trading_allowed = False
    elif str((details.get("account_overview") or {}).get("source") or "snapshot") != "live":
        trading_allowed = False
    elif account.get("trading_blocked") or account.get("account_blocked"):
        trading_allowed = False

    summary = {
        "trading_allowed": trading_allowed,
        "account_source": None if account_overview is None else account_overview.get("source"),
        "environment": None if account_overview is None else account_overview.get("environment"),
        "open_position_count": len(open_positions),
        "open_execution_count": len(open_execution_attempts),
        "risk_breach_count": risk_breach_count,
        "reconciliation_mismatch_count": reconciliation_mismatch_count,
        "mark_health_status": mark_health_status,
        "account_error": account_error,
    }

    details.update(
        {
            "open_execution_attempts": [
                _summarize_execution_attempt(row)
                for row in _sorted_by_activity(open_execution_attempts)
            ],
            "open_positions": open_positions,
            "top_positions": top_positions,
            "mark_health": {
                "status": mark_health_status,
                "missing_mark_count": missing_mark_count,
                "stale_mark_count": stale_mark_count,
                "broker_unquoted_position_count": broker_unquoted_positions,
                "mark_error": mark_error,
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
def build_sessions_view(
    *,
    db_target: str | None = None,
    session_id: str | None = None,
    session_date: str | None = None,
    limit: int = 25,
    storage: Any | None = None,
) -> dict[str, Any]:
    generated_at = _utc_now()
    attention: list[dict[str, str]] = []
    if session_id is not None:
        try:
            session = get_session_detail(
                db_target=db_target or "",
                session_id=session_id,
                profit_target=DEFAULT_ANALYSIS_PROFIT_TARGET,
                stop_multiple=DEFAULT_ANALYSIS_STOP_MULTIPLE,
                storage=storage,
            )
        except ValueError as exc:
            raise OpsLookupError(str(exc)) from exc

        analysis = session.get("analysis") if isinstance(session.get("analysis"), Mapping) else {}
        diagnostics = analysis.get("diagnostics") if isinstance(analysis, Mapping) and isinstance(analysis.get("diagnostics"), Mapping) else {}
        verdict = _as_text(diagnostics.get("overall_verdict"))
        board_watchlist_pnl_spread = _board_watchlist_pnl_spread(analysis if isinstance(analysis, Mapping) else None)
        ideas = list((analysis.get("outcomes") or {}).get("ideas") or []) if isinstance(analysis, Mapping) else []
        top_modeled_ideas, bottom_modeled_ideas = _rank_modeled_ideas([dict(row) for row in ideas if isinstance(row, Mapping)])

        statuses = [_combine_statuses("healthy")]
        session_status = str(session.get("status") or "idle")
        if session_status == "failed":
            statuses.append("blocked")
            attention.append(
                _attention(
                    severity="high",
                    code="session_failed",
                    message=f"Session {session_id} has a failed collector run.",
                )
            )
        elif session_status == "degraded":
            statuses.append("degraded")
            attention.append(
                _attention(
                    severity="medium",
                    code="session_degraded",
                    message=f"Session {session_id} is degraded.",
                )
            )

        latest_slot = session.get("latest_slot") if isinstance(session.get("latest_slot"), Mapping) else {}
        capture_status = _as_text(latest_slot.get("capture_status"))
        if capture_status not in {None, "healthy"}:
            statuses.append("degraded")
            attention.append(
                _attention(
                    severity="medium",
                    code="capture_degraded",
                    message="The latest collector slot capture status is degraded.",
                )
            )

        if str(session.get("risk_status") or "") == "blocked":
            statuses.append("blocked")
            attention.append(
                _attention(
                    severity="high",
                    code="session_risk_blocked",
                    message=_as_text(session.get("risk_note")) or "Session risk policy is blocked.",
                )
            )
        elif str(session.get("risk_status") or "") not in {"", "ok", "disabled"}:
            statuses.append("degraded")

        if str(session.get("reconciliation_status") or "") == "mismatch":
            statuses.append("degraded")
            attention.append(
                _attention(
                    severity="medium",
                    code="session_reconciliation_mismatch",
                    message=_as_text(session.get("reconciliation_note")) or "Session reconciliation has mismatches.",
                )
            )

        control = session.get("control") if isinstance(session.get("control"), Mapping) else {}
        statuses.append(_control_status(control))
        if verdict == "weak":
            statuses.append("degraded")
            attention.append(
                _attention(
                    severity="medium",
                    code="post_market_weak",
                    message="Latest post-market verdict is weak.",
                )
            )

        portfolio = session.get("portfolio") if isinstance(session.get("portfolio"), Mapping) else {}
        portfolio_summary = portfolio.get("summary") if isinstance(portfolio.get("summary"), Mapping) else {}
        current_cycle = session.get("current_cycle") if isinstance(session.get("current_cycle"), Mapping) else {}
        recommendations = list(analysis.get("recommendations") or []) if isinstance(analysis, Mapping) else []
        slot_runs = [
            _summarize_slot_run(row)
            for row in list(session.get("slot_runs") or [])[:10]
        ]
        alerts = [_summarize_alert(row) for row in list(session.get("alerts") or [])[:25]]
        executions = [
            _summarize_execution_attempt(row)
            for row in list(session.get("executions") or [])[:25]
            if isinstance(row, Mapping)
        ]

        return {
            "status": _combine_statuses(*statuses),
            "generated_at": generated_at,
            "summary": {
                "view": "detail",
                "session_id": session.get("session_id"),
                "label": session.get("label"),
                "session_date": session.get("session_date"),
                "latest_capture_status": capture_status,
                "risk_status": session.get("risk_status"),
                "reconciliation_status": session.get("reconciliation_status"),
                "control_mode": control.get("mode"),
                "alert_count": len(list(session.get("alerts") or [])),
                "execution_count": len(list(session.get("executions") or [])),
                "open_position_count": portfolio_summary.get("open_position_count"),
                "post_market_verdict": verdict,
                "board_watchlist_pnl_spread": board_watchlist_pnl_spread,
            },
            "attention": attention,
            "details": {
                "view": "detail",
                "current_cycle_id": current_cycle.get("cycle_id"),
                "current_cycle_generated_at": current_cycle.get("generated_at"),
                "board_count": len(list(current_cycle.get("board_candidates") or [])),
                "watchlist_count": len(list(current_cycle.get("watchlist_candidates") or [])),
                "slot_runs": slot_runs,
                "alerts": alerts,
                "executions": executions,
                "portfolio_summary": portfolio_summary,
                "recommendations": recommendations,
                "top_modeled_ideas": top_modeled_ideas,
                "bottom_modeled_ideas": bottom_modeled_ideas,
                "post_market_verdict": verdict,
                "board_watchlist_pnl_spread": board_watchlist_pnl_spread,
            },
        }

    listed = list_existing_sessions(
        db_target=db_target or "",
        limit=limit,
        session_date=session_date,
        storage=storage,
    )
    rows = [dict(row) for row in listed.get("sessions") or []]
    enriched_rows: list[dict[str, Any]] = []
    post_market_store = storage.post_market
    for row in rows:
        post_market = (
            _post_market_view(
                None
                if not post_market_store.schema_ready()
                else post_market_store.get_latest_run(
                    label=str(row["label"]),
                    session_date=str(row["session_date"]),
                    succeeded_only=True,
                )
            )
        )
        operator_status = "healthy"
        if str(row.get("status") or "") == "failed":
            operator_status = "blocked"
        elif str(row.get("status") or "") in {"degraded"} or str(row.get("latest_capture_status") or "") not in {"", "healthy"}:
            operator_status = "degraded"
        elif str(row.get("status") or "") == "idle":
            operator_status = "idle"
        if post_market["overall_verdict"] == "weak":
            operator_status = _combine_statuses(operator_status, "degraded")
        enriched = {
            **row,
            "operator_status": operator_status,
            "post_market_verdict": post_market["overall_verdict"],
            "board_watchlist_pnl_spread": post_market["board_watchlist_pnl_spread"],
        }
        enriched_rows.append(enriched)
        if operator_status in {"blocked", "degraded"}:
            attention.append(
                _attention(
                    severity="high" if operator_status == "blocked" else "medium",
                    code="session_attention_required",
                    message=f"{row['session_id']} ({row['label']}) is {operator_status}.",
                )
            )

    overall_status = "idle" if not enriched_rows else _combine_statuses(*(row["operator_status"] for row in enriched_rows))
    return {
        "status": overall_status,
        "generated_at": generated_at,
        "summary": {
            "view": "list",
            "session_count": len(enriched_rows),
            "session_date": session_date,
            "status_counts": dict(Counter(str(row.get("operator_status") or "unknown") for row in enriched_rows)),
            "verdict_counts": dict(
                Counter(str(row.get("post_market_verdict") or "none") for row in enriched_rows)
            ),
        },
        "attention": attention[:10],
        "details": {
            "view": "list",
            "sessions": enriched_rows,
        },
    }
