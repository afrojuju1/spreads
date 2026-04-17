from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from core.db.decorators import with_storage
from core.services.bot_analytics import build_automation_performance_summary
from core.services.broker_sync import BROKER_SYNC_KEY
from core.services.execution import OPEN_STATUSES
from core.services.execution_lifecycle import (
    classify_open_execution_attempt,
    is_open_execution_attempt_status,
    resolve_execution_attempt_source_job,
    resolve_execution_submit_job_run_id,
)
from core.services.risk_manager import assess_position_risk
from core.services.selection_summary import aggregate_selection_summaries as _aggregate_selection_summaries
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_float as _coerce_float,
    coerce_int as _coerce_int,
    utc_now_iso as _utc_now,
)
from core.jobs.orchestration import NEW_YORK

from .collectors import (
    _bot_runtime_summary,
    _latest_live_collectors,
    _market_session_context,
)
from .shared import (
    _attention,
    _automation_dispatch_gap_summary,
    _combine_statuses,
    _control_status,
    _seconds_since,
    _sorted_by_activity,
)

OPEN_POSITION_STATUSES = ["open", "partial_close"]
BROKER_SYNC_STALE_AFTER_SECONDS = 15 * 60
MARK_STALE_AFTER_SECONDS = 15 * 60
TOP_POSITION_LIMIT = 5


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
    summary = payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
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
        if _seconds_since(row.get("updated_at") or row.get("created_at"), now=now)
        is not None
        and (_seconds_since(row.get("updated_at") or row.get("created_at"), now=now) or 0)
        <= 24 * 60 * 60
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
            source_definitions[source_job_key] = job_store.get_job_definition(source_job_key)
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


@with_storage()
def build_trading_health(
    *,
    db_target: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    from . import (
        get_account_overview,
        get_control_state_snapshot,
        resolve_execution_kill_switch_reason,
    )

    generated_at = _utc_now()
    now = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
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
    automation_dispatch_gap = _automation_dispatch_gap_summary(
        details["automation_performance"]
    )
    details["automation_dispatch_gap"] = automation_dispatch_gap
    if automation_dispatch_gap["has_dispatch_gap"]:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="automation_entry_dispatch_gap",
                message=(
                    f"Automation selected {automation_dispatch_gap['selected_count']} entry "
                    f"opportunity(s) today, but {automation_dispatch_gap['dispatch_window_elapsed_count']} "
                    "aged out before submission."
                ),
            )
        )
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
        from core.services.positions import enrich_position_row

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
        "account_source": None if account_overview is None else account_overview.get("source"),
        "environment": None if account_overview is None else account_overview.get("environment"),
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
        "automation_entry_intent_count": _coerce_int(
            (details.get("automation_runtime") or {}).get("entry_intent_count")
        )
        or 0,
        "automation_management_intent_count": _coerce_int(
            (details.get("automation_runtime") or {}).get("management_intent_count")
        )
        or 0,
        "automation_dispatch_gap_count": _coerce_int(
            automation_dispatch_gap.get("dispatch_window_elapsed_count")
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
