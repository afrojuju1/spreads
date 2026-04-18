from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.db.decorators import with_storage
from core.services.audit_snapshot import DEFAULT_EVENT_SCAN_LIMIT, build_audit_snapshot
from core.services.execution import OPEN_STATUSES
from core.services.value_coercion import as_text as _as_text, utc_now_iso as _utc_now

from .shared import (
    OpsLookupError,
    _attention,
    _combine_statuses,
    _control_status,
    _session_status,
)


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
        audit_snapshot = build_audit_snapshot(
            db_target=db_target or "",
            pipeline_id=pipeline_id,
            market_date=market_date,
            timeline_limit=timeline_limit,
            event_scan_limit=event_scan_limit,
            storage=storage,
        )
    except ValueError as exc:
        raise OpsLookupError(str(exc)) from exc

    target = (
        audit_snapshot.get("target")
        if isinstance(audit_snapshot.get("target"), Mapping)
        else {}
    )
    timeline_stats = (
        audit_snapshot.get("timeline_stats")
        if isinstance(audit_snapshot.get("timeline_stats"), Mapping)
        else {}
    )
    state_summary = (
        audit_snapshot.get("state_summary")
        if isinstance(audit_snapshot.get("state_summary"), Mapping)
        else {}
    )
    explanations = (
        audit_snapshot.get("explanations")
        if isinstance(audit_snapshot.get("explanations"), Mapping)
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
        audit_snapshot.get("post_market")
        if isinstance(audit_snapshot.get("post_market"), Mapping)
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
                    f"Audit hit the event scan limit of {timeline_stats.get('event_scan_limit')}; "
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
                for row in list(audit_snapshot.get("slot_runs") or [])
                if isinstance(row, Mapping)
            ],
            "alerts": [
                dict(row)
                for row in list(audit_snapshot.get("alerts") or [])
                if isinstance(row, Mapping)
            ],
            "selected_opportunities": selected_opportunities,
            "risk_decisions": risk_decisions,
            "execution_outcomes": execution_outcomes,
            "control_actions": control_actions,
            "timeline_stats": dict(timeline_stats),
            "timeline": [
                dict(row)
                for row in list(audit_snapshot.get("timeline") or [])
                if isinstance(row, Mapping)
            ],
        },
    }
