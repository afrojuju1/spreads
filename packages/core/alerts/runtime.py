from __future__ import annotations

from typing import Any, Mapping

from core.services.alert_delivery import plan_alert_delivery
from core.services.live_pipelines import build_live_run_scope_id

RUNTIME_ENTRY_SELECTED_ALERT_TYPE = "runtime_entry_selected"


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _as_float(value: Any) -> float | None:
    try:
        if value not in (None, ""):
            return float(value)
    except (TypeError, ValueError):
        return None
    return None


def runtime_entry_selected_key(
    *,
    market_date: str,
    bot_id: str,
    automation_id: str,
    opportunity_id: str,
) -> str:
    return (
        f"{RUNTIME_ENTRY_SELECTED_ALERT_TYPE}|{market_date}|{bot_id}|"
        f"{automation_id}|{opportunity_id}"
    )


def _selected_score(
    *,
    opportunity: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> float:
    for key in ("score",):
        value = _as_float(decision.get(key))
        if value is not None:
            return value
    for key in ("execution_score", "promotion_score"):
        value = _as_float(opportunity.get(key))
        if value is not None:
            return value
    candidate = opportunity.get("candidate")
    if isinstance(candidate, Mapping):
        value = _as_float(candidate.get("quality_score"))
        if value is not None:
            return value
    return 0.0


def _candidate_payload(
    *,
    opportunity: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_source = opportunity.get("candidate")
    candidate = dict(candidate_source) if isinstance(candidate_source, Mapping) else {}
    score = _selected_score(opportunity=opportunity, decision=decision)
    symbol = _as_text(opportunity.get("underlying_symbol")) or _as_text(
        candidate.get("underlying_symbol")
    )
    strategy = _as_text(candidate.get("strategy")) or _as_text(
        opportunity.get("strategy_family")
    )
    candidate.setdefault("underlying_symbol", symbol or "UNKNOWN")
    candidate.setdefault("strategy", strategy or "unknown")
    candidate.setdefault("quality_score", score)
    candidate.setdefault("promotion_score", opportunity.get("promotion_score") or score)
    candidate.setdefault("execution_score", opportunity.get("execution_score") or score)
    candidate.setdefault("selection_notes", list(opportunity.get("reason_codes") or []))
    candidate.setdefault("setup_status", "selected")
    candidate.setdefault("calendar_status", "unknown")
    candidate.setdefault("data_status", "unknown")
    return candidate


def _alert_description(
    *,
    bot_id: str,
    automation_id: str,
    decision: Mapping[str, Any],
    execution_intent: Mapping[str, Any] | None,
    execution_mode: str | None,
    approval_mode: str | None,
) -> str:
    score = _as_float(decision.get("score"))
    score_text = "n/a" if score is None else f"{score:.1f}"
    intent_id = (
        None
        if execution_intent is None
        else _as_text(execution_intent.get("execution_intent_id"))
    )
    details = [
        f"{bot_id}/{automation_id} selected this opportunity for entry",
        f"score {score_text}",
    ]
    if execution_mode:
        details.append(f"execution {execution_mode}")
    if approval_mode:
        details.append(f"approval {approval_mode}")
    if intent_id:
        details.append(f"intent {intent_id}")
    return "; ".join(details) + "."


def plan_runtime_entry_selected_alert(
    *,
    alert_store: Any,
    job_store: Any,
    bot_id: str,
    automation_id: str,
    market_date: str,
    run_key: str,
    opportunity: Mapping[str, Any],
    decision: Mapping[str, Any],
    execution_intent: Mapping[str, Any] | None,
    execution_mode: str | None,
    approval_mode: str | None,
    planner_job_run_id: str | None = None,
    dispatch_job_run_id: str | None = None,
) -> dict[str, Any] | None:
    if alert_store is None or job_store is None:
        return None
    if hasattr(alert_store, "schema_ready") and not alert_store.schema_ready():
        return None
    if hasattr(job_store, "schema_ready") and not job_store.schema_ready():
        return None

    opportunity_id = str(opportunity["opportunity_id"])
    candidate_source = opportunity.get("candidate")
    candidate_mapping = candidate_source if isinstance(candidate_source, Mapping) else {}
    symbol = (
        _as_text(opportunity.get("underlying_symbol"))
        or _as_text(candidate_mapping.get("underlying_symbol"))
        or "UNKNOWN"
    )
    label = _as_text(opportunity.get("label")) or f"{bot_id}:{automation_id}"
    session_id = build_live_run_scope_id(label, market_date)
    cycle_id = _as_text(opportunity.get("cycle_id")) or run_key
    candidate = _candidate_payload(opportunity=opportunity, decision=decision)
    execution_intent_id = (
        None
        if execution_intent is None
        else _as_text(execution_intent.get("execution_intent_id"))
    )
    decision_id = _as_text(decision.get("opportunity_decision_id"))
    score = _selected_score(opportunity=opportunity, decision=decision)
    reason_codes = [str(value) for value in list(decision.get("reason_codes") or [])]
    details = {
        "bot_id": bot_id,
        "automation_id": automation_id,
        "opportunity_id": opportunity_id,
        "opportunity_decision_id": decision_id,
        "execution_intent_id": execution_intent_id,
        "dispatch_job_run_id": dispatch_job_run_id,
        "score": score,
        "rank": decision.get("rank"),
        "reason_codes": reason_codes,
        "execution_mode": execution_mode,
        "approval_mode": approval_mode,
    }
    payload = {
        "created_at": _as_text(decision.get("decided_at")) or _as_text(
            opportunity.get("updated_at")
        ),
        "session_date": market_date,
        "label": label,
        "cycle_id": cycle_id,
        "symbol": symbol,
        "alert_type": RUNTIME_ENTRY_SELECTED_ALERT_TYPE,
        "strategy_mode": str(opportunity.get("strategy_family") or "runtime_entry"),
        "profile": str(opportunity.get("profile") or "runtime"),
        "candidate": candidate,
        "description": _alert_description(
            bot_id=bot_id,
            automation_id=automation_id,
            decision=decision,
            execution_intent=execution_intent,
            execution_mode=execution_mode,
            approval_mode=approval_mode,
        ),
        "details": details,
    }
    if not payload["created_at"]:
        from core.services.value_coercion import utc_now_iso

        payload["created_at"] = utc_now_iso()

    record, created = plan_alert_delivery(
        alert_store=alert_store,
        job_store=job_store,
        payload=payload,
        dedupe_key=runtime_entry_selected_key(
            market_date=market_date,
            bot_id=bot_id,
            automation_id=automation_id,
            opportunity_id=opportunity_id,
        ),
        dedupe_state=details,
        session_id=session_id,
        planner_job_run_id=planner_job_run_id,
        source="alerts.runtime",
        correlation_id=run_key,
    )
    return {
        "alert_id": record.get("alert_id"),
        "alert_type": RUNTIME_ENTRY_SELECTED_ALERT_TYPE,
        "status": record.get("status"),
        "created": created,
        "delivery_job_run_id": record.get("delivery_job_run_id"),
    }


__all__ = [
    "RUNTIME_ENTRY_SELECTED_ALERT_TYPE",
    "plan_runtime_entry_selected_alert",
    "runtime_entry_selected_key",
]
