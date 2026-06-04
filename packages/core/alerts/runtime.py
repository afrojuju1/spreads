from __future__ import annotations

from typing import Any, Mapping

from core.services.alert_delivery import plan_alert_delivery
from core.services.runtime_identity import build_live_run_scope_id

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
    trading_strategy_id: str,
    trade_signal_id: str,
) -> str:
    return f"{RUNTIME_ENTRY_SELECTED_ALERT_TYPE}|{market_date}|{trading_strategy_id}|{trade_signal_id}"


def _selected_score(
    *,
    trade_signal: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> float:
    for key in ("score",):
        value = _as_float(decision.get(key))
        if value is not None:
            return value
    for key in ("execution_score", "promotion_score"):
        value = _as_float(trade_signal.get(key))
        if value is not None:
            return value
    candidate = trade_signal.get("candidate")
    if isinstance(candidate, Mapping):
        value = _as_float(candidate.get("quality_score"))
        if value is not None:
            return value
    return 0.0


def _candidate_payload(
    *,
    trade_signal: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_source = trade_signal.get("candidate")
    candidate = dict(candidate_source) if isinstance(candidate_source, Mapping) else {}
    score = _selected_score(trade_signal=trade_signal, decision=decision)
    symbol = _as_text(trade_signal.get("underlying_symbol")) or _as_text(candidate.get("underlying_symbol"))
    strategy = _as_text(candidate.get("strategy")) or _as_text(trade_signal.get("strategy_family"))
    candidate.setdefault("underlying_symbol", symbol or "UNKNOWN")
    candidate.setdefault("strategy", strategy or "unknown")
    candidate.setdefault("quality_score", score)
    candidate.setdefault("promotion_score", trade_signal.get("promotion_score") or score)
    candidate.setdefault("execution_score", trade_signal.get("execution_score") or score)
    candidate.setdefault("selection_notes", list(trade_signal.get("reason_codes") or []))
    candidate.setdefault("setup_status", "selected")
    candidate.setdefault("calendar_status", "unknown")
    candidate.setdefault("data_status", "unknown")
    return candidate


def _execution_admission_payload(
    execution_intent: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(execution_intent, Mapping):
        return None
    payload = execution_intent.get("payload")
    if not isinstance(payload, Mapping):
        return None
    raw = payload.get("execution_admission")
    if not isinstance(raw, Mapping):
        return None
    admission = {
        "status": _as_text(raw.get("status")),
        "reason": _as_text(raw.get("reason")),
        "message": _as_text(raw.get("message")),
        "evaluated_at": _as_text(raw.get("evaluated_at")),
        "admissible_quantity": (None if raw.get("admissible_quantity") in (None, "") else int(raw.get("admissible_quantity"))),
        "required_buying_power": _as_float(raw.get("required_buying_power")),
        "available_buying_power": _as_float(raw.get("available_buying_power")),
        "account_available_buying_power": _as_float(raw.get("account_available_buying_power")),
        "reserved_buying_power": _as_float(raw.get("reserved_buying_power")),
        "buying_power_basis": _as_text(raw.get("buying_power_basis")),
        "buying_power_source_field": _as_text(raw.get("buying_power_source_field")),
        "broker_buying_power_status": _as_text(raw.get("broker_buying_power_status")),
        "limiting_constraint": _as_text(raw.get("limiting_constraint")),
        "strategy_risk_budget": _as_float(raw.get("strategy_risk_budget")),
    }
    if not any(value not in (None, "") for value in admission.values()):
        return None
    return admission


def _alert_description(
    *,
    trading_strategy_id: str,
    decision: Mapping[str, Any],
    execution_intent: Mapping[str, Any] | None,
    execution_admission: Mapping[str, Any] | None,
    execution_mode: str | None,
    approval_mode: str | None,
) -> str:
    score = _as_float(decision.get("score"))
    score_text = "n/a" if score is None else f"{score:.1f}"
    intent_id = None if execution_intent is None else _as_text(execution_intent.get("execution_intent_id"))
    details = [
        f"{trading_strategy_id} selected this trade signal for entry",
        f"score {score_text}",
    ]
    if execution_mode:
        details.append(f"execution {execution_mode}")
    if approval_mode:
        details.append(f"approval {approval_mode}")
    if intent_id:
        details.append(f"intent {intent_id}")
    if isinstance(execution_admission, Mapping):
        admission_status = _as_text(execution_admission.get("status"))
        admission_reason = _as_text(execution_admission.get("reason"))
        admissible_quantity = execution_admission.get("admissible_quantity")
        if admission_status == "admissible" and admissible_quantity not in (None, ""):
            details.append(f"account qty {int(admissible_quantity)}")
        elif admission_status:
            rendered = f"account {admission_status}"
            if admission_reason:
                rendered += f" {admission_reason}"
            details.append(rendered)
    return "; ".join(details) + "."


def plan_runtime_entry_selected_alert(
    *,
    alert_store: Any,
    job_store: Any,
    trading_strategy_id: str,
    market_date: str,
    run_key: str,
    trade_signal: Mapping[str, Any],
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

    trade_signal_id = _as_text(trade_signal.get("trade_signal_id")) or _as_text(decision.get("trade_signal_id"))
    candidate_source = trade_signal.get("candidate")
    candidate_mapping = candidate_source if isinstance(candidate_source, Mapping) else {}
    symbol = _as_text(trade_signal.get("underlying_symbol")) or _as_text(candidate_mapping.get("underlying_symbol")) or "UNKNOWN"
    label = _as_text(trade_signal.get("label")) or trading_strategy_id
    session_id = build_live_run_scope_id(label, market_date)
    cycle_id = _as_text(trade_signal.get("source_cycle_id")) or run_key
    candidate = _candidate_payload(trade_signal=trade_signal, decision=decision)
    execution_intent_id = None if execution_intent is None else _as_text(execution_intent.get("execution_intent_id"))
    score = _selected_score(trade_signal=trade_signal, decision=decision)
    reason_codes = [str(value) for value in list(decision.get("reason_codes") or [])]
    execution_admission = _execution_admission_payload(execution_intent)
    details = {
        "trading_strategy_id": trading_strategy_id,
        "trade_decision_id": _as_text(decision.get("trade_decision_id")),
        "trade_signal_id": trade_signal_id,
        "execution_intent_id": execution_intent_id,
        "dispatch_job_run_id": dispatch_job_run_id,
        "score": score,
        "rank": decision.get("rank"),
        "reason_codes": reason_codes,
        "execution_mode": execution_mode,
        "approval_mode": approval_mode,
        "execution_admission_status": None if execution_admission is None else execution_admission.get("status"),
        "execution_admission_reason": None if execution_admission is None else execution_admission.get("reason"),
    }
    payload = {
        "created_at": _as_text(decision.get("decided_at")) or _as_text(trade_signal.get("updated_at")),
        "session_date": market_date,
        "label": label,
        "cycle_id": cycle_id,
        "symbol": symbol,
        "alert_type": RUNTIME_ENTRY_SELECTED_ALERT_TYPE,
        "strategy_mode": str(trade_signal.get("strategy_family") or "runtime_entry"),
        "profile": str(trade_signal.get("profile") or "runtime"),
        "candidate": candidate,
        "execution_admission": execution_admission,
        "description": _alert_description(
            trading_strategy_id=trading_strategy_id,
            decision=decision,
            execution_intent=execution_intent,
            execution_admission=execution_admission,
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
            trading_strategy_id=trading_strategy_id,
            trade_signal_id=trade_signal_id or execution_intent_id,
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
