from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.money import money_float, option_contract_notional
from core.services.account_capacity import (
    estimate_buying_power_requirement,
    resolve_available_buying_power,
)
from core.services.admission_lifecycle import (
    admission_allows_attempt,
    normalize_lifecycle_admission,
)
from core.services.trading_strategy_runtime import resolve_entry_runtime
from core.services.execution_lifecycle import (
    OPEN_ATTEMPT_STATUS_LIST,
    resolve_execution_attempt_filled_quantity,
)
from core.services.risk_manager import (
    resolve_position_size_policy,
)
from core.services.session_positions import (
    OPEN_TRADE_INTENT,
)
from core.value_coercion import (
    as_text,
    coerce_float,
    coerce_int,
    utc_now_iso,
)
from .policy import (
    normalize_execution_policy,
)


class ExecutionAdmissionError(ValueError):
    def __init__(self, message: str, *, admission: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.admission = dict(admission)


def _execution_admission_payload_from_risk_evaluation(
    risk_evaluation: Mapping[str, Any],
    *,
    admission_kind: str = "open_execution",
    source_object_type: str | None = None,
    source_object_id: str | None = None,
    session_date: str | None = None,
    requested_notional: float | None = None,
    max_loss: float | None = None,
) -> dict[str, Any]:
    metrics = risk_evaluation.get("metrics") if isinstance(risk_evaluation.get("metrics"), Mapping) else {}
    position_sizing = metrics.get("position_sizing") if isinstance(metrics.get("position_sizing"), Mapping) else {}
    requested_quantity = max(coerce_float(metrics.get("requested_quantity")) or 0.0, 0.0)
    required_buying_power = coerce_float(metrics.get("required_buying_power"))
    available_buying_power = coerce_float(metrics.get("available_broker_buying_power"))
    reserved_buying_power = coerce_float(metrics.get("broker_reserved_buying_power"))
    account_available_buying_power = None
    if available_buying_power is not None:
        account_available_buying_power = money_float(available_buying_power + max(reserved_buying_power or 0.0, 0.0))
    reason_codes = [str(value).strip() for value in risk_evaluation.get("reason_codes") or [] if str(value).strip()]
    resolved_status = str(risk_evaluation.get("status") or "unknown").strip().lower()
    resolved_reason = None if reason_codes[:1] == ["approved"] else reason_codes[0] if reason_codes else None
    admissible_quantity = coerce_int(metrics.get("recommended_quantity"))
    if resolved_status == "blocked" and admissible_quantity is None:
        admissible_quantity = 0
    snapshot = {
        "status": "approved" if resolved_status == "approved" else resolved_status,
        "reason": resolved_reason,
        "message": str(risk_evaluation.get("note") or "") or None,
        "evaluated_at": utc_now_iso(),
        "admissible_quantity": admissible_quantity,
        "required_buying_power": required_buying_power,
        "available_buying_power": available_buying_power,
        "account_available_buying_power": account_available_buying_power,
        "reserved_buying_power": reserved_buying_power,
        "buying_power_basis": as_text(metrics.get("buying_power_basis")),
        "buying_power_source_field": as_text(metrics.get("broker_buying_power_source_field")),
        "broker_buying_power_status": as_text(metrics.get("broker_buying_power_status")),
        "limiting_constraint": as_text(position_sizing.get("limiting_constraint")),
        "strategy_risk_budget": coerce_float(metrics.get("strategy_risk_budget")),
        "position_size_pct_of_available_balance": coerce_float(position_sizing.get("position_size_pct_of_available_balance")),
        "position_size_budget": coerce_float(position_sizing.get("position_size_budget")),
        "requested_quantity": None if requested_quantity <= 0 else int(requested_quantity),
    }
    return normalize_lifecycle_admission(
        snapshot,
        admission_kind=admission_kind,
        source_object_type=source_object_type,
        source_object_id=source_object_id,
        session_date=session_date,
        requested_quantity=None if requested_quantity <= 0 else int(requested_quantity),
        requested_notional=requested_notional,
        max_loss=max_loss,
        policy_snapshot=risk_evaluation.get("policy") if isinstance(risk_evaluation.get("policy"), Mapping) else {},
        metrics=metrics,
        evidence=risk_evaluation.get("evidence") if isinstance(risk_evaluation.get("evidence"), Mapping) else {},
        reason_codes=reason_codes,
        blockers=[str(value) for value in risk_evaluation.get("blockers") or [] if str(value).strip()],
    )


def _execution_admission_payload_from_account_capacity(
    *,
    attempt: Mapping[str, Any],
    account_capacity: Mapping[str, Any],
) -> dict[str, Any]:
    request = attempt.get("request") if isinstance(attempt.get("request"), Mapping) else {}
    required_buying_power = coerce_float(account_capacity.get("required_buying_power"))
    available_buying_power = coerce_float(account_capacity.get("available_buying_power"))
    reserved_buying_power = coerce_float(account_capacity.get("reserved_buying_power"))
    requested_quantity = max(coerce_float(attempt.get("quantity")) or 0.0, 0.0)
    admissible_quantity = 0
    if requested_quantity > 0 and required_buying_power is not None and required_buying_power > 0 and available_buying_power is not None:
        admissible_quantity = max(
            int(available_buying_power // (required_buying_power / requested_quantity)),
            0,
        )
    account_available_buying_power = None
    if available_buying_power is not None:
        account_available_buying_power = round(
            available_buying_power + max(reserved_buying_power or 0.0, 0.0),
            2,
        )
    buying_power_basis = as_text(
        estimate_buying_power_requirement(
            dict(attempt.get("candidate") or {}),
            1.0,
            limit_price=coerce_float(attempt.get("limit_price")),
        ).get("basis")
    )
    snapshot = {
        "status": "blocked",
        "reason": as_text(account_capacity.get("reason")),
        "message": as_text(account_capacity.get("message")),
        "evaluated_at": utc_now_iso(),
        "admissible_quantity": admissible_quantity,
        "required_buying_power": required_buying_power,
        "available_buying_power": available_buying_power,
        "account_available_buying_power": account_available_buying_power,
        "reserved_buying_power": reserved_buying_power,
        "buying_power_basis": buying_power_basis,
        "buying_power_source_field": as_text(account_capacity.get("source_field")),
        "broker_buying_power_status": "ok",
        "limiting_constraint": "available_broker_buying_power",
        "strategy_risk_budget": None,
        "requested_quantity": None if requested_quantity <= 0 else int(requested_quantity),
    }
    source_object_id = as_text(request.get("execution_intent_id")) or as_text(attempt.get("execution_attempt_id"))
    return normalize_lifecycle_admission(
        snapshot,
        admission_kind="submit_account_capacity",
        source_object_type="execution_intent" if as_text(request.get("execution_intent_id")) is not None else "execution_attempt",
        source_object_id=source_object_id,
        session_date=as_text(attempt.get("session_date")) or as_text(attempt.get("market_date")),
        requested_quantity=None if requested_quantity <= 0 else int(requested_quantity),
        requested_notional=option_contract_notional(
            coerce_float(attempt.get("limit_price")),
            None if requested_quantity <= 0 else int(requested_quantity),
        ),
        policy_snapshot=request.get("risk_policy") if isinstance(request.get("risk_policy"), Mapping) else {},
        capability_snapshot=account_capacity,
        metrics={
            "required_buying_power": required_buying_power,
            "available_buying_power": available_buying_power,
            "reserved_buying_power": reserved_buying_power,
            "account_available_buying_power": account_available_buying_power,
            "buying_power_basis": buying_power_basis,
            "buying_power_source_field": as_text(account_capacity.get("source_field")),
            "admissible_quantity": admissible_quantity,
        },
        evidence={"account_capacity": dict(account_capacity)},
        reason_codes=[as_text(account_capacity.get("reason")) or "insufficient_buying_power"],
        blockers=[as_text(account_capacity.get("reason")) or "insufficient_buying_power"],
    )


def _execution_admission_payload_from_broker_rejection(
    *,
    attempt: Mapping[str, Any],
    classified_error: Mapping[str, Any],
) -> dict[str, Any]:
    request = attempt.get("request") if isinstance(attempt.get("request"), Mapping) else {}
    quantity = max(coerce_float(attempt.get("quantity")) or 0.0, 0.0)
    requirement = estimate_buying_power_requirement(
        dict(attempt.get("candidate") or {}),
        quantity,
        limit_price=coerce_float(attempt.get("limit_price")),
    )
    required_buying_power = coerce_float(requirement.get("required_buying_power"))
    if as_text(classified_error.get("reason")) not in {
        "insufficient_options_buying_power",
        "insufficient_buying_power",
    }:
        required_buying_power = None
    snapshot = {
        "status": "blocked",
        "reason": as_text(classified_error.get("reason")),
        "message": as_text(classified_error.get("message")),
        "evaluated_at": utc_now_iso(),
        "admissible_quantity": 0,
        "required_buying_power": required_buying_power,
        "available_buying_power": None,
        "account_available_buying_power": None,
        "reserved_buying_power": None,
        "buying_power_basis": as_text(requirement.get("basis")),
        "buying_power_source_field": None,
        "broker_buying_power_status": "rejected",
        "limiting_constraint": None,
        "strategy_risk_budget": None,
        "requested_quantity": None if quantity <= 0 else int(quantity),
    }
    source_object_id = as_text(request.get("execution_intent_id")) or as_text(attempt.get("execution_attempt_id"))
    return normalize_lifecycle_admission(
        snapshot,
        admission_kind="broker_rejection",
        source_object_type="execution_intent" if as_text(request.get("execution_intent_id")) is not None else "execution_attempt",
        source_object_id=source_object_id,
        session_date=as_text(attempt.get("session_date")) or as_text(attempt.get("market_date")),
        requested_quantity=None if quantity <= 0 else int(quantity),
        requested_notional=option_contract_notional(
            coerce_float(attempt.get("limit_price")),
            None if quantity <= 0 else int(quantity),
        ),
        policy_snapshot=request.get("risk_policy") if isinstance(request.get("risk_policy"), Mapping) else {},
        capability_snapshot=classified_error,
        metrics={
            "required_buying_power": required_buying_power,
            "buying_power_basis": as_text(requirement.get("basis")),
            "broker_buying_power_status": "rejected",
        },
        evidence={"classified_error": dict(classified_error), "buying_power_requirement": dict(requirement)},
        reason_codes=[as_text(classified_error.get("reason")) or "broker_rejected"],
        blockers=[as_text(classified_error.get("reason")) or "broker_rejected"],
    )


def _execution_admission_payload_from_submission_guard(
    *,
    attempt: Mapping[str, Any],
    guard: Mapping[str, Any],
) -> dict[str, Any]:
    request = attempt.get("request") if isinstance(attempt.get("request"), Mapping) else {}
    quantity = max(coerce_float(attempt.get("quantity")) or 0.0, 0.0)
    reason = as_text(guard.get("reason")) or "submission_guard_blocked"
    message = as_text(guard.get("message")) or "Execution submission was blocked by a pre-submit guard."
    evidence = guard.get("evidence") if isinstance(guard.get("evidence"), Mapping) else {}
    source_object_id = as_text(request.get("execution_intent_id")) or as_text(attempt.get("execution_attempt_id"))
    return normalize_lifecycle_admission(
        {
            "status": "blocked",
            "reason": reason,
            "message": message,
            "evaluated_at": utc_now_iso(),
            "admissible_quantity": 0,
            "requested_quantity": None if quantity <= 0 else int(quantity),
        },
        admission_kind="submit_structure_guard",
        source_object_type="execution_intent" if as_text(request.get("execution_intent_id")) is not None else "execution_attempt",
        source_object_id=source_object_id,
        session_date=as_text(attempt.get("session_date")) or as_text(attempt.get("market_date")),
        requested_quantity=None if quantity <= 0 else int(quantity),
        requested_notional=option_contract_notional(
            coerce_float(attempt.get("limit_price")),
            None if quantity <= 0 else int(quantity),
        ),
        policy_snapshot=request.get("risk_policy") if isinstance(request.get("risk_policy"), Mapping) else {},
        capability_snapshot={"submission_guard": dict(guard)},
        metrics={
            "requested_quantity": None if quantity <= 0 else int(quantity),
            "requested_limit_price": coerce_float(attempt.get("limit_price")),
            **({} if not isinstance(evidence, Mapping) else dict(evidence)),
        },
        evidence={
            "submission_guard": dict(guard),
            "order": dict(request.get("order")) if isinstance(request.get("order"), Mapping) else {},
        },
        reason_codes=[str(value) for value in guard.get("reason_codes") or [reason] if str(value).strip()],
        blockers=[str(value) for value in guard.get("blockers") or [reason] if str(value).strip()],
    )


def _metadata_policy(metadata: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = metadata.get(key)
    return dict(value) if isinstance(value, Mapping) else {}


def _direct_order_execution_policy(
    metadata: Mapping[str, Any],
    *,
    risk_policy: Mapping[str, Any] | None,
    quantity: int,
) -> dict[str, Any]:
    raw_policy = _metadata_policy(metadata, "execution_policy")
    raw_policy.setdefault("enabled", True)
    raw_policy.setdefault("mode", "top_promotable")
    raw_policy.setdefault("quantity", quantity)
    if as_text(raw_policy.get("deployment_mode")) is None:
        raw_policy["deployment_mode"] = (
            as_text(metadata.get("deployment_mode"))
            or as_text(metadata.get("execution_deployment_mode"))
            or ("live_auto" if as_text(metadata.get("execution_mode")) == "live" else "paper_auto")
        )
    return normalize_execution_policy(
        {
            "execution_policy": raw_policy,
            "risk_policy": dict(risk_policy) if isinstance(risk_policy, Mapping) else None,
        }
    )


def _admission_source_from_metadata(
    metadata: Mapping[str, Any],
    *,
    fallback_type: str,
    fallback_id: str | None,
) -> tuple[str, str | None]:
    execution_intent_id = as_text(metadata.get("execution_intent_id"))
    if execution_intent_id is not None:
        return "execution_intent", execution_intent_id
    position_id = as_text(metadata.get("position_id"))
    if position_id is not None:
        return "position", position_id
    return fallback_type, fallback_id


def _metadata_trade_refs(metadata: Mapping[str, Any]) -> dict[str, str | None]:
    execution_admission = metadata.get("execution_admission") if isinstance(metadata.get("execution_admission"), Mapping) else {}
    return {
        "trade_signal_id": as_text(metadata.get("trade_signal_id")),
        "trade_decision_id": as_text(metadata.get("trade_decision_id")),
        "admission_decision_id": as_text(metadata.get("admission_decision_id")) or as_text(execution_admission.get("admission_decision_id")),
    }


def _attempt_source_from_metadata(
    metadata: Mapping[str, Any],
    *,
    fallback_type: str,
    fallback_id: str | None,
) -> tuple[str, str | None]:
    source_object_type = as_text(metadata.get("source_object_type"))
    source_object_id = as_text(metadata.get("source_object_id"))
    if source_object_type is not None and source_object_id is not None:
        return source_object_type, source_object_id

    trade_decision_id = as_text(metadata.get("trade_decision_id"))
    if trade_decision_id is not None:
        return "trade_decision", trade_decision_id
    trade_signal_id = as_text(metadata.get("trade_signal_id"))
    if trade_signal_id is not None:
        return "trade_signal", trade_signal_id

    close_decision = metadata.get("close_decision")
    if isinstance(close_decision, Mapping):
        close_decision_id = as_text(close_decision.get("close_decision_id"))
        if close_decision_id is not None:
            return "close_decision", close_decision_id

    source = metadata.get("source")
    if isinstance(source, Mapping):
        source_type = as_text(source.get("source_object_type")) or as_text(source.get("kind")) or as_text(source.get("source_type"))
        source_id = as_text(source.get("source_object_id")) or as_text(source.get("id")) or as_text(source.get("source_id"))
        if source_type is not None and source_id is not None:
            return source_type, source_id

    position_id = as_text(metadata.get("position_id"))
    if position_id is not None:
        return "position", position_id
    execution_intent_id = as_text(metadata.get("execution_intent_id"))
    if execution_intent_id is not None:
        return "execution_intent", execution_intent_id
    return fallback_type, fallback_id


def _attempt_ref_kwargs(
    metadata: Mapping[str, Any],
    *,
    fallback_type: str,
    fallback_id: str | None,
) -> dict[str, str | None]:
    source_object_type, source_object_id = _attempt_source_from_metadata(
        metadata,
        fallback_type=fallback_type,
        fallback_id=fallback_id,
    )
    return {
        "source_object_type": source_object_type,
        "source_object_id": source_object_id,
        **_metadata_trade_refs(metadata),
    }


def _raise_if_admission_blocks(admission: Mapping[str, Any]) -> None:
    if admission_allows_attempt(admission):
        return
    message = as_text(admission.get("message")) or as_text(admission.get("reason")) or "Execution admission blocked."
    raise ExecutionAdmissionError(message, admission=admission)


def _approved_execution_admission(
    *,
    admission_kind: str,
    source_object_type: str | None,
    source_object_id: str | None,
    session_date: str | None,
    requested_quantity: int | None,
    requested_notional: float | None,
    reason: str,
    message: str,
    max_loss: float | None = None,
    policy_snapshot: Mapping[str, Any] | None = None,
    evidence: Mapping[str, Any] | None = None,
    decided_at: str | None = None,
) -> dict[str, Any]:
    return normalize_lifecycle_admission(
        {
            "status": "approved",
            "reason": reason,
            "message": message,
            "evaluated_at": decided_at or utc_now_iso(),
        },
        admission_kind=admission_kind,
        source_object_type=source_object_type,
        source_object_id=source_object_id,
        session_date=session_date,
        requested_quantity=requested_quantity,
        requested_notional=requested_notional,
        max_loss=max_loss,
        policy_snapshot=policy_snapshot,
        evidence=evidence,
        reason_codes=[reason],
    )


def _pending_open_attempt_buying_power(
    *,
    execution_store: Any,
    exclude_execution_attempt_id: str | None = None,
) -> float:
    list_for_status = getattr(execution_store, "list_attempts_by_status", None)
    if not callable(list_for_status):
        return 0.0
    rows = list_for_status(
        statuses=list(OPEN_ATTEMPT_STATUS_LIST),
        trade_intent=OPEN_TRADE_INTENT,
        limit=200,
    )
    reserved_buying_power = 0.0
    for row in rows:
        attempt = dict(row)
        if exclude_execution_attempt_id is not None and as_text(attempt.get("execution_attempt_id")) == exclude_execution_attempt_id:
            continue
        requested_quantity = max(coerce_float(attempt.get("quantity")) or 0.0, 0.0)
        if requested_quantity <= 0:
            continue
        filled_quantity = min(
            resolve_execution_attempt_filled_quantity(attempt),
            requested_quantity,
        )
        pending_quantity = max(requested_quantity - filled_quantity, 0.0)
        if pending_quantity <= 0:
            continue
        requirement = estimate_buying_power_requirement(
            dict(attempt.get("candidate") or {}),
            pending_quantity,
            limit_price=coerce_float(attempt.get("limit_price")),
        )
        required_buying_power = coerce_float(requirement.get("required_buying_power"))
        if required_buying_power is None:
            continue
        reserved_buying_power += required_buying_power
    return money_float(reserved_buying_power)


def _validate_submit_account_capacity(
    *,
    execution_store: Any,
    attempt: Mapping[str, Any],
    client: Any,
) -> dict[str, Any]:
    requirement = estimate_buying_power_requirement(
        dict(attempt.get("candidate") or {}),
        coerce_float(attempt.get("quantity")) or 0.0,
        limit_price=coerce_float(attempt.get("limit_price")),
    )
    required_buying_power = coerce_float(requirement.get("required_buying_power"))
    if required_buying_power is None:
        return {"ok": True}

    try:
        account_payload = client.get_account()
    except Exception as exc:
        return {
            "ok": True,
            "status": "unavailable",
            "error_text": str(exc),
        }

    available_snapshot = resolve_available_buying_power(account_payload)
    available_buying_power = coerce_float(available_snapshot.get("available_buying_power"))
    if available_buying_power is None:
        return {
            "ok": True,
            "status": "unavailable",
            "error_text": "Broker account payload did not include usable buying power fields.",
        }

    reserved_buying_power = _pending_open_attempt_buying_power(
        execution_store=execution_store,
        exclude_execution_attempt_id=as_text(attempt.get("execution_attempt_id")),
    )
    remaining_buying_power = round(
        max(available_buying_power - reserved_buying_power, 0.0),
        2,
    )
    if required_buying_power > remaining_buying_power:
        source_field = as_text(available_snapshot.get("source_field"))
        source_note = "" if source_field is None else f" from {source_field}"
        return {
            "ok": False,
            "reason": "insufficient_broker_buying_power",
            "message": (
                "Open execution is blocked because broker buying power is insufficient"
                f"{source_note} (requires {required_buying_power:.2f}, "
                f"available {remaining_buying_power:.2f} after "
                f"{reserved_buying_power:.2f} reserved)."
            ),
            "required_buying_power": required_buying_power,
            "available_buying_power": remaining_buying_power,
            "reserved_buying_power": reserved_buying_power,
            "source_field": source_field,
        }
    return {
        "ok": True,
        "status": "ok",
        "required_buying_power": required_buying_power,
        "available_buying_power": remaining_buying_power,
        "reserved_buying_power": reserved_buying_power,
        "source_field": as_text(available_snapshot.get("source_field")),
    }


def _strategy_position_size_policy(
    *,
    trading_strategy_id: str | None,
) -> dict[str, float | None]:
    if trading_strategy_id is None:
        return {
            "max_risk_per_trade": None,
            "position_size_pct_of_available_balance": None,
        }
    try:
        runtime = resolve_entry_runtime(trading_strategy_id=trading_strategy_id)
    except ValueError:
        return {
            "max_risk_per_trade": None,
            "position_size_pct_of_available_balance": None,
        }
    return resolve_position_size_policy(runtime.build_settings.risk_defaults)
