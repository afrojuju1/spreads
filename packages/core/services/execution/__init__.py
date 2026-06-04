from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.db.decorators import with_storage
from core.integrations.alpaca.client import AlpacaRequestError
from core.integrations.alpaca.errors import classify_alpaca_request_error
from core.services.account_capacity import (
    estimate_buying_power_requirement,
    resolve_available_buying_power,
)
from core.services.admission_lifecycle import (
    admission_allows_attempt,
    normalize_lifecycle_admission,
)
from core.services.trading_strategy_runtime import resolve_entry_runtime
from core.services.candidate_policy import resolve_candidate_profile, resolve_deployment_quality_thresholds
from core.services.execution_lifecycle import (
    OPEN_ATTEMPT_STATUS_LIST,
    PENDING_SUBMISSION_STATUS,
    SUBMIT_UNKNOWN_STATUS,
    resolve_execution_attempt_filled_quantity,
)
from core.services.execution_portfolio import build_structure_quote_snapshot
from core.services.option_structures import (
    build_order_payload,
    candidate_legs,
    closing_legs,
    legs_identity_key,
    net_premium_kind,
    normalize_legs,
    order_payload_legs,
    structure_quote_snapshot,
)
from core.services.positions import enrich_position_row
from core.services.runtime_identity import (
    build_live_run_scope_id,
    build_pipeline_id,
    resolve_pipeline_policy_fields,
)
from core.services.risk_manager import (
    CLOSE_RECONCILIATION_MAX_AGE_SECONDS,
    build_open_candidate_position_sizing,
    evaluate_open_execution,
    resolve_position_size_policy,
    validate_close_execution,
)
from core.services.session_positions import (
    CLOSE_TRADE_INTENT,
    OPEN_TRADE_INTENT,
    resolve_trade_intent,
)
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_float as _coerce_float,
    coerce_int as _coerce_int,
    utc_now_iso as _utc_now,
)
from .alpaca_adapter import create_alpaca_order_adapter
from .attempts import (
    _flatten_order_snapshot,
    _get_attempt_payload,
    _publish_execution_attempt_event,
    _queue_execution_attempt,
    _reconcile_submit_unknown_attempt,
    _require_execution_schema,
    _submission_message,
    _sync_attempt_state,
    _sync_fill_rows,
    _sync_linked_execution_intent,
    list_session_execution_attempts as list_session_execution_attempts,
)
from .guard import run_open_execution_guard as run_open_execution_guard
from .policy import (
    _validate_open_timing_window,
    normalize_execution_policy,
)
from .runtimes import (
    ALPACA_DIRECT_RUNTIME,
    execution_runtime_from_request,
    normalize_execution_runtime,
)
from .shared import (
    BROKER_NAME,
    DEFAULT_ENTRY_PRICING_MODE,
    DEFAULT_MAX_CREDIT_CONCESSION,
    DEFAULT_MIN_CREDIT_RETENTION_PCT,
    EXECUTION_SCHEMA_MESSAGE as EXECUTION_SCHEMA_MESSAGE,
    OPEN_STATUSES,
    _candidate_with_payload,
    _clamp_fraction,
    _execution_attempt_id,
    _execution_client_order_id,
    _is_terminal_status,
    _normalize_limit_value,
    _resolve_completed_at,
    _strategy_family_from_payload,
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
    requested_quantity = max(_coerce_float(metrics.get("requested_quantity")) or 0.0, 0.0)
    required_buying_power = _coerce_float(metrics.get("required_buying_power"))
    available_buying_power = _coerce_float(metrics.get("available_broker_buying_power"))
    reserved_buying_power = _coerce_float(metrics.get("broker_reserved_buying_power"))
    account_available_buying_power = None
    if available_buying_power is not None:
        account_available_buying_power = round(
            available_buying_power + max(reserved_buying_power or 0.0, 0.0),
            2,
        )
    reason_codes = [str(value).strip() for value in risk_evaluation.get("reason_codes") or [] if str(value).strip()]
    resolved_status = str(risk_evaluation.get("status") or "unknown").strip().lower()
    resolved_reason = None if reason_codes[:1] == ["approved"] else reason_codes[0] if reason_codes else None
    admissible_quantity = _coerce_int(metrics.get("recommended_quantity"))
    if resolved_status == "blocked" and admissible_quantity is None:
        admissible_quantity = 0
    snapshot = {
        "status": "approved" if resolved_status == "approved" else resolved_status,
        "reason": resolved_reason,
        "message": str(risk_evaluation.get("note") or "") or None,
        "evaluated_at": _utc_now(),
        "admissible_quantity": admissible_quantity,
        "required_buying_power": required_buying_power,
        "available_buying_power": available_buying_power,
        "account_available_buying_power": account_available_buying_power,
        "reserved_buying_power": reserved_buying_power,
        "buying_power_basis": _as_text(metrics.get("buying_power_basis")),
        "buying_power_source_field": _as_text(metrics.get("broker_buying_power_source_field")),
        "broker_buying_power_status": _as_text(metrics.get("broker_buying_power_status")),
        "limiting_constraint": _as_text(position_sizing.get("limiting_constraint")),
        "strategy_risk_budget": _coerce_float(metrics.get("strategy_risk_budget")),
        "position_size_pct_of_available_balance": _coerce_float(position_sizing.get("position_size_pct_of_available_balance")),
        "position_size_budget": _coerce_float(position_sizing.get("position_size_budget")),
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
    required_buying_power = _coerce_float(account_capacity.get("required_buying_power"))
    available_buying_power = _coerce_float(account_capacity.get("available_buying_power"))
    reserved_buying_power = _coerce_float(account_capacity.get("reserved_buying_power"))
    requested_quantity = max(_coerce_float(attempt.get("quantity")) or 0.0, 0.0)
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
    buying_power_basis = _as_text(
        estimate_buying_power_requirement(
            dict(attempt.get("candidate") or {}),
            1.0,
            limit_price=_coerce_float(attempt.get("limit_price")),
        ).get("basis")
    )
    snapshot = {
        "status": "blocked",
        "reason": _as_text(account_capacity.get("reason")),
        "message": _as_text(account_capacity.get("message")),
        "evaluated_at": _utc_now(),
        "admissible_quantity": admissible_quantity,
        "required_buying_power": required_buying_power,
        "available_buying_power": available_buying_power,
        "account_available_buying_power": account_available_buying_power,
        "reserved_buying_power": reserved_buying_power,
        "buying_power_basis": buying_power_basis,
        "buying_power_source_field": _as_text(account_capacity.get("source_field")),
        "broker_buying_power_status": "ok",
        "limiting_constraint": "available_broker_buying_power",
        "strategy_risk_budget": None,
        "requested_quantity": None if requested_quantity <= 0 else int(requested_quantity),
    }
    source_object_id = _as_text(request.get("execution_intent_id")) or _as_text(attempt.get("execution_attempt_id"))
    return normalize_lifecycle_admission(
        snapshot,
        admission_kind="submit_account_capacity",
        source_object_type="execution_intent" if _as_text(request.get("execution_intent_id")) is not None else "execution_attempt",
        source_object_id=source_object_id,
        session_date=_as_text(attempt.get("session_date")) or _as_text(attempt.get("market_date")),
        requested_quantity=None if requested_quantity <= 0 else int(requested_quantity),
        requested_notional=_execution_notional(
            quantity=None if requested_quantity <= 0 else int(requested_quantity),
            limit_price=_coerce_float(attempt.get("limit_price")),
        ),
        policy_snapshot=request.get("risk_policy") if isinstance(request.get("risk_policy"), Mapping) else {},
        capability_snapshot=account_capacity,
        metrics={
            "required_buying_power": required_buying_power,
            "available_buying_power": available_buying_power,
            "reserved_buying_power": reserved_buying_power,
            "account_available_buying_power": account_available_buying_power,
            "buying_power_basis": buying_power_basis,
            "buying_power_source_field": _as_text(account_capacity.get("source_field")),
            "admissible_quantity": admissible_quantity,
        },
        evidence={"account_capacity": dict(account_capacity)},
        reason_codes=[_as_text(account_capacity.get("reason")) or "insufficient_buying_power"],
        blockers=[_as_text(account_capacity.get("reason")) or "insufficient_buying_power"],
    )


def _execution_admission_payload_from_broker_rejection(
    *,
    attempt: Mapping[str, Any],
    classified_error: Mapping[str, Any],
) -> dict[str, Any]:
    request = attempt.get("request") if isinstance(attempt.get("request"), Mapping) else {}
    quantity = max(_coerce_float(attempt.get("quantity")) or 0.0, 0.0)
    requirement = estimate_buying_power_requirement(
        dict(attempt.get("candidate") or {}),
        quantity,
        limit_price=_coerce_float(attempt.get("limit_price")),
    )
    required_buying_power = _coerce_float(requirement.get("required_buying_power"))
    if _as_text(classified_error.get("reason")) not in {
        "insufficient_options_buying_power",
        "insufficient_buying_power",
    }:
        required_buying_power = None
    snapshot = {
        "status": "blocked",
        "reason": _as_text(classified_error.get("reason")),
        "message": _as_text(classified_error.get("message")),
        "evaluated_at": _utc_now(),
        "admissible_quantity": 0,
        "required_buying_power": required_buying_power,
        "available_buying_power": None,
        "account_available_buying_power": None,
        "reserved_buying_power": None,
        "buying_power_basis": _as_text(requirement.get("basis")),
        "buying_power_source_field": None,
        "broker_buying_power_status": "rejected",
        "limiting_constraint": None,
        "strategy_risk_budget": None,
        "requested_quantity": None if quantity <= 0 else int(quantity),
    }
    source_object_id = _as_text(request.get("execution_intent_id")) or _as_text(attempt.get("execution_attempt_id"))
    return normalize_lifecycle_admission(
        snapshot,
        admission_kind="broker_rejection",
        source_object_type="execution_intent" if _as_text(request.get("execution_intent_id")) is not None else "execution_attempt",
        source_object_id=source_object_id,
        session_date=_as_text(attempt.get("session_date")) or _as_text(attempt.get("market_date")),
        requested_quantity=None if quantity <= 0 else int(quantity),
        requested_notional=_execution_notional(
            quantity=None if quantity <= 0 else int(quantity),
            limit_price=_coerce_float(attempt.get("limit_price")),
        ),
        policy_snapshot=request.get("risk_policy") if isinstance(request.get("risk_policy"), Mapping) else {},
        capability_snapshot=classified_error,
        metrics={
            "required_buying_power": required_buying_power,
            "buying_power_basis": _as_text(requirement.get("basis")),
            "broker_buying_power_status": "rejected",
        },
        evidence={"classified_error": dict(classified_error), "buying_power_requirement": dict(requirement)},
        reason_codes=[_as_text(classified_error.get("reason")) or "broker_rejected"],
        blockers=[_as_text(classified_error.get("reason")) or "broker_rejected"],
    )


def _execution_notional(*, quantity: int | None, limit_price: float | None, multiplier: float = 100.0) -> float | None:
    if quantity is None or quantity <= 0 or limit_price is None or limit_price <= 0:
        return None
    return round(float(quantity) * float(limit_price) * multiplier, 2)


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
    if _as_text(raw_policy.get("deployment_mode")) is None:
        raw_policy["deployment_mode"] = (
            _as_text(metadata.get("deployment_mode"))
            or _as_text(metadata.get("execution_deployment_mode"))
            or ("live_auto" if _as_text(metadata.get("execution_mode")) == "live" else "paper_auto")
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
    execution_intent_id = _as_text(metadata.get("execution_intent_id"))
    if execution_intent_id is not None:
        return "execution_intent", execution_intent_id
    position_id = _as_text(metadata.get("position_id"))
    if position_id is not None:
        return "position", position_id
    return fallback_type, fallback_id


def _metadata_trade_refs(metadata: Mapping[str, Any]) -> dict[str, str | None]:
    execution_admission = metadata.get("execution_admission") if isinstance(metadata.get("execution_admission"), Mapping) else {}
    return {
        "trade_signal_id": _as_text(metadata.get("trade_signal_id")),
        "trade_decision_id": _as_text(metadata.get("trade_decision_id")),
        "admission_decision_id": _as_text(metadata.get("admission_decision_id")) or _as_text(execution_admission.get("admission_decision_id")),
    }


def _attempt_source_from_metadata(
    metadata: Mapping[str, Any],
    *,
    fallback_type: str,
    fallback_id: str | None,
) -> tuple[str, str | None]:
    source_object_type = _as_text(metadata.get("source_object_type"))
    source_object_id = _as_text(metadata.get("source_object_id"))
    if source_object_type is not None and source_object_id is not None:
        return source_object_type, source_object_id

    trade_decision_id = _as_text(metadata.get("trade_decision_id"))
    if trade_decision_id is not None:
        return "trade_decision", trade_decision_id
    trade_signal_id = _as_text(metadata.get("trade_signal_id"))
    if trade_signal_id is not None:
        return "trade_signal", trade_signal_id

    close_decision = metadata.get("close_decision")
    if isinstance(close_decision, Mapping):
        close_decision_id = _as_text(close_decision.get("close_decision_id"))
        if close_decision_id is not None:
            return "close_decision", close_decision_id

    source = metadata.get("source")
    if isinstance(source, Mapping):
        source_type = _as_text(source.get("source_object_type")) or _as_text(source.get("kind")) or _as_text(source.get("source_type"))
        source_id = _as_text(source.get("source_object_id")) or _as_text(source.get("id")) or _as_text(source.get("source_id"))
        if source_type is not None and source_id is not None:
            return source_type, source_id

    position_id = _as_text(metadata.get("position_id"))
    if position_id is not None:
        return "position", position_id
    execution_intent_id = _as_text(metadata.get("execution_intent_id"))
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
    message = _as_text(admission.get("message")) or _as_text(admission.get("reason")) or "Execution admission blocked."
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
            "evaluated_at": decided_at or _utc_now(),
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


def _resolve_candidate_entry_prices(
    candidate_payload: dict[str, Any],
) -> tuple[float | None, float | None]:
    midpoint_value = _normalize_limit_value(candidate_payload.get("midpoint_value", candidate_payload.get("midpoint_credit")))
    natural_value = _normalize_limit_value(candidate_payload.get("natural_value", candidate_payload.get("natural_credit")))
    return midpoint_value, natural_value


def _entry_fill_ratio(
    *,
    midpoint_value: float,
    natural_value: float,
    premium_kind: str | None,
) -> float:
    if midpoint_value <= 0 or natural_value <= 0:
        return 0.0
    if premium_kind == "debit":
        return round(_clamp_fraction(midpoint_value / natural_value, maximum=1.0), 4)
    return round(_clamp_fraction(natural_value / midpoint_value, maximum=1.0), 4)


def _execution_retention_bound(
    *,
    midpoint_value: float,
    premium_kind: str | None,
    min_retention_pct: float,
) -> float:
    if premium_kind == "debit":
        return round(max(midpoint_value / min_retention_pct, midpoint_value), 4)
    return round(midpoint_value * min_retention_pct, 4)


def _quote_record_sort_key(record: dict[str, Any]) -> tuple[str, str]:
    return (
        _as_text(record.get("quote_timestamp")) or "",
        _as_text(record.get("captured_at")) or "",
    )


def _latest_quote_records_by_symbol(
    quote_records: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    latest: dict[str, tuple[tuple[str, str], dict[str, Any]]] = {}
    for record in quote_records:
        symbol = _as_text(record.get("option_symbol"))
        if symbol is None:
            continue
        sort_key = _quote_record_sort_key(record)
        current = latest.get(symbol)
        if current is None or sort_key >= current[0]:
            latest[symbol] = (sort_key, dict(record))
    return {symbol: row for symbol, (_, row) in latest.items()}


def _resolve_reactive_quote_snapshot(
    candidate: dict[str, Any],
    quote_records: list[dict[str, Any]],
) -> dict[str, Any] | None:
    latest_quotes = _latest_quote_records_by_symbol(quote_records)
    candidate_payload = _candidate_with_payload(candidate)
    strategy_family = _strategy_family_from_payload(candidate_payload)
    legs = candidate_legs(candidate_payload)
    sources = {
        str(record.get("option_symbol")): str(record.get("source"))
        for record in quote_records
        if str(record.get("option_symbol") or "").strip() and str(record.get("source") or "").strip()
    }
    return structure_quote_snapshot(
        legs=legs,
        strategy_family=strategy_family,
        quotes_by_symbol=latest_quotes,
        sources_by_symbol=sources,
    )


def _resolve_reactive_auto_execution(
    *,
    candidate: dict[str, Any],
    execution_policy: dict[str, Any],
    quote_records: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    candidate_payload = _candidate_with_payload(candidate)
    if not quote_records:
        return {
            "ok": False,
            "reason": "awaiting_reactive_quotes",
            "message": "Automatic 0DTE execution skipped because reactive quote capture did not return any quotes.",
        }

    live_snapshot = _resolve_reactive_quote_snapshot(candidate, quote_records)
    if live_snapshot is None:
        return {
            "ok": False,
            "reason": "awaiting_reactive_quotes",
            "message": "Automatic 0DTE execution skipped because a current spread quote snapshot was not available.",
        }

    strategy_family = _strategy_family_from_payload(candidate_payload)
    premium_kind = net_premium_kind(strategy_family)
    live_midpoint_value = _normalize_limit_value(live_snapshot.get("midpoint_value"))
    live_natural_value = _normalize_limit_value(live_snapshot.get("natural_value"))
    if live_midpoint_value is None or live_natural_value is None or live_midpoint_value <= 0 or live_natural_value <= 0:
        return {
            "ok": False,
            "reason": "live_quotes_not_executable",
            "message": "Automatic 0DTE execution skipped because live spread quotes were not executable.",
            "reactive_quote": live_snapshot,
        }

    scanned_midpoint_value = _normalize_limit_value(candidate_payload.get("midpoint_credit"))
    if scanned_midpoint_value is not None:
        retention_bound = _execution_retention_bound(
            midpoint_value=scanned_midpoint_value,
            premium_kind=premium_kind,
            min_retention_pct=float(execution_policy["min_credit_retention_pct"]),
        )
        if premium_kind == "debit" and live_midpoint_value > retention_bound:
            return {
                "ok": False,
                "reason": "live_debit_above_ceiling",
                "message": ("Automatic 0DTE execution skipped because the live spread debit rose above the concession ceiling."),
                "reactive_quote": {
                    **live_snapshot,
                    "debit_ceiling": retention_bound,
                },
            }
        if premium_kind != "debit" and live_midpoint_value < retention_bound:
            return {
                "ok": False,
                "reason": "live_credit_below_floor",
                "message": ("Automatic 0DTE execution skipped because the live spread credit fell below the retention floor."),
                "reactive_quote": {
                    **live_snapshot,
                    "credit_floor": retention_bound,
                },
            }

    pricing_candidate = {
        **candidate_payload,
        "midpoint_credit": live_midpoint_value,
        "natural_credit": live_natural_value,
        "fill_ratio": _entry_fill_ratio(
            midpoint_value=live_midpoint_value,
            natural_value=live_natural_value,
            premium_kind=premium_kind,
        ),
    }
    limit_price = _resolve_open_limit_price(
        candidate_payload=pricing_candidate,
        explicit_limit_price=None,
        execution_policy=execution_policy,
    )
    return {
        "ok": True,
        "limit_price": limit_price,
        "reactive_quote": {
            **live_snapshot,
            "fill_ratio": pricing_candidate["fill_ratio"],
            "limit_price": limit_price,
        },
    }


def _capped_structure_return_on_risk(
    *,
    midpoint_value: float | None,
    span_value: float | None,
    premium_kind: str | None,
) -> float | None:
    if midpoint_value is None or span_value is None or midpoint_value <= 0 or span_value <= 0 or midpoint_value >= span_value:
        return None
    if premium_kind == "debit":
        return round((span_value - midpoint_value) / midpoint_value, 4)
    return round(midpoint_value / (span_value - midpoint_value), 4)


def _candidate_capped_structure_span(
    candidate_payload: Mapping[str, Any],
) -> float | None:
    width = _coerce_float(candidate_payload.get("width"))
    if width is not None and width > 0:
        return width
    max_profit = _coerce_float(candidate_payload.get("max_profit"))
    max_loss = _coerce_float(candidate_payload.get("max_loss"))
    if max_profit is None or max_loss is None:
        return None
    span_dollars = max_profit + max_loss
    if span_dollars <= 0:
        return None
    return round(span_dollars / 100.0, 4)


def _validate_live_deployment_quality(
    *,
    candidate_payload: Mapping[str, Any],
    deployment_mode: str | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    profile = resolve_candidate_profile(candidate_payload)
    thresholds = resolve_deployment_quality_thresholds(profile)
    minimum_return_on_risk = _coerce_float(thresholds.get("min_execution_return_on_risk"))
    if minimum_return_on_risk is None:
        return {
            "ok": True,
            "profile": profile,
        }

    strategy_family = _strategy_family_from_payload(candidate_payload)
    premium_kind = net_premium_kind(strategy_family)
    if strategy_family in {"long_straddle", "long_strangle"}:
        return {
            "ok": False,
            "reason": "long_vol_live_execution_disabled",
            "message": ("Open execution is blocked because long-vol earnings structures " "are still shadow-only in the live path."),
            "profile": profile,
        }
    legs = candidate_legs(candidate_payload)
    span_value = _candidate_capped_structure_span(candidate_payload)
    if not legs or span_value is None or span_value <= 0:
        return {
            "ok": False,
            "reason": "live_deployment_quality_unavailable",
            "message": (
                "Open execution is blocked because the candidate is missing capped-risk " "structure geometry for live deployment validation."
            ),
            "profile": profile,
        }

    live_snapshot, error_text = build_structure_quote_snapshot(
        legs=legs,
        strategy_family=strategy_family,
        client=client,
    )
    if live_snapshot is None:
        return {
            "ok": False,
            "reason": "live_quotes_unavailable",
            "message": ("Open execution is blocked because a current live multi-leg structure " "snapshot is unavailable."),
            "profile": profile,
            "quote_error": error_text,
        }

    live_midpoint_value = _normalize_limit_value(live_snapshot.get("midpoint_value"))
    live_return_on_risk = _capped_structure_return_on_risk(
        midpoint_value=live_midpoint_value,
        span_value=span_value,
        premium_kind=premium_kind,
    )
    if live_midpoint_value is None or live_return_on_risk is None:
        return {
            "ok": False,
            "reason": "live_quotes_not_executable",
            "message": ("Open execution is blocked because the live structure quotes were not executable."),
            "profile": profile,
            "live_quote": live_snapshot,
        }

    if live_return_on_risk < minimum_return_on_risk:
        return {
            "ok": False,
            "reason": "live_return_on_risk_below_floor",
            "message": (
                "Open execution is blocked because live return on risk "
                f"{live_return_on_risk:.4f} is below the deployment floor "
                f"{minimum_return_on_risk:.4f}."
            ),
            "profile": profile,
            "live_quote": {
                **live_snapshot,
                "span_value": span_value,
                "live_return_on_risk": live_return_on_risk,
                "minimum_return_on_risk": minimum_return_on_risk,
            },
        }

    return {
        "ok": True,
        "profile": profile,
        "live_quote": {
            **live_snapshot,
            "span_value": span_value,
            "live_return_on_risk": live_return_on_risk,
            "minimum_return_on_risk": minimum_return_on_risk,
        },
    }


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
        if exclude_execution_attempt_id is not None and _as_text(attempt.get("execution_attempt_id")) == exclude_execution_attempt_id:
            continue
        requested_quantity = max(_coerce_float(attempt.get("quantity")) or 0.0, 0.0)
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
            limit_price=_coerce_float(attempt.get("limit_price")),
        )
        required_buying_power = _coerce_float(requirement.get("required_buying_power"))
        if required_buying_power is None:
            continue
        reserved_buying_power += required_buying_power
    return round(reserved_buying_power, 2)


def _validate_submit_account_capacity(
    *,
    execution_store: Any,
    attempt: Mapping[str, Any],
    client: Any,
) -> dict[str, Any]:
    requirement = estimate_buying_power_requirement(
        dict(attempt.get("candidate") or {}),
        _coerce_float(attempt.get("quantity")) or 0.0,
        limit_price=_coerce_float(attempt.get("limit_price")),
    )
    required_buying_power = _coerce_float(requirement.get("required_buying_power"))
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
    available_buying_power = _coerce_float(available_snapshot.get("available_buying_power"))
    if available_buying_power is None:
        return {
            "ok": True,
            "status": "unavailable",
            "error_text": "Broker account payload did not include usable buying power fields.",
        }

    reserved_buying_power = _pending_open_attempt_buying_power(
        execution_store=execution_store,
        exclude_execution_attempt_id=_as_text(attempt.get("execution_attempt_id")),
    )
    remaining_buying_power = round(
        max(available_buying_power - reserved_buying_power, 0.0),
        2,
    )
    if required_buying_power > remaining_buying_power:
        source_field = _as_text(available_snapshot.get("source_field"))
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
        "source_field": _as_text(available_snapshot.get("source_field")),
    }


def _resolve_open_limit_price(
    *,
    candidate_payload: dict[str, Any],
    explicit_limit_price: float | None,
    execution_policy: dict[str, Any],
) -> float:
    premium_kind = net_premium_kind(_strategy_family_from_payload(candidate_payload))
    explicit_value = _normalize_limit_value(explicit_limit_price)
    if explicit_value is not None:
        return round(max(explicit_value, 0.01), 2)

    midpoint_value, natural_value = _resolve_candidate_entry_prices(candidate_payload)
    if midpoint_value is None:
        order_payload = dict(candidate_payload.get("order_payload") or {})
        midpoint_value = _normalize_limit_value(order_payload.get("limit_price"))
    if midpoint_value is None or midpoint_value <= 0:
        raise ValueError("Execution limit price must be positive")

    pricing_mode = str(execution_policy.get("pricing_mode") or DEFAULT_ENTRY_PRICING_MODE)
    if pricing_mode == "midpoint" or natural_value is None or natural_value <= 0:
        return round(max(midpoint_value, 0.01), 2)

    fill_ratio = _clamp_fraction(_coerce_float(candidate_payload.get("fill_ratio")) or 0.0, maximum=1.0)
    min_credit_retention_pct = _clamp_fraction(
        _coerce_float(execution_policy.get("min_credit_retention_pct")) or DEFAULT_MIN_CREDIT_RETENTION_PCT,
        minimum=0.5,
        maximum=1.0,
    )
    max_credit_concession = max(
        _coerce_float(execution_policy.get("max_credit_concession")) or DEFAULT_MAX_CREDIT_CONCESSION,
        0.0,
    )
    if premium_kind == "debit":
        debit_ceiling = _execution_retention_bound(
            midpoint_value=midpoint_value,
            premium_kind=premium_kind,
            min_retention_pct=min_credit_retention_pct,
        )
        max_concession_to_ceiling = max(debit_ceiling - midpoint_value, 0.0)
        fill_ratio_concession = max(natural_value - midpoint_value, 0.0) * max(1.0 - fill_ratio, 0.0)
        concession = min(fill_ratio_concession, max_credit_concession, max_concession_to_ceiling)
        return round(
            min(
                max(midpoint_value + concession, 0.01),
                max(natural_value, 0.01),
                debit_ceiling,
            ),
            2,
        )

    credit_floor = max(natural_value, midpoint_value * min_credit_retention_pct, 0.01)
    max_concession_to_floor = max(midpoint_value - credit_floor, 0.0)
    fill_ratio_concession = max(midpoint_value - natural_value, 0.0) * max(1.0 - fill_ratio, 0.0)
    concession = min(fill_ratio_concession, max_credit_concession, max_concession_to_floor)
    return round(max(midpoint_value - concession, credit_floor, 0.01), 2)


def _classify_auto_execution_block(exc: Exception) -> dict[str, Any] | None:
    if not isinstance(exc, ValueError):
        return None
    message = str(exc).strip()
    if not message:
        return None
    if message.startswith("Open execution exceeds ") and message.endswith("."):
        constraint = message.removeprefix("Open execution exceeds ").removesuffix(".")
        return {
            "reason": "risk_policy_blocked",
            "message": message,
            "block_category": "risk_policy",
            "constraint": constraint,
        }
    if message == "Open execution is blocked because the quote snapshot is stale.":
        return {
            "reason": "stale_quote",
            "message": message,
            "block_category": "quote_freshness",
        }
    if message == "Open execution is blocked because the exit force-close window has already started.":
        return {
            "reason": "force_close_window_started",
            "message": message,
            "block_category": "timing_window",
        }
    if message.startswith("Open execution is blocked because only "):
        return {
            "reason": "insufficient_time_to_force_close",
            "message": message,
            "block_category": "timing_window",
        }
    if message == "Execution is blocked by SPREADS_EXECUTION_KILL_SWITCH.":
        return {
            "reason": "kill_switch_blocked",
            "message": message,
            "block_category": "kill_switch",
        }
    if message == "Open execution is blocked because control mode is halted.":
        return {
            "reason": "control_mode_halted",
            "message": message,
            "block_category": "control_mode",
        }
    if message.startswith("Open execution is blocked on a live Alpaca account."):
        return {
            "reason": "environment_blocked",
            "message": message,
            "block_category": "environment",
        }
    if message in {
        "Open execution is blocked because a current live spread snapshot is unavailable.",
        "Open execution is blocked because a current live multi-leg structure snapshot is unavailable.",
    }:
        return {
            "reason": "live_quotes_unavailable",
            "message": message,
            "block_category": "deployment_quality",
        }
    if message in {
        "Open execution is blocked because the live spread quotes were not executable.",
        "Open execution is blocked because the live structure quotes were not executable.",
    }:
        return {
            "reason": "live_quotes_not_executable",
            "message": message,
            "block_category": "deployment_quality",
        }
    if message.startswith("Open execution is blocked because live return on risk "):
        return {
            "reason": "live_return_on_risk_below_floor",
            "message": message,
            "block_category": "deployment_quality",
        }
    return None


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


def _request_recommended_quantity(
    request_metadata: Mapping[str, Any] | dict[str, Any] | None,
) -> int | None:
    if not isinstance(request_metadata, Mapping):
        return None
    execution_intent = request_metadata.get("execution_intent")
    if isinstance(execution_intent, Mapping):
        evidence = execution_intent.get("evidence")
        if isinstance(evidence, Mapping):
            quantity = _coerce_int(evidence.get("recommended_quantity"))
            if quantity is not None and quantity > 0:
                return quantity
    execution_admission = request_metadata.get("execution_admission")
    if isinstance(execution_admission, Mapping):
        quantity = _coerce_int(execution_admission.get("admissible_quantity"))
        if quantity is not None and quantity > 0:
            return quantity
    allocation_decision = request_metadata.get("allocation_decision")
    if not isinstance(allocation_decision, Mapping):
        return None
    budget_impact = allocation_decision.get("budget_impact")
    if isinstance(budget_impact, Mapping):
        quantity = _coerce_int(budget_impact.get("recommended_contracts"))
        if quantity is not None and quantity > 0:
            return quantity
    evidence = allocation_decision.get("evidence")
    if not isinstance(evidence, Mapping):
        return None
    position_sizing = evidence.get("position_sizing")
    if not isinstance(position_sizing, Mapping):
        return None
    quantity = _coerce_int(position_sizing.get("recommended_quantity"))
    if quantity is None or quantity <= 0:
        return None
    return quantity


def _resolve_open_submission_quantity(
    *,
    execution_store: Any,
    session_id: str,
    candidate: dict[str, Any],
    explicit_quantity: int | None,
    limit_price: float | None,
    request_metadata: Mapping[str, Any] | dict[str, Any] | None,
    risk_policy: dict[str, Any] | None,
    execution_policy: dict[str, Any],
    trading_strategy_id: str | None,
) -> tuple[int, dict[str, float | None]]:
    position_size_policy = _strategy_position_size_policy(
        trading_strategy_id=trading_strategy_id,
    )
    if explicit_quantity is not None:
        return explicit_quantity, position_size_policy

    quantity_hints: list[int] = []
    request_quantity = _request_recommended_quantity(request_metadata)
    if request_quantity is not None and request_quantity > 0:
        quantity_hints.append(request_quantity)
    risk_sizing = build_open_candidate_position_sizing(
        execution_store=execution_store,
        session_id=session_id,
        candidate=candidate,
        limit_price=limit_price,
        risk_policy=risk_policy,
        strategy_risk_budget=position_size_policy["max_risk_per_trade"],
        position_size_pct_of_available_balance=position_size_policy["position_size_pct_of_available_balance"],
    )
    risk_quantity = _coerce_int(risk_sizing.get("recommended_quantity"))
    if bool(risk_sizing.get("applies")) and risk_quantity is not None and risk_quantity > 0:
        quantity_hints.append(risk_quantity)
    if bool(execution_policy.get("quantity_configured")):
        policy_cap = _coerce_int(execution_policy.get("quantity"))
        if policy_cap is not None and policy_cap > 0:
            quantity_hints.append(policy_cap)
    if quantity_hints:
        return max(min(quantity_hints), 1), position_size_policy

    candidate_payload = _candidate_with_payload(candidate)
    order_payload = dict(candidate_payload.get("order_payload") or {})
    fallback_quantity = _coerce_int(order_payload.get("qty")) or 1
    return fallback_quantity, position_size_policy


def _build_order_request(
    *,
    candidate: dict[str, Any],
    quantity: int | None,
    limit_price: float | None,
    execution_policy: dict[str, Any],
    client_order_id: str,
) -> tuple[dict[str, Any], int, float]:
    candidate_payload = _candidate_with_payload(candidate)
    strategy_family = _strategy_family_from_payload(candidate_payload)
    order_payload = dict(candidate_payload.get("order_payload") or {})
    resolved_legs = order_payload_legs(
        order_payload,
        expiration_date=_as_text(candidate_payload.get("expiration_date")),
    ) or candidate_legs(candidate_payload)
    if not resolved_legs:
        raise ValueError("Selected live candidate does not include an executable order payload")
    resolved_quantity = quantity if quantity is not None else _coerce_int(order_payload.get("qty")) or 1
    if resolved_quantity <= 0:
        raise ValueError("Execution quantity must be positive")
    resolved_limit_price = _resolve_open_limit_price(
        candidate_payload=candidate_payload,
        explicit_limit_price=limit_price,
        execution_policy=execution_policy,
    )
    request = build_order_payload(
        legs=resolved_legs,
        limit_price=resolved_limit_price,
        strategy_family=strategy_family,
        trade_intent=OPEN_TRADE_INTENT,
        quantity=resolved_quantity,
    )
    request["client_order_id"] = client_order_id
    return request, int(resolved_quantity), round(float(resolved_limit_price), 2)


def _build_close_order_request(
    *,
    position: dict[str, Any],
    quantity: int | None,
    limit_price: float | None,
    client_order_id: str,
) -> tuple[dict[str, Any], int, float]:
    remaining_quantity = _coerce_float(position.get("remaining_quantity"))
    if remaining_quantity is None or remaining_quantity <= 0:
        raise ValueError("Session position does not have remaining quantity to close")
    resolved_quantity = quantity if quantity is not None else int(round(remaining_quantity))
    if resolved_quantity <= 0:
        raise ValueError("Close quantity must be positive")
    if resolved_quantity > remaining_quantity:
        raise ValueError("Close quantity exceeds the remaining session position quantity")

    resolved_limit_price = limit_price if limit_price is not None else _coerce_float(position.get("close_mark"))
    resolved_limit_price = _normalize_limit_value(resolved_limit_price)
    if resolved_limit_price is None or resolved_limit_price <= 0:
        raise ValueError("Close execution requires a positive limit price or a quoted close mark")

    strategy_family = _strategy_family_from_payload(position)
    resolved_legs = normalize_legs(position.get("legs"))
    if not resolved_legs:
        resolved_legs = candidate_legs(position)
    if not resolved_legs:
        raise ValueError("Close execution requires canonical position legs")
    request = build_order_payload(
        legs=closing_legs(resolved_legs),
        limit_price=float(resolved_limit_price),
        strategy_family=strategy_family,
        trade_intent=CLOSE_TRADE_INTENT,
        quantity=resolved_quantity,
    )
    request["client_order_id"] = client_order_id
    return request, int(resolved_quantity), round(float(resolved_limit_price), 2)


def _normalize_submit_order_request(
    *,
    payload: Mapping[str, Any],
    order_request: Mapping[str, Any],
) -> dict[str, Any]:
    request = dict(order_request)
    request_legs = order_payload_legs(
        request,
        expiration_date=_as_text(payload.get("expiration_date")),
    ) or normalize_legs(payload.get("legs"))
    if not request_legs or len(request_legs) != 1:
        return request
    requires_single_leg_rebuild = str(request.get("order_class") or "").strip().lower() == "mleg" or "symbol" not in request or "side" not in request
    if not requires_single_leg_rebuild:
        return request
    limit_price = _coerce_float(request.get("limit_price")) or _coerce_float(payload.get("limit_price"))
    if limit_price is None:
        return request
    quantity = _coerce_int(request.get("qty")) or _coerce_int(payload.get("quantity")) or 1
    normalized_request = build_order_payload(
        legs=request_legs,
        limit_price=limit_price,
        strategy_family=_strategy_family_from_payload(payload),
        trade_intent=str(payload.get("trade_intent") or OPEN_TRADE_INTENT),
        quantity=quantity,
    )
    client_order_id = _as_text(request.get("client_order_id"))
    if client_order_id is not None:
        normalized_request["client_order_id"] = client_order_id
    return normalized_request


@with_storage()
def refresh_live_session_execution(
    *,
    db_target: str,
    session_id: str,
    execution_attempt_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")
    if str(attempt["session_id"]) != session_id:
        raise ValueError(f"Execution {execution_attempt_id} does not belong to session {session_id}")
    if _as_text(attempt.get("broker_order_id")) is None and str(attempt.get("status") or "") == PENDING_SUBMISSION_STATUS:
        payload = _get_attempt_payload(execution_store, execution_attempt_id)
        return {
            "action": "refresh",
            "changed": False,
            "message": "Execution is still queued for broker submission.",
            "attempt": payload,
        }
    if _as_text(attempt.get("broker_order_id")) is None and str(attempt.get("status") or "") == SUBMIT_UNKNOWN_STATUS:
        client_order_id = _as_text(attempt.get("client_order_id"))
        if client_order_id is None:
            payload = _get_attempt_payload(execution_store, execution_attempt_id)
            message = "Execution submit outcome is uncertain and cannot be reconciled " "because the client order id is missing."
            _sync_linked_execution_intent(
                execution_store=execution_store,
                attempt=payload,
                event_type="submit_unknown_unresolved",
                message=message,
            )
            return {
                "action": "refresh",
                "changed": False,
                "message": message,
                "attempt": payload,
            }
        adapter = create_alpaca_order_adapter()
        reconciled_attempt = _reconcile_submit_unknown_attempt(
            execution_store=execution_store,
            attempt=attempt,
            client=adapter.client,
        )
        if reconciled_attempt is None:
            payload = _get_attempt_payload(execution_store, execution_attempt_id)
            message = "Execution submit outcome is uncertain and no broker order has been " f"found yet for client_order_id {client_order_id}."
            _sync_linked_execution_intent(
                execution_store=execution_store,
                attempt=payload,
                event_type="submit_unknown_unresolved",
                message=message,
            )
            return {
                "action": "refresh",
                "changed": False,
                "message": message,
                "attempt": payload,
            }
        message = f"Reconciled execution {execution_attempt_id} via client_order_id " f"{client_order_id}: {reconciled_attempt['status']}."
        _publish_execution_attempt_event(reconciled_attempt, message=message)
        _sync_linked_execution_intent(
            execution_store=execution_store,
            attempt=reconciled_attempt,
            event_type="reconciled",
            message=message,
        )
        return {
            "action": "refresh",
            "changed": True,
            "message": message,
            "attempt": reconciled_attempt,
        }
    broker_order_id = _as_text(attempt.get("broker_order_id"))
    if broker_order_id is None:
        raise ValueError("Execution does not have a broker order id to refresh")

    adapter = create_alpaca_order_adapter()
    order_snapshot = adapter.get_order_snapshot(broker_order_id, nested=True)
    payload = _sync_attempt_state(
        execution_store=execution_store,
        attempt=dict(attempt),
        client=adapter.client,
        order_snapshot=order_snapshot,
    )
    message = f"Refreshed execution {execution_attempt_id}: {payload['status']}."
    _publish_execution_attempt_event(payload, message=message)
    _sync_linked_execution_intent(
        execution_store=execution_store,
        attempt=payload,
        event_type="refreshed",
        message=message,
    )
    return {
        "action": "refresh",
        "changed": True,
        "message": message,
        "attempt": payload,
    }


@with_storage()
def submit_position_close_by_id(
    *,
    db_target: str,
    position_id: str,
    quantity: int | None = None,
    limit_price: float | None = None,
    request_metadata: dict[str, Any] | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    job_store = storage.jobs
    if not execution_store.portfolio_schema_ready():
        raise ValueError(f"Unknown position_id: {position_id}")
    stored_position = execution_store.get_position(position_id)
    if stored_position is None:
        raise ValueError(f"Unknown position_id: {position_id}")
    position = enrich_position_row(dict(stored_position))
    if str(position.get("position_status") or position.get("status") or "open") == "closed":
        raise ValueError("Position is already closed")

    existing_attempts = execution_store.list_open_attempts_for_position(
        position_id=position_id,
        statuses=sorted(OPEN_STATUSES),
    )
    if existing_attempts:
        payload = _get_attempt_payload(
            execution_store,
            str(existing_attempts[0]["execution_attempt_id"]),
        )
        return {
            "action": "submit",
            "changed": False,
            "message": "An active close execution already exists for this position.",
            "attempt": payload,
        }

    requested_at = _utc_now()
    client_order_id = _execution_client_order_id()
    trade_intent = resolve_trade_intent(CLOSE_TRADE_INTENT)
    attempt_id: str | None = None
    try:
        order_request, resolved_quantity, resolved_limit_price = _build_close_order_request(
            position=position,
            quantity=quantity,
            limit_price=limit_price,
            client_order_id=client_order_id,
        )
        validate_close_execution(
            position=position,
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            max_reconciliation_age_seconds=CLOSE_RECONCILIATION_MAX_AGE_SECONDS,
        )
        pipeline_id = _as_text(position.get("pipeline_id"))
        label = _as_text(position.get("label"))
        market_date = _as_text(position.get("market_date"))
        if pipeline_id is None or label is None or market_date is None:
            raise ValueError("Position is missing pipeline or market_date")
        policy_fields = resolve_pipeline_policy_fields(
            profile=(position.get("risk_policy") or {}).get("profile"),
            root_symbol=str(position["underlying_symbol"]),
        )
        attempt_legs = normalize_legs(order_request.get("legs")) or normalize_legs(position.get("legs"))
        attempt_id = _execution_attempt_id()
        attempt_structure_identity = (
            legs_identity_key(
                strategy=_strategy_family_from_payload(position),
                legs=attempt_legs,
            )
            if attempt_legs
            else None
        )
        close_source_type, close_source_id = _admission_source_from_metadata(
            request_metadata or {},
            fallback_type="position",
            fallback_id=position_id,
        )
        execution_admission = _approved_execution_admission(
            admission_kind="position_close",
            source_object_type=close_source_type,
            source_object_id=close_source_id,
            session_date=market_date,
            requested_quantity=resolved_quantity,
            requested_notional=_execution_notional(
                quantity=resolved_quantity,
                limit_price=resolved_limit_price,
            ),
            reason="close_validation_passed",
            message="Close order passed position and order validation.",
            policy_snapshot=(
                request_metadata.get("risk_policy")
                if isinstance(request_metadata, Mapping) and isinstance(request_metadata.get("risk_policy"), Mapping)
                else {}
            ),
            evidence={
                "position_id": position_id,
                "trade_intent": trade_intent,
                "order_validation": "passed",
            },
            decided_at=requested_at,
        )
        attempt_refs = _attempt_ref_kwargs(
            request_metadata or {},
            fallback_type="position",
            fallback_id=position_id,
        )
        attempt = execution_store.create_attempt(
            execution_attempt_id=attempt_id,
            session_id=build_live_run_scope_id(label, market_date),
            session_date=market_date,
            label=label,
            pipeline_id=pipeline_id,
            trading_strategy_id=_as_text(position.get("trading_strategy_id")),
            market_date=market_date,
            cycle_id=None,
            opportunity_id=None,
            risk_decision_id=None,
            candidate_id=None,
            attempt_context="position_close",
            candidate_generated_at=None,
            run_id=None,
            job_run_id=None,
            underlying_symbol=str(position["underlying_symbol"]),
            strategy=str(position["strategy"]),
            expiration_date=_as_text(position.get("expiration_date")),
            structure_identity=attempt_structure_identity,
            legs=attempt_legs,
            order_payload=dict(order_request),
            economics=dict(position.get("economics") or {}),
            trade_intent=trade_intent,
            position_id=position_id,
            root_symbol=str(position["underlying_symbol"]),
            strategy_family=_strategy_family_from_payload(position),
            style_profile=str(position.get("style_profile") or policy_fields["style_profile"]),
            horizon_intent=str(position.get("horizon_intent") or policy_fields["horizon_intent"]),
            product_class=str(position.get("product_class") or policy_fields["product_class"]),
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            requested_at=requested_at,
            status=PENDING_SUBMISSION_STATUS,
            broker=BROKER_NAME,
            client_order_id=client_order_id,
            request={
                **({} if request_metadata is None else request_metadata),
                **{key: value for key, value in attempt_refs.items() if value is not None},
                "trade_intent": trade_intent,
                "position_id": position_id,
                "execution_admission": execution_admission,
                "order": order_request,
            },
            candidate={},
            **attempt_refs,
        )
        payload = _queue_execution_attempt(
            job_store=job_store,
            execution_store=execution_store,
            attempt=attempt,
        )
        message = _submission_message(payload, queued=True)
        return {
            "action": "submit",
            "changed": True,
            "message": message,
            "attempt": payload,
        }
    except Exception as exc:
        if attempt_id is not None:
            current_attempt = execution_store.get_attempt(attempt_id)
            if current_attempt is not None and str(current_attempt.get("status") or "") == PENDING_SUBMISSION_STATUS:
                execution_store.update_attempt(
                    execution_attempt_id=attempt_id,
                    status="failed",
                    client_order_id=client_order_id,
                    completed_at=requested_at,
                    error_text=str(exc),
                    position_id=position_id,
                )
                payload = _get_attempt_payload(execution_store, attempt_id)
                _publish_execution_attempt_event(
                    payload,
                    message=f"Close execution failed before submission: {exc}",
                )
        raise


@with_storage()
def submit_equity_order(
    *,
    db_target: str,
    symbol: str,
    side: str,
    quantity: int,
    limit_price: float,
    time_in_force: str = "day",
    label: str = "manual_equity",
    market_date: str | None = None,
    execution_runtime: str | None = None,
    request_metadata: dict[str, Any] | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    normalized_runtime = normalize_execution_runtime(execution_runtime)
    metadata = dict(request_metadata or {})

    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        raise ValueError("Equity order requires a symbol")
    normalized_side = str(side or "").strip().lower()
    if normalized_side not in {"buy", "sell"}:
        raise ValueError("Equity order side must be buy or sell")
    resolved_quantity = int(quantity)
    if resolved_quantity <= 0:
        raise ValueError("Equity order quantity must be positive")
    resolved_limit_price = _normalize_limit_value(limit_price)
    if resolved_limit_price is None or resolved_limit_price <= 0:
        raise ValueError("Equity order requires a positive limit price")
    resolved_time_in_force = str(time_in_force or "day").strip().lower()
    if resolved_time_in_force not in {"day", "gtc"}:
        raise ValueError("Equity order time_in_force must be day or gtc")

    resolved_trade_intent = resolve_trade_intent(
        _as_text(metadata.get("trade_intent")) or (OPEN_TRADE_INTENT if normalized_side == "buy" else CLOSE_TRADE_INTENT)
    )
    if normalized_side == "buy":
        position_intent = "buy_to_open" if resolved_trade_intent == OPEN_TRADE_INTENT else "buy_to_close"
    else:
        position_intent = "sell_to_open" if resolved_trade_intent == OPEN_TRADE_INTENT else "sell_to_close"
    leg_role = "short" if position_intent in {"sell_to_open", "buy_to_close"} else "long"
    position_id = _as_text(metadata.get("position_id"))
    requested_at = _utc_now()
    resolved_market_date = market_date or datetime.now(UTC).date().isoformat()
    resolved_label = _as_text(label) or "manual_equity"
    client_order_id = _execution_client_order_id()
    attempt_id = _execution_attempt_id()
    order_request = {
        "symbol": normalized_symbol,
        "side": normalized_side,
        "qty": str(resolved_quantity),
        "type": "limit",
        "limit_price": f"{resolved_limit_price:.2f}",
        "time_in_force": resolved_time_in_force,
        "client_order_id": client_order_id,
        "position_intent": position_intent,
    }
    legs = [
        {
            "symbol": normalized_symbol,
            "side": normalized_side,
            "position_intent": position_intent,
            "ratio_qty": "1",
            "role": leg_role,
        }
    ]
    strategy = "equity_short" if leg_role == "short" else "equity_long"
    pipeline_id = build_pipeline_id(resolved_label)
    equity_source_type, equity_source_id = _admission_source_from_metadata(
        metadata,
        fallback_type="direct_equity_order",
        fallback_id=attempt_id,
    )
    equity_risk_policy = _metadata_policy(metadata, "risk_policy")
    equity_execution_policy = _direct_order_execution_policy(
        metadata,
        risk_policy=equity_risk_policy,
        quantity=resolved_quantity,
    )
    execution_admission = _approved_execution_admission(
        admission_kind=f"direct_equity_{resolved_trade_intent}",
        source_object_type=equity_source_type,
        source_object_id=equity_source_id,
        session_date=resolved_market_date,
        requested_quantity=resolved_quantity,
        requested_notional=_execution_notional(
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            multiplier=1.0,
        ),
        reason="direct_equity_request_validated",
        message="Direct equity order passed request validation.",
        policy_snapshot={
            "risk_policy": equity_risk_policy,
            "execution_policy": equity_execution_policy,
        },
        evidence={
            "asset_class": "equity",
            "symbol": normalized_symbol,
            "side": normalized_side,
            "position_intent": position_intent,
        },
        decided_at=requested_at,
    )
    attempt_refs = _attempt_ref_kwargs(
        metadata,
        fallback_type="direct_equity_order",
        fallback_id=attempt_id,
    )
    attempt_created = False
    submitted_order: dict[str, Any] | None = None
    try:
        attempt = execution_store.create_attempt(
            execution_attempt_id=attempt_id,
            session_id=build_live_run_scope_id(resolved_label, resolved_market_date),
            session_date=resolved_market_date,
            label=resolved_label,
            pipeline_id=pipeline_id,
            trading_strategy_id=_as_text(metadata.get("trading_strategy_id")),
            market_date=resolved_market_date,
            cycle_id=None,
            opportunity_id=None,
            risk_decision_id=None,
            candidate_id=None,
            attempt_context="equity_order",
            candidate_generated_at=None,
            run_id=None,
            job_run_id=None,
            underlying_symbol=normalized_symbol,
            strategy=strategy,
            expiration_date=None,
            structure_identity=None,
            legs=legs,
            order_payload=order_request,
            economics={},
            trade_intent=resolved_trade_intent,
            position_id=position_id,
            root_symbol=normalized_symbol,
            strategy_family=strategy,
            style_profile=_as_text(metadata.get("style_profile")) or "manual_equity",
            horizon_intent=_as_text(metadata.get("horizon_intent")) or "manual",
            product_class="single_name_equity",
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            requested_at=requested_at,
            status=PENDING_SUBMISSION_STATUS,
            broker=BROKER_NAME,
            client_order_id=client_order_id,
            request={
                **{key: value for key, value in attempt_refs.items() if value is not None},
                "trade_intent": resolved_trade_intent,
                "execution_runtime": normalized_runtime,
                "execution_policy": equity_execution_policy,
                "asset_class": "equity",
                "position_intent": position_intent,
                **({} if position_id is None else {"position_id": position_id}),
                **(
                    {}
                    if _as_text(metadata.get("trading_strategy_id")) is None
                    else {"trading_strategy_id": _as_text(metadata.get("trading_strategy_id"))}
                ),
                **({} if _as_text(metadata.get("config_hash")) is None else {"config_hash": _as_text(metadata.get("config_hash"))}),
                **(
                    {}
                    if _as_text(metadata.get("execution_intent_id")) is None
                    else {"execution_intent_id": _as_text(metadata.get("execution_intent_id"))}
                ),
                **({} if not isinstance(metadata.get("exit_policy"), Mapping) else {"exit_policy": dict(metadata["exit_policy"])}),
                **({} if not isinstance(metadata.get("risk_policy"), Mapping) else {"risk_policy": dict(metadata["risk_policy"])}),
                "execution_admission": execution_admission,
                **({} if not isinstance(metadata.get("source"), Mapping) else {"source": dict(metadata["source"])}),
                "order": order_request,
            },
            candidate={},
            **attempt_refs,
        )
        attempt_created = True
        adapter = create_alpaca_order_adapter()
        submission = adapter.submit_order(order_request)
        submitted_order = submission.submitted_order
        synced_attempt = _sync_equity_attempt_state(
            execution_store=execution_store,
            attempt=attempt,
            client=adapter.client,
            order_snapshot=submission.order_snapshot,
        )
        message = f"Submitted equity {normalized_side} for " f"{resolved_quantity} {normalized_symbol}."
        _publish_execution_attempt_event(synced_attempt, message=message)
        return {
            "action": "submit",
            "changed": True,
            "message": message,
            "attempt": synced_attempt,
        }
    except AlpacaRequestError as exc:
        if submitted_order is None and attempt_created:
            classified_error = classify_alpaca_request_error(exc)
            execution_store.update_attempt(
                execution_attempt_id=attempt_id,
                status="failed",
                client_order_id=client_order_id,
                completed_at=requested_at,
                error_text=str(classified_error["message"]),
            )
            failed_attempt = _get_attempt_payload(execution_store, attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=("Equity execution failed before submission: " f"{classified_error['message']}"),
            )
            if bool(classified_error.get("terminal")):
                return {
                    "action": "submit",
                    "changed": True,
                    "status": "blocked",
                    "reason": str(classified_error["reason"]),
                    "message": str(classified_error["message"]),
                    "attempt": failed_attempt,
                }
        raise
    except Exception as exc:
        if submitted_order is None and attempt_created:
            execution_store.update_attempt(
                execution_attempt_id=attempt_id,
                status="failed",
                client_order_id=client_order_id,
                completed_at=requested_at,
                error_text=str(exc),
            )
            failed_attempt = _get_attempt_payload(execution_store, attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=f"Equity execution failed before submission: {exc}",
            )
        raise


@with_storage()
def submit_option_order(
    *,
    db_target: str,
    symbol: str,
    side: str,
    quantity: int,
    limit_price: float,
    time_in_force: str = "day",
    label: str = "manual_option",
    market_date: str | None = None,
    underlying_symbol: str | None = None,
    strategy_family: str = "long_call",
    expiration_date: str | None = None,
    option_type: str | None = None,
    strike: float | None = None,
    execution_runtime: str | None = None,
    request_metadata: dict[str, Any] | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    normalized_runtime = normalize_execution_runtime(execution_runtime)
    if normalized_runtime != ALPACA_DIRECT_RUNTIME:
        raise ValueError("Single-leg option orders currently require alpaca_direct runtime")

    metadata = dict(request_metadata or {})
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        raise ValueError("Option order requires a contract symbol")
    normalized_underlying = str(underlying_symbol or metadata.get("underlying_symbol") or "").strip().upper()
    if not normalized_underlying:
        raise ValueError("Option order requires an underlying symbol")
    normalized_side = str(side or "").strip().lower()
    if normalized_side not in {"buy", "sell"}:
        raise ValueError("Option order side must be buy or sell")
    resolved_quantity = int(quantity)
    if resolved_quantity <= 0:
        raise ValueError("Option order quantity must be positive")
    resolved_limit_price = _normalize_limit_value(limit_price)
    if resolved_limit_price is None or resolved_limit_price <= 0:
        raise ValueError("Option order requires a positive limit price")
    resolved_time_in_force = str(time_in_force or "day").strip().lower()
    if resolved_time_in_force != "day":
        raise ValueError("Option order time_in_force must be day")

    resolved_strategy_family = str(strategy_family or "long_call").strip().lower()
    if resolved_strategy_family not in {"long_call", "long_put"}:
        raise ValueError("Direct option orders currently support long_call and long_put")
    resolved_trade_intent = resolve_trade_intent(
        _as_text(metadata.get("trade_intent")) or (OPEN_TRADE_INTENT if normalized_side == "buy" else CLOSE_TRADE_INTENT)
    )
    if resolved_trade_intent == OPEN_TRADE_INTENT and normalized_side != "buy":
        raise ValueError("Long option opens must buy to open")
    if resolved_trade_intent == CLOSE_TRADE_INTENT and normalized_side != "sell":
        raise ValueError("Long option closes must sell to close")
    position_intent = "buy_to_open" if resolved_trade_intent == OPEN_TRADE_INTENT else "sell_to_close"
    resolved_option_type = (_as_text(option_type) or ("call" if resolved_strategy_family == "long_call" else "put")).lower()
    if resolved_option_type not in {"call", "put"}:
        raise ValueError("Option order option_type must be call or put")

    resolved_expiration = _as_text(expiration_date)
    position_id = _as_text(metadata.get("position_id"))
    requested_at = _utc_now()
    resolved_market_date = market_date or datetime.now(UTC).date().isoformat()
    resolved_label = _as_text(label) or "manual_option"
    client_order_id = _execution_client_order_id()
    attempt_id = _execution_attempt_id()
    legs = [
        {
            "symbol": normalized_symbol,
            "side": normalized_side,
            "position_intent": position_intent,
            "ratio_qty": "1",
            "role": "long",
            "expiration_date": resolved_expiration,
            "strike": strike,
            "option_type": resolved_option_type,
        }
    ]
    order_request = build_order_payload(
        legs=legs,
        limit_price=resolved_limit_price,
        strategy_family=resolved_strategy_family,
        trade_intent=resolved_trade_intent,
        quantity=resolved_quantity,
    )
    order_request["client_order_id"] = client_order_id
    profile = _as_text(metadata.get("profile")) or "weekly"
    policy_fields = resolve_pipeline_policy_fields(
        profile=profile,
        root_symbol=normalized_underlying,
    )
    option_selection = dict(metadata.get("option_selection")) if isinstance(metadata.get("option_selection"), Mapping) else {}
    option_quote_metrics = _metadata_policy(option_selection, "quote_metrics")
    candidate_generated_at = _as_text(option_quote_metrics.get("timestamp")) or requested_at
    candidate_payload = {
        "underlying_symbol": normalized_underlying,
        "strategy": resolved_strategy_family,
        "strategy_family": resolved_strategy_family,
        "profile": profile,
        "generated_at": candidate_generated_at,
        "expiration_date": resolved_expiration,
        "underlying_price": _coerce_float(metadata.get("underlying_price")),
        "legs": legs,
        "order_payload": dict(order_request),
        "structure_identity": legs_identity_key(
            strategy=resolved_strategy_family,
            legs=legs,
        ),
        "width": 0.0,
        "midpoint_credit": resolved_limit_price,
        "natural_credit": resolved_limit_price,
        "max_profit": None,
        "max_loss": round(resolved_limit_price * 100.0, 2),
        "option_selection": option_selection,
    }
    option_source_type, option_source_id = _admission_source_from_metadata(
        metadata,
        fallback_type="direct_option_order",
        fallback_id=attempt_id,
    )
    requested_option_notional = _execution_notional(
        quantity=resolved_quantity,
        limit_price=resolved_limit_price,
    )
    option_risk_policy = _metadata_policy(metadata, "risk_policy")
    option_execution_policy = _direct_order_execution_policy(
        metadata,
        risk_policy=option_risk_policy,
        quantity=resolved_quantity,
    )
    if resolved_trade_intent == OPEN_TRADE_INTENT:
        position_size_policy = _strategy_position_size_policy(
            trading_strategy_id=_as_text(metadata.get("trading_strategy_id")),
        )
        risk_evaluation = evaluate_open_execution(
            execution_store=execution_store,
            session_id=build_live_run_scope_id(resolved_label, resolved_market_date),
            candidate=candidate_payload,
            cycle={
                "session_date": resolved_market_date,
                "label": resolved_label,
                "generated_at": candidate_generated_at,
            },
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            risk_policy=option_risk_policy,
            execution_policy=option_execution_policy,
            strategy_risk_budget=position_size_policy["max_risk_per_trade"],
            position_size_pct_of_available_balance=position_size_policy["position_size_pct_of_available_balance"],
        )
        execution_admission = _execution_admission_payload_from_risk_evaluation(
            risk_evaluation,
            admission_kind="direct_option_open",
            source_object_type=option_source_type,
            source_object_id=option_source_id,
            session_date=resolved_market_date,
            requested_notional=requested_option_notional,
            max_loss=requested_option_notional,
        )
        _raise_if_admission_blocks(execution_admission)
    else:
        execution_admission = _approved_execution_admission(
            admission_kind="direct_option_close",
            source_object_type=option_source_type,
            source_object_id=option_source_id,
            session_date=resolved_market_date,
            requested_quantity=resolved_quantity,
            requested_notional=requested_option_notional,
            reason="direct_option_close_request_validated",
            message="Direct option close order passed request validation.",
            policy_snapshot=option_risk_policy,
            evidence={
                "asset_class": "option",
                "symbol": normalized_symbol,
                "underlying_symbol": normalized_underlying,
                "position_intent": position_intent,
            },
            decided_at=requested_at,
        )
    attempt_refs = _attempt_ref_kwargs(
        metadata,
        fallback_type="direct_option_order",
        fallback_id=attempt_id,
    )
    attempt_created = False
    submitted_order: dict[str, Any] | None = None
    try:
        attempt = execution_store.create_attempt(
            execution_attempt_id=attempt_id,
            session_id=build_live_run_scope_id(resolved_label, resolved_market_date),
            session_date=resolved_market_date,
            label=resolved_label,
            pipeline_id=build_pipeline_id(resolved_label),
            trading_strategy_id=_as_text(metadata.get("trading_strategy_id")),
            market_date=resolved_market_date,
            cycle_id=None,
            opportunity_id=None,
            risk_decision_id=None,
            candidate_id=None,
            attempt_context="option_order",
            candidate_generated_at=None,
            run_id=None,
            job_run_id=None,
            underlying_symbol=normalized_underlying,
            strategy=resolved_strategy_family,
            expiration_date=resolved_expiration,
            structure_identity=str(candidate_payload["structure_identity"]),
            legs=legs,
            order_payload=order_request,
            economics={
                "midpoint_credit": resolved_limit_price,
                "natural_credit": resolved_limit_price,
                "max_profit": None,
                "max_loss": round(resolved_limit_price * 100.0, 2),
            },
            trade_intent=resolved_trade_intent,
            position_id=position_id,
            root_symbol=normalized_underlying,
            strategy_family=resolved_strategy_family,
            style_profile=_as_text(metadata.get("style_profile")) or str(policy_fields["style_profile"]),
            horizon_intent=_as_text(metadata.get("horizon_intent")) or str(policy_fields["horizon_intent"]),
            product_class=_as_text(metadata.get("product_class")) or str(policy_fields["product_class"]),
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            requested_at=requested_at,
            status=PENDING_SUBMISSION_STATUS,
            broker=BROKER_NAME,
            client_order_id=client_order_id,
            request={
                **{key: value for key, value in attempt_refs.items() if value is not None},
                "trade_intent": resolved_trade_intent,
                "execution_runtime": normalized_runtime,
                "execution_policy": option_execution_policy,
                "asset_class": "option",
                "position_intent": position_intent,
                **({} if position_id is None else {"position_id": position_id}),
                **(
                    {}
                    if _as_text(metadata.get("trading_strategy_id")) is None
                    else {"trading_strategy_id": _as_text(metadata.get("trading_strategy_id"))}
                ),
                **({} if _as_text(metadata.get("config_hash")) is None else {"config_hash": _as_text(metadata.get("config_hash"))}),
                **(
                    {}
                    if _as_text(metadata.get("execution_intent_id")) is None
                    else {"execution_intent_id": _as_text(metadata.get("execution_intent_id"))}
                ),
                **({} if not isinstance(metadata.get("exit_policy"), Mapping) else {"exit_policy": dict(metadata["exit_policy"])}),
                **({} if not isinstance(metadata.get("risk_policy"), Mapping) else {"risk_policy": dict(metadata["risk_policy"])}),
                "execution_admission": execution_admission,
                **({} if not isinstance(metadata.get("source"), Mapping) else {"source": dict(metadata["source"])}),
                **(
                    {}
                    if _as_text(metadata.get("original_limit_price")) is None
                    else {"original_limit_price": _coerce_float(metadata.get("original_limit_price"))}
                ),
                **(
                    {}
                    if _as_text(metadata.get("previous_limit_price")) is None
                    else {"previous_limit_price": _coerce_float(metadata.get("previous_limit_price"))}
                ),
                **(
                    {}
                    if _as_text(metadata.get("previous_execution_attempt_id")) is None
                    else {"previous_execution_attempt_id": _as_text(metadata.get("previous_execution_attempt_id"))}
                ),
                **(
                    {}
                    if _as_text(metadata.get("supersedes_execution_intent_id")) is None
                    else {"supersedes_execution_intent_id": _as_text(metadata.get("supersedes_execution_intent_id"))}
                ),
                **({} if _coerce_int(metadata.get("reprice_count")) is None else {"reprice_count": _coerce_int(metadata.get("reprice_count"))}),
                **({} if not isinstance(metadata.get("repricing_policy"), Mapping) else {"repricing_policy": dict(metadata["repricing_policy"])}),
                **({} if not option_selection else {"option_selection": option_selection}),
                "order": order_request,
            },
            candidate=candidate_payload,
            **attempt_refs,
        )
        attempt_created = True
        adapter = create_alpaca_order_adapter()
        submission = adapter.submit_order(order_request)
        submitted_order = submission.submitted_order
        synced_attempt = _sync_attempt_state(
            execution_store=execution_store,
            attempt=dict(attempt),
            client=adapter.client,
            order_snapshot=submission.order_snapshot,
        )
        message = f"Submitted option {normalized_side} for " f"{resolved_quantity} {normalized_symbol}."
        _publish_execution_attempt_event(synced_attempt, message=message)
        return {
            "action": "submit",
            "changed": True,
            "message": message,
            "attempt": synced_attempt,
        }
    except AlpacaRequestError as exc:
        if submitted_order is None and attempt_created:
            classified_error = classify_alpaca_request_error(exc)
            execution_store.update_attempt(
                execution_attempt_id=attempt_id,
                status="failed",
                client_order_id=client_order_id,
                completed_at=requested_at,
                error_text=str(classified_error["message"]),
                position_id=position_id,
            )
            failed_attempt = _get_attempt_payload(execution_store, attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=("Option execution failed before submission: " f"{classified_error['message']}"),
            )
            if bool(classified_error.get("terminal")):
                return {
                    "action": "submit",
                    "changed": True,
                    "status": "blocked",
                    "reason": str(classified_error["reason"]),
                    "message": str(classified_error["message"]),
                    "attempt": failed_attempt,
                }
        raise
    except Exception as exc:
        if submitted_order is None and attempt_created:
            execution_store.update_attempt(
                execution_attempt_id=attempt_id,
                status="failed",
                client_order_id=client_order_id,
                completed_at=requested_at,
                error_text=str(exc),
                position_id=position_id,
            )
            failed_attempt = _get_attempt_payload(execution_store, attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=f"Option execution failed before submission: {exc}",
            )
        raise


@with_storage()
def refresh_execution_attempt(
    *,
    db_target: str,
    execution_attempt_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")
    session_id = _as_text(attempt.get("session_id"))
    if session_id is None:
        label = _as_text(attempt.get("label"))
        market_date = _as_text(attempt.get("market_date")) or _as_text(attempt.get("session_date"))
        if label is None or market_date is None:
            raise ValueError("Execution attempt is missing session compatibility fields")
        session_id = build_live_run_scope_id(label, market_date)
    return refresh_live_session_execution(
        db_target=db_target,
        session_id=session_id,
        execution_attempt_id=execution_attempt_id,
        storage=storage,
    )


@with_storage()
def cancel_execution_attempt(
    *,
    db_target: str,
    execution_attempt_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")

    status = str(attempt.get("status") or "").strip().lower()
    if _is_terminal_status(status):
        payload = _get_attempt_payload(execution_store, execution_attempt_id)
        return {
            "action": "cancel",
            "changed": False,
            "message": f"Execution is already terminal: {payload['status']}.",
            "attempt": payload,
        }

    broker_order_id = _as_text(attempt.get("broker_order_id"))
    if broker_order_id is None:
        if status != PENDING_SUBMISSION_STATUS:
            raise ValueError("Execution does not have a broker order id to cancel")
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status="canceled",
            completed_at=_utc_now(),
            position_id=_as_text(attempt.get("position_id")),
        )
        payload = _get_attempt_payload(execution_store, execution_attempt_id)
        message = f"Canceled queued execution {execution_attempt_id} before broker submit."
        _publish_execution_attempt_event(payload, message=message)
        _sync_linked_execution_intent(
            execution_store=execution_store,
            attempt=payload,
            state="canceled",
            event_type="canceled",
            message=message,
        )
        return {
            "action": "cancel",
            "changed": True,
            "message": message,
            "attempt": payload,
        }

    adapter = create_alpaca_order_adapter()
    order_snapshot = adapter.request_cancel(broker_order_id)
    execution_store.update_attempt(
        execution_attempt_id=execution_attempt_id,
        status="pending_cancel",
        position_id=_as_text(attempt.get("position_id")),
    )
    if order_snapshot is None:
        payload = _get_attempt_payload(execution_store, execution_attempt_id)
    else:
        payload = _sync_attempt_state(
            execution_store=execution_store,
            attempt=dict(attempt),
            client=adapter.client,
            order_snapshot=order_snapshot,
        )
    message = f"Requested cancel for execution {execution_attempt_id}: {payload['status']}."
    _publish_execution_attempt_event(payload, message=message)
    _sync_linked_execution_intent(
        execution_store=execution_store,
        attempt=payload,
        event_type="cancel_requested",
        message=message,
    )
    return {
        "action": "cancel",
        "changed": True,
        "message": message,
        "attempt": payload,
    }


def _sync_equity_attempt_state(
    *,
    execution_store: Any,
    attempt: Mapping[str, Any],
    client: Any,
    order_snapshot: dict[str, Any],
) -> dict[str, Any]:
    order_rows = _flatten_order_snapshot(order_snapshot)
    persisted_orders = [
        dict(row)
        for row in execution_store.upsert_orders(
            execution_attempt_id=str(attempt["execution_attempt_id"]),
            rows=order_rows,
        )
    ]
    try:
        fill_rows = _sync_fill_rows(
            client=client,
            session_date=str(attempt["session_date"]),
            persisted_orders=persisted_orders,
        )
    except Exception:
        fill_rows = []
    if fill_rows:
        execution_store.upsert_fills(
            execution_attempt_id=str(attempt["execution_attempt_id"]),
            rows=fill_rows,
        )

    status = str(order_snapshot.get("status") or attempt.get("status") or "unknown").lower()
    completed_at = _resolve_completed_at(order_snapshot) if _is_terminal_status(status) else None
    execution_store.update_attempt(
        execution_attempt_id=str(attempt["execution_attempt_id"]),
        status=status,
        broker_order_id=_as_text(order_snapshot.get("id")),
        client_order_id=_as_text(order_snapshot.get("client_order_id")),
        submitted_at=_as_text(order_snapshot.get("submitted_at")) or str(attempt["requested_at"]),
        completed_at=completed_at,
        error_text=None,
    )
    payload = _get_attempt_payload(execution_store, str(attempt["execution_attempt_id"]))
    request = dict(payload.get("request") or {})
    should_sync_position = str(request.get("trade_intent") or "") == OPEN_TRADE_INTENT
    should_sync_position = should_sync_position or _as_text(request.get("position_id")) is not None
    if should_sync_position:
        try:
            from core.services.session_positions import sync_session_position_from_attempt

            sync_session_position_from_attempt(
                execution_store=execution_store,
                attempt=payload,
            )
            payload = _get_attempt_payload(execution_store, str(attempt["execution_attempt_id"]))
        except Exception:
            pass
    return payload


@with_storage()
def run_execution_submit(
    *,
    db_target: str,
    execution_attempt_id: str,
    heartbeat: Any | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")

    payload = _get_attempt_payload(execution_store, execution_attempt_id)
    broker_order_id = _as_text(payload.get("broker_order_id"))
    status = str(payload.get("status") or "")
    if broker_order_id is not None or status != PENDING_SUBMISSION_STATUS:
        return {
            "status": "skipped",
            "reason": "attempt_already_submitted",
            "execution_attempt_id": execution_attempt_id,
            "attempt_status": status,
            "broker_order_id": broker_order_id,
        }

    request = dict(payload.get("request") or {})
    execution_runtime_from_request(request)
    order_request = request.get("order")
    if not isinstance(order_request, dict) or not order_request:
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status="failed",
            completed_at=_utc_now(),
            error_text="Execution attempt is missing its broker order payload.",
            position_id=_as_text(payload.get("position_id")),
        )
        failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
        _publish_execution_attempt_event(
            failed_attempt,
            message="Execution failed before submission: missing broker order payload.",
        )
        _sync_linked_execution_intent(
            execution_store=execution_store,
            attempt=failed_attempt,
            state="failed",
            event_type="failed",
            message="Execution failed before submission: missing broker order payload.",
        )
        raise ValueError("Execution attempt is missing its broker order payload.")

    requested_at = _as_text(payload.get("requested_at")) or _utc_now()
    client_order_id = _as_text(payload.get("client_order_id"))

    if str(payload.get("trade_intent") or OPEN_TRADE_INTENT) == OPEN_TRADE_INTENT:
        request_execution_policy = request.get("execution_policy") if isinstance(request.get("execution_policy"), Mapping) else {}
        timing_gate = _validate_open_timing_window(
            exit_policy=request.get("exit_policy"),
            current_time=datetime.now(UTC),
            profile=resolve_candidate_profile(dict(payload.get("candidate") or {})),
            deployment_mode=str(request_execution_policy.get("deployment_mode") or ""),
        )
        if not timing_gate["allowed"]:
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status="failed",
                completed_at=_utc_now(),
                error_text=str(timing_gate["message"]),
                position_id=_as_text(payload.get("position_id")),
            )
            failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=f"Execution failed before submission: {timing_gate['message']}",
            )
            _sync_linked_execution_intent(
                execution_store=execution_store,
                attempt=failed_attempt,
                state="failed",
                event_type="failed",
                message=f"Execution failed before submission: {timing_gate['message']}",
            )
            return {
                "status": "blocked",
                "reason": str(timing_gate["reason"]),
                "execution_attempt_id": execution_attempt_id,
                "message": str(timing_gate["message"]),
                "attempt": failed_attempt,
            }

    if callable(heartbeat):
        heartbeat()
    adapter = create_alpaca_order_adapter()
    client = adapter.client
    if str(payload.get("trade_intent") or OPEN_TRADE_INTENT) == OPEN_TRADE_INTENT:
        request_payload = payload.get("request") if isinstance(payload.get("request"), Mapping) else {}
        request_execution_policy = request_payload.get("execution_policy") if isinstance(request_payload.get("execution_policy"), Mapping) else {}
        live_deployment_quality = _validate_live_deployment_quality(
            candidate_payload=dict(payload.get("candidate") or {}),
            deployment_mode=str(request_execution_policy.get("deployment_mode") or ""),
            client=client,
        )
        if not live_deployment_quality["ok"]:
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status="failed",
                completed_at=_utc_now(),
                error_text=str(live_deployment_quality["message"]),
                position_id=_as_text(payload.get("position_id")),
            )
            failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=("Execution failed before submission: " f"{live_deployment_quality['message']}"),
            )
            _sync_linked_execution_intent(
                execution_store=execution_store,
                attempt=failed_attempt,
                state="failed",
                event_type="failed",
                message=("Execution failed before submission: " f"{live_deployment_quality['message']}"),
            )
            return {
                "status": "blocked",
                "reason": str(live_deployment_quality["reason"]),
                "execution_attempt_id": execution_attempt_id,
                "message": str(live_deployment_quality["message"]),
                "attempt": failed_attempt,
                **({} if live_deployment_quality.get("live_quote") is None else {"live_quote": dict(live_deployment_quality["live_quote"])}),
            }
        account_capacity = _validate_submit_account_capacity(
            execution_store=execution_store,
            attempt=payload,
            client=client,
        )
        if not account_capacity["ok"]:
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status="failed",
                completed_at=_utc_now(),
                error_text=str(account_capacity["message"]),
                position_id=_as_text(payload.get("position_id")),
            )
            failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=("Execution failed before submission: " f"{account_capacity['message']}"),
            )
            _sync_linked_execution_intent(
                execution_store=execution_store,
                attempt=failed_attempt,
                state="failed",
                event_type="failed",
                message=("Execution failed before submission: " f"{account_capacity['message']}"),
                payload_updates={
                    "execution_admission": _execution_admission_payload_from_account_capacity(
                        attempt=payload,
                        account_capacity=account_capacity,
                    )
                },
            )
            return {
                "status": "blocked",
                "reason": str(account_capacity["reason"]),
                "execution_attempt_id": execution_attempt_id,
                "message": str(account_capacity["message"]),
                "attempt": failed_attempt,
            }
    order_request = _normalize_submit_order_request(
        payload=payload,
        order_request=order_request,
    )

    submitted_order: dict[str, Any] | None = None
    try:
        submission = adapter.submit_order(order_request)
        submitted_order = submission.submitted_order
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status=str(submitted_order.get("status") or "submitted").lower(),
            broker_order_id=_as_text(submitted_order.get("id")),
            client_order_id=_as_text(submitted_order.get("client_order_id")) or client_order_id,
            submitted_at=_as_text(submitted_order.get("submitted_at")) or requested_at,
            position_id=_as_text(payload.get("position_id")),
        )
        if callable(heartbeat):
            heartbeat()
        synced_attempt = _sync_attempt_state(
            execution_store=execution_store,
            attempt=payload,
            client=client,
            order_snapshot=submission.order_snapshot,
        )
        message = _submission_message(synced_attempt, queued=False)
        _publish_execution_attempt_event(synced_attempt, message=message)
        _sync_linked_execution_intent(
            execution_store=execution_store,
            attempt=synced_attempt,
            event_type="submitted",
            message=message,
        )
        return {
            "status": "submitted",
            "execution_attempt_id": execution_attempt_id,
            "message": message,
            "attempt": synced_attempt,
        }
    except AlpacaRequestError as exc:
        if submitted_order is None:
            classified_error = classify_alpaca_request_error(exc)
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status="failed",
                client_order_id=client_order_id,
                completed_at=requested_at,
                error_text=str(classified_error["message"]),
                position_id=_as_text(payload.get("position_id")),
            )
            failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=("Execution failed before submission: " f"{classified_error['message']}"),
            )
            _sync_linked_execution_intent(
                execution_store=execution_store,
                attempt=failed_attempt,
                state="failed",
                event_type="failed",
                message=("Execution failed before submission: " f"{classified_error['message']}"),
                payload_updates=(
                    {
                        "execution_admission": _execution_admission_payload_from_broker_rejection(
                            attempt=payload,
                            classified_error=classified_error,
                        )
                    }
                    if bool(classified_error.get("terminal"))
                    else None
                ),
            )
            if bool(classified_error.get("terminal")):
                return {
                    "status": "blocked",
                    "reason": str(classified_error["reason"]),
                    "execution_attempt_id": execution_attempt_id,
                    "message": str(classified_error["message"]),
                    "attempt": failed_attempt,
                }
            raise
        broker_order_id = _as_text(submitted_order.get("id"))
        submitted_status = str(submitted_order.get("status") or "submitted").lower()
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status=submitted_status,
            broker_order_id=broker_order_id,
            client_order_id=_as_text(submitted_order.get("client_order_id")) or client_order_id,
            submitted_at=_as_text(submitted_order.get("submitted_at")) or requested_at,
            completed_at=_resolve_completed_at(submitted_order) if _is_terminal_status(submitted_status) else None,
            error_text=str(exc),
            position_id=_as_text(payload.get("position_id")),
        )
        failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
        _publish_execution_attempt_event(
            failed_attempt,
            message=(f"Order {broker_order_id or execution_attempt_id} was submitted, " f"but local execution sync failed: {exc}"),
        )
        _sync_linked_execution_intent(
            execution_store=execution_store,
            attempt=failed_attempt,
            event_type="submit_unknown",
            message=(f"Order {broker_order_id or execution_attempt_id} was submitted, " f"but local execution sync failed: {exc}"),
        )
        raise
    except Exception as exc:
        if submitted_order is None:
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status="failed",
                client_order_id=client_order_id,
                completed_at=requested_at,
                error_text=str(exc),
                position_id=_as_text(payload.get("position_id")),
            )
            failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=f"Execution failed before submission: {exc}",
            )
            _sync_linked_execution_intent(
                execution_store=execution_store,
                attempt=failed_attempt,
                state="failed",
                event_type="failed",
                message=f"Execution failed before submission: {exc}",
            )
            raise
        broker_order_id = _as_text(submitted_order.get("id"))
        submitted_status = str(submitted_order.get("status") or "submitted").lower()
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status=submitted_status,
            broker_order_id=broker_order_id,
            client_order_id=_as_text(submitted_order.get("client_order_id")) or client_order_id,
            submitted_at=_as_text(submitted_order.get("submitted_at")) or requested_at,
            completed_at=_resolve_completed_at(submitted_order) if _is_terminal_status(submitted_status) else None,
            error_text=str(exc),
            position_id=_as_text(payload.get("position_id")),
        )
        failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
        _publish_execution_attempt_event(
            failed_attempt,
            message=(f"Order {broker_order_id or execution_attempt_id} was submitted, " f"but local execution sync failed: {exc}"),
        )
        _sync_linked_execution_intent(
            execution_store=execution_store,
            attempt=failed_attempt,
            event_type="submit_unknown",
            message=(f"Order {broker_order_id or execution_attempt_id} was submitted, " f"but local execution sync failed: {exc}"),
        )
        raise
