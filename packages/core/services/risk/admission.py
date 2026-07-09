from __future__ import annotations

from collections.abc import Mapping
import os
from datetime import UTC, datetime
from typing import Any


from core.money import money_float, money_sum_float
from core.services.risk.buying_power import (
    estimate_buying_power_requirement,
)
from core.services.alpaca import (
    create_alpaca_client_from_env,
    resolve_trading_environment,
)
from core.services.deployment_policy import (
    live_deployment_block_reason,
    resolve_execution_deployment_mode,
)
from core.services.option_structures import (
    candidate_legs,
    position_legs,
)
from core.value_coercion import (
    as_text,
    coerce_bool,
    coerce_float,
    coerce_int,
    unique_text_list,
    utc_now_iso,
)
from core.storage.serializers import parse_datetime
from core.services.risk.candidates import (
    _candidate_entry_notional,
    _candidate_max_loss,
    _candidate_with_payload,
)
from core.services.risk.policy import (
    DEFERRED_EXECUTION_READINESS_REASON,
    ENTRY_CAPACITY_ADMISSION_BOUNDARY,
    OPEN_POSITION_STATUSES,
    PORTFOLIO_ADMISSION_BOUNDARY,
    PROTECTION_ADMISSIBLE_STATUSES,
    PROTECTION_ADMISSION_BOUNDARY,
    normalize_risk_policy,
)
from core.services.risk.exposures import (
    _open_attempts,
    _open_positions,
    _pending_open_attempt_exposures,
    _session_open_metrics,
    live_broker_buying_power_snapshot,
)
from core.services.risk.sizing import (
    _effective_max_contracts_per_position,
    build_candidate_position_sizing,
    build_open_candidate_position_sizing,
    strategy_supports_position_sizing,
)




def _current_trading_environment() -> str:
    client = create_alpaca_client_from_env()
    return resolve_trading_environment(client.trading_base_url)










def _deferred_execution_readiness_payload() -> dict[str, Any]:
    return {
        "status": "not_evaluated",
        "reason": DEFERRED_EXECUTION_READINESS_REASON,
        "message": "Final quote, broker, and order-submit readiness is evaluated by the execution submit path.",
        "evaluated_by": "broker_activity",
    }


def build_entry_capacity_admission_payload(
    *,
    status: str,
    reason: str | None,
    message: str | None,
    admissible_quantity: int | None,
    required_buying_power: float | None,
    available_buying_power: float | None,
    account_available_buying_power: float | None = None,
    reserved_buying_power: float | None = None,
    buying_power_basis: str | None = None,
    buying_power_source_field: str | None = None,
    broker_buying_power_status: str | None = None,
    limiting_constraint: str | None = None,
    strategy_risk_budget: float | None = None,
    position_size_pct_of_available_balance: float | None = None,
    position_size_budget: float | None = None,
    protection_admission: Mapping[str, Any] | None = None,
    portfolio_admission: Mapping[str, Any] | None = None,
    evaluated_at: str | None = None,
) -> dict[str, Any]:
    normalized_status = as_text(status) or "unknown"
    capacity_reason = as_text(reason)
    capacity_status = "admissible" if normalized_status in {"admissible", "approved", "ok", "pass", "passed"} else normalized_status
    if capacity_status == "admissible":
        reason_codes = [capacity_reason or "capacity_admissible"]
        blockers: list[str] = []
        top_level_reason = None if capacity_reason in {None, "capacity_admissible"} else capacity_reason
    else:
        reason_codes = [capacity_reason] if capacity_reason else []
        blockers = list(reason_codes)
        top_level_reason = capacity_reason
    execution_readiness = _deferred_execution_readiness_payload()
    protection_admission_payload = (
        dict(protection_admission)
        if isinstance(protection_admission, Mapping)
        else {
            "status": "not_evaluated",
            "reason": "protection_admission_not_evaluated",
            "message": "Protection admission was not evaluated for this admission payload.",
            "admission_boundary": PROTECTION_ADMISSION_BOUNDARY,
            "reason_codes": [],
            "blockers": [],
        }
    )
    protection_status_raw = as_text(protection_admission_payload.get("status")) or "not_evaluated"
    protection_status = "admissible" if protection_status_raw in PROTECTION_ADMISSIBLE_STATUSES else protection_status_raw
    protection_reason = as_text(protection_admission_payload.get("reason"))
    protection_reason_codes = unique_text_list(protection_admission_payload.get("reason_codes"), accept_scalar=True)
    if protection_status in {"admissible", "blocked", "unknown"} and protection_reason and protection_reason not in protection_reason_codes:
        protection_reason_codes.append(protection_reason)
    protection_blockers = unique_text_list(protection_admission_payload.get("blockers"), accept_scalar=True)
    if protection_status in {"blocked", "unknown"} and protection_reason and protection_reason not in protection_blockers:
        protection_blockers.append(protection_reason)

    portfolio_admission_payload = (
        dict(portfolio_admission)
        if isinstance(portfolio_admission, Mapping)
        else {
            "status": "not_evaluated",
            "reason": "portfolio_admission_not_evaluated",
            "message": "Portfolio admission was not evaluated for this admission payload.",
            "admission_boundary": PORTFOLIO_ADMISSION_BOUNDARY,
            "reason_codes": [],
            "blockers": [],
        }
    )
    portfolio_status_raw = as_text(portfolio_admission_payload.get("status")) or "not_evaluated"
    portfolio_status = "admissible" if portfolio_status_raw in {"admissible", "approved", "ok", "pass", "passed"} else portfolio_status_raw
    portfolio_reason = as_text(portfolio_admission_payload.get("reason"))
    portfolio_reason_codes = unique_text_list(portfolio_admission_payload.get("reason_codes"), accept_scalar=True)
    if portfolio_status in {"admissible", "blocked", "unknown"} and portfolio_reason and portfolio_reason not in portfolio_reason_codes:
        portfolio_reason_codes.append(portfolio_reason)
    portfolio_blockers = unique_text_list(portfolio_admission_payload.get("blockers"), accept_scalar=True)
    if portfolio_status in {"blocked", "unknown"} and portfolio_reason and portfolio_reason not in portfolio_blockers:
        portfolio_blockers.append(portfolio_reason)

    capacity_admission = {
        "status": capacity_status,
        "reason": capacity_reason or ("capacity_admissible" if capacity_status == "admissible" else None),
        "message": message,
        "admissible_quantity": admissible_quantity,
        "required_buying_power": required_buying_power,
        "available_buying_power": available_buying_power,
        "limiting_constraint": limiting_constraint,
        "reason_codes": reason_codes,
        "blockers": blockers,
    }
    combined_reason_codes = unique_text_list(
        [*reason_codes, *protection_reason_codes, *portfolio_reason_codes],
        accept_scalar=True,
    )
    combined_blockers = unique_text_list(
        [*blockers, *protection_blockers, *portfolio_blockers],
        accept_scalar=True,
    )
    top_level_status = "admissible" if capacity_status == "admissible" else capacity_status
    top_level_message = message
    top_level_boundary = ENTRY_CAPACITY_ADMISSION_BOUNDARY
    if protection_status in {"blocked", "unknown"}:
        top_level_status = protection_status
        top_level_reason = protection_reason
        top_level_message = as_text(protection_admission_payload.get("message")) or top_level_message
        top_level_boundary = PROTECTION_ADMISSION_BOUNDARY
    elif portfolio_status in {"blocked", "unknown"}:
        top_level_status = portfolio_status
        top_level_reason = portfolio_reason
        top_level_message = as_text(portfolio_admission_payload.get("message")) or top_level_message
        top_level_boundary = PORTFOLIO_ADMISSION_BOUNDARY
    elif portfolio_status == "admissible" and top_level_status == "admissible":
        top_level_reason = None

    return {
        "status": top_level_status,
        "reason": top_level_reason,
        "message": top_level_message,
        "admission_boundary": top_level_boundary,
        "capacity_admission_kind": ENTRY_CAPACITY_ADMISSION_BOUNDARY,
        "capacity_admission_status": capacity_status,
        "protection_admission_status": protection_status,
        "protection_admission_reason": protection_reason,
        "portfolio_admission_status": portfolio_status,
        "portfolio_admission_reason": portfolio_reason,
        "execution_readiness_status": execution_readiness["status"],
        "execution_readiness_reason": execution_readiness["reason"],
        "capacity_admission": capacity_admission,
        "protection_admission": protection_admission_payload,
        "portfolio_admission": portfolio_admission_payload,
        "execution_readiness": execution_readiness,
        "reason_codes": combined_reason_codes,
        "blockers": combined_blockers,
        "evaluated_at": evaluated_at or utc_now_iso(),
        "admissible_quantity": admissible_quantity,
        "required_buying_power": required_buying_power,
        "available_buying_power": available_buying_power,
        "account_available_buying_power": account_available_buying_power,
        "reserved_buying_power": reserved_buying_power,
        "buying_power_basis": buying_power_basis,
        "buying_power_source_field": buying_power_source_field,
        "broker_buying_power_status": broker_buying_power_status,
        "limiting_constraint": limiting_constraint,
        "strategy_risk_budget": strategy_risk_budget,
        "position_size_pct_of_available_balance": position_size_pct_of_available_balance,
        "position_size_budget": position_size_budget,
    }


def build_execution_admission_snapshot(
    *,
    execution_store: Any,
    candidate: dict[str, Any],
    limit_price: float | None,
    strategy_risk_budget: float | None = None,
    position_size_pct_of_available_balance: float | None = None,
    protection_admission: Mapping[str, Any] | None = None,
    portfolio_admission: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    broker_buying_power = live_broker_buying_power_snapshot(execution_store)
    buying_power_requirement = estimate_buying_power_requirement(
        candidate,
        1.0,
        limit_price=limit_price,
    )
    required_buying_power = coerce_float(buying_power_requirement.get("required_buying_power"))
    available_buying_power = coerce_float(broker_buying_power.get("remaining_buying_power"))
    sizing = build_candidate_position_sizing(
        candidate=candidate,
        limit_price=limit_price,
        strategy_risk_budget=strategy_risk_budget,
        position_size_pct_of_available_balance=position_size_pct_of_available_balance,
        available_broker_buying_power=available_buying_power,
    )
    limiting_constraint = as_text(sizing.get("limiting_constraint"))
    admissible_quantity = coerce_int(sizing.get("recommended_quantity"))

    def capacity_payload(
        *,
        status: str,
        reason: str | None,
        message: str | None,
        quantity: int | None,
    ) -> dict[str, Any]:
        return build_entry_capacity_admission_payload(
            status=status,
            reason=reason,
            message=message,
            admissible_quantity=quantity,
            required_buying_power=required_buying_power,
            available_buying_power=available_buying_power,
            account_available_buying_power=coerce_float(broker_buying_power.get("available_buying_power")),
            reserved_buying_power=coerce_float(broker_buying_power.get("reserved_buying_power")),
            buying_power_basis=as_text(buying_power_requirement.get("basis")),
            buying_power_source_field=as_text(broker_buying_power.get("source_field")),
            broker_buying_power_status=as_text(broker_buying_power.get("status")),
            limiting_constraint=limiting_constraint,
            strategy_risk_budget=strategy_risk_budget,
            position_size_pct_of_available_balance=coerce_float(sizing.get("position_size_pct_of_available_balance")),
            position_size_budget=coerce_float(sizing.get("position_size_budget")),
            protection_admission=protection_admission,
            portfolio_admission=portfolio_admission,
        )

    if str(broker_buying_power.get("status") or "") != "ok":
        reason = "broker_buying_power_unavailable"
        return capacity_payload(
            status="unknown",
            reason=reason,
            message=as_text(broker_buying_power.get("error_text")) or "Broker buying power is unavailable.",
            quantity=None,
        )
    if required_buying_power is None:
        reason = "unsupported_buying_power_estimate"
        return capacity_payload(
            status="unknown",
            reason=reason,
            message="Buying power estimate is unavailable for this structure.",
            quantity=None,
        )

    resolved_quantity = max(int(admissible_quantity or 0), 0)
    if resolved_quantity <= 0:
        reason = "insufficient_broker_buying_power"
        message = "Current account buying power cannot carry one contract."
        if limiting_constraint == "position_size_pct_of_available_balance":
            reason = "position_size_budget_exhausted"
            message = "Configured position size budget does not allow one contract."
        elif limiting_constraint == "max_risk_per_trade":
            reason = "max_risk_per_trade_exhausted"
            message = "Configured strategy risk budget does not allow one contract."
        elif limiting_constraint not in {None, "", "available_broker_buying_power"}:
            reason = "execution_capacity_unavailable"
            message = "Current execution capacity does not allow one contract."
        if available_buying_power is not None and required_buying_power is not None and reason == "insufficient_broker_buying_power":
            message = (
                "Current account buying power cannot carry one contract "
                f"(requires {required_buying_power:.2f}, "
                f"available {available_buying_power:.2f})."
            )
        return capacity_payload(status="blocked", reason=reason, message=message, quantity=0)

    message = f"Current account can carry up to {resolved_quantity} contract"
    if resolved_quantity != 1:
        message += "s"
    message += " now."
    return capacity_payload(status="admissible", reason=None, message=message, quantity=resolved_quantity)


def _broker_position_side(position: Mapping[str, Any]) -> str | None:
    side = as_text(position.get("side"))
    if side in {"long", "short"}:
        return side
    quantity = coerce_float(position.get("qty"))
    if quantity is None:
        return None
    if quantity > 0:
        return "long"
    if quantity < 0:
        return "short"
    return None


def _candidate_broker_position_conflicts(
    candidate: dict[str, Any],
) -> list[dict[str, str]]:
    resolved_candidate = _candidate_with_payload(candidate)
    resolved_legs = candidate_legs(resolved_candidate)
    if not resolved_legs:
        return []
    try:
        broker_positions = create_alpaca_client_from_env().list_positions()
    except Exception:
        return []

    broker_positions_by_symbol = {
        symbol: side
        for position in broker_positions
        if isinstance(position, Mapping)
        and (symbol := as_text(position.get("symbol"))) is not None
        and (side := _broker_position_side(position)) is not None
    }
    conflicts: list[dict[str, str]] = []
    for leg in resolved_legs:
        symbol = as_text(leg.get("symbol"))
        role = as_text(leg.get("role"))
        if symbol is None or role not in {"short", "long"}:
            continue
        broker_side = broker_positions_by_symbol.get(symbol)
        requested_side = "short" if role == "short" else "long"
        if broker_side is None or broker_side == requested_side:
            continue
        conflicts.append(
            {
                "symbol": symbol,
                "broker_side": broker_side,
                "requested_role": role,
                "requested_position_intent": ("sell_to_open" if role == "short" else "buy_to_open"),
            }
        )
    return conflicts




def _kill_switch_reason() -> str | None:
    if coerce_bool(os.environ.get("SPREADS_EXECUTION_KILL_SWITCH"), default=False):
        return "Execution is blocked by SPREADS_EXECUTION_KILL_SWITCH."
    return None


def resolve_execution_kill_switch_reason() -> str | None:
    return _kill_switch_reason()


def _environment_reason(
    normalized_policy: dict[str, Any],
    *,
    execution_policy: dict[str, Any] | None = None,
) -> str | None:
    environment = _current_trading_environment()
    deployment_mode = resolve_execution_deployment_mode(
        execution_policy,
        risk_policy=normalized_policy,
    )
    return live_deployment_block_reason(
        deployment_mode=deployment_mode,
        environment=environment,
        allow_live_env=coerce_bool(os.environ.get("SPREADS_ALLOW_LIVE_TRADING"), default=False),
    )


def _candidate_timestamp(candidate: dict[str, Any], cycle: dict[str, Any]) -> datetime | None:
    return parse_datetime(as_text(candidate.get("generated_at")) or as_text(cycle.get("generated_at")))


def assess_position_risk(
    *,
    position: dict[str, Any],
    risk_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_policy = normalize_risk_policy(risk_policy or position.get("risk_policy"))
    remaining_quantity = coerce_float(position.get("remaining_quantity")) or 0.0
    if str(position.get("status") or "") == "closed" or remaining_quantity <= 0:
        return {
            "status": "ok",
            "note": "Position is closed.",
            "policy": normalized_policy,
        }
    if not normalized_policy["enabled"]:
        return {
            "status": "disabled",
            "note": "Risk policy snapshot is disabled for this position.",
            "policy": normalized_policy,
        }

    reasons: list[str] = []
    max_contracts_per_position = coerce_int(normalized_policy.get("max_contracts_per_position"))
    if max_contracts_per_position is not None and remaining_quantity > max_contracts_per_position:
        reasons.append("remaining quantity exceeds max_contracts_per_position")

    entry_notional = coerce_float(position.get("entry_notional"))
    max_position_notional = coerce_float(normalized_policy.get("max_position_notional"))
    if entry_notional is not None and max_position_notional is not None and entry_notional > max_position_notional:
        reasons.append("entry notional exceeds max_position_notional")

    max_loss = coerce_float(position.get("max_loss"))
    max_position_max_loss = coerce_float(normalized_policy.get("max_position_max_loss"))
    if max_loss is not None and max_position_max_loss is not None and max_loss > max_position_max_loss:
        reasons.append("max loss exceeds max_position_max_loss")

    if reasons:
        return {
            "status": "breach",
            "note": "; ".join(reasons),
            "policy": normalized_policy,
        }
    return {
        "status": "ok",
        "note": "Position is within its snapshotted risk limits.",
        "policy": normalized_policy,
    }


def build_session_risk_snapshot(
    *,
    execution_store: Any,
    session_id: str,
    risk_policy: dict[str, Any] | None,
    execution_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_policy = normalize_risk_policy(risk_policy)

    if hasattr(execution_store, "portfolio_schema_ready") and not execution_store.portfolio_schema_ready():
        return {
            "status": "unknown",
            "note": "Portfolio position storage is not available yet.",
            "policy": normalized_policy,
        }

    kill_switch_reason = _kill_switch_reason()
    if kill_switch_reason is not None:
        return {
            "status": "blocked",
            "note": kill_switch_reason,
            "policy": normalized_policy,
        }

    try:
        environment_reason = _environment_reason(
            normalized_policy,
            execution_policy=execution_policy,
        )
    except Exception as exc:
        return {
            "status": "unknown",
            "note": f"Could not resolve the trading environment: {exc}",
            "policy": normalized_policy,
        }
    if environment_reason is not None:
        return {
            "status": "blocked",
            "note": environment_reason,
            "policy": normalized_policy,
        }

    if not normalized_policy["enabled"]:
        return {
            "status": "disabled",
            "note": "Risk policy is disabled for this session.",
            "policy": normalized_policy,
        }

    open_positions = _open_positions(execution_store, session_id=session_id)
    open_attempts = _open_attempts(execution_store, session_id=session_id)
    pending_attempts = _pending_open_attempt_exposures(open_attempts)
    metrics = _session_open_metrics(open_positions, pending_attempts)
    reasons: list[str] = []

    if metrics["active_open_position_count"] >= float(normalized_policy["max_open_positions_per_session"]):
        reasons.append("max_open_positions_per_session reached")
    if metrics["active_open_contract_count"] >= float(normalized_policy["max_contracts_per_session"]):
        reasons.append("max_contracts_per_session reached")

    max_session_notional = coerce_float(normalized_policy.get("max_session_notional"))
    if max_session_notional is not None and metrics["active_entry_notional_total"] >= max_session_notional:
        reasons.append("max_session_notional reached")

    max_session_max_loss = coerce_float(normalized_policy.get("max_session_max_loss"))
    if max_session_max_loss is not None and metrics["active_max_loss_total"] >= max_session_max_loss:
        reasons.append("max_session_max_loss reached")

    if reasons:
        return {
            "status": "blocked",
            "note": "; ".join(reasons),
            "policy": normalized_policy,
            "metrics": metrics,
        }
    return {
        "status": "ok",
        "note": "Strategy run can submit new executions under the current risk policy.",
        "policy": normalized_policy,
        "metrics": metrics,
    }


def evaluate_open_execution(
    *,
    execution_store: Any,
    session_id: str,
    candidate: dict[str, Any],
    cycle: dict[str, Any],
    quantity: int,
    limit_price: float | None,
    risk_policy: dict[str, Any] | None,
    execution_policy: dict[str, Any] | None = None,
    strategy_risk_budget: float | None = None,
    position_size_pct_of_available_balance: float | None = None,
) -> dict[str, Any]:
    normalized_policy = normalize_risk_policy(risk_policy)
    open_positions = _open_positions(execution_store, session_id=session_id)
    open_attempts = _open_attempts(execution_store, session_id=session_id)
    pending_attempts = _pending_open_attempt_exposures(open_attempts)
    session_metrics = _session_open_metrics(open_positions, pending_attempts)
    position_notional = _candidate_entry_notional(candidate, quantity, limit_price)
    position_max_loss = _candidate_max_loss(candidate, quantity)
    candidate_timestamp = _candidate_timestamp(candidate, cycle)
    candidate_age_seconds = None
    if candidate_timestamp is not None:
        candidate_age_seconds = round((datetime.now(UTC) - candidate_timestamp).total_seconds(), 3)
    underlying_symbol = str(candidate["underlying_symbol"])
    strategy = str(candidate["strategy"])
    matching_underlyings = [position for position in open_positions if str(position.get("underlying_symbol")) == underlying_symbol]
    matching_pending_underlyings = [
        attempt
        for attempt in pending_attempts
        if bool(attempt.get("occupies_position_slot")) and str(attempt.get("underlying_symbol")) == underlying_symbol
    ]
    matching_strategy = [position for position in matching_underlyings if str(position.get("strategy")) == strategy]
    matching_pending_strategy = [attempt for attempt in matching_pending_underlyings if str(attempt.get("strategy")) == strategy]
    session_notional = session_metrics["active_entry_notional_total"]
    session_max_loss = session_metrics["active_max_loss_total"]
    open_contracts = session_metrics["active_open_contract_count"]
    buying_power_requirement = estimate_buying_power_requirement(
        candidate,
        quantity,
        limit_price=limit_price,
    )
    required_buying_power = coerce_float(buying_power_requirement.get("required_buying_power"))
    metrics = {
        **session_metrics,
        "requested_quantity": int(quantity),
        "requested_limit_price": limit_price,
        "candidate_age_seconds": candidate_age_seconds,
        "position_notional": position_notional,
        "position_max_loss": position_max_loss,
        "session_notional_before": money_float(session_notional),
        "session_notional_after": (None if position_notional is None else money_sum_float([session_notional, position_notional])),
        "session_max_loss_before": money_float(session_max_loss),
        "session_max_loss_after": (None if position_max_loss is None else money_sum_float([session_max_loss, position_max_loss])),
        "matching_underlying_count": (len(matching_underlyings) + len(matching_pending_underlyings)),
        "matching_underlying_strategy_count": (len(matching_strategy) + len(matching_pending_strategy)),
        "strategy_risk_budget": strategy_risk_budget,
        "position_size_pct_of_available_balance": position_size_pct_of_available_balance,
        "required_buying_power": required_buying_power,
        "buying_power_basis": as_text(buying_power_requirement.get("basis")),
    }
    sizing = build_open_candidate_position_sizing(
        execution_store=execution_store,
        session_id=session_id,
        candidate=candidate,
        limit_price=limit_price,
        risk_policy=risk_policy,
        strategy_risk_budget=strategy_risk_budget,
        position_size_pct_of_available_balance=position_size_pct_of_available_balance,
    )
    metrics["position_sizing"] = sizing
    metrics["recommended_quantity"] = int(sizing["recommended_quantity"])
    metrics["recommended_position_max_loss"] = sizing["recommended_max_loss"]
    metrics["recommended_position_notional"] = sizing["recommended_entry_notional"]
    metrics["available_broker_buying_power"] = coerce_float(sizing.get("available_broker_buying_power"))
    metrics["broker_buying_power_status"] = as_text(sizing.get("broker_buying_power_status"))
    metrics["broker_reserved_buying_power"] = coerce_float(sizing.get("broker_reserved_buying_power"))
    metrics["broker_buying_power_source_field"] = as_text(sizing.get("broker_buying_power_source_field"))

    kill_switch_reason = _kill_switch_reason()
    if kill_switch_reason is not None:
        return {
            "status": "blocked",
            "note": kill_switch_reason,
            "reason_codes": ["kill_switch_enabled"],
            "blockers": ["kill_switch_enabled"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    try:
        environment_reason = _environment_reason(
            normalized_policy,
            execution_policy=execution_policy,
        )
    except Exception as exc:
        return {
            "status": "unknown",
            "note": f"Could not resolve the trading environment: {exc}",
            "reason_codes": ["environment_resolution_failed"],
            "blockers": ["environment_resolution_failed"],
            "policy": normalized_policy,
            "metrics": metrics,
        }
    if environment_reason is not None:
        return {
            "status": "blocked",
            "note": environment_reason,
            "reason_codes": ["live_environment_blocked"],
            "blockers": ["live_environment_blocked"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    broker_position_conflicts = _candidate_broker_position_conflicts(candidate)
    metrics["broker_position_conflict_count"] = len(broker_position_conflicts)
    metrics["broker_position_conflict_symbols"] = [conflict["symbol"] for conflict in broker_position_conflicts]
    if broker_position_conflicts:
        conflict_summary = ", ".join(
            (f"{conflict['symbol']} " f"(broker {conflict['broker_side']}, request {conflict['requested_position_intent']})")
            for conflict in broker_position_conflicts[:4]
        )
        if len(broker_position_conflicts) > 4:
            conflict_summary += ", …"
        return {
            "status": "blocked",
            "note": ("Open execution conflicts with existing broker-held option legs: " f"{conflict_summary}."),
            "reason_codes": ["broker_position_intent_conflict"],
            "blockers": ["broker_position_intent_conflict"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if not normalized_policy["enabled"]:
        return {
            "status": "approved",
            "note": "Risk policy is disabled for this submission.",
            "reason_codes": ["risk_policy_disabled"],
            "blockers": [],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    max_contracts_per_position = _effective_max_contracts_per_position(
        candidate=candidate,
        normalized_policy=normalized_policy,
    )
    if max_contracts_per_position is not None and quantity > max_contracts_per_position:
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_contracts_per_position.",
            "reason_codes": ["max_contracts_per_position_exceeded"],
            "blockers": ["max_contracts_per_position_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    stale_quote_after_seconds = coerce_float(normalized_policy.get("stale_quote_after_seconds"))
    if candidate_age_seconds is not None and stale_quote_after_seconds is not None and candidate_age_seconds > stale_quote_after_seconds:
        return {
            "status": "blocked",
            "note": "Open execution is blocked because the quote snapshot is stale.",
            "reason_codes": ["stale_quote_snapshot"],
            "blockers": ["stale_quote_snapshot"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if session_metrics["active_open_position_count"] >= int(normalized_policy["max_open_positions_per_session"]):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_open_positions_per_session.",
            "reason_codes": ["max_open_positions_per_session_exceeded"],
            "blockers": ["max_open_positions_per_session_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if len(matching_underlyings) + len(matching_pending_underlyings) >= int(normalized_policy["max_open_positions_per_underlying"]):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_open_positions_per_underlying.",
            "reason_codes": ["max_open_positions_per_underlying_exceeded"],
            "blockers": ["max_open_positions_per_underlying_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if len(matching_strategy) + len(matching_pending_strategy) >= int(normalized_policy["max_open_positions_per_underlying_strategy"]):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_open_positions_per_underlying_strategy.",
            "reason_codes": ["max_open_positions_per_underlying_strategy_exceeded"],
            "blockers": ["max_open_positions_per_underlying_strategy_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if open_contracts + quantity > float(normalized_policy["max_contracts_per_session"]):
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_contracts_per_session.",
            "reason_codes": ["max_contracts_per_session_exceeded"],
            "blockers": ["max_contracts_per_session_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    max_position_notional = coerce_float(normalized_policy.get("max_position_notional"))
    if position_notional is not None and max_position_notional is not None and position_notional > max_position_notional:
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_position_notional.",
            "reason_codes": ["max_position_notional_exceeded"],
            "blockers": ["max_position_notional_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    max_session_notional = coerce_float(normalized_policy.get("max_session_notional"))
    if position_notional is not None and max_session_notional is not None and session_notional + position_notional > max_session_notional:
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_session_notional.",
            "reason_codes": ["max_session_notional_exceeded"],
            "blockers": ["max_session_notional_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    max_position_max_loss = coerce_float(normalized_policy.get("max_position_max_loss"))
    if position_max_loss is not None and max_position_max_loss is not None and position_max_loss > max_position_max_loss:
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_position_max_loss.",
            "reason_codes": ["max_position_max_loss_exceeded"],
            "blockers": ["max_position_max_loss_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    if (
        strategy_risk_budget is not None
        and position_size_pct_of_available_balance is None
        and strategy_supports_position_sizing(strategy)
        and position_max_loss is not None
        and position_max_loss > strategy_risk_budget
    ):
        return {
            "status": "blocked",
            "note": "Open execution exceeds strategy max_risk_per_trade.",
            "reason_codes": ["strategy_risk_budget_exceeded"],
            "blockers": ["strategy_risk_budget_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    recommended_quantity = coerce_int(sizing.get("recommended_quantity"))
    limiting_constraint = as_text(sizing.get("limiting_constraint"))
    if recommended_quantity is not None and recommended_quantity >= 0 and quantity > recommended_quantity:
        if limiting_constraint == "position_size_pct_of_available_balance":
            return {
                "status": "blocked",
                "note": ("Open execution exceeds the configured position-size budget " "derived from available broker buying power."),
                "reason_codes": ["position_size_budget_exceeded"],
                "blockers": ["position_size_budget_exceeded"],
                "policy": normalized_policy,
                "metrics": metrics,
            }
        if limiting_constraint == "max_risk_per_trade":
            return {
                "status": "blocked",
                "note": "Open execution exceeds strategy max_risk_per_trade.",
                "reason_codes": ["strategy_risk_budget_exceeded"],
                "blockers": ["strategy_risk_budget_exceeded"],
                "policy": normalized_policy,
                "metrics": metrics,
            }

    available_broker_buying_power = coerce_float(sizing.get("available_broker_buying_power"))
    if required_buying_power is not None and available_broker_buying_power is not None and required_buying_power > available_broker_buying_power:
        source_field = as_text(sizing.get("broker_buying_power_source_field"))
        source_note = "" if source_field is None else f" from {source_field}"
        return {
            "status": "blocked",
            "note": (
                "Open execution exceeds available broker buying power"
                f"{source_note} (requires {required_buying_power:.2f}, "
                f"available {available_broker_buying_power:.2f})."
            ),
            "reason_codes": ["insufficient_broker_buying_power"],
            "blockers": ["insufficient_broker_buying_power"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    max_session_max_loss = coerce_float(normalized_policy.get("max_session_max_loss"))
    if position_max_loss is not None and max_session_max_loss is not None and session_max_loss + position_max_loss > max_session_max_loss:
        return {
            "status": "blocked",
            "note": "Open execution exceeds max_session_max_loss.",
            "reason_codes": ["max_session_max_loss_exceeded"],
            "blockers": ["max_session_max_loss_exceeded"],
            "policy": normalized_policy,
            "metrics": metrics,
        }

    return {
        "status": "approved",
        "note": "Open execution is approved under the current risk policy.",
        "reason_codes": ["approved"],
        "blockers": [],
        "policy": normalized_policy,
        "metrics": metrics,
    }


def validate_open_execution(
    *,
    execution_store: Any,
    session_id: str,
    candidate: dict[str, Any],
    cycle: dict[str, Any],
    quantity: int,
    limit_price: float | None,
    risk_policy: dict[str, Any] | None,
    execution_policy: dict[str, Any] | None = None,
    strategy_risk_budget: float | None = None,
    position_size_pct_of_available_balance: float | None = None,
) -> dict[str, Any]:
    decision = evaluate_open_execution(
        execution_store=execution_store,
        session_id=session_id,
        candidate=candidate,
        cycle=cycle,
        quantity=quantity,
        limit_price=limit_price,
        risk_policy=risk_policy,
        execution_policy=execution_policy,
        strategy_risk_budget=strategy_risk_budget,
        position_size_pct_of_available_balance=position_size_pct_of_available_balance,
    )
    if decision["status"] in {"blocked", "unknown"}:
        raise ValueError(str(decision["note"]))
    return dict(decision["policy"])


def validate_close_execution(
    *,
    position: dict[str, Any],
    quantity: int,
    limit_price: float | None = None,
    now: datetime | None = None,
    max_reconciliation_age_seconds: float | None = None,
) -> dict[str, Any]:
    position_status = str(position.get("position_status") or position.get("status") or "").lower()
    if position_status and position_status not in OPEN_POSITION_STATUSES:
        raise ValueError("Position is already closed.")
    remaining_quantity = coerce_float(position.get("remaining_quantity"))
    if remaining_quantity is None or remaining_quantity <= 0:
        raise ValueError("Position does not have remaining quantity to close.")
    if quantity <= 0:
        raise ValueError("Close quantity must be positive.")
    if quantity > remaining_quantity:
        raise ValueError("Close quantity exceeds the remaining position quantity.")
    if limit_price is not None and limit_price <= 0:
        raise ValueError("Close execution requires a positive limit price.")
    if not position_legs(position):
        raise ValueError("Position is missing the broker symbols required to close.")
    if max_reconciliation_age_seconds is not None:
        reconciliation_status = as_text(position.get("reconciliation_status"))
        if reconciliation_status != "matched":
            raise ValueError("Position broker reconciliation is not matched; " "wait for broker sync before closing.")
        last_reconciled_at = parse_datetime(as_text(position.get("last_reconciled_at")))
        if last_reconciled_at is None:
            raise ValueError("Position broker reconciliation is missing; " "wait for broker sync before closing.")
        reconciliation_age = ((now or datetime.now(UTC)) - last_reconciled_at.astimezone(UTC)).total_seconds()
        if reconciliation_age > max_reconciliation_age_seconds:
            raise ValueError("Position broker reconciliation is stale; " "wait for broker sync before closing.")
    return {
        "status": "ok",
    }
