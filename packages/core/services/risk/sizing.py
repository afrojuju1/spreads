from __future__ import annotations

from collections.abc import Mapping
from typing import Any


from core.money import money_scaled_float
from core.services.risk.buying_power import (
    estimate_buying_power_requirement,
)
from core.services.option_structures import (
    normalize_strategy_family,
)
from core.value_coercion import (
    as_text,
    coerce_float,
    coerce_int,
)

from core.services.risk.candidates import _candidate_entry_notional, _candidate_max_loss, _candidate_payload
from core.services.risk.policy import POSITION_SIZING_STRATEGIES, normalize_risk_policy
from core.services.risk.exposures import (
    _open_attempts,
    _open_positions,
    _pending_open_attempt_exposures,
    _session_open_metrics,
    live_broker_buying_power_snapshot,
)

def _max_contracts_for_budget(
    unit_exposure: float | None,
    budget: float | None,
) -> int | None:
    if unit_exposure is None or unit_exposure <= 0 or budget is None or budget < 0:
        return None
    return max(int(budget // unit_exposure), 0)


def resolve_position_size_policy(
    risk_defaults: Mapping[str, Any] | None,
) -> dict[str, float | None]:
    defaults = risk_defaults if isinstance(risk_defaults, Mapping) else {}
    return {
        "max_risk_per_trade": coerce_float(defaults.get("max_risk_per_trade")),
        "position_size_pct_of_available_balance": coerce_float(defaults.get("position_size_pct_of_available_balance")),
    }


def _position_size_budget(
    *,
    available_broker_buying_power: float | None,
    position_size_pct_of_available_balance: float | None,
) -> float | None:
    if (
        available_broker_buying_power is None
        or available_broker_buying_power < 0
        or position_size_pct_of_available_balance is None
        or position_size_pct_of_available_balance <= 0
    ):
        return None
    return round(
        max(available_broker_buying_power, 0.0) * float(position_size_pct_of_available_balance),
        2,
    )


def strategy_supports_position_sizing(strategy_family: Any) -> bool:
    normalized = normalize_strategy_family(strategy_family)
    return normalized in POSITION_SIZING_STRATEGIES


def build_candidate_position_sizing(
    *,
    candidate: dict[str, Any],
    limit_price: float | None,
    max_contracts_per_position: int | None = None,
    remaining_session_contracts: int | None = None,
    max_position_notional: float | None = None,
    remaining_session_notional: float | None = None,
    max_position_max_loss: float | None = None,
    remaining_session_max_loss: float | None = None,
    strategy_risk_budget: float | None = None,
    position_size_pct_of_available_balance: float | None = None,
    available_broker_buying_power: float | None = None,
) -> dict[str, Any]:
    candidate_payload = _candidate_payload(candidate)
    strategy_family = normalize_strategy_family(candidate_payload.get("strategy") or candidate.get("strategy") or candidate.get("strategy_family"))
    applies = strategy_supports_position_sizing(strategy_family)
    per_contract_entry_notional = _candidate_entry_notional(candidate, 1.0, limit_price)
    per_contract_max_loss = _candidate_max_loss(candidate, 1.0)
    buying_power_requirement = estimate_buying_power_requirement(
        candidate,
        1.0,
        limit_price=limit_price,
    )
    per_contract_required_buying_power = coerce_float(buying_power_requirement.get("required_buying_power"))
    position_size_budget = _position_size_budget(
        available_broker_buying_power=available_broker_buying_power,
        position_size_pct_of_available_balance=position_size_pct_of_available_balance,
    )
    effective_strategy_risk_budget = strategy_risk_budget
    if position_size_pct_of_available_balance is not None and available_broker_buying_power is not None:
        effective_strategy_risk_budget = None
    constraints: list[tuple[str, int]] = []

    def _append_constraint(name: str, value: int | None) -> None:
        if value is None:
            return
        constraints.append((name, max(int(value), 0)))

    _append_constraint("max_contracts_per_position", max_contracts_per_position)
    _append_constraint("max_contracts_per_session", remaining_session_contracts)
    _append_constraint(
        "max_position_notional",
        _max_contracts_for_budget(per_contract_entry_notional, max_position_notional),
    )
    _append_constraint(
        "max_session_notional",
        _max_contracts_for_budget(
            per_contract_entry_notional,
            remaining_session_notional,
        ),
    )
    _append_constraint(
        "max_position_max_loss",
        _max_contracts_for_budget(per_contract_max_loss, max_position_max_loss),
    )
    _append_constraint(
        "max_session_max_loss",
        _max_contracts_for_budget(
            per_contract_max_loss,
            remaining_session_max_loss,
        ),
    )
    _append_constraint(
        "position_size_pct_of_available_balance",
        _max_contracts_for_budget(
            per_contract_required_buying_power,
            position_size_budget,
        ),
    )
    _append_constraint(
        "max_risk_per_trade",
        _max_contracts_for_budget(
            per_contract_max_loss,
            effective_strategy_risk_budget,
        ),
    )
    _append_constraint(
        "available_broker_buying_power",
        _max_contracts_for_budget(
            per_contract_required_buying_power,
            available_broker_buying_power,
        ),
    )

    recommended_quantity = 1
    limiting_constraint = None
    effective_constraints = constraints
    if not applies:
        effective_constraints = [item for item in constraints if item[0] == "available_broker_buying_power"]
    if effective_constraints:
        limiting_constraint, recommended_quantity = min(
            effective_constraints,
            key=lambda item: (item[1], item[0]),
        )

    recommended_entry_notional = (
        None if per_contract_entry_notional is None else money_scaled_float(per_contract_entry_notional, recommended_quantity)
    )
    recommended_max_loss = None if per_contract_max_loss is None else money_scaled_float(per_contract_max_loss, recommended_quantity)
    return {
        "applies": applies,
        "strategy_family": strategy_family or None,
        "per_contract_entry_notional": per_contract_entry_notional,
        "per_contract_max_loss": per_contract_max_loss,
        "per_contract_required_buying_power": per_contract_required_buying_power,
        "buying_power_basis": as_text(buying_power_requirement.get("basis")),
        "position_size_pct_of_available_balance": (
            None if position_size_pct_of_available_balance is None else float(position_size_pct_of_available_balance)
        ),
        "position_size_budget": position_size_budget,
        "available_broker_buying_power": available_broker_buying_power,
        "constraints": {name: value for name, value in constraints},
        "effective_constraints": {name: value for name, value in effective_constraints},
        "limiting_constraint": limiting_constraint,
        "recommended_quantity": int(recommended_quantity),
        "recommended_entry_notional": recommended_entry_notional,
        "recommended_max_loss": recommended_max_loss,
    }


def _effective_max_contracts_per_position(
    *,
    candidate: dict[str, Any],
    normalized_policy: dict[str, Any],
) -> int | None:
    max_contracts_per_position = coerce_int(normalized_policy.get("max_contracts_per_position"))
    if max_contracts_per_position is None:
        return None
    if bool(normalized_policy.get("max_contracts_per_position_configured")):
        return max_contracts_per_position
    strategy_family = _candidate_payload(candidate).get("strategy") or candidate.get("strategy") or candidate.get("strategy_family")
    if strategy_supports_position_sizing(strategy_family):
        return None
    return max_contracts_per_position


def build_open_candidate_position_sizing(
    *,
    execution_store: Any,
    session_id: str,
    candidate: dict[str, Any],
    limit_price: float | None,
    risk_policy: dict[str, Any] | None,
    strategy_risk_budget: float | None = None,
    position_size_pct_of_available_balance: float | None = None,
) -> dict[str, Any]:
    normalized_policy = normalize_risk_policy(risk_policy)
    open_positions = _open_positions(execution_store, session_id=session_id)
    open_attempts = _open_attempts(execution_store, session_id=session_id)
    pending_attempts = _pending_open_attempt_exposures(open_attempts)
    session_metrics = _session_open_metrics(open_positions, pending_attempts)
    broker_buying_power = live_broker_buying_power_snapshot(execution_store)
    session_notional = session_metrics["active_entry_notional_total"]
    session_max_loss = session_metrics["active_max_loss_total"]
    open_contracts = session_metrics["active_open_contract_count"]
    sizing = build_candidate_position_sizing(
        candidate=candidate,
        limit_price=limit_price,
        max_contracts_per_position=_effective_max_contracts_per_position(
            candidate=candidate,
            normalized_policy=normalized_policy,
        ),
        remaining_session_contracts=max(
            int(normalized_policy["max_contracts_per_session"] - open_contracts),
            0,
        ),
        max_position_notional=coerce_float(normalized_policy.get("max_position_notional")),
        remaining_session_notional=(
            None
            if coerce_float(normalized_policy.get("max_session_notional")) is None
            else max(
                float(coerce_float(normalized_policy.get("max_session_notional")) or 0.0) - session_notional,
                0.0,
            )
        ),
        max_position_max_loss=coerce_float(normalized_policy.get("max_position_max_loss")),
        remaining_session_max_loss=(
            None
            if coerce_float(normalized_policy.get("max_session_max_loss")) is None
            else max(
                float(coerce_float(normalized_policy.get("max_session_max_loss")) or 0.0) - session_max_loss,
                0.0,
            )
        ),
        strategy_risk_budget=strategy_risk_budget,
        position_size_pct_of_available_balance=position_size_pct_of_available_balance,
        available_broker_buying_power=coerce_float(broker_buying_power.get("remaining_buying_power")),
    )
    return {
        **sizing,
        "broker_buying_power_status": as_text(broker_buying_power.get("status")),
        "broker_buying_power_source_field": as_text(broker_buying_power.get("source_field")),
        "broker_account_available_buying_power": coerce_float(broker_buying_power.get("available_buying_power")),
        "broker_reserved_buying_power": coerce_float(broker_buying_power.get("reserved_buying_power")),
        "broker_capacity_error_text": as_text(broker_buying_power.get("error_text")),
        "broker_reservation_count": coerce_int(broker_buying_power.get("reservation_count")),
        "broker_unsupported_reservation_count": coerce_int(broker_buying_power.get("unsupported_reservation_count")),
    }
