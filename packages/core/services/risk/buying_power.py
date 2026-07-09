from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.money import money_float, money_scaled_float, option_contract_notional
from core.services.option_structures import (
    candidate_legs,
    net_premium_kind,
    normalize_strategy_family,
    structure_barrier_strike,
    structure_width,
)
from core.value_coercion import coerce_float as _coerce_float

BUYING_POWER_SOURCE_FIELDS = (
    "options_buying_power",
    "non_marginable_buying_power",
    "buying_power",
    "cash",
    "equity",
)


def _candidate_payload(candidate: Mapping[str, Any] | dict[str, Any]) -> dict[str, Any]:
    payload = candidate.get("candidate")
    if isinstance(payload, Mapping):
        return dict(payload)
    if not isinstance(candidate, Mapping):
        return {}
    merged = dict(candidate)
    economics = candidate.get("economics")
    if isinstance(economics, Mapping):
        merged = {
            **merged,
            **dict(economics),
        }
    return merged


def _resolve_entry_price(
    payload: Mapping[str, Any],
    *,
    limit_price: float | None,
    premium_kind: str | None,
) -> float | None:
    if limit_price is not None and limit_price > 0:
        return float(limit_price)
    if premium_kind == "debit":
        for key in ("midpoint_debit", "natural_debit", "midpoint_value"):
            value = _coerce_float(payload.get(key))
            if value is not None and value > 0:
                return value
    for key in (
        "midpoint_credit",
        "natural_credit",
        "midpoint_debit",
        "natural_debit",
        "midpoint_value",
    ):
        value = _coerce_float(payload.get(key))
        if value is not None and value > 0:
            return value
    return None


def _resolve_max_loss_per_contract(
    payload: Mapping[str, Any],
    *,
    strategy_family: str,
    limit_price: float | None,
) -> float | None:
    max_loss = _coerce_float(payload.get("max_loss"))
    if max_loss is not None and max_loss > 0:
        return money_float(max_loss)

    premium_kind = net_premium_kind(strategy_family)
    entry_price = _resolve_entry_price(
        payload,
        limit_price=limit_price,
        premium_kind=premium_kind,
    )
    width = _coerce_float(payload.get("width"))
    if width is None:
        width = structure_width(
            candidate_legs(payload),
            strategy=strategy_family,
        )
    if width is not None and width > 0 and entry_price is not None and entry_price > 0:
        if premium_kind == "credit":
            return option_contract_notional(max(width - entry_price, 0.0), 1.0)
        if premium_kind == "debit":
            return option_contract_notional(entry_price, 1.0)
    if entry_price is not None and entry_price > 0:
        return option_contract_notional(entry_price, 1.0)
    return None


def _resolve_underlying_reference_price(payload: Mapping[str, Any]) -> float | None:
    for key in (
        "underlying_price",
        "underlying_last_price",
        "underlying_mark",
        "spot_price",
        "current_price",
    ):
        value = _coerce_float(payload.get(key))
        if value is not None and value > 0:
            return value
    return None


def estimate_buying_power_requirement(
    candidate: Mapping[str, Any] | dict[str, Any],
    quantity: float,
    limit_price: float | None = None,
) -> dict[str, Any]:
    resolved_quantity = max(float(quantity or 0.0), 0.0)
    if resolved_quantity <= 0:
        return {
            "required_buying_power": 0.0,
            "basis": None,
            "strategy_family": None,
        }

    payload = _candidate_payload(candidate)
    strategy_family = normalize_strategy_family(payload.get("strategy") or candidate.get("strategy") or candidate.get("strategy_family"))
    premium_kind = net_premium_kind(strategy_family)
    entry_price = _resolve_entry_price(
        payload,
        limit_price=limit_price,
        premium_kind=premium_kind,
    )
    max_loss_per_contract = _resolve_max_loss_per_contract(
        payload,
        strategy_family=strategy_family,
        limit_price=limit_price,
    )

    if strategy_family == "short_put":
        barrier_strike = structure_barrier_strike(
            candidate_legs(payload),
            strategy=strategy_family,
        )
        secured_requirement = None if barrier_strike is None or barrier_strike <= 0 else option_contract_notional(barrier_strike, resolved_quantity)
        max_loss_requirement = None if max_loss_per_contract is None else money_scaled_float(max_loss_per_contract, resolved_quantity)
        requirement = None
        if secured_requirement is not None:
            requirement = secured_requirement
        if max_loss_requirement is not None:
            requirement = max(requirement or 0.0, max_loss_requirement)
        return {
            "required_buying_power": requirement,
            "basis": "cash_secured_put",
            "strategy_family": strategy_family,
        }

    if strategy_family == "short_call":
        if max_loss_per_contract is not None:
            return {
                "required_buying_power": money_scaled_float(max_loss_per_contract, resolved_quantity),
                "basis": "short_call_max_loss",
                "strategy_family": strategy_family,
            }
        reference_price = _resolve_underlying_reference_price(payload)
        barrier_strike = structure_barrier_strike(
            candidate_legs(payload),
            strategy=strategy_family,
        )
        collateral_price = max(
            reference_price or 0.0,
            barrier_strike or 0.0,
        )
        requirement = None if collateral_price <= 0 else option_contract_notional(collateral_price, resolved_quantity)
        return {
            "required_buying_power": requirement,
            "basis": "short_call_conservative",
            "strategy_family": strategy_family,
        }

    if strategy_family in {
        "call_credit_spread",
        "put_credit_spread",
        "iron_condor",
    }:
        requirement = None if max_loss_per_contract is None else money_scaled_float(max_loss_per_contract, resolved_quantity)
        return {
            "required_buying_power": requirement,
            "basis": "defined_risk_max_loss",
            "strategy_family": strategy_family,
        }

    if strategy_family in {
        "call_debit_spread",
        "put_debit_spread",
        "long_call",
        "long_put",
        "long_straddle",
        "long_strangle",
    }:
        if entry_price is not None and entry_price > 0:
            return {
                "required_buying_power": option_contract_notional(entry_price, resolved_quantity),
                "basis": "net_debit",
                "strategy_family": strategy_family,
            }
        requirement = None if max_loss_per_contract is None else money_scaled_float(max_loss_per_contract, resolved_quantity)
        return {
            "required_buying_power": requirement,
            "basis": "max_loss",
            "strategy_family": strategy_family,
        }

    return {
        "required_buying_power": None,
        "basis": None,
        "strategy_family": strategy_family or None,
    }


def estimate_required_buying_power(
    candidate: Mapping[str, Any] | dict[str, Any],
    quantity: float,
    limit_price: float | None = None,
) -> float | None:
    requirement = estimate_buying_power_requirement(
        candidate,
        quantity,
        limit_price=limit_price,
    )
    return _coerce_float(requirement.get("required_buying_power"))


def resolve_available_buying_power(
    account_payload: Mapping[str, Any] | dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(account_payload, Mapping):
        return {
            "available_buying_power": None,
            "source_field": None,
        }
    for key in BUYING_POWER_SOURCE_FIELDS:
        value = _coerce_float(account_payload.get(key))
        if value is None:
            continue
        return {
            "available_buying_power": money_float(max(value, 0.0)),
            "source_field": key,
        }
    return {
        "available_buying_power": None,
        "source_field": None,
    }


__all__ = [
    "BUYING_POWER_SOURCE_FIELDS",
    "estimate_buying_power_requirement",
    "estimate_required_buying_power",
    "resolve_available_buying_power",
]
