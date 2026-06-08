from __future__ import annotations

from typing import Any

from core.services.option_structures import (
    build_order_payload,
    legs_identity_key,
    normalize_legs,
    normalize_position_intent,
)


def _opening_leg(contract: Any, *, role: str) -> dict[str, Any]:
    return {
        "symbol": str(contract.symbol),
        "side": "sell" if role == "short" else "buy",
        "position_intent": normalize_position_intent(
            "open",
            role=role,
            trade_intent="open",
        ),
        "ratio_qty": "1",
        "role": role,
        "expiration_date": str(contract.expiration_date),
        "strike": float(contract.strike_price),
    }


def _build_candidate_structure(
    *,
    strategy: str,
    legs: list[dict[str, Any]],
    limit_price: float,
) -> dict[str, Any]:
    normalized_legs = normalize_legs(legs)
    if not normalized_legs:
        raise ValueError("Candidate structure requires canonical legs")
    return {
        "legs": normalized_legs,
        "structure_identity": legs_identity_key(
            strategy=strategy,
            legs=normalized_legs,
        ),
        "order_payload": build_order_payload(
            legs=normalized_legs,
            limit_price=limit_price,
            strategy_family=strategy,
            trade_intent="open",
        ),
    }


def build_vertical_candidate_structure(
    *,
    strategy: str,
    short_contract: Any,
    long_contract: Any,
    limit_price: float,
) -> dict[str, Any]:
    return _build_candidate_structure(
        strategy=strategy,
        limit_price=limit_price,
        legs=[
            _opening_leg(short_contract, role="short"),
            _opening_leg(long_contract, role="long"),
        ],
    )


def build_iron_condor_candidate_structure(
    *,
    short_put_contract: Any,
    long_put_contract: Any,
    short_call_contract: Any,
    long_call_contract: Any,
    limit_price: float,
) -> dict[str, Any]:
    return _build_candidate_structure(
        strategy="iron_condor",
        limit_price=limit_price,
        legs=[
            _opening_leg(short_put_contract, role="short"),
            _opening_leg(long_put_contract, role="long"),
            _opening_leg(short_call_contract, role="short"),
            _opening_leg(long_call_contract, role="long"),
        ],
    )


def build_long_candidate_structure(
    *,
    strategy: str,
    contracts: list[Any],
    limit_price: float,
) -> dict[str, Any]:
    return _build_candidate_structure(
        strategy=strategy,
        limit_price=limit_price,
        legs=[_opening_leg(contract, role="long") for contract in contracts],
    )


def build_short_candidate_structure(
    *,
    strategy: str,
    contracts: list[Any],
    limit_price: float,
) -> dict[str, Any]:
    return _build_candidate_structure(
        strategy=strategy,
        limit_price=limit_price,
        legs=[_opening_leg(contract, role="short") for contract in contracts],
    )


__all__ = [
    "build_iron_condor_candidate_structure",
    "build_long_candidate_structure",
    "build_short_candidate_structure",
    "build_vertical_candidate_structure",
]
