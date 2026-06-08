from __future__ import annotations

import argparse
import math
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from statistics import fmean
from typing import Any, Mapping

from core.common import clamp
from core.domain.models import OptionSnapshot, SpreadCandidate
from core.services.option_structures import (
    normalize_legs,
    signed_net_limit_price,
)
from core.services.strategy_candidate_builders.runtime_context import candidate_reference_datetime
from core.services.strategy_candidate_builders.runtime_context import option_expiry_close

_LATTICE_STEPS = 31


@dataclass(frozen=True)
class StructureAnalytics:
    average_implied_volatility: float | None = None
    model_implied_volatility: float | None = None
    net_delta: float | None = None
    net_gamma: float | None = None
    net_theta: float | None = None
    net_vega: float | None = None
    probability_of_profit: float | None = None
    breakeven_touch_probability: float | None = None
    expected_value_dollars: float | None = None
    slippage_adjusted_expected_value_dollars: float | None = None
    entry_slippage_dollars: float | None = None


def _leg_position_sign(leg: Mapping[str, Any]) -> float | None:
    role = str(leg.get("role") or "").strip().lower()
    if role == "long":
        return 1.0
    if role == "short":
        return -1.0
    position_intent = str(leg.get("position_intent") or "").strip().lower()
    if position_intent in {"buy_to_open", "sell_to_close"}:
        return 1.0
    if position_intent in {"sell_to_open", "buy_to_close"}:
        return -1.0
    return None


def _leg_ratio_qty(leg: Mapping[str, Any]) -> float:
    raw_value = leg.get("ratio_qty")
    if raw_value in (None, ""):
        return 1.0
    try:
        return abs(float(raw_value))
    except (TypeError, ValueError):
        return 1.0


def _average_implied_volatility(
    *,
    legs: list[dict[str, Any]],
    snapshots_by_symbol: Mapping[str, OptionSnapshot],
) -> float | None:
    values = [
        float(snapshot.implied_volatility)
        for leg in legs
        if (snapshot := snapshots_by_symbol.get(str(leg.get("symbol") or ""))) is not None and snapshot.implied_volatility is not None
    ]
    if not values:
        return None
    return round(fmean(values), 4)


def _model_implied_volatility(
    *,
    legs: list[dict[str, Any]],
    snapshots_by_symbol: Mapping[str, OptionSnapshot],
    fallback_average_implied_volatility: float | None,
) -> float | None:
    weighted_total = 0.0
    total_weight = 0.0
    for leg in legs:
        snapshot = snapshots_by_symbol.get(str(leg.get("symbol") or ""))
        if snapshot is None or snapshot.implied_volatility is None:
            continue
        vega = snapshot.vega
        if vega is None:
            continue
        weight = abs(float(vega)) * _leg_ratio_qty(leg)
        if weight <= 0:
            continue
        weighted_total += float(snapshot.implied_volatility) * weight
        total_weight += weight
    if total_weight > 0:
        return round(weighted_total / total_weight, 4)
    return fallback_average_implied_volatility


def _net_greek(
    *,
    greek_name: str,
    legs: list[dict[str, Any]],
    snapshots_by_symbol: Mapping[str, OptionSnapshot],
) -> float | None:
    total = 0.0
    resolved = False
    for leg in legs:
        symbol = str(leg.get("symbol") or "")
        snapshot = snapshots_by_symbol.get(symbol)
        sign = _leg_position_sign(leg)
        if snapshot is None or sign is None:
            continue
        greek_value = getattr(snapshot, greek_name)
        if greek_value is None:
            continue
        total += sign * _leg_ratio_qty(leg) * float(greek_value)
        resolved = True
    if not resolved:
        return None
    return round(total, 4)


def _time_to_expiry_years(
    *,
    expiration_date: str,
    args: argparse.Namespace,
) -> float:
    reference_at = candidate_reference_datetime(args) or datetime.now(UTC)
    expiry_at = option_expiry_close(expiration_date)
    total_seconds = max((expiry_at - reference_at).total_seconds(), 0.0)
    return total_seconds / (365.0 * 24.0 * 60.0 * 60.0)


def _terminal_position_value(
    *,
    legs: list[dict[str, Any]],
    terminal_spot: float,
) -> float:
    total = 0.0
    for leg in legs:
        option_type = str(leg.get("option_type") or "").strip().lower()
        strike = leg.get("strike")
        sign = _leg_position_sign(leg)
        if option_type not in {"call", "put"} or strike in (None, "") or sign is None:
            continue
        strike_value = float(strike)
        intrinsic = max(terminal_spot - strike_value, 0.0) if option_type == "call" else max(strike_value - terminal_spot, 0.0)
        total += sign * _leg_ratio_qty(leg) * intrinsic
    return total


def _candidate_barriers(candidate: SpreadCandidate) -> tuple[float | None, float | None]:
    lower = None if candidate.lower_breakeven in (None, "") else float(candidate.lower_breakeven)
    upper = None if candidate.upper_breakeven in (None, "") else float(candidate.upper_breakeven)
    return lower, upper


def _touches_barrier(
    *,
    spot_price: float,
    lower_barrier: float | None,
    upper_barrier: float | None,
) -> bool:
    return (lower_barrier is not None and spot_price <= lower_barrier) or (upper_barrier is not None and spot_price >= upper_barrier)


def _lattice_spot_price(
    *,
    initial_spot: float,
    up_factor: float,
    step: int,
    up_moves: int,
) -> float:
    return initial_spot * math.pow(up_factor, (2 * up_moves) - step)


def _structure_lattice_metrics(
    *,
    candidate: SpreadCandidate,
    legs: list[dict[str, Any]],
    implied_volatility: float,
    years_to_expiry: float,
) -> tuple[float, float, float, float | None]:
    lower_barrier, upper_barrier = _candidate_barriers(candidate)
    if float(candidate.underlying_price) <= 0 or implied_volatility <= 0 or years_to_expiry <= 0:
        return (0.0, 0.0, 0.0, None)

    midpoint_entry_cost = signed_net_limit_price(
        limit_price=float(candidate.midpoint_credit),
        strategy_family=candidate.strategy,
        trade_intent="open",
    )
    natural_entry_cost = signed_net_limit_price(
        limit_price=float(candidate.natural_credit),
        strategy_family=candidate.strategy,
        trade_intent="open",
    )

    step_count = _LATTICE_STEPS
    dt = years_to_expiry / float(step_count)
    sigma_sqrt_dt = implied_volatility * math.sqrt(dt)
    if sigma_sqrt_dt <= 0:
        return (0.0, 0.0, 0.0, None)
    up_factor = math.exp(sigma_sqrt_dt)
    down_factor = 1.0 / up_factor
    denominator = up_factor - down_factor
    if math.isclose(denominator, 0.0, abs_tol=1e-12):
        return (0.0, 0.0, 0.0, None)
    up_probability = clamp((1.0 - down_factor) / denominator, 0.0, 1.0)
    down_probability = 1.0 - up_probability

    total_probabilities = [1.0]
    untouched_probabilities = [
        (
            0.0
            if _touches_barrier(
                spot_price=float(candidate.underlying_price),
                lower_barrier=lower_barrier,
                upper_barrier=upper_barrier,
            )
            else 1.0
        )
    ]
    for step in range(1, step_count + 1):
        next_total = [0.0] * (step + 1)
        next_untouched = [0.0] * (step + 1)
        for previous_up_moves, total_probability in enumerate(total_probabilities):
            if total_probability > 0:
                next_total[previous_up_moves] += total_probability * down_probability
                next_total[previous_up_moves + 1] += total_probability * up_probability

            untouched_probability = untouched_probabilities[previous_up_moves]
            if untouched_probability <= 0:
                continue

            down_spot = _lattice_spot_price(
                initial_spot=float(candidate.underlying_price),
                up_factor=up_factor,
                step=step,
                up_moves=previous_up_moves,
            )
            if not _touches_barrier(
                spot_price=down_spot,
                lower_barrier=lower_barrier,
                upper_barrier=upper_barrier,
            ):
                next_untouched[previous_up_moves] += untouched_probability * down_probability

            up_spot = _lattice_spot_price(
                initial_spot=float(candidate.underlying_price),
                up_factor=up_factor,
                step=step,
                up_moves=previous_up_moves + 1,
            )
            if not _touches_barrier(
                spot_price=up_spot,
                lower_barrier=lower_barrier,
                upper_barrier=upper_barrier,
            ):
                next_untouched[previous_up_moves + 1] += untouched_probability * up_probability

        total_probabilities = next_total
        untouched_probabilities = next_untouched

    probability_of_profit = 0.0
    expected_value_dollars = 0.0
    slippage_adjusted_expected_value_dollars = 0.0
    for up_moves, terminal_probability in enumerate(total_probabilities):
        if terminal_probability <= 0:
            continue
        terminal_spot = _lattice_spot_price(
            initial_spot=float(candidate.underlying_price),
            up_factor=up_factor,
            step=step_count,
            up_moves=up_moves,
        )
        terminal_value = _terminal_position_value(
            legs=legs,
            terminal_spot=terminal_spot,
        )
        midpoint_pnl = (terminal_value - midpoint_entry_cost) * 100.0
        natural_pnl = (terminal_value - natural_entry_cost) * 100.0
        if midpoint_pnl > 0:
            probability_of_profit += terminal_probability
        expected_value_dollars += midpoint_pnl * terminal_probability
        slippage_adjusted_expected_value_dollars += natural_pnl * terminal_probability

    touch_probability = None
    if lower_barrier is not None or upper_barrier is not None:
        touch_probability = round(
            clamp(1.0 - sum(untouched_probabilities), 0.0, 1.0),
            4,
        )

    return (
        round(clamp(probability_of_profit, 0.0, 1.0), 4),
        round(expected_value_dollars, 2),
        round(slippage_adjusted_expected_value_dollars, 2),
        touch_probability,
    )


def _entry_slippage_dollars(
    *,
    candidate: SpreadCandidate,
) -> float:
    midpoint_entry_cost = signed_net_limit_price(
        limit_price=float(candidate.midpoint_credit),
        strategy_family=candidate.strategy,
        trade_intent="open",
    )
    natural_entry_cost = signed_net_limit_price(
        limit_price=float(candidate.natural_credit),
        strategy_family=candidate.strategy,
        trade_intent="open",
    )
    return round(abs(natural_entry_cost - midpoint_entry_cost) * 100.0, 2)


def build_structure_analytics(
    candidate: SpreadCandidate,
    *,
    snapshots_by_symbol: Mapping[str, OptionSnapshot],
    args: argparse.Namespace,
) -> StructureAnalytics:
    normalized_legs = normalize_legs(
        candidate.legs,
        expiration_date=candidate.expiration_date,
    )
    if not normalized_legs:
        return StructureAnalytics()

    average_implied_volatility = _average_implied_volatility(
        legs=normalized_legs,
        snapshots_by_symbol=snapshots_by_symbol,
    )
    model_implied_volatility = _model_implied_volatility(
        legs=normalized_legs,
        snapshots_by_symbol=snapshots_by_symbol,
        fallback_average_implied_volatility=average_implied_volatility,
    )
    net_delta = _net_greek(
        greek_name="delta",
        legs=normalized_legs,
        snapshots_by_symbol=snapshots_by_symbol,
    )
    net_gamma = _net_greek(
        greek_name="gamma",
        legs=normalized_legs,
        snapshots_by_symbol=snapshots_by_symbol,
    )
    net_theta = _net_greek(
        greek_name="theta",
        legs=normalized_legs,
        snapshots_by_symbol=snapshots_by_symbol,
    )
    net_vega = _net_greek(
        greek_name="vega",
        legs=normalized_legs,
        snapshots_by_symbol=snapshots_by_symbol,
    )

    probability_of_profit = None
    breakeven_touch_probability = None
    expected_value_dollars = None
    slippage_adjusted_expected_value_dollars = None
    years_to_expiry = _time_to_expiry_years(
        expiration_date=candidate.expiration_date,
        args=args,
    )
    if model_implied_volatility is not None and model_implied_volatility > 0 and years_to_expiry > 0:
        (
            probability_of_profit,
            expected_value_dollars,
            slippage_adjusted_expected_value_dollars,
            breakeven_touch_probability,
        ) = _structure_lattice_metrics(
            candidate=candidate,
            legs=normalized_legs,
            implied_volatility=model_implied_volatility,
            years_to_expiry=years_to_expiry,
        )
    entry_slippage_dollars = _entry_slippage_dollars(candidate=candidate)

    return StructureAnalytics(
        average_implied_volatility=average_implied_volatility,
        model_implied_volatility=model_implied_volatility,
        net_delta=net_delta,
        net_gamma=net_gamma,
        net_theta=net_theta,
        net_vega=net_vega,
        probability_of_profit=probability_of_profit,
        breakeven_touch_probability=breakeven_touch_probability,
        expected_value_dollars=expected_value_dollars,
        slippage_adjusted_expected_value_dollars=slippage_adjusted_expected_value_dollars,
        entry_slippage_dollars=entry_slippage_dollars,
    )


def attach_structure_analytics(
    candidate: SpreadCandidate,
    *,
    snapshots_by_symbol: Mapping[str, OptionSnapshot],
    args: argparse.Namespace,
) -> SpreadCandidate:
    analytics = build_structure_analytics(
        candidate,
        snapshots_by_symbol=snapshots_by_symbol,
        args=args,
    )
    return replace(
        candidate,
        average_implied_volatility=analytics.average_implied_volatility,
        model_implied_volatility=analytics.model_implied_volatility,
        net_delta=analytics.net_delta,
        net_gamma=analytics.net_gamma,
        net_theta=analytics.net_theta,
        net_vega=analytics.net_vega,
        probability_of_profit=analytics.probability_of_profit,
        breakeven_touch_probability=analytics.breakeven_touch_probability,
        expected_value_dollars=analytics.expected_value_dollars,
        slippage_adjusted_expected_value_dollars=analytics.slippage_adjusted_expected_value_dollars,
        entry_slippage_dollars=analytics.entry_slippage_dollars,
    )


__all__ = [
    "StructureAnalytics",
    "attach_structure_analytics",
    "build_structure_analytics",
]
