from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.services.option_structures import candidate_legs, leg_role
from core.services.value_coercion import as_text as _as_text, coerce_float as _as_float


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _strategy_family(strategy: str | None) -> str:
    normalized = str(strategy or "").strip().lower()
    return {
        "call_credit": "call_credit_spread",
        "put_credit": "put_credit_spread",
        "call_debit": "call_debit_spread",
        "put_debit": "put_debit_spread",
        "long_straddle": "long_straddle",
        "long_strangle": "long_strangle",
    }.get(normalized, normalized or "unknown")


def _candidate_payload(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return dict(candidate)


def _leg_value(candidate: Mapping[str, Any], *, role_name: str, key: str) -> Any:
    return candidate.get(f"{role_name}_{key}")


def _candidate_quote_quality(candidate: Mapping[str, Any]) -> dict[str, Any]:
    normalized = _candidate_payload(candidate)
    explicit = normalized.get("candidate_quote_quality")
    if isinstance(explicit, Mapping):
        return dict(explicit)

    legs = candidate_legs(candidate)
    quality_components: list[float] = []
    quoted_leg_count = 0
    liquid_leg_count = 0
    min_open_interest: float | None = None
    min_volume: float | None = None
    min_quote_size: float | None = None
    average_relative_spread_values: list[float] = []

    for leg in legs:
        role_name = _as_text(leg.get("role")) or leg_role(
            side=leg.get("side"),
            position_intent=leg.get("position_intent"),
        )
        if role_name is None:
            continue
        bid = _as_float(_leg_value(normalized, role_name=role_name, key="bid"))
        ask = _as_float(_leg_value(normalized, role_name=role_name, key="ask"))
        midpoint = _as_float(_leg_value(normalized, role_name=role_name, key="midpoint"))
        relative_spread = _as_float(_leg_value(normalized, role_name=role_name, key="relative_spread"))
        if relative_spread is None and bid is not None and ask is not None and midpoint not in (None, 0.0):
            relative_spread = max(ask - bid, 0.0) / midpoint
        bid_size = _as_float(_leg_value(normalized, role_name=role_name, key="bid_size"))
        ask_size = _as_float(_leg_value(normalized, role_name=role_name, key="ask_size"))
        open_interest = _as_float(_leg_value(normalized, role_name=role_name, key="open_interest"))
        volume = _as_float(_leg_value(normalized, role_name=role_name, key="volume"))
        if midpoint not in (None, 0.0):
            quoted_leg_count += 1
        if relative_spread is not None:
            average_relative_spread_values.append(relative_spread)
            quality_components.append(_clamp((0.18 - relative_spread) / 0.18, 0.0, 1.0))
        if bid_size is not None and ask_size is not None:
            leg_min_size = min(bid_size, ask_size)
            min_quote_size = leg_min_size if min_quote_size is None else min(min_quote_size, leg_min_size)
            quality_components.append(_clamp(leg_min_size / 10.0, 0.0, 1.0))
            if leg_min_size >= 1.0:
                liquid_leg_count += 1
        if open_interest is not None:
            min_open_interest = open_interest if min_open_interest is None else min(min_open_interest, open_interest)
            quality_components.append(_clamp(open_interest / 500.0, 0.0, 1.0))
        if volume is not None:
            min_volume = volume if min_volume is None else min(min_volume, volume)
            quality_components.append(_clamp(volume / 100.0, 0.0, 1.0))

    quality_score = round(sum(quality_components) / float(len(quality_components)), 4) if quality_components else None
    average_relative_spread = (
        round(
            sum(average_relative_spread_values) / float(len(average_relative_spread_values)),
            4,
        )
        if average_relative_spread_values
        else None
    )
    quality_state = "missing"
    if quality_score is not None:
        if quality_score >= 0.8:
            quality_state = "strong"
        elif quality_score >= 0.55:
            quality_state = "acceptable"
        else:
            quality_state = "weak"
    return {
        "leg_count": len(legs),
        "quoted_leg_count": quoted_leg_count,
        "liquid_leg_count": liquid_leg_count,
        "quality_score": quality_score,
        "quality_state": quality_state,
        "average_relative_spread": average_relative_spread,
        "min_open_interest": None if min_open_interest is None else round(min_open_interest, 2),
        "min_volume": None if min_volume is None else round(min_volume, 2),
        "min_quote_size": None if min_quote_size is None else round(min_quote_size, 2),
    }


def build_earnings_signal_evidence(
    candidate: Mapping[str, Any],
    *,
    family: str | None = None,
) -> dict[str, Any]:
    payload = _candidate_payload(candidate)
    symbol = _as_text(payload.get("underlying_symbol"))
    resolved_family = family or _strategy_family(_as_text(payload.get("strategy")))
    return {
        "family": resolved_family,
        "symbol": None if symbol is None else symbol.upper(),
        "candidate_quote_quality": _candidate_quote_quality(payload),
        "setup_context": {
            "setup_status": _as_text(payload.get("setup_status")),
            "setup_score": _as_float(payload.get("setup_score")),
            "setup_intraday_score": _as_float(payload.get("setup_intraday_score")),
            "fill_ratio": _as_float(payload.get("fill_ratio")),
        },
    }
