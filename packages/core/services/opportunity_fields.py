from __future__ import annotations

from typing import Any, Mapping

from core.services.option_structures import (
    candidate_legs,
    payload_display_fields,
    payload_structure_identity,
    primary_short_long_symbols,
)


_CANDIDATE_ECONOMICS_FIELDS = (
    "midpoint_credit",
    "natural_credit",
    "max_profit",
    "max_loss",
    "return_on_risk",
    "fill_ratio",
    "expected_value_dollars",
    "slippage_adjusted_expected_value_dollars",
    "entry_slippage_dollars",
)

_CANDIDATE_STRATEGY_METRIC_FIELDS = (
    "width",
    "short_strike",
    "long_strike",
    "breakeven",
    "lower_breakeven",
    "upper_breakeven",
    "breakeven_cushion_pct",
    "expected_move",
    "expected_move_pct",
    "expected_move_source_strike",
    "underlying_price",
    "side_balance_score",
    "wing_symmetry_ratio",
    "average_implied_volatility",
    "model_implied_volatility",
    "probability_of_profit",
    "breakeven_touch_probability",
    "net_delta",
    "net_gamma",
    "net_theta",
    "net_vega",
)

_CANDIDATE_EVIDENCE_METRIC_FIELDS = (
    "average_implied_volatility",
    "model_implied_volatility",
    "probability_of_profit",
    "breakeven_touch_probability",
    "expected_value_dollars",
    "slippage_adjusted_expected_value_dollars",
    "entry_slippage_dollars",
    "net_delta",
    "net_gamma",
    "net_theta",
    "net_vega",
)

_CANDIDATE_POLICY_CONTEXT_FIELDS = (
    "ranking_policy",
    "ranking_policy_status",
    "ranking_policy_blockers",
    "ranking_policy_margin_to_pass",
)

_CANDIDATE_RANKING_VECTOR_FIELDS = (
    "probability_of_profit",
    "breakeven_touch_probability",
    "expected_value_dollars",
    "slippage_adjusted_expected_value_dollars",
    "entry_slippage_dollars",
    "model_implied_volatility",
)

_RISK_HINT_FIELDS = (
    "midpoint_credit",
    "natural_credit",
    "max_loss",
    "return_on_risk",
    "fill_ratio",
    "width",
    "average_implied_volatility",
    "model_implied_volatility",
    "probability_of_profit",
    "breakeven_touch_probability",
    "expected_value_dollars",
    "slippage_adjusted_expected_value_dollars",
    "entry_slippage_dollars",
    "net_delta",
    "net_gamma",
    "net_theta",
    "net_vega",
)


def _candidate_slice(
    candidate: Mapping[str, Any], *, fields: tuple[str, ...]
) -> dict[str, Any]:
    return {field: candidate.get(field) for field in fields}


def candidate_economics(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return _candidate_slice(candidate, fields=_CANDIDATE_ECONOMICS_FIELDS)


def candidate_strategy_metrics(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return _candidate_slice(candidate, fields=_CANDIDATE_STRATEGY_METRIC_FIELDS)


def candidate_evidence_metrics(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return _candidate_slice(candidate, fields=_CANDIDATE_EVIDENCE_METRIC_FIELDS)


def candidate_policy_context(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return _candidate_slice(candidate, fields=_CANDIDATE_POLICY_CONTEXT_FIELDS)


def candidate_ranking_vector(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return _candidate_slice(candidate, fields=_CANDIDATE_RANKING_VECTOR_FIELDS)


def candidate_ranking_summary_row(
    candidate: Mapping[str, Any],
    *,
    default_symbol: Any = None,
) -> dict[str, Any]:
    legs = candidate_legs(candidate)
    short_symbol, long_symbol = primary_short_long_symbols(legs)
    display = payload_display_fields(candidate)
    return {
        "underlying_symbol": str(
            candidate.get("underlying_symbol") or default_symbol or "-"
        ),
        "strategy": str(candidate.get("strategy") or "unknown"),
        "expiration_date": candidate.get("expiration_date"),
        "short_symbol": display.get("short_symbol") or short_symbol,
        "long_symbol": display.get("long_symbol") or long_symbol,
        "symbol_path": display.get("symbol_path"),
        "structure_identity": payload_structure_identity(candidate),
        "quality_score": float(candidate.get("quality_score") or 0.0),
        "midpoint_credit": float(candidate.get("midpoint_credit") or 0.0),
        "return_on_risk": float(candidate.get("return_on_risk") or 0.0),
        "setup_status": candidate.get("setup_status"),
        **candidate_policy_context(candidate),
        **candidate_ranking_vector(candidate),
    }


def risk_hints(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return _candidate_slice(candidate, fields=_RISK_HINT_FIELDS)


__all__ = [
    "candidate_economics",
    "candidate_evidence_metrics",
    "candidate_policy_context",
    "candidate_ranking_summary_row",
    "candidate_ranking_vector",
    "candidate_strategy_metrics",
    "risk_hints",
]
