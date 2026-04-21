from __future__ import annotations

from typing import Any, Mapping


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


def risk_hints(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return _candidate_slice(candidate, fields=_RISK_HINT_FIELDS)


__all__ = [
    "candidate_economics",
    "candidate_evidence_metrics",
    "candidate_policy_context",
    "candidate_strategy_metrics",
    "risk_hints",
]
