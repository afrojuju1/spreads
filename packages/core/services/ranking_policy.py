from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any

from core.services.value_coercion import coerce_float as _coerce_float

RANKING_POLICY_THRESHOLD_FIELDS = (
    "min_probability_of_profit",
    "min_expected_value_dollars",
    "min_slippage_adjusted_expected_value_dollars",
    "max_entry_slippage_dollars",
    "min_model_implied_volatility",
    "max_model_implied_volatility",
)

RANKING_POLICY_WEIGHT_FIELDS = (
    "probability_of_profit",
    "expected_value_dollars",
    "slippage_adjusted_expected_value_dollars",
    "entry_slippage_dollars",
    "model_implied_volatility",
)

_DIRECT_THRESHOLD_KEYS = {field_name: f"ranking_{field_name}" for field_name in RANKING_POLICY_THRESHOLD_FIELDS}
_DIRECT_WEIGHT_KEYS = {field_name: f"ranking_weight_{field_name}" for field_name in RANKING_POLICY_WEIGHT_FIELDS}


def _read_value(source: Mapping[str, Any] | Any, key: str) -> Any:
    if isinstance(source, Mapping):
        return source.get(key)
    return getattr(source, key, None)


def resolve_ranking_policy_payload(source: Mapping[str, Any] | Any | None) -> dict[str, Any]:
    if source is None:
        return {}

    if isinstance(source, Mapping):
        nested = source.get("ranking_policy")
        if isinstance(nested, Mapping):
            source = nested

    payload: dict[str, Any] = {}
    for field_name in RANKING_POLICY_THRESHOLD_FIELDS:
        value = _coerce_float(
            _read_value(source, field_name)
            if _read_value(source, field_name) is not None
            else _read_value(source, _DIRECT_THRESHOLD_KEYS[field_name])
        )
        if value is not None:
            payload[field_name] = value

    weights: dict[str, float] = {}
    for field_name in RANKING_POLICY_WEIGHT_FIELDS:
        value = _coerce_float(
            _read_value(source, field_name) if _read_value(source, field_name) is not None else _read_value(source, _DIRECT_WEIGHT_KEYS[field_name])
        )
        if value is not None:
            weights[field_name] = value
    if weights:
        payload["weights"] = weights
    return payload


def evaluate_candidate_ranking_policy(
    candidate: Mapping[str, Any] | Any,
    *,
    policy_source: Mapping[str, Any] | Any | None,
) -> dict[str, Any]:
    policy = resolve_ranking_policy_payload(policy_source)
    blockers: list[str] = []
    margin_to_pass: dict[str, float | None] = {}

    def metric(metric_name: str) -> float | None:
        return _coerce_float(_read_value(candidate, metric_name))

    probability_of_profit = metric("probability_of_profit")
    minimum_probability_of_profit = _coerce_float(policy.get("min_probability_of_profit"))
    if minimum_probability_of_profit is not None:
        if probability_of_profit is None:
            blockers.append("probability_of_profit_missing")
            margin_to_pass["probability_of_profit"] = None
        elif probability_of_profit < minimum_probability_of_profit:
            blockers.append("probability_of_profit_below_floor")
            margin_to_pass["probability_of_profit"] = round(
                minimum_probability_of_profit - probability_of_profit,
                6,
            )
        else:
            margin_to_pass["probability_of_profit"] = 0.0

    expected_value_dollars = metric("expected_value_dollars")
    minimum_expected_value_dollars = _coerce_float(policy.get("min_expected_value_dollars"))
    if minimum_expected_value_dollars is not None:
        if expected_value_dollars is None:
            blockers.append("expected_value_dollars_missing")
            margin_to_pass["expected_value_dollars"] = None
        elif expected_value_dollars < minimum_expected_value_dollars:
            blockers.append("expected_value_dollars_below_floor")
            margin_to_pass["expected_value_dollars"] = round(
                minimum_expected_value_dollars - expected_value_dollars,
                6,
            )
        else:
            margin_to_pass["expected_value_dollars"] = 0.0

    slippage_adjusted_expected_value_dollars = metric("slippage_adjusted_expected_value_dollars")
    minimum_slippage_adjusted_expected_value_dollars = _coerce_float(policy.get("min_slippage_adjusted_expected_value_dollars"))
    if minimum_slippage_adjusted_expected_value_dollars is not None:
        if slippage_adjusted_expected_value_dollars is None:
            blockers.append("slippage_adjusted_expected_value_dollars_missing")
            margin_to_pass["slippage_adjusted_expected_value_dollars"] = None
        elif slippage_adjusted_expected_value_dollars < minimum_slippage_adjusted_expected_value_dollars:
            blockers.append("slippage_adjusted_expected_value_dollars_below_floor")
            margin_to_pass["slippage_adjusted_expected_value_dollars"] = round(
                minimum_slippage_adjusted_expected_value_dollars - slippage_adjusted_expected_value_dollars,
                6,
            )
        else:
            margin_to_pass["slippage_adjusted_expected_value_dollars"] = 0.0

    entry_slippage_dollars = metric("entry_slippage_dollars")
    maximum_entry_slippage_dollars = _coerce_float(policy.get("max_entry_slippage_dollars"))
    if maximum_entry_slippage_dollars is not None:
        if entry_slippage_dollars is None:
            blockers.append("entry_slippage_dollars_missing")
            margin_to_pass["entry_slippage_dollars"] = None
        elif entry_slippage_dollars > maximum_entry_slippage_dollars:
            blockers.append("entry_slippage_dollars_above_ceiling")
            margin_to_pass["entry_slippage_dollars"] = round(
                entry_slippage_dollars - maximum_entry_slippage_dollars,
                6,
            )
        else:
            margin_to_pass["entry_slippage_dollars"] = 0.0

    model_implied_volatility = metric("model_implied_volatility")
    minimum_model_implied_volatility = _coerce_float(policy.get("min_model_implied_volatility"))
    maximum_model_implied_volatility = _coerce_float(policy.get("max_model_implied_volatility"))
    if minimum_model_implied_volatility is not None or maximum_model_implied_volatility is not None:
        if model_implied_volatility is None:
            blockers.append("model_implied_volatility_missing")
            margin_to_pass["model_implied_volatility"] = None
        else:
            if minimum_model_implied_volatility is not None and model_implied_volatility < minimum_model_implied_volatility:
                blockers.append("model_implied_volatility_below_floor")
                margin_to_pass["model_implied_volatility"] = round(
                    minimum_model_implied_volatility - model_implied_volatility,
                    6,
                )
            if maximum_model_implied_volatility is not None and model_implied_volatility > maximum_model_implied_volatility:
                blockers.append("model_implied_volatility_above_ceiling")
                margin_to_pass["model_implied_volatility"] = round(
                    model_implied_volatility - maximum_model_implied_volatility,
                    6,
                )
            if "model_implied_volatility" not in margin_to_pass:
                margin_to_pass["model_implied_volatility"] = 0.0

    return {
        "policy": policy,
        "status": "blocked" if blockers else "passed",
        "blockers": tuple(blockers),
        "margin_to_pass": margin_to_pass,
    }


def build_strategy_ranking_policy_snapshot(
    entries: Iterable[tuple[str, Mapping[str, Any] | Any]],
) -> dict[str, Any]:
    by_strategy_family: dict[str, dict[str, Any]] = {}
    for strategy_family, source in entries:
        rendered = str(strategy_family or "").strip()
        if not rendered:
            continue
        policy = resolve_ranking_policy_payload(source)
        if policy:
            by_strategy_family[rendered] = policy
    if not by_strategy_family:
        return {}
    return {"by_strategy_family": dict(sorted(by_strategy_family.items()))}


def build_ranking_policy_gate_summary(
    *,
    items: Iterable[Mapping[str, Any] | Any],
    status_field: str = "ranking_policy_status",
    blockers_field: str = "ranking_policy_blockers",
) -> dict[str, Any]:
    status_counts: Counter[str] = Counter()
    blocker_counts: Counter[str] = Counter()
    for item in items:
        status = str(_read_value(item, status_field) or "").strip().lower()
        if status:
            status_counts[status] += 1
        blockers = _read_value(item, blockers_field)
        if not isinstance(blockers, (list, tuple)):
            continue
        for blocker in blockers:
            rendered = str(blocker or "").strip()
            if rendered:
                blocker_counts[rendered] += 1
    return {
        "status_counts": dict(sorted(status_counts.items())),
        "blocker_counts": dict(sorted(blocker_counts.items())),
    }


__all__ = [
    "RANKING_POLICY_THRESHOLD_FIELDS",
    "RANKING_POLICY_WEIGHT_FIELDS",
    "build_ranking_policy_gate_summary",
    "build_strategy_ranking_policy_snapshot",
    "evaluate_candidate_ranking_policy",
    "resolve_ranking_policy_payload",
]
