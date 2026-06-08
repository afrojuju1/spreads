from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from core.value_coercion import coerce_float as _coerce_float

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


@dataclass(frozen=True)
class NumericRankingGate:
    metric_name: str
    floor_key: str | None = None
    ceiling_key: str | None = None
    missing_blocker: str = ""
    below_floor_blocker: str | None = None
    above_ceiling_blocker: str | None = None


RANKING_POLICY_GATES = (
    NumericRankingGate(
        metric_name="probability_of_profit",
        floor_key="min_probability_of_profit",
        missing_blocker="probability_of_profit_missing",
        below_floor_blocker="probability_of_profit_below_floor",
    ),
    NumericRankingGate(
        metric_name="expected_value_dollars",
        floor_key="min_expected_value_dollars",
        missing_blocker="expected_value_dollars_missing",
        below_floor_blocker="expected_value_dollars_below_floor",
    ),
    NumericRankingGate(
        metric_name="slippage_adjusted_expected_value_dollars",
        floor_key="min_slippage_adjusted_expected_value_dollars",
        missing_blocker="slippage_adjusted_expected_value_dollars_missing",
        below_floor_blocker="slippage_adjusted_expected_value_dollars_below_floor",
    ),
    NumericRankingGate(
        metric_name="entry_slippage_dollars",
        ceiling_key="max_entry_slippage_dollars",
        missing_blocker="entry_slippage_dollars_missing",
        above_ceiling_blocker="entry_slippage_dollars_above_ceiling",
    ),
    NumericRankingGate(
        metric_name="model_implied_volatility",
        floor_key="min_model_implied_volatility",
        ceiling_key="max_model_implied_volatility",
        missing_blocker="model_implied_volatility_missing",
        below_floor_blocker="model_implied_volatility_below_floor",
        above_ceiling_blocker="model_implied_volatility_above_ceiling",
    ),
)


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


def _evaluate_numeric_ranking_gate(
    *,
    gate: NumericRankingGate,
    metric_value: float | None,
    policy: Mapping[str, Any],
    blockers: list[str],
    margin_to_pass: dict[str, float | None],
) -> None:
    floor = _coerce_float(policy.get(gate.floor_key)) if gate.floor_key is not None else None
    ceiling = _coerce_float(policy.get(gate.ceiling_key)) if gate.ceiling_key is not None else None
    if floor is None and ceiling is None:
        return
    if metric_value is None:
        blockers.append(gate.missing_blocker)
        margin_to_pass[gate.metric_name] = None
        return
    if floor is not None and metric_value < floor:
        if gate.below_floor_blocker is not None:
            blockers.append(gate.below_floor_blocker)
        margin_to_pass[gate.metric_name] = round(floor - metric_value, 6)
    if ceiling is not None and metric_value > ceiling:
        if gate.above_ceiling_blocker is not None:
            blockers.append(gate.above_ceiling_blocker)
        margin_to_pass[gate.metric_name] = round(metric_value - ceiling, 6)
    if gate.metric_name not in margin_to_pass:
        margin_to_pass[gate.metric_name] = 0.0


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

    for gate in RANKING_POLICY_GATES:
        _evaluate_numeric_ranking_gate(
            gate=gate,
            metric_value=metric(gate.metric_name),
            policy=policy,
            blockers=blockers,
            margin_to_pass=margin_to_pass,
        )

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
