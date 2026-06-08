from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from core.value_coercion import coerce_float as _coerce_float, coerce_int as _coerce_int
from core.services.entry_recipes import evaluate_entry_recipes
from core.services.ranking_policy import evaluate_candidate_ranking_policy
from core.services.strategy_registry import resolve_strategy_definition


@dataclass(frozen=True)
class CandidateFilterContext:
    candidate: Mapping[str, Any]
    payload: Mapping[str, Any]


CandidateFilterRule = Callable[[CandidateFilterContext], list[str]]


def _normalized_symbols(value: Any) -> set[str]:
    return {str(symbol).upper() for symbol in list(value or []) if str(symbol or "").strip()}


def _normalized_widths(value: Any) -> set[float]:
    return {round(float(item), 4) for item in list(value or []) if item not in (None, "")}


def _normalized_entry_recipe_refs(value: Any) -> tuple[str, ...]:
    return tuple(str(recipe_ref) for recipe_ref in list(value or []) if str(recipe_ref or "").strip())


def build_candidate_filter(
    *,
    strategy_id: str | None = None,
    symbols: list[str] | tuple[str, ...] | None = None,
    dte_min: int | None = None,
    dte_max: int | None = None,
    short_delta_min: float | None = None,
    short_delta_max: float | None = None,
    allowed_widths: list[float] | tuple[float, ...] | None = None,
    min_open_interest: int | None = None,
    max_leg_spread_pct_mid: float | None = None,
    min_return_on_risk: float | None = None,
    ranking_policy: Mapping[str, Any] | None = None,
    entry_recipe_refs: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    normalized_strategy_id = str(strategy_id or "").strip()
    if normalized_strategy_id:
        payload["strategy_id"] = normalized_strategy_id

    normalized_symbols = sorted(_normalized_symbols(symbols))
    if normalized_symbols:
        payload["symbols"] = normalized_symbols

    if dte_min is not None:
        payload["dte_min"] = int(dte_min)
    if dte_max is not None:
        payload["dte_max"] = int(dte_max)
    if short_delta_min is not None:
        payload["short_delta_min"] = float(short_delta_min)
    if short_delta_max is not None:
        payload["short_delta_max"] = float(short_delta_max)

    normalized_widths = sorted(_normalized_widths(allowed_widths))
    if normalized_widths:
        payload["allowed_widths"] = normalized_widths

    if min_open_interest is not None:
        payload["min_open_interest"] = int(min_open_interest)
    if max_leg_spread_pct_mid is not None:
        payload["max_leg_spread_pct_mid"] = float(max_leg_spread_pct_mid)
    if min_return_on_risk is not None:
        payload["min_return_on_risk"] = float(min_return_on_risk)

    normalized_ranking_policy = dict(ranking_policy or {})
    if normalized_ranking_policy:
        payload["ranking_policy"] = normalized_ranking_policy

    normalized_entry_recipe_refs = list(_normalized_entry_recipe_refs(entry_recipe_refs))
    if normalized_entry_recipe_refs:
        payload["entry_recipe_refs"] = normalized_entry_recipe_refs

    return payload


def _strategy_family_reasons(context: CandidateFilterContext) -> list[str]:
    payload = context.payload
    candidate = context.candidate
    reasons: list[str] = []
    strategy_id = str(payload.get("strategy_id") or "").strip()
    if strategy_id:
        strategy = resolve_strategy_definition(strategy_id)
        if not strategy.matches_candidate(dict(candidate)):
            reasons.append("strategy_family_mismatch")
    return reasons


def _symbol_scope_reasons(context: CandidateFilterContext) -> list[str]:
    symbols = _normalized_symbols(context.payload.get("symbols"))
    if symbols:
        underlying_symbol = str(context.candidate.get("underlying_symbol") or "").upper()
        if underlying_symbol not in symbols:
            return ["symbol_out_of_scope"]
    return []


def _dte_reasons(context: CandidateFilterContext) -> list[str]:
    candidate = context.candidate
    payload = context.payload
    reasons: list[str] = []
    days_to_expiration = _coerce_int(candidate.get("days_to_expiration"))
    dte_min = _coerce_int(payload.get("dte_min"))
    dte_max = _coerce_int(payload.get("dte_max"))
    if dte_min is not None and days_to_expiration is not None:
        if days_to_expiration < dte_min:
            reasons.append("dte_below_min")
    if dte_max is not None and days_to_expiration is not None:
        if days_to_expiration > dte_max:
            reasons.append("dte_above_max")
    return reasons


def _short_delta_reasons(context: CandidateFilterContext) -> list[str]:
    candidate = context.candidate
    payload = context.payload
    reasons: list[str] = []
    short_delta = _coerce_float(candidate.get("short_delta"))
    if short_delta is not None:
        short_delta = abs(short_delta)
        short_delta_min = _coerce_float(payload.get("short_delta_min"))
        short_delta_max = _coerce_float(payload.get("short_delta_max"))
        if short_delta_min is not None and short_delta < short_delta_min:
            reasons.append("short_delta_below_min")
        if short_delta_max is not None and short_delta > short_delta_max:
            reasons.append("short_delta_above_max")
    return reasons


def _width_reasons(context: CandidateFilterContext) -> list[str]:
    candidate = context.candidate
    allowed_widths = _normalized_widths(context.payload.get("allowed_widths"))
    if allowed_widths:
        width = candidate.get("width")
        if width in (None, ""):
            return ["width_missing"]
        elif round(float(width), 4) not in allowed_widths:
            return ["width_not_allowed"]
    return []


def _open_interest_reasons(context: CandidateFilterContext) -> list[str]:
    candidate = context.candidate
    min_open_interest = _coerce_int(context.payload.get("min_open_interest"))
    if min_open_interest is not None:
        short_oi = _coerce_int(candidate.get("short_open_interest")) or 0
        long_oi = _coerce_int(candidate.get("long_open_interest")) or 0
        if min(short_oi, long_oi) < min_open_interest:
            return ["open_interest_below_floor"]
    return []


def _relative_spread_reasons(context: CandidateFilterContext) -> list[str]:
    candidate = context.candidate
    payload = context.payload
    spread_ceiling = _coerce_float(
        payload.get("max_leg_spread_pct_mid") if payload.get("max_leg_spread_pct_mid") is not None else payload.get("max_relative_spread")
    )
    if spread_ceiling is not None:
        short_spread = _coerce_float(candidate.get("short_relative_spread")) or 0.0
        long_spread = _coerce_float(candidate.get("long_relative_spread")) or 0.0
        if max(short_spread, long_spread) > spread_ceiling:
            return ["relative_spread_above_ceiling"]
    return []


def _return_on_risk_reasons(context: CandidateFilterContext) -> list[str]:
    minimum_return_on_risk = _coerce_float(context.payload.get("min_return_on_risk"))
    if minimum_return_on_risk is not None:
        return_on_risk = _coerce_float(context.candidate.get("return_on_risk"))
        if return_on_risk is None or return_on_risk < minimum_return_on_risk:
            return ["return_on_risk_below_floor"]
    return []


def _ranking_policy_reasons(context: CandidateFilterContext) -> list[str]:
    ranking_policy = dict(context.payload.get("ranking_policy") or {})
    if ranking_policy:
        ranking_evaluation = evaluate_candidate_ranking_policy(
            context.candidate,
            policy_source=ranking_policy,
        )
        return list(ranking_evaluation["blockers"])
    return []


def _entry_recipe_reasons(context: CandidateFilterContext) -> list[str]:
    entry_recipe_refs = _normalized_entry_recipe_refs(context.payload.get("entry_recipe_refs"))
    if entry_recipe_refs:
        recipe_result = evaluate_entry_recipes(dict(context.candidate), entry_recipe_refs)
        if not recipe_result.passed:
            return list(recipe_result.reason_codes)
    return []


CANDIDATE_FILTER_RULES: tuple[CandidateFilterRule, ...] = (
    _strategy_family_reasons,
    _symbol_scope_reasons,
    _dte_reasons,
    _short_delta_reasons,
    _width_reasons,
    _open_interest_reasons,
    _relative_spread_reasons,
    _return_on_risk_reasons,
    _ranking_policy_reasons,
    _entry_recipe_reasons,
)


def candidate_filter_reasons(
    candidate: Mapping[str, Any],
    candidate_filter: Mapping[str, Any] | None,
) -> list[str]:
    context = CandidateFilterContext(
        candidate=candidate,
        payload=dict(candidate_filter or {}),
    )
    reasons: list[str] = []
    for rule in CANDIDATE_FILTER_RULES:
        reasons.extend(rule(context))
    return reasons


def candidate_matches_filter(
    candidate: Mapping[str, Any],
    candidate_filter: Mapping[str, Any] | None,
) -> bool:
    return not candidate_filter_reasons(candidate, candidate_filter)


__all__ = [
    "build_candidate_filter",
    "candidate_filter_reasons",
    "candidate_matches_filter",
]
