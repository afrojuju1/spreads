from __future__ import annotations

from typing import Any, Mapping

from core.value_coercion import coerce_float as _coerce_float, coerce_int as _coerce_int
from core.services.entry_recipes import evaluate_entry_recipes
from core.services.ranking_policy import evaluate_candidate_ranking_policy
from core.services.strategy_registry import resolve_strategy_definition


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

    normalized_symbols = sorted({str(symbol).upper() for symbol in list(symbols or []) if str(symbol or "").strip()})
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

    normalized_widths = sorted({round(float(value), 4) for value in list(allowed_widths or []) if value not in (None, "")})
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

    normalized_entry_recipe_refs = [str(recipe_ref) for recipe_ref in list(entry_recipe_refs or []) if str(recipe_ref or "").strip()]
    if normalized_entry_recipe_refs:
        payload["entry_recipe_refs"] = normalized_entry_recipe_refs

    return payload


def candidate_filter_reasons(
    candidate: Mapping[str, Any],
    candidate_filter: Mapping[str, Any] | None,
) -> list[str]:
    payload = dict(candidate_filter or {})
    reasons: list[str] = []

    strategy_id = str(payload.get("strategy_id") or "").strip()
    if strategy_id:
        strategy = resolve_strategy_definition(strategy_id)
        if not strategy.matches_candidate(dict(candidate)):
            reasons.append("strategy_family_mismatch")

    symbols = {str(symbol).upper() for symbol in list(payload.get("symbols") or []) if str(symbol or "").strip()}
    if symbols:
        underlying_symbol = str(candidate.get("underlying_symbol") or "").upper()
        if underlying_symbol not in symbols:
            reasons.append("symbol_out_of_scope")

    days_to_expiration = _coerce_int(candidate.get("days_to_expiration"))
    dte_min = _coerce_int(payload.get("dte_min"))
    dte_max = _coerce_int(payload.get("dte_max"))
    if dte_min is not None and days_to_expiration is not None:
        if days_to_expiration < dte_min:
            reasons.append("dte_below_min")
    if dte_max is not None and days_to_expiration is not None:
        if days_to_expiration > dte_max:
            reasons.append("dte_above_max")

    short_delta = _coerce_float(candidate.get("short_delta"))
    if short_delta is not None:
        short_delta = abs(short_delta)
        short_delta_min = _coerce_float(payload.get("short_delta_min"))
        short_delta_max = _coerce_float(payload.get("short_delta_max"))
        if short_delta_min is not None and short_delta < short_delta_min:
            reasons.append("short_delta_below_min")
        if short_delta_max is not None and short_delta > short_delta_max:
            reasons.append("short_delta_above_max")

    allowed_widths = {round(float(value), 4) for value in list(payload.get("allowed_widths") or []) if value not in (None, "")}
    if allowed_widths:
        width = candidate.get("width")
        if width in (None, ""):
            reasons.append("width_missing")
        elif round(float(width), 4) not in allowed_widths:
            reasons.append("width_not_allowed")

    min_open_interest = _coerce_int(payload.get("min_open_interest"))
    if min_open_interest is not None:
        short_oi = _coerce_int(candidate.get("short_open_interest")) or 0
        long_oi = _coerce_int(candidate.get("long_open_interest")) or 0
        if min(short_oi, long_oi) < min_open_interest:
            reasons.append("open_interest_below_floor")

    spread_ceiling = _coerce_float(
        payload.get("max_leg_spread_pct_mid") if payload.get("max_leg_spread_pct_mid") is not None else payload.get("max_relative_spread")
    )
    if spread_ceiling is not None:
        short_spread = _coerce_float(candidate.get("short_relative_spread")) or 0.0
        long_spread = _coerce_float(candidate.get("long_relative_spread")) or 0.0
        if max(short_spread, long_spread) > spread_ceiling:
            reasons.append("relative_spread_above_ceiling")

    minimum_return_on_risk = _coerce_float(payload.get("min_return_on_risk"))
    if minimum_return_on_risk is not None:
        return_on_risk = _coerce_float(candidate.get("return_on_risk"))
        if return_on_risk is None or return_on_risk < minimum_return_on_risk:
            reasons.append("return_on_risk_below_floor")

    ranking_policy = dict(payload.get("ranking_policy") or {})
    if ranking_policy:
        ranking_evaluation = evaluate_candidate_ranking_policy(
            candidate,
            policy_source=ranking_policy,
        )
        reasons.extend(list(ranking_evaluation["blockers"]))

    entry_recipe_refs = tuple(str(recipe_ref) for recipe_ref in list(payload.get("entry_recipe_refs") or []) if str(recipe_ref or "").strip())
    if entry_recipe_refs:
        recipe_result = evaluate_entry_recipes(dict(candidate), entry_recipe_refs)
        if not recipe_result.passed:
            reasons.extend(list(recipe_result.reason_codes))
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
