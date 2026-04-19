from __future__ import annotations

from collections import Counter
from typing import Any, Mapping

from core.services.automation_runtime import EntryRuntime
from core.services.entry_recipes import evaluate_entry_recipes
from core.services.strategy_registry import resolve_strategy_definition


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    numeric = _coerce_float(value)
    return None if numeric is None else int(numeric)


def match_runtime_candidate(
    candidate: Mapping[str, Any],
    runtime: EntryRuntime,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    strategy = resolve_strategy_definition(runtime.strategy_id)
    if not strategy.matches_candidate(dict(candidate)):
        reasons.append("strategy_family_mismatch")

    underlying_symbol = str(candidate.get("underlying_symbol") or "").upper()
    if runtime.symbols and underlying_symbol not in set(runtime.symbols):
        reasons.append("symbol_out_of_scope")

    days_to_expiration = _coerce_int(candidate.get("days_to_expiration"))
    if (
        runtime.build_settings.dte_min is not None
        and days_to_expiration is not None
        and days_to_expiration < runtime.build_settings.dte_min
    ):
        reasons.append("dte_below_min")
    if (
        runtime.build_settings.dte_max is not None
        and days_to_expiration is not None
        and days_to_expiration > runtime.build_settings.dte_max
    ):
        reasons.append("dte_above_max")

    short_delta = _coerce_float(candidate.get("short_delta"))
    if short_delta is not None:
        short_delta = abs(short_delta)
        if (
            runtime.build_settings.short_delta_min is not None
            and short_delta < runtime.build_settings.short_delta_min
        ):
            reasons.append("short_delta_below_min")
        if (
            runtime.build_settings.short_delta_max is not None
            and short_delta > runtime.build_settings.short_delta_max
        ):
            reasons.append("short_delta_above_max")

    width = _coerce_float(candidate.get("width"))
    if width is not None and runtime.build_settings.width_points:
        allowed_widths = {
            round(value, 4) for value in runtime.build_settings.width_points
        }
        if round(width, 4) not in allowed_widths:
            reasons.append("width_not_allowed")

    open_interest_floor = runtime.build_settings.min_open_interest
    if open_interest_floor is not None:
        short_oi = _coerce_int(candidate.get("short_open_interest")) or 0
        long_oi = _coerce_int(candidate.get("long_open_interest")) or 0
        if min(short_oi, long_oi) < open_interest_floor:
            reasons.append("open_interest_below_floor")

    spread_ceiling = runtime.build_settings.max_leg_spread_pct_mid
    if spread_ceiling is not None:
        short_spread = _coerce_float(candidate.get("short_relative_spread")) or 0.0
        long_spread = _coerce_float(candidate.get("long_relative_spread")) or 0.0
        if max(short_spread, long_spread) > spread_ceiling:
            reasons.append("relative_spread_above_ceiling")

    minimum_return_on_risk = runtime.build_settings.min_return_on_risk
    if minimum_return_on_risk is not None:
        return_on_risk = _coerce_float(candidate.get("return_on_risk"))
        if return_on_risk is None or return_on_risk < minimum_return_on_risk:
            reasons.append("return_on_risk_below_floor")

    recipe_result = evaluate_entry_recipes(
        dict(candidate),
        runtime.entry_recipe_refs,
    )
    if not recipe_result.passed:
        reasons.extend(recipe_result.reason_codes)

    return not reasons, reasons


def filter_runtime_candidate_rows(
    *,
    rows: list[Mapping[str, Any]],
    runtime: EntryRuntime,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    filtered_rows: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()

    for row in rows:
        candidate = dict(row)
        matched, reasons = match_runtime_candidate(candidate, runtime)
        if not matched:
            reason_counts.update(reasons)
            continue
        candidate["runtime_recipe_refs"] = list(runtime.entry_recipe_refs)
        filtered_rows.append(candidate)

    return filtered_rows, dict(sorted(reason_counts.items()))


def filter_runtime_symbol_candidates(
    *,
    symbol_candidates: Mapping[str, list[Mapping[str, Any]]],
    runtime: EntryRuntime,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
    filtered: dict[str, list[dict[str, Any]]] = {}
    reason_counts: Counter[str] = Counter()

    for symbol, rows in symbol_candidates.items():
        filtered_rows, row_reason_counts = filter_runtime_candidate_rows(
            rows=list(rows or []),
            runtime=runtime,
        )
        if filtered_rows:
            filtered[str(symbol)] = filtered_rows
        reason_counts.update(row_reason_counts)

    return filtered, dict(sorted(reason_counts.items()))


__all__ = [
    "filter_runtime_candidate_rows",
    "filter_runtime_symbol_candidates",
    "match_runtime_candidate",
]
