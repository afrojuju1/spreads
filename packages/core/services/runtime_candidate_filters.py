from __future__ import annotations

from collections import Counter
from typing import Any, Mapping

from core.services.automation_runtime import EntryRuntime
from core.services.replay_filters import build_candidate_filter, candidate_filter_reasons


def match_runtime_candidate(
    candidate: Mapping[str, Any],
    runtime: EntryRuntime,
) -> tuple[bool, list[str]]:
    reasons = candidate_filter_reasons(candidate, _runtime_match_filter(runtime))
    return not reasons, reasons


def build_runtime_candidate_filter(runtime: EntryRuntime) -> dict[str, Any]:
    return build_candidate_filter(
        allowed_widths=runtime.build_settings.width_points,
        entry_recipe_refs=runtime.entry_recipe_refs,
    )


def _runtime_match_filter(runtime: EntryRuntime) -> dict[str, Any]:
    return build_candidate_filter(
        strategy_id=runtime.strategy_id,
        symbols=runtime.symbols,
        dte_min=runtime.build_settings.dte_min,
        dte_max=runtime.build_settings.dte_max,
        short_delta_min=runtime.build_settings.short_delta_min,
        short_delta_max=runtime.build_settings.short_delta_max,
        allowed_widths=runtime.build_settings.width_points,
        min_open_interest=runtime.build_settings.min_open_interest,
        max_leg_spread_pct_mid=runtime.build_settings.max_leg_spread_pct_mid,
        min_return_on_risk=runtime.build_settings.min_return_on_risk,
        ranking_policy=runtime.build_settings.ranking_policy,
        entry_recipe_refs=runtime.entry_recipe_refs,
    )


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
    "build_runtime_candidate_filter",
    "match_runtime_candidate",
]
