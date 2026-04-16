from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from core.services.option_structures import normalize_strategy_family


@dataclass(frozen=True)
class EntryRecipeResult:
    passed: bool
    reason_codes: tuple[str, ...] = ()


def _setup_is_usable(candidate: dict[str, Any]) -> bool:
    return str(candidate.get("setup_status") or "unknown").strip().lower() in {
        "favorable",
        "neutral",
    }


def _trend_support(candidate: dict[str, Any]) -> EntryRecipeResult:
    if normalize_strategy_family(candidate.get("strategy")) != "put_credit_spread":
        return EntryRecipeResult(False, ("recipe_family_mismatch",))
    if not _setup_is_usable(candidate):
        return EntryRecipeResult(False, ("trend_support_setup_unusable",))
    return EntryRecipeResult(True)


def _trend_resistance(candidate: dict[str, Any]) -> EntryRecipeResult:
    if normalize_strategy_family(candidate.get("strategy")) != "call_credit_spread":
        return EntryRecipeResult(False, ("recipe_family_mismatch",))
    if not _setup_is_usable(candidate):
        return EntryRecipeResult(False, ("trend_resistance_setup_unusable",))
    return EntryRecipeResult(True)


def _neutral_range(candidate: dict[str, Any]) -> EntryRecipeResult:
    if normalize_strategy_family(candidate.get("strategy")) != "iron_condor":
        return EntryRecipeResult(False, ("recipe_family_mismatch",))
    if not _setup_is_usable(candidate):
        return EntryRecipeResult(False, ("neutral_range_setup_unusable",))
    side_balance = candidate.get("side_balance_score")
    if side_balance not in (None, "") and float(side_balance) < 0.35:
        return EntryRecipeResult(False, ("neutral_range_balance_too_low",))
    wing_ratio = candidate.get("wing_symmetry_ratio")
    if wing_ratio not in (None, ""):
        ratio = float(wing_ratio)
        if ratio <= 0 or ratio > 2.5:
            return EntryRecipeResult(False, ("neutral_range_wings_not_symmetric",))
    return EntryRecipeResult(True)


ENTRY_RECIPE_REGISTRY: dict[str, Callable[[dict[str, Any]], EntryRecipeResult]] = {
    "trend_support": _trend_support,
    "trend_resistance": _trend_resistance,
    "neutral_range": _neutral_range,
}


def evaluate_entry_recipes(
    candidate: dict[str, Any], recipe_refs: tuple[str, ...]
) -> EntryRecipeResult:
    reason_codes: list[str] = []
    for recipe_ref in recipe_refs:
        recipe = ENTRY_RECIPE_REGISTRY.get(str(recipe_ref))
        if recipe is None:
            reason_codes.append(f"unknown_entry_recipe:{recipe_ref}")
            continue
        result = recipe(candidate)
        if not result.passed:
            reason_codes.extend(
                result.reason_codes or (f"entry_recipe_failed:{recipe_ref}",)
            )
    if reason_codes:
        return EntryRecipeResult(False, tuple(reason_codes))
    return EntryRecipeResult(True)


__all__ = [
    "ENTRY_RECIPE_REGISTRY",
    "EntryRecipeResult",
    "evaluate_entry_recipes",
]
