from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from core.services.exit_manager import (
    evaluate_exit_policy,
    resolve_exit_policy_snapshot,
)


DEFAULT_EXIT_POLICY = {
    "enabled": True,
    "profit_target_pct": 0.5,
    "stop_multiple": 2.0,
    "force_close_minutes_before_close": 10,
}
RECIPE_DISABLED_THRESHOLD = 1_000_000.0


@dataclass(frozen=True)
class ManagementRecipeDecision:
    recipe_ref: str | None
    should_close: bool
    reason: str
    limit_price: float | None = None
    limit_price_source: str | None = None


@dataclass(frozen=True)
class CompiledManagementRecipe:
    recipe_ref: str
    exit_policy: dict[str, Any] | None = None
    non_closing_reason: str | None = None


def build_exit_policy_from_recipe_refs(
    recipe_refs: tuple[str, ...],
    *,
    existing_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    policy = dict(DEFAULT_EXIT_POLICY)
    if isinstance(existing_policy, dict):
        policy.update(existing_policy)
    if not recipe_refs:
        return policy
    for recipe_ref in recipe_refs:
        normalized = str(recipe_ref or "").strip().lower()
        if normalized == "take_profit_50pct":
            policy["profit_target_pct"] = 0.5
        elif normalized == "max_loss_2x_credit":
            policy["stop_multiple"] = 2.0
        elif normalized == "expiry_day_exit":
            policy.setdefault("force_close_minutes_before_close", 10)
    return policy


def _position_with_recipe_policy(
    position: dict[str, Any],
    *,
    exit_policy: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(position)
    payload["exit_policy"] = dict(exit_policy)
    return payload


def _position_session_date(position: dict[str, Any]) -> str | None:
    rendered = str(position.get("session_date") or position.get("market_date") or "")
    rendered = rendered.strip()
    return rendered or None


def _compile_recipe(
    recipe_ref: str,
    *,
    session_date: str | None,
) -> CompiledManagementRecipe:
    normalized = str(recipe_ref or "").strip().lower()
    if normalized == "take_profit_50pct":
        return CompiledManagementRecipe(
            recipe_ref=recipe_ref,
            exit_policy={
                "enabled": True,
                "profit_target_pct": 0.5,
                "stop_multiple": RECIPE_DISABLED_THRESHOLD,
                "force_close_at": None,
            },
        )
    if normalized == "max_loss_2x_credit":
        return CompiledManagementRecipe(
            recipe_ref=recipe_ref,
            exit_policy={
                "enabled": True,
                "profit_target_pct": RECIPE_DISABLED_THRESHOLD,
                "stop_multiple": 2.0,
                "force_close_at": None,
            },
        )
    if normalized == "expiry_day_exit":
        if session_date is None:
            return CompiledManagementRecipe(
                recipe_ref=recipe_ref,
                non_closing_reason="missing_session_date_for_expiry_day_exit",
            )
        return CompiledManagementRecipe(
            recipe_ref=recipe_ref,
            exit_policy=resolve_exit_policy_snapshot(
                session_date=session_date,
                payload={
                    "exit_policy": {
                        "enabled": True,
                        "profit_target_pct": RECIPE_DISABLED_THRESHOLD,
                        "stop_multiple": RECIPE_DISABLED_THRESHOLD,
                        "force_close_minutes_before_close": 10,
                    }
                },
            ),
        )
    return CompiledManagementRecipe(
        recipe_ref=recipe_ref,
        non_closing_reason=f"unknown_management_recipe:{normalized or 'missing'}",
    )


def compile_management_recipes(
    recipe_refs: tuple[str, ...],
    *,
    session_date: str | None = None,
) -> tuple[CompiledManagementRecipe, ...]:
    return tuple(
        _compile_recipe(recipe_ref, session_date=session_date)
        for recipe_ref in recipe_refs
    )


def _evaluate_compiled_recipe(
    recipe: CompiledManagementRecipe,
    *,
    position: dict[str, Any],
    mark: float | None,
    now: datetime,
) -> ManagementRecipeDecision:
    if recipe.exit_policy is None:
        return ManagementRecipeDecision(
            recipe_ref=recipe.recipe_ref,
            should_close=False,
            reason=str(recipe.non_closing_reason or "hold"),
        )
    decision = evaluate_exit_policy(
        position=_position_with_recipe_policy(position, exit_policy=recipe.exit_policy),
        mark=mark,
        now=now,
    )
    return ManagementRecipeDecision(
        recipe_ref=recipe.recipe_ref,
        should_close=bool(decision.get("should_close")),
        reason=str(decision.get("reason") or "hold"),
        limit_price=(
            None
            if decision.get("limit_price") in (None, "")
            else float(decision["limit_price"])
        ),
        limit_price_source=(
            None
            if decision.get("limit_price_source") in (None, "")
            else str(decision["limit_price_source"])
        ),
    )


def evaluate_compiled_management_recipes(
    recipes: tuple[CompiledManagementRecipe, ...],
    *,
    position: dict[str, Any],
    mark: float | None,
    now: datetime | None = None,
) -> ManagementRecipeDecision:
    if not recipes:
        return ManagementRecipeDecision(
            recipe_ref=None,
            should_close=False,
            reason="hold",
        )
    current_time = now or datetime.now(UTC)
    pending_decision: ManagementRecipeDecision | None = None
    for recipe in recipes:
        decision = _evaluate_compiled_recipe(
            recipe,
            position=position,
            mark=mark,
            now=current_time,
        )
        if decision.should_close:
            return decision
        if (
            decision.reason not in {"hold", "policy_disabled"}
            and pending_decision is None
        ):
            pending_decision = decision
    return pending_decision or ManagementRecipeDecision(
        recipe_ref=None,
        should_close=False,
        reason="hold",
    )


def evaluate_management_recipes(
    recipe_refs: tuple[str, ...],
    *,
    position: dict[str, Any],
    mark: float | None,
    now: datetime | None = None,
) -> ManagementRecipeDecision:
    return evaluate_compiled_management_recipes(
        compile_management_recipes(
            recipe_refs,
            session_date=_position_session_date(position),
        ),
        position=position,
        mark=mark,
        now=now,
    )


__all__ = [
    "CompiledManagementRecipe",
    "DEFAULT_EXIT_POLICY",
    "ManagementRecipeDecision",
    "build_exit_policy_from_recipe_refs",
    "compile_management_recipes",
    "evaluate_compiled_management_recipes",
    "evaluate_management_recipes",
]
