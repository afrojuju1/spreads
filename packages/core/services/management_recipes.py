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


def _evaluate_recipe(
    recipe_ref: str,
    *,
    position: dict[str, Any],
    mark: float | None,
    now: datetime,
) -> ManagementRecipeDecision:
    normalized = str(recipe_ref or "").strip().lower()
    if normalized == "take_profit_50pct":
        decision = evaluate_exit_policy(
            position=_position_with_recipe_policy(
                position,
                exit_policy={
                    "enabled": True,
                    "profit_target_pct": 0.5,
                    "stop_multiple": RECIPE_DISABLED_THRESHOLD,
                    "force_close_at": None,
                },
            ),
            mark=mark,
            now=now,
        )
    elif normalized == "max_loss_2x_credit":
        decision = evaluate_exit_policy(
            position=_position_with_recipe_policy(
                position,
                exit_policy={
                    "enabled": True,
                    "profit_target_pct": RECIPE_DISABLED_THRESHOLD,
                    "stop_multiple": 2.0,
                    "force_close_at": None,
                },
            ),
            mark=mark,
            now=now,
        )
    elif normalized == "expiry_day_exit":
        session_date = str(
            position.get("session_date") or position.get("market_date") or ""
        )
        exit_policy = resolve_exit_policy_snapshot(
            session_date=session_date,
            payload={
                "exit_policy": {
                    "enabled": True,
                    "profit_target_pct": RECIPE_DISABLED_THRESHOLD,
                    "stop_multiple": RECIPE_DISABLED_THRESHOLD,
                    "force_close_minutes_before_close": 10,
                }
            },
        )
        decision = evaluate_exit_policy(
            position=_position_with_recipe_policy(position, exit_policy=exit_policy),
            mark=mark,
            now=now,
        )
    else:
        return ManagementRecipeDecision(
            recipe_ref=recipe_ref,
            should_close=False,
            reason=f"unknown_management_recipe:{normalized or 'missing'}",
        )

    return ManagementRecipeDecision(
        recipe_ref=recipe_ref,
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


def evaluate_management_recipes(
    recipe_refs: tuple[str, ...],
    *,
    position: dict[str, Any],
    mark: float | None,
    now: datetime | None = None,
) -> ManagementRecipeDecision:
    if not recipe_refs:
        return ManagementRecipeDecision(
            recipe_ref=None,
            should_close=False,
            reason="hold",
        )
    current_time = now or datetime.now(UTC)
    pending_decision: ManagementRecipeDecision | None = None
    for recipe_ref in recipe_refs:
        decision = _evaluate_recipe(
            recipe_ref,
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


__all__ = [
    "DEFAULT_EXIT_POLICY",
    "ManagementRecipeDecision",
    "build_exit_policy_from_recipe_refs",
    "evaluate_management_recipes",
]
