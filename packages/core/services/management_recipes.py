from __future__ import annotations

from typing import Any


DEFAULT_EXIT_POLICY = {
    "enabled": True,
    "profit_target_pct": 0.5,
    "stop_multiple": 2.0,
    "force_close_minutes_before_close": 10,
}


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


__all__ = ["DEFAULT_EXIT_POLICY", "build_exit_policy_from_recipe_refs"]
