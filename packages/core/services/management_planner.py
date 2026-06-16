from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from core.money import option_limit_price
from core.services.trading_engine.close_policy import evaluate_exit_policy
from core.services.trading_strategy_runtime_models import ManagementRuntime
from core.value_coercion import coerce_float


def _resolve_management_limit_price(
    position: dict[str, Any],
) -> tuple[float | None, str | None]:
    mark = coerce_float(position.get("close_mark"))
    if mark is not None and mark > 0:
        return option_limit_price(mark), "mark"
    width = coerce_float(position.get("width"))
    if width is not None and width > 0:
        return option_limit_price(width), "width"
    return None, None


def plan_position_management(
    *,
    runtime: ManagementRuntime,
    position: dict[str, Any],
    flatten_due: bool,
    now: datetime | None = None,
) -> dict[str, Any]:
    if flatten_due:
        limit_price, limit_price_source = _resolve_management_limit_price(position)
        if limit_price is None:
            return {"should_close": False, "reason": "awaiting_flatten_price"}
        return {
            "should_close": True,
            "reason": "strategy_flatten",
            "limit_price": limit_price,
            "limit_price_source": limit_price_source,
            "recipe_ref": None,
            "decision_details": None,
        }

    current_time = now or datetime.now(UTC)
    policy_decision = evaluate_exit_policy(
        position={**position, "exit_policy": runtime.management_policy},
        mark=coerce_float(position.get("close_mark")),
        now=current_time,
    )
    return {
        "should_close": bool(policy_decision.get("should_close")),
        "reason": str(policy_decision.get("reason") or "hold"),
        "limit_price": coerce_float(policy_decision.get("limit_price")),
        "limit_price_source": policy_decision.get("limit_price_source"),
        "recipe_ref": None,
        "decision_details": dict(policy_decision),
    }


__all__ = ["plan_position_management"]
