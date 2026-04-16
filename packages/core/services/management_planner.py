from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from core.services.automation_runtime import ManagementRuntime
from core.services.management_recipes import evaluate_management_recipes


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _resolve_management_limit_price(
    position: dict[str, Any],
) -> tuple[float | None, str | None]:
    mark = _coerce_float(position.get("close_mark"))
    if mark is not None and mark > 0:
        return round(max(mark, 0.01), 2), "mark"
    width = _coerce_float(position.get("width"))
    if width is not None and width > 0:
        return round(max(width, 0.01), 2), "width"
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
            "reason": "bot_flatten",
            "limit_price": limit_price,
            "limit_price_source": limit_price_source,
            "recipe_ref": None,
        }

    decision = evaluate_management_recipes(
        runtime.management_recipe_refs,
        position=position,
        mark=_coerce_float(position.get("close_mark")),
        now=now or datetime.now(UTC),
    )
    return {
        "should_close": decision.should_close,
        "reason": decision.reason,
        "limit_price": decision.limit_price,
        "limit_price_source": decision.limit_price_source,
        "recipe_ref": decision.recipe_ref,
    }


__all__ = ["plan_position_management"]
