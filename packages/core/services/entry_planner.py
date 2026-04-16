from __future__ import annotations

from typing import Any


def score_opportunity(row: dict[str, Any]) -> float:
    for key in ("execution_score", "promotion_score"):
        value = row.get(key)
        try:
            if value not in (None, ""):
                return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def plan_entry_selection(
    *,
    opportunities: list[dict[str, Any]],
    controls_allowed: bool,
    controls_reason: str | None,
    bot_metrics: dict[str, Any],
    min_score: float,
) -> dict[str, Any]:
    selected: dict[str, Any] | None = None
    if (
        controls_allowed
        and opportunities
        and score_opportunity(opportunities[0]) >= min_score
    ):
        selected = opportunities[0]

    decisions: list[dict[str, Any]] = []
    for rank, opportunity in enumerate(opportunities, start=1):
        opportunity_id = str(opportunity["opportunity_id"])
        if not controls_allowed:
            state = "blocked"
            reason_codes = [controls_reason or "bot_entry_blocked"]
        else:
            state = (
                "selected"
                if selected is not None and opportunity_id == selected["opportunity_id"]
                else "rejected"
            )
            reason_codes = [
                "selected_for_entry"
                if state == "selected"
                else "lower_ranked_than_selected_opportunity"
            ]
        decisions.append(
            {
                "opportunity_id": opportunity_id,
                "state": state,
                "score": score_opportunity(opportunity),
                "rank": rank,
                "reason_codes": reason_codes,
                "payload": {
                    "opportunity": {
                        "opportunity_id": opportunity_id,
                        "underlying_symbol": opportunity.get("underlying_symbol"),
                        "strategy_family": opportunity.get("strategy_family"),
                    },
                    **(
                        {}
                        if controls_reason is None
                        else {"control_reason": controls_reason}
                    ),
                    "bot_metrics": bot_metrics,
                },
            }
        )
    return {"selected": selected, "decisions": decisions}


__all__ = ["plan_entry_selection", "score_opportunity"]
