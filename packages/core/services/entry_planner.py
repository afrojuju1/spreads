from __future__ import annotations

from typing import Any


def _normalized_selection_states(
    selection_states: tuple[str, ...] | list[str] | None,
) -> set[str] | None:
    if selection_states in (None, ()):
        return None
    normalized = {
        str(value).strip().lower()
        for value in list(selection_states or [])
        if str(value).strip()
    }
    return normalized or None


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
    eligible_selection_states: tuple[str, ...] | list[str] | None = None,
) -> dict[str, Any]:
    eligible_states = _normalized_selection_states(eligible_selection_states)
    eligible_opportunities = [
        opportunity
        for opportunity in opportunities
        if eligible_states is None
        or str(opportunity.get("selection_state") or "").strip().lower()
        in eligible_states
    ]
    selected: dict[str, Any] | None = None
    if (
        controls_allowed
        and eligible_opportunities
        and score_opportunity(eligible_opportunities[0]) >= min_score
    ):
        selected = eligible_opportunities[0]

    decisions: list[dict[str, Any]] = []
    for rank, opportunity in enumerate(opportunities, start=1):
        opportunity_id = str(opportunity["opportunity_id"])
        if not controls_allowed:
            state = "blocked"
            reason_codes = [controls_reason or "bot_entry_blocked"]
        elif (
            eligible_states is not None
            and str(opportunity.get("selection_state") or "").strip().lower()
            not in eligible_states
        ):
            state = "rejected"
            reason_codes = ["selection_state_not_entry_eligible"]
        elif selected is None and score_opportunity(opportunity) < min_score:
            state = "rejected"
            reason_codes = ["score_below_min_opportunity_score"]
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
    return {
        "selected": selected,
        "decisions": decisions,
        "eligible_opportunity_count": len(eligible_opportunities),
    }


__all__ = ["plan_entry_selection", "score_opportunity"]
