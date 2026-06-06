from __future__ import annotations

from typing import Any


def _normalized_selection_states(
    selection_states: tuple[str, ...] | list[str] | None,
) -> set[str] | None:
    if selection_states in (None, ()):
        return None
    normalized = {str(value).strip().lower() for value in list(selection_states or []) if str(value).strip()}
    return normalized or None


def score_signal(row: dict[str, Any]) -> float:
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
    signals: list[dict[str, Any]],
    controls_allowed: bool,
    controls_reason: str | None,
    bot_metrics: dict[str, Any],
    min_score: float,
    eligible_selection_states: tuple[str, ...] | list[str] | None = None,
) -> dict[str, Any]:
    eligible_states = _normalized_selection_states(eligible_selection_states)
    eligible_signals = [
        signal for signal in signals if eligible_states is None or str(signal.get("selection_state") or "").strip().lower() in eligible_states
    ]
    selected: dict[str, Any] | None = None
    if controls_allowed and eligible_signals and score_signal(eligible_signals[0]) >= min_score:
        selected = eligible_signals[0]

    decisions: list[dict[str, Any]] = []
    for rank, signal in enumerate(signals, start=1):
        signal_id = str(signal["trade_signal_id"])
        if not controls_allowed:
            state = "blocked"
            reason_codes = [controls_reason or "bot_entry_blocked"]
        elif eligible_states is not None and str(signal.get("selection_state") or "").strip().lower() not in eligible_states:
            state = "rejected"
            reason_codes = ["selection_state_not_entry_eligible"]
        elif selected is None and score_signal(signal) < min_score:
            state = "rejected"
            reason_codes = ["score_below_min_signal_score"]
        else:
            state = "selected" if selected is not None and signal_id == selected["trade_signal_id"] else "rejected"
            reason_codes = ["selected_for_entry" if state == "selected" else "lower_ranked_than_selected_signal"]
        decisions.append(
            {
                "trade_signal_id": signal_id,
                "state": state,
                "score": score_signal(signal),
                "rank": rank,
                "reason_codes": reason_codes,
                "payload": {
                    "signal": {
                        "trade_signal_id": signal_id,
                        "underlying_symbol": signal.get("underlying_symbol"),
                        "trade_structure": signal.get("trade_structure") or signal.get("strategy_family"),
                    },
                    **({} if controls_reason is None else {"control_reason": controls_reason}),
                    "bot_metrics": bot_metrics,
                },
            }
        )
    return {
        "selected": selected,
        "decisions": decisions,
        "eligible_signal_count": len(eligible_signals),
    }


__all__ = ["plan_entry_selection", "score_signal"]
