from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Any, Mapping

from core.services.analysis_helpers import resolved_estimated_pnl
from core.services.selection_terms import (
    MONITOR_SELECTION_STATE,
    PROMOTABLE_SELECTION_STATE,
)


def classify_vwap_regime(setup: Mapping[str, Any] | None, strategy: str) -> str:
    if not setup:
        return "unknown"
    value = setup.get("spot_vs_vwap_pct")
    if value is None:
        return "unknown"
    pct = float(value)
    if strategy == "put_credit":
        if pct > 0.0015:
            return "supportive"
        if pct < -0.0015:
            return "adverse"
    else:
        if pct < -0.0015:
            return "supportive"
        if pct > 0.0015:
            return "adverse"
    return "neutral"


def classify_trend_regime(setup: Mapping[str, Any] | None, strategy: str) -> str:
    if not setup:
        return "unknown"
    value = setup.get("intraday_return_pct")
    if value is None:
        return "unknown"
    pct = float(value)
    if strategy == "put_credit":
        if pct > 0.004:
            return "supportive"
        if pct < -0.004:
            return "adverse"
    else:
        if pct < -0.004:
            return "supportive"
        if pct > 0.004:
            return "adverse"
    return "neutral"


def classify_opening_range_regime(
    setup: Mapping[str, Any] | None, strategy: str
) -> str:
    if not setup:
        return "unknown"
    value = setup.get("opening_range_break_pct")
    latest_close = setup.get("latest_close")
    opening_range_high = setup.get("opening_range_high")
    opening_range_low = setup.get("opening_range_low")
    if value is None and latest_close is None:
        return "unknown"
    if value is not None and float(value) > 0.001:
        return "supportive_breakout"
    if strategy == "put_credit":
        if (
            latest_close is not None
            and opening_range_low is not None
            and float(latest_close) < float(opening_range_low)
        ):
            return "adverse_break"
    else:
        if (
            latest_close is not None
            and opening_range_high is not None
            and float(latest_close) > float(opening_range_high)
        ):
            return "adverse_break"
    return "inside_range"


def classify_session_extreme_regime(setup: Mapping[str, Any] | None) -> str:
    if not setup:
        return "unknown"
    value = setup.get("distance_to_session_extreme_pct")
    if value is None:
        return "unknown"
    pct = float(value)
    if pct < 0.003:
        return "near_extreme"
    if pct > 0.008:
        return "room_to_extreme"
    return "neutral"


def aggregate_signal_dimension(
    ideas: list[dict[str, Any]],
    *,
    field: str,
) -> list[dict[str, Any]]:
    grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in ideas:
        grouped_rows[str(item.get(field) or "unknown")].append(item)

    rows: list[dict[str, Any]] = []
    for key, group in grouped_rows.items():
        resolved = [item for item in group if item["outcome_bucket"] in {"win", "loss"}]
        pnl_values = [resolved_estimated_pnl(item) for item in group]
        realized_pnls = [value for value in pnl_values if value is not None]
        wins = sum(1 for item in group if item["outcome_bucket"] == "win")
        losses = sum(1 for item in group if item["outcome_bucket"] == "loss")
        rows.append(
            {
                "bucket": key,
                "count": len(group),
                "promotable_count": sum(
                    1
                    for item in group
                    if item["selection_state"] == PROMOTABLE_SELECTION_STATE
                ),
                "monitor_count": sum(
                    1
                    for item in group
                    if item["selection_state"] == MONITOR_SELECTION_STATE
                ),
                "win_count": wins,
                "loss_count": losses,
                "still_open_count": sum(
                    1 for item in group if item["outcome_bucket"] == "still_open"
                ),
                "unavailable_count": sum(
                    1 for item in group if item["outcome_bucket"] == "unavailable"
                ),
                "resolved_count": len(resolved),
                "win_rate": None if not resolved else wins / len(resolved),
                "average_estimated_pnl": (
                    None if not realized_pnls else mean(realized_pnls)
                ),
                "average_latest_score": mean(
                    float(item["latest_score"]) for item in group
                ),
            }
        )

    rows.sort(
        key=lambda row: (
            -int(row["count"]),
            -(row["win_rate"] if row["win_rate"] is not None else -1.0),
            -float(row["average_latest_score"]),
            row["bucket"],
        )
    )
    return rows


def build_signal_tuning(outcomes: Mapping[str, Any]) -> dict[str, Any]:
    ideas = list(outcomes["ideas"])
    dimensions = {
        "selection_state": aggregate_signal_dimension(ideas, field="selection_state"),
        "symbol": aggregate_signal_dimension(ideas, field="underlying_symbol"),
        "strategy": aggregate_signal_dimension(ideas, field="strategy"),
        "score_bucket": aggregate_signal_dimension(ideas, field="score_bucket"),
        "setup_status": aggregate_signal_dimension(ideas, field="setup_status"),
        "calendar_status": aggregate_signal_dimension(ideas, field="calendar_status"),
        "session_phase": aggregate_signal_dimension(ideas, field="session_phase"),
        "vwap_regime": aggregate_signal_dimension(ideas, field="vwap_regime"),
        "trend_regime": aggregate_signal_dimension(ideas, field="trend_regime"),
        "opening_range_regime": aggregate_signal_dimension(
            ideas,
            field="opening_range_regime",
        ),
        "session_extreme_regime": aggregate_signal_dimension(
            ideas,
            field="session_extreme_regime",
        ),
        "greeks_source": aggregate_signal_dimension(ideas, field="greeks_source"),
    }

    resolved_rank_candidates: list[dict[str, Any]] = []
    provisional_rank_candidates: list[dict[str, Any]] = []
    for dimension, rows in dimensions.items():
        for row in rows:
            if row["count"] < 2:
                continue
            candidate = {"dimension": dimension, **row}
            if row["resolved_count"] > 0:
                resolved_rank_candidates.append(candidate)
            elif row["average_estimated_pnl"] is not None:
                provisional_rank_candidates.append(candidate)

    strongest = sorted(
        resolved_rank_candidates,
        key=lambda row: (
            -(row["win_rate"] if row["win_rate"] is not None else -1.0),
            -(
                row["average_estimated_pnl"]
                if row["average_estimated_pnl"] is not None
                else float("-inf")
            ),
            -int(row["resolved_count"]),
            -int(row["count"]),
            row["dimension"],
            row["bucket"],
        ),
    )[:5]
    weakest = sorted(
        resolved_rank_candidates,
        key=lambda row: (
            row["win_rate"] if row["win_rate"] is not None else 2.0,
            row["average_estimated_pnl"]
            if row["average_estimated_pnl"] is not None
            else float("inf"),
            -int(row["resolved_count"]),
            -int(row["count"]),
            row["dimension"],
            row["bucket"],
        ),
    )[:5]
    provisional_strongest = sorted(
        provisional_rank_candidates,
        key=lambda row: (
            -(
                row["average_estimated_pnl"]
                if row["average_estimated_pnl"] is not None
                else float("-inf")
            ),
            -int(row["count"]),
            row["dimension"],
            row["bucket"],
        ),
    )[:5]
    provisional_weakest = sorted(
        provisional_rank_candidates,
        key=lambda row: (
            row["average_estimated_pnl"]
            if row["average_estimated_pnl"] is not None
            else float("inf"),
            -int(row["count"]),
            row["dimension"],
            row["bucket"],
        ),
    )[:5]

    return {
        "sample_size": len(ideas),
        "dimensions": dimensions,
        "strongest_signals": strongest,
        "weakest_signals": weakest,
        "provisional_strongest_signals": provisional_strongest,
        "provisional_weakest_signals": provisional_weakest,
    }
