from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

from spreads.services.value_coercion import as_text, coerce_int

LIVE_SELECTION_STATES = ("promotable", "monitor")


def live_selection_counts(
    opportunities: Sequence[Mapping[str, Any]] | None,
    *,
    states: Sequence[str] = LIVE_SELECTION_STATES,
) -> dict[str, int]:
    counts = {str(state): 0 for state in states if as_text(state) is not None}
    for row in list(opportunities or []):
        if not isinstance(row, Mapping):
            continue
        if str(row.get("eligibility") or "live") != "live":
            continue
        selection_state = str(row.get("selection_state") or "")
        if selection_state in counts:
            counts[selection_state] += 1
    return counts


def selection_summary_payload(value: Any) -> dict[str, Any]:
    payload = value if isinstance(value, Mapping) else {}
    blocker_counts = (
        payload.get("blocker_counts")
        if isinstance(payload.get("blocker_counts"), Mapping)
        else {}
    )
    return {
        "opportunity_count": coerce_int(payload.get("opportunity_count")) or 0,
        "strategy_family_counts": _counter_map(payload.get("strategy_family_counts")),
        "earnings_phase_counts": _counter_map(payload.get("earnings_phase_counts")),
        "selection_state_counts": _counter_map(payload.get("selection_state_counts")),
        "blocker_counts": {
            str(category): _counter_map(counts)
            for category, counts in blocker_counts.items()
            if as_text(category) is not None
        },
        "timing_confidence_counts": _counter_map(payload.get("timing_confidence_counts")),
        "shadow_only_count": coerce_int(payload.get("shadow_only_count")) or 0,
        "auto_live_eligible_count": (
            coerce_int(payload.get("auto_live_eligible_count")) or 0
        ),
    }


def aggregate_selection_summaries(
    summaries: Sequence[Mapping[str, Any]] | None,
) -> dict[str, Any]:
    strategy_family_counts: Counter[str] = Counter()
    earnings_phase_counts: Counter[str] = Counter()
    selection_state_counts: Counter[str] = Counter()
    timing_confidence_counts: Counter[str] = Counter()
    blocker_counts: dict[str, Counter[str]] = {
        "policy": Counter(),
        "signal_gate": Counter(),
        "quote_liquidity": Counter(),
        "execution_gate": Counter(),
    }
    opportunity_count = 0
    shadow_only_count = 0
    auto_live_eligible_count = 0
    for summary in list(summaries or []):
        payload = selection_summary_payload(summary)
        opportunity_count += int(payload["opportunity_count"])
        shadow_only_count += int(payload["shadow_only_count"])
        auto_live_eligible_count += int(payload["auto_live_eligible_count"])
        strategy_family_counts.update(payload["strategy_family_counts"])
        earnings_phase_counts.update(payload["earnings_phase_counts"])
        selection_state_counts.update(payload["selection_state_counts"])
        timing_confidence_counts.update(payload["timing_confidence_counts"])
        for category, counts in payload["blocker_counts"].items():
            blocker_counts.setdefault(str(category), Counter()).update(counts)
    return {
        "opportunity_count": opportunity_count,
        "strategy_family_counts": dict(strategy_family_counts),
        "earnings_phase_counts": dict(earnings_phase_counts),
        "selection_state_counts": dict(selection_state_counts),
        "blocker_counts": {
            category: dict(counter) for category, counter in blocker_counts.items()
        },
        "timing_confidence_counts": dict(timing_confidence_counts),
        "shadow_only_count": shadow_only_count,
        "auto_live_eligible_count": auto_live_eligible_count,
    }


def _counter_map(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    return {
        str(key): coerce_int(raw_value) or 0
        for key, raw_value in value.items()
        if as_text(key) is not None
    }


__all__ = [
    "aggregate_selection_summaries",
    "live_selection_counts",
    "selection_summary_payload",
]
