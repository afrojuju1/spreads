from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

from core.services.value_coercion import as_text, coerce_float, coerce_int

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
    blocker_counts = payload.get("blocker_counts") if isinstance(payload.get("blocker_counts"), Mapping) else {}
    top_candidates = _candidate_preview_list(payload.get("top_candidates"))
    return {
        "opportunity_count": coerce_int(payload.get("opportunity_count")) or 0,
        "candidate_symbol_count": coerce_int(payload.get("candidate_symbol_count")) or 0,
        "candidate_count": coerce_int(payload.get("candidate_count")) or 0,
        "matched_discovery_opportunity_count": (coerce_int(payload.get("matched_discovery_opportunity_count")) or 0),
        "strategy_family_counts": _counter_map(payload.get("strategy_family_counts")),
        "earnings_phase_counts": _counter_map(payload.get("earnings_phase_counts")),
        "selection_state_counts": _counter_map(payload.get("selection_state_counts")),
        "scoring_state_counts": _counter_map(payload.get("scoring_state_counts")),
        "runtime_filter_reason_counts": _counter_map(payload.get("runtime_filter_reason_counts")),
        "rejection_reason_counts": _counter_map(payload.get("rejection_reason_counts")),
        "blocker_counts": {str(category): _counter_map(counts) for category, counts in blocker_counts.items() if as_text(category) is not None},
        "timing_confidence_counts": _counter_map(payload.get("timing_confidence_counts")),
        "shadow_only_count": coerce_int(payload.get("shadow_only_count")) or 0,
        "auto_live_eligible_count": (coerce_int(payload.get("auto_live_eligible_count")) or 0),
        "selection_source": as_text(payload.get("selection_source")),
        "status": as_text(payload.get("status")),
        "message": as_text(payload.get("message")),
        "top_candidates": top_candidates,
    }


def strategy_sync_summary_payload(value: Any) -> dict[str, Any]:
    payload = value if isinstance(value, Mapping) else {}
    return {
        "strategy_runs_upserted": (coerce_int(payload.get("strategy_runs_upserted")) or 0),
        "runtime_opportunities_upserted": (coerce_int(payload.get("runtime_opportunities_upserted")) or 0),
        "runtime_opportunities_expired": (coerce_int(payload.get("runtime_opportunities_expired")) or 0),
        "runtime_selection_summary": selection_summary_payload(payload.get("runtime_selection_summary")),
    }


def aggregate_selection_summaries(
    summaries: Sequence[Mapping[str, Any]] | None,
) -> dict[str, Any]:
    candidate_symbol_count = 0
    candidate_count = 0
    matched_discovery_opportunity_count = 0
    strategy_family_counts: Counter[str] = Counter()
    earnings_phase_counts: Counter[str] = Counter()
    selection_state_counts: Counter[str] = Counter()
    scoring_state_counts: Counter[str] = Counter()
    runtime_filter_reason_counts: Counter[str] = Counter()
    rejection_reason_counts: Counter[str] = Counter()
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
        candidate_symbol_count += int(payload["candidate_symbol_count"])
        candidate_count += int(payload["candidate_count"])
        matched_discovery_opportunity_count += int(payload["matched_discovery_opportunity_count"])
        shadow_only_count += int(payload["shadow_only_count"])
        auto_live_eligible_count += int(payload["auto_live_eligible_count"])
        strategy_family_counts.update(payload["strategy_family_counts"])
        earnings_phase_counts.update(payload["earnings_phase_counts"])
        selection_state_counts.update(payload["selection_state_counts"])
        scoring_state_counts.update(payload["scoring_state_counts"])
        runtime_filter_reason_counts.update(payload["runtime_filter_reason_counts"])
        rejection_reason_counts.update(payload["rejection_reason_counts"])
        timing_confidence_counts.update(payload["timing_confidence_counts"])
        for category, counts in payload["blocker_counts"].items():
            blocker_counts.setdefault(str(category), Counter()).update(counts)
    return {
        "opportunity_count": opportunity_count,
        "candidate_symbol_count": candidate_symbol_count,
        "candidate_count": candidate_count,
        "matched_discovery_opportunity_count": matched_discovery_opportunity_count,
        "strategy_family_counts": dict(strategy_family_counts),
        "earnings_phase_counts": dict(earnings_phase_counts),
        "selection_state_counts": dict(selection_state_counts),
        "scoring_state_counts": dict(scoring_state_counts),
        "runtime_filter_reason_counts": dict(runtime_filter_reason_counts),
        "rejection_reason_counts": dict(rejection_reason_counts),
        "blocker_counts": {category: dict(counter) for category, counter in blocker_counts.items()},
        "timing_confidence_counts": dict(timing_confidence_counts),
        "shadow_only_count": shadow_only_count,
        "auto_live_eligible_count": auto_live_eligible_count,
    }


def _counter_map(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): coerce_int(raw_value) or 0 for key, raw_value in value.items() if as_text(key) is not None}


def _candidate_preview_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    previews: list[dict[str, Any]] = []
    for item in list(value)[:5]:
        if not isinstance(item, Mapping):
            continue
        score_thresholds = item.get("score_thresholds") if isinstance(item.get("score_thresholds"), Mapping) else {}
        previews.append(
            {
                "underlying_symbol": as_text(item.get("underlying_symbol")),
                "strategy": as_text(item.get("strategy")),
                "structure_identity": as_text(item.get("structure_identity")),
                "quality_score": coerce_float(item.get("quality_score")),
                "promotion_score": coerce_float(item.get("promotion_score")),
                "execution_score": coerce_float(item.get("execution_score")),
                "selection_score": coerce_float(item.get("selection_score")),
                "selection_state": as_text(item.get("selection_state")),
                "scoring_state": as_text(item.get("scoring_state")),
                "scoring_state_reason": as_text(item.get("scoring_state_reason")),
                "setup_status": as_text(item.get("setup_status")),
                "ranking_policy_status": as_text(item.get("ranking_policy_status")),
                "monitor_floor": coerce_float(item.get("monitor_floor") or score_thresholds.get("monitor_floor")),
                "promotion_floor": coerce_float(item.get("promotion_floor") or score_thresholds.get("promotion_floor")),
                "min_opportunity_score": coerce_float(item.get("min_opportunity_score")),
                "min_opportunity_score_delta": coerce_float(item.get("min_opportunity_score_delta")),
                "reason_codes": [str(code) for code in list(item.get("reason_codes") or []) if as_text(code) is not None],
            }
        )
    return previews


__all__ = [
    "strategy_sync_summary_payload",
    "aggregate_selection_summaries",
    "live_selection_counts",
    "selection_summary_payload",
]
