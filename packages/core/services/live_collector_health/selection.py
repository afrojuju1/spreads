from __future__ import annotations

from collections import Counter
from typing import Any, Mapping, Sequence

from .shared import _normalize_text_list, _read_int


def _strategy_family(strategy: Any) -> str:
    normalized = str(strategy or "").strip().lower()
    return {
        "call_credit": "call_credit_spread",
        "put_credit": "put_credit_spread",
        "call_debit": "call_debit_spread",
        "put_debit": "put_debit_spread",
        "long_straddle": "long_straddle",
        "long_strangle": "long_strangle",
        "iron_condor": "iron_condor",
    }.get(normalized, normalized or "unknown")


def _candidate_payload(row: Mapping[str, Any]) -> Mapping[str, Any]:
    candidate = row.get("candidate")
    return candidate if isinstance(candidate, Mapping) else row


def _normalized_strategy_family(row: Mapping[str, Any]) -> str:
    candidate = _candidate_payload(row)
    return _strategy_family(
        candidate.get("strategy")
        or row.get("strategy")
        or row.get("selected_strategy_family")
    )


def _normalized_selection_state(row: Mapping[str, Any]) -> str:
    return str(row.get("selection_state") or "unknown").strip().lower() or "unknown"


def _normalized_timing_confidence(row: Mapping[str, Any]) -> str:
    candidate = _candidate_payload(row)
    rendered = str(candidate.get("earnings_timing_confidence") or "").strip().lower()
    return rendered or "unknown"


def _normalized_earnings_phase(row: Mapping[str, Any]) -> str:
    candidate = _candidate_payload(row)
    rendered = str(candidate.get("earnings_phase") or "").strip().lower()
    return rendered or "clean"


def _score_evidence(row: Mapping[str, Any]) -> Mapping[str, Any]:
    candidate = _candidate_payload(row)
    evidence = candidate.get("score_evidence")
    return evidence if isinstance(evidence, Mapping) else {}


def _signal_gate_blockers(row: Mapping[str, Any]) -> list[str]:
    evidence = _score_evidence(row)
    signal_gate = evidence.get("signal_gate")
    if not isinstance(signal_gate, Mapping):
        return []
    return _normalize_text_list(signal_gate.get("blockers"))


def _scoring_blockers(row: Mapping[str, Any]) -> list[str]:
    candidate = _candidate_payload(row)
    return _normalize_text_list(candidate.get("scoring_blockers"))


def _execution_blockers(row: Mapping[str, Any]) -> list[str]:
    candidate = _candidate_payload(row)
    return _normalize_text_list(candidate.get("execution_blockers"))


def _quote_liquidity_blocker(code: str) -> bool:
    normalized = code.strip().lower()
    if not normalized:
        return False
    return any(
        token in normalized
        for token in (
            "quote",
            "liquidity",
            "spread",
            "midpoint",
            "fill_ratio",
            "data_quality",
        )
    )


def build_selection_summary(
    opportunities: Sequence[Mapping[str, Any]] | None,
) -> dict[str, Any]:
    rows = [dict(row) for row in list(opportunities or []) if isinstance(row, Mapping)]
    strategy_family_counts: Counter[str] = Counter()
    earnings_phase_counts: Counter[str] = Counter()
    selection_state_counts: Counter[str] = Counter()
    timing_confidence_counts: Counter[str] = Counter()
    blocker_counts = {
        "policy": Counter(),
        "signal_gate": Counter(),
        "quote_liquidity": Counter(),
        "execution_gate": Counter(),
    }
    shadow_only_count = 0
    auto_live_eligible_count = 0

    for row in rows:
        strategy_family_counts[_normalized_strategy_family(row)] += 1
        earnings_phase_counts[_normalized_earnings_phase(row)] += 1
        selection_state_counts[_normalized_selection_state(row)] += 1
        timing_confidence_counts[_normalized_timing_confidence(row)] += 1

        eligibility = str(row.get("eligibility") or "live").strip().lower()
        if eligibility != "live":
            shadow_only_count += 1

        signal_gate_blockers = _signal_gate_blockers(row)
        for blocker in signal_gate_blockers:
            blocker_counts["signal_gate"][blocker] += 1

        scoring_blockers = [
            blocker
            for blocker in _scoring_blockers(row)
            if blocker not in signal_gate_blockers
        ]
        execution_blockers = _execution_blockers(row)

        live_ready = (
            eligibility == "live"
            and _normalized_selection_state(row) == "promotable"
            and not signal_gate_blockers
            and not execution_blockers
            and not scoring_blockers
        )
        if live_ready:
            auto_live_eligible_count += 1

        for blocker in scoring_blockers:
            category = (
                "quote_liquidity" if _quote_liquidity_blocker(blocker) else "policy"
            )
            blocker_counts[category][blocker] += 1

        for blocker in execution_blockers:
            category = (
                "quote_liquidity"
                if _quote_liquidity_blocker(blocker)
                else "execution_gate"
            )
            blocker_counts[category][blocker] += 1

    return {
        "opportunity_count": len(rows),
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


def normalize_selection_summary(
    summary: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(summary, Mapping):
        return None
    blocker_counts_payload = (
        summary.get("blocker_counts")
        if isinstance(summary.get("blocker_counts"), Mapping)
        else {}
    )
    return {
        "opportunity_count": _read_int(summary, "opportunity_count"),
        "strategy_family_counts": {
            str(key): _read_int(summary.get("strategy_family_counts"), key)
            for key in sorted(dict(summary.get("strategy_family_counts") or {}))
        },
        "earnings_phase_counts": {
            str(key): _read_int(summary.get("earnings_phase_counts"), key)
            for key in sorted(dict(summary.get("earnings_phase_counts") or {}))
        },
        "selection_state_counts": {
            str(key): _read_int(summary.get("selection_state_counts"), key)
            for key in sorted(dict(summary.get("selection_state_counts") or {}))
        },
        "blocker_counts": {
            category: {
                str(key): _read_int(counts, key) for key in sorted(dict(counts or {}))
            }
            for category, counts in (
                (str(key), blocker_counts_payload.get(key))
                for key in sorted(dict(blocker_counts_payload))
            )
        },
        "timing_confidence_counts": {
            str(key): _read_int(summary.get("timing_confidence_counts"), key)
            for key in sorted(dict(summary.get("timing_confidence_counts") or {}))
        },
        "shadow_only_count": _read_int(summary, "shadow_only_count"),
        "auto_live_eligible_count": _read_int(summary, "auto_live_eligible_count"),
    }


__all__ = ["build_selection_summary", "normalize_selection_summary"]
