from __future__ import annotations

from typing import Any

from core.services.option_structures import candidate_legs, legs_identity_key


def _capture_candidate_identity(candidate: dict[str, Any]) -> tuple[str, str, str]:
    return (
        legs_identity_key(
            strategy=candidate.get("strategy"),
            legs=candidate_legs(candidate),
        ),
        str(candidate.get("underlying_symbol") or ""),
        str(candidate.get("expiration_date") or ""),
    )


def _opportunity_is_live_capture_eligible(opportunity: dict[str, Any]) -> bool:
    eligibility = (
        str(
            opportunity.get("eligibility_state")
            or opportunity.get("eligibility")
            or "live"
        )
        .strip()
        .lower()
    )
    return eligibility == "live"


def build_capture_candidates(
    *,
    promotable_candidates: list[dict[str, Any]],
    monitor_candidates: list[dict[str, Any]],
    opportunities: list[dict[str, Any]],
    monitor_limit: int,
) -> list[dict[str, Any]]:
    capture_candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    def add_candidate(candidate: dict[str, Any]) -> None:
        identity = _capture_candidate_identity(candidate)
        if not all(identity) or identity in seen:
            return
        seen.add(identity)
        capture_candidates.append(dict(candidate))

    for candidate in promotable_candidates:
        add_candidate(candidate)
    for candidate in monitor_candidates[: max(monitor_limit, 0)]:
        add_candidate(candidate)

    target_count = max(
        len(promotable_candidates) + max(monitor_limit, 0),
        max(monitor_limit, 0),
    )
    if len(capture_candidates) >= target_count:
        return capture_candidates

    ranked_opportunities = sorted(
        (
            dict(item)
            for item in opportunities
            if isinstance(item, dict)
            and str(item.get("selection_state") or "") in {"promotable", "monitor"}
            and _opportunity_is_live_capture_eligible(item)
        ),
        key=lambda item: (
            int(item.get("selection_rank") or 999_999),
            str(item.get("selection_state") or ""),
        ),
    )
    for row in ranked_opportunities:
        candidate_payload = row.get("candidate")
        if isinstance(candidate_payload, dict) and candidate_payload:
            add_candidate(dict(candidate_payload))
        else:
            add_candidate(row)
        if len(capture_candidates) >= target_count:
            break

    return capture_candidates


def build_preselection_capture_candidates(
    symbol_candidates: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    capture_candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for rows in symbol_candidates.values():
        for candidate in rows:
            if not isinstance(candidate, dict):
                continue
            identity = _capture_candidate_identity(candidate)
            if not all(identity) or identity in seen:
                continue
            seen.add(identity)
            capture_candidates.append(dict(candidate))
    capture_candidates.sort(
        key=lambda candidate: float(candidate.get("quality_score") or 0.0),
        reverse=True,
    )
    return capture_candidates


__all__ = [
    "build_capture_candidates",
    "build_preselection_capture_candidates",
]
