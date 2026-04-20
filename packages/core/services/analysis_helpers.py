from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.services.option_structures import payload_display_fields, payload_structure_identity

SCORE_BUCKETS = (
    (85.0, "85+"),
    (75.0, "75-84"),
    (65.0, "65-74"),
    (55.0, "55-64"),
)


def candidate_identity(candidate: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(candidate.get("underlying_symbol") or candidate.get("symbol") or ""),
        str(candidate["strategy"]),
        str(candidate["expiration_date"]),
        str(payload_structure_identity(candidate, strategy=candidate.get("strategy")) or ""),
    )


def candidate_display(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **payload_display_fields(candidate),
        "structure_identity": payload_structure_identity(
            candidate,
            strategy=candidate.get("strategy"),
        ),
    }


def score_bucket_label(score: float | None) -> str:
    if score is None:
        return "unknown"
    for threshold, label in SCORE_BUCKETS:
        if score >= threshold:
            return label
    return "<55"


def resolved_estimated_pnl(item: Mapping[str, Any]) -> float | None:
    expiry_value = item.get("estimated_expiry_pnl")
    if expiry_value is not None:
        return float(expiry_value)
    close_value = item.get("estimated_close_pnl")
    if close_value is not None:
        return float(close_value)
    return None


def candidate_session_phase(candidate: Mapping[str, Any]) -> str:
    notes = candidate.get("selection_notes") or candidate.get("board_notes") or []
    for note in notes:
        if isinstance(note, str) and note.startswith("session-"):
            return note.removeprefix("session-")
    return "unknown"


__all__ = [
    "candidate_display",
    "candidate_session_phase",
    "candidate_identity",
    "resolved_estimated_pnl",
    "score_bucket_label",
]
