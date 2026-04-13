from __future__ import annotations

from collections.abc import Mapping
from typing import Any

SCORE_BUCKETS = (
    (85.0, "85+"),
    (75.0, "75-84"),
    (65.0, "65-74"),
    (55.0, "55-64"),
)


def candidate_identity(candidate: Mapping[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(candidate["underlying_symbol"]),
        str(candidate["strategy"]),
        str(candidate["expiration_date"]),
        str(candidate["short_symbol"]),
        str(candidate["long_symbol"]),
    )


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
    "candidate_session_phase",
    "candidate_identity",
    "resolved_estimated_pnl",
    "score_bucket_label",
]
