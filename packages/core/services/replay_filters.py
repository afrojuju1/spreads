from __future__ import annotations

from typing import Any, Mapping


def build_candidate_filter(
    *,
    allowed_widths: list[float] | tuple[float, ...] | None = None,
) -> dict[str, Any]:
    normalized_widths = sorted(
        {
            round(float(value), 4)
            for value in list(allowed_widths or [])
            if value not in (None, "")
        }
    )
    if not normalized_widths:
        return {}
    return {"allowed_widths": normalized_widths}


def candidate_matches_filter(
    candidate: Mapping[str, Any],
    candidate_filter: Mapping[str, Any] | None,
) -> bool:
    payload = dict(candidate_filter or {})
    allowed_widths = {
        round(float(value), 4)
        for value in list(payload.get("allowed_widths") or [])
        if value not in (None, "")
    }
    if allowed_widths:
        width = candidate.get("width")
        if width in (None, ""):
            return False
        if round(float(width), 4) not in allowed_widths:
            return False
    return True


__all__ = ["build_candidate_filter", "candidate_matches_filter"]
