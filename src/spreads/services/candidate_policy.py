from __future__ import annotations

from typing import Any, Mapping


def candidate_requires_favorable_setup(candidate: Mapping[str, Any]) -> bool:
    return str(candidate.get("profile") or "").lower() == "0dte"


def candidate_has_intraday_setup_context(candidate: Mapping[str, Any]) -> bool:
    if bool(candidate.get("setup_has_intraday_context")):
        return True
    score = candidate.get("setup_intraday_score")
    if score not in (None, ""):
        return True
    minutes = candidate.get("setup_intraday_minutes")
    try:
        return int(float(minutes)) > 0
    except (TypeError, ValueError):
        return False


__all__ = [
    "candidate_has_intraday_setup_context",
    "candidate_requires_favorable_setup",
]
