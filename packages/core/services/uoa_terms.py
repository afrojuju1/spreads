from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

UOA_NONE_DECISION_STATE = "none"
UOA_EMERGING_DECISION_STATE = "emerging"
UOA_NOTABLE_DECISION_STATE = "notable"
UOA_HIGH_DECISION_STATE = "high"
UOA_CRITICAL_DECISION_STATE = "critical"

UOA_DECISION_STATES = (
    UOA_EMERGING_DECISION_STATE,
    UOA_NOTABLE_DECISION_STATE,
    UOA_HIGH_DECISION_STATE,
    UOA_CRITICAL_DECISION_STATE,
)

LEGACY_UOA_DECISION_STATE_ALIASES = {
    "watchlist": UOA_EMERGING_DECISION_STATE,
    "monitor": UOA_EMERGING_DECISION_STATE,
    "board": UOA_NOTABLE_DECISION_STATE,
    "promotable": UOA_NOTABLE_DECISION_STATE,
}

UOA_DECISION_STATE_ORDER = {
    UOA_NONE_DECISION_STATE: 0,
    UOA_EMERGING_DECISION_STATE: 1,
    UOA_NOTABLE_DECISION_STATE: 2,
    UOA_HIGH_DECISION_STATE: 3,
    UOA_CRITICAL_DECISION_STATE: 4,
}


def normalize_uoa_decision_state(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip().lower()
    if not rendered:
        return None
    if rendered in UOA_DECISION_STATE_ORDER:
        return rendered
    return LEGACY_UOA_DECISION_STATE_ALIASES.get(rendered)


def uoa_decision_state_rank(value: Any) -> int:
    normalized = normalize_uoa_decision_state(value)
    if normalized is None:
        return len(UOA_DECISION_STATE_ORDER)
    return UOA_DECISION_STATE_ORDER[normalized]


def uoa_decision_counts(
    rows: Iterable[Mapping[str, Any]],
    *,
    field: str = "decision_state",
) -> dict[str, int]:
    counts = {state: 0 for state in UOA_DECISION_STATES}
    for row in rows:
        normalized = normalize_uoa_decision_state(row.get(field))
        if normalized in counts:
            counts[normalized] += 1
    return counts


__all__ = [
    "LEGACY_UOA_DECISION_STATE_ALIASES",
    "UOA_CRITICAL_DECISION_STATE",
    "UOA_DECISION_STATES",
    "UOA_EMERGING_DECISION_STATE",
    "UOA_HIGH_DECISION_STATE",
    "UOA_NONE_DECISION_STATE",
    "UOA_NOTABLE_DECISION_STATE",
    "normalize_uoa_decision_state",
    "uoa_decision_counts",
    "uoa_decision_state_rank",
]
