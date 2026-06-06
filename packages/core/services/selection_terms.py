from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

PROMOTABLE_SELECTION_STATE = "promotable"
MONITOR_SELECTION_STATE = "monitor"
SELECTION_STATES = (
    PROMOTABLE_SELECTION_STATE,
    MONITOR_SELECTION_STATE,
)
LEGACY_SELECTION_STATE_ALIASES = {
    "board": PROMOTABLE_SELECTION_STATE,
    "watchlist": MONITOR_SELECTION_STATE,
    "recovered": MONITOR_SELECTION_STATE,
}
SELECTION_STATE_ORDER = {
    PROMOTABLE_SELECTION_STATE: 0,
    MONITOR_SELECTION_STATE: 1,
}

def normalize_selection_state(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip().lower()
    if not rendered:
        return None
    if rendered in SELECTION_STATE_ORDER:
        return rendered
    return LEGACY_SELECTION_STATE_ALIASES.get(rendered)


def selection_state_rank(value: Any) -> int:
    normalized = normalize_selection_state(value)
    if normalized is None:
        return len(SELECTION_STATE_ORDER)
    return SELECTION_STATE_ORDER[normalized]


def selection_state_counts(
    rows: Iterable[Mapping[str, Any]],
    *,
    field: str = "selection_state",
) -> dict[str, int]:
    counts = {state: 0 for state in SELECTION_STATES}
    for row in rows:
        normalized = normalize_selection_state(row.get(field))
        if normalized in counts:
            counts[normalized] += 1
    return counts


def promotable_monitor_pnl_spread(values: Mapping[str, Any] | None) -> float | None:
    if not isinstance(values, Mapping):
        return None
    promotable = values.get(
        PROMOTABLE_SELECTION_STATE, values.get("board")
    )
    monitor = values.get(
        MONITOR_SELECTION_STATE, values.get("watchlist")
    )
    if promotable in (None, "") or monitor in (None, ""):
        return None
    return round(float(promotable) - float(monitor), 2)


__all__ = [
    "LEGACY_SELECTION_STATE_ALIASES",
    "MONITOR_SELECTION_STATE",
    "PROMOTABLE_SELECTION_STATE",
    "SELECTION_STATES",
    "selection_state_counts",
    "selection_state_rank",
    "normalize_selection_state",
    "promotable_monitor_pnl_spread",
]
