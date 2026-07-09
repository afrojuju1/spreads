from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from core.value_coercion import as_mapping

TOP_BLOCKER_LIMIT = 10


def top_blockers(counter: Counter[str]) -> dict[str, int]:
    return dict(counter.most_common(TOP_BLOCKER_LIMIT))


def sorted_counts(counter: Counter[str]) -> dict[str, int]:
    return dict(sorted((str(key), int(count)) for key, count in counter.items() if int(count) > 0))


def bump_count(mapping: dict[str, int], value: Any, count: int = 1) -> None:
    key = str(value or "unknown")
    mapping[key] = int(mapping.get(key, 0)) + int(count)


def newer_desc_asc(value_at: datetime | None, value_id: Any, current_at: datetime | None, current_id: Any) -> bool:
    if value_at is None:
        return False
    if current_at is None:
        return True
    if value_at != current_at:
        return value_at > current_at
    return str(value_id or "") < str(current_id or "")


def newer_desc_desc(value_at: datetime | None, value_id: Any, current_at: datetime | None, current_id: Any) -> bool:
    if value_at is None:
        return False
    if current_at is None:
        return True
    if value_at != current_at:
        return value_at > current_at
    return str(value_id or "") > str(current_id or "")


def set_latest_activity(latest_activity: dict[str, datetime], strategy_id: str, value: datetime | None) -> None:
    if value is not None and (strategy_id not in latest_activity or value > latest_activity[strategy_id]):
        latest_activity[strategy_id] = value


def sort_nested_counts(mapping: Mapping[str, Any]) -> dict[str, dict[str, int]]:
    sorted_mapping: dict[str, dict[str, int]] = {}
    for outer_key, inner_mapping in sorted(as_mapping(mapping).items()):
        sorted_mapping[str(outer_key)] = dict(sorted((str(inner_key), int(count)) for inner_key, count in as_mapping(inner_mapping).items()))
    return sorted_mapping
