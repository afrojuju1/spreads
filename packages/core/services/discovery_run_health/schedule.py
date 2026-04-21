from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

from core.storage.serializers import parse_datetime


DISCOVERY_RUN_SLOT_GRACE_SECONDS = 120


def evaluate_discovery_run_schedule_health(
    *,
    schedule_summary: Mapping[str, Any] | None,
    latest_run: Mapping[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    payload = (
        schedule_summary if isinstance(schedule_summary, Mapping) else {}
    )
    state = str(payload.get("state") or "")
    expected_slot_at = parse_datetime(payload.get("expected_current_slot_at"))
    if state not in {"active", "settling"} or expected_slot_at is None:
        return {
            "overdue": False,
            "state": state or "unknown",
            "expected_current_slot_at": payload.get("expected_current_slot_at"),
            "lag_slot_count": 0,
            "message": None,
        }

    if now < expected_slot_at + timedelta(seconds=DISCOVERY_RUN_SLOT_GRACE_SECONDS):
        return {
            "overdue": False,
            "state": state,
            "expected_current_slot_at": payload.get("expected_current_slot_at"),
            "lag_slot_count": 0,
            "message": None,
        }

    latest_slot_at = None
    if isinstance(latest_run, Mapping):
        latest_slot_at = parse_datetime(
            latest_run.get("slot_at") or latest_run.get("scheduled_for")
        )
    if latest_slot_at is not None and latest_slot_at >= expected_slot_at:
        return {
            "overdue": False,
            "state": state,
            "expected_current_slot_at": payload.get("expected_current_slot_at"),
            "lag_slot_count": 0,
            "message": None,
        }

    interval_seconds = max(int(payload.get("interval_seconds") or 0), 1)
    lag_slot_count = 1
    if latest_slot_at is not None and expected_slot_at > latest_slot_at:
        lag_slot_count = max(
            int((expected_slot_at - latest_slot_at).total_seconds() // interval_seconds),
            1,
        )
    return {
        "overdue": True,
        "state": state,
        "expected_current_slot_at": payload.get("expected_current_slot_at"),
        "lag_slot_count": lag_slot_count,
        "message": (
            f"Expected discovery slot {payload.get('expected_current_slot_at')} "
            "has not been enqueued or completed."
        ),
    }


__all__ = [
    "DISCOVERY_RUN_SLOT_GRACE_SECONDS",
    "evaluate_discovery_run_schedule_health",
]
