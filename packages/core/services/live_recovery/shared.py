from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from core.services.value_coercion import as_text as _as_text
from core.storage.serializers import parse_datetime

LIVE_SLOT_STATUS_EXPECTED = "expected"
LIVE_SLOT_STATUS_QUEUED = "queued"
LIVE_SLOT_STATUS_RUNNING = "running"
LIVE_SLOT_STATUS_SUCCEEDED = "succeeded"
LIVE_SLOT_STATUS_MISSED = "missed"
LIVE_SLOT_STATUS_RECOVERED = "recovered_analysis_only"
LIVE_SLOT_STATUS_UNRECOVERABLE = "unrecoverable"
LIVE_SLOT_TERMINAL_STATUSES = {
    LIVE_SLOT_STATUS_SUCCEEDED,
    LIVE_SLOT_STATUS_RECOVERED,
    LIVE_SLOT_STATUS_UNRECOVERABLE,
}

RECOVERY_STATE_CLEAR = "clear"
RECOVERY_STATE_RECONCILING = "reconciling"
RECOVERY_STATE_RECOVERING = "recovering"
RECOVERY_STATE_BLOCKED_WAITING_FRESH_SLOT = "blocked_waiting_fresh_slot"

CAPTURE_TARGET_REASON_PROMOTABLE = "promotable"
CAPTURE_TARGET_REASON_MONITOR = "monitor"
CAPTURE_TARGET_REASON_PENDING_EXECUTION = "pending_execution"
CAPTURE_TARGET_REASON_OPEN_POSITION = "open_position"

CAPTURE_OWNER_LIVE_SESSION = "live_session"
CAPTURE_OWNER_EXECUTION_ATTEMPT = "execution_attempt"
CAPTURE_OWNER_SESSION_POSITION = "session_position"
CAPTURE_OWNER_RECOVERY_SESSION = "recovery_session"

RECOVERY_CONTROL_REASON_CODE = "collector_gap_active"
RECOVERY_CONTROL_CLEAR_REASON_CODE = "collector_gap_cleared"

RECOVERY_TRACKED_REASONS = {
    CAPTURE_TARGET_REASON_PROMOTABLE,
    CAPTURE_TARGET_REASON_MONITOR,
    CAPTURE_TARGET_REASON_PENDING_EXECUTION,
    CAPTURE_TARGET_REASON_OPEN_POSITION,
}
OPEN_POSITION_CAPTURE_STATUSES = [
    "pending_open",
    "partial_open",
    "open",
    "partial_close",
]


def _option_sort_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("underlying_symbol") or ""),
        str(row.get("strategy") or ""),
        str(row.get("option_symbol") or ""),
    )


def _slot_timestamp(row: Mapping[str, Any]) -> datetime | None:
    return parse_datetime(_as_text(row.get("slot_at")))


def _slot_details(row: Mapping[str, Any]) -> dict[str, Any]:
    details = row.get("slot_details")
    if isinstance(details, Mapping):
        return dict(details)
    raw = row.get("slot_details_json")
    if isinstance(raw, Mapping):
        return dict(raw)
    return {}


def _fresh_slot(row: Mapping[str, Any]) -> bool:
    if str(row.get("status") or "") != LIVE_SLOT_STATUS_SUCCEEDED:
        return False
    capture_status = str(row.get("capture_status") or "").strip().lower()
    return capture_status in {"healthy", "idle"}


def _resume_eligible_slot(row: Mapping[str, Any]) -> bool:
    if str(row.get("status") or "") != LIVE_SLOT_STATUS_SUCCEEDED:
        return False
    capture_status = str(row.get("capture_status") or "").strip().lower()
    return capture_status in {"healthy", "baseline_only", "idle"}
