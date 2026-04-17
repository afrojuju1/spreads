from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from core.services.live_collector_health import TRADEABILITY_STATE_RECOVERY_ONLY
from core.services.value_coercion import as_text as _as_text

from .shared import (
    RECOVERY_CONTROL_REASON_CODE,
    RECOVERY_STATE_CLEAR,
    RECOVERY_STATE_RECOVERING,
    LIVE_SLOT_STATUS_MISSED,
    LIVE_SLOT_STATUS_RECOVERED,
    LIVE_SLOT_STATUS_UNRECOVERABLE,
    _fresh_slot,
    _resume_eligible_slot,
    _slot_timestamp,
)


def resolve_live_slot_stale_after_seconds(interval_seconds: int | None) -> int:
    normalized_interval = max(int(interval_seconds or 0), 1)
    return max(normalized_interval, 90)


def summarize_session_slot_health(
    slot_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    ordered_rows = sorted(
        [dict(row) for row in slot_rows],
        key=lambda row: _slot_timestamp(row) or datetime.fromtimestamp(0, UTC),
    )
    latest_slot_at = None
    latest_slot_status = None
    latest_gap_slot_at = None
    latest_fresh_slot_at = None
    latest_resume_slot_at = None
    missed_slot_count = 0
    unrecoverable_slot_count = 0
    recovered_slot_count = 0
    for row in ordered_rows:
        slot_at = _as_text(row.get("slot_at"))
        if slot_at is not None:
            latest_slot_at = slot_at
        latest_slot_status = str(row.get("status") or latest_slot_status or "")
        status = str(row.get("status") or "")
        if status == LIVE_SLOT_STATUS_MISSED:
            missed_slot_count += 1
            latest_gap_slot_at = slot_at or latest_gap_slot_at
        elif status == LIVE_SLOT_STATUS_UNRECOVERABLE:
            unrecoverable_slot_count += 1
            latest_gap_slot_at = slot_at or latest_gap_slot_at
        elif status == LIVE_SLOT_STATUS_RECOVERED:
            recovered_slot_count += 1
            latest_gap_slot_at = slot_at or latest_gap_slot_at
        if _fresh_slot(row):
            latest_fresh_slot_at = slot_at or latest_fresh_slot_at
        if _resume_eligible_slot(row):
            latest_resume_slot_at = slot_at or latest_resume_slot_at

    unresolved_gap_active = missed_slot_count > 0
    recovery_state = (
        RECOVERY_STATE_RECOVERING if unresolved_gap_active else RECOVERY_STATE_CLEAR
    )
    gap_active = recovery_state != RECOVERY_STATE_CLEAR
    return {
        "gap_active": gap_active,
        "recovery_state": recovery_state,
        "missed_slot_count": missed_slot_count,
        "recovered_slot_count": recovered_slot_count,
        "unrecoverable_slot_count": unrecoverable_slot_count,
        "latest_slot_at": latest_slot_at,
        "latest_slot_status": latest_slot_status,
        "latest_gap_slot_at": latest_gap_slot_at,
        "latest_fresh_slot_at": latest_fresh_slot_at,
        "latest_resume_slot_at": latest_resume_slot_at,
    }


def list_session_slot_health_by_session_id(
    *,
    recovery_store: Any,
    session_ids: list[str],
    session_date: str | None = None,
) -> dict[str, dict[str, Any]]:
    if not session_ids or not recovery_store.schema_ready():
        return {}
    rows = recovery_store.list_live_session_slots(
        session_ids=session_ids,
        session_date=session_date,
        limit=max(len(session_ids) * 500, 1000),
        ascending=True,
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        session_id = _as_text(row.get("session_id"))
        if session_id is None:
            continue
        grouped[session_id].append(dict(row))
    return {
        session_id: summarize_session_slot_health(slot_rows)
        for session_id, slot_rows in grouped.items()
    }


def load_session_slot_health(
    *,
    recovery_store: Any,
    session_id: str,
) -> dict[str, Any]:
    if not recovery_store.schema_ready():
        return {
            "gap_active": False,
            "recovery_state": RECOVERY_STATE_CLEAR,
            "missed_slot_count": 0,
            "recovered_slot_count": 0,
            "unrecoverable_slot_count": 0,
            "latest_slot_at": None,
            "latest_slot_status": None,
            "latest_gap_slot_at": None,
            "latest_fresh_slot_at": None,
            "latest_resume_slot_at": None,
        }
    rows = recovery_store.list_live_session_slots(
        session_id=session_id,
        limit=500,
        ascending=True,
    )
    return summarize_session_slot_health(rows)


def merge_live_action_gate_with_recovery(
    *,
    base_gate: Mapping[str, Any] | None,
    slot_health: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    gate = {} if not isinstance(base_gate, Mapping) else dict(base_gate)
    if not isinstance(slot_health, Mapping):
        return None if not gate else gate
    recovery_state = str(slot_health.get("recovery_state") or RECOVERY_STATE_CLEAR)
    if recovery_state == RECOVERY_STATE_CLEAR:
        return None if not gate else gate
    return {
        "status": "blocked",
        "reason_code": RECOVERY_CONTROL_REASON_CODE,
        "message": (
            "Live actions are blocked while collector gaps are being reconciled."
            if recovery_state == RECOVERY_STATE_RECOVERING
            else "Live actions stay blocked until one successful post-gap live slot completes."
        ),
        "allow_alerts": False,
        "allow_auto_execution": False,
        "tradeability_state": TRADEABILITY_STATE_RECOVERY_ONLY,
        "recovery_state": recovery_state,
        "gap_active": bool(slot_health.get("gap_active")),
        "missed_slot_count": int(slot_health.get("missed_slot_count") or 0),
        "unrecoverable_slot_count": int(
            slot_health.get("unrecoverable_slot_count") or 0
        ),
    }
