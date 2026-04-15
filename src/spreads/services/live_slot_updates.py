from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from spreads.services.value_coercion import as_text


def write_live_session_slot(
    recovery_store: Any,
    *,
    job_key: str,
    session_id: str,
    session_date: str,
    label: str,
    slot_at: str | datetime,
    status: str,
    scheduled_for: str | datetime | None = None,
    job_run_id: str | None = None,
    capture_status: str | None = None,
    recovery_note: str | None = None,
    slot_details: Mapping[str, Any] | None = None,
    queued_at: str | datetime | None = None,
    started_at: str | datetime | None = None,
    finished_at: str | datetime | None = None,
    updated_at: str | datetime | None = None,
) -> Any:
    payload: dict[str, Any] = {
        "job_key": str(job_key),
        "session_id": str(session_id),
        "session_date": str(session_date),
        "label": str(label),
        "slot_at": slot_at,
        "status": status,
        "scheduled_for": scheduled_for,
        "job_run_id": job_run_id,
        "capture_status": capture_status,
        "recovery_note": recovery_note,
        "queued_at": queued_at,
        "started_at": started_at,
        "finished_at": finished_at,
        "updated_at": updated_at,
    }
    if slot_details is not None:
        payload["slot_details"] = dict(slot_details)
    return recovery_store.upsert_live_session_slot(**payload)


def update_live_session_slot_from_row(
    recovery_store: Any,
    *,
    slot_row: Mapping[str, Any],
    status: str,
    recovery_note: str | None = None,
    slot_details: Mapping[str, Any] | None = None,
    finished_at: str | datetime | None = None,
    updated_at: str | datetime | None = None,
) -> Any:
    details = slot_details
    if details is None:
        raw_details = slot_row.get("slot_details")
        if isinstance(raw_details, Mapping):
            details = raw_details
        else:
            raw_details = slot_row.get("slot_details_json")
            details = raw_details if isinstance(raw_details, Mapping) else None
    return write_live_session_slot(
        recovery_store,
        job_key=str(slot_row["job_key"]),
        session_id=str(slot_row["session_id"]),
        session_date=str(slot_row["session_date"]),
        label=str(slot_row["label"]),
        slot_at=str(slot_row["slot_at"]),
        scheduled_for=as_text(slot_row.get("scheduled_for")),
        status=status,
        job_run_id=as_text(slot_row.get("job_run_id")),
        capture_status=as_text(slot_row.get("capture_status")),
        recovery_note=recovery_note,
        slot_details=details,
        queued_at=as_text(slot_row.get("queued_at")),
        started_at=as_text(slot_row.get("started_at")),
        finished_at=finished_at,
        updated_at=updated_at,
    )


__all__ = [
    "update_live_session_slot_from_row",
    "write_live_session_slot",
]
