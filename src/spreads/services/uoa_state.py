from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from spreads.db.decorators import with_storage
from spreads.services.live_runtime import get_live_session_for_cycle


def _cycle_id_from_run_payload(run_payload: Mapping[str, Any]) -> str | None:
    result = run_payload.get("result")
    if isinstance(result, Mapping):
        cycle_id = str(result.get("cycle_id") or "").strip()
        if cycle_id:
            return cycle_id
        cycle_ids = result.get("cycle_ids")
        if isinstance(cycle_ids, Sequence) and not isinstance(cycle_ids, (str, bytes)):
            for item in cycle_ids:
                rendered = str(item or "").strip()
                if rendered:
                    return rendered
    rendered = str(run_payload.get("cycle_id") or "").strip()
    return rendered or None


def _build_uoa_state_payload(state: Mapping[str, Any]) -> dict[str, Any]:
    job_run = state.get("job_run")
    if not isinstance(job_run, Mapping):
        raise ValueError("No completed live collector run was found")
    return {
        "job_run": {
            "job_run_id": job_run.get("job_run_id"),
            "job_key": job_run.get("job_key"),
            "job_type": job_run.get("job_type"),
            "status": job_run.get("status"),
            "scheduled_for": job_run.get("scheduled_for"),
            "started_at": job_run.get("started_at"),
            "finished_at": job_run.get("finished_at"),
            "session_id": job_run.get("session_id"),
            "slot_at": job_run.get("slot_at"),
            "worker_name": job_run.get("worker_name"),
        },
        "cycle": (
            dict(state.get("cycle") or {})
            if isinstance(state.get("cycle"), Mapping)
            else None
        ),
        "quote_capture": dict(state.get("quote_capture") or {}),
        "trade_capture": dict(state.get("trade_capture") or {}),
        "uoa_summary": dict(state.get("uoa_summary") or {}),
        "uoa_quote_summary": dict(state.get("uoa_quote_summary") or {}),
        "uoa_decisions": dict(state.get("uoa_decisions") or {}),
        "selection_summary": dict(state.get("selection_summary") or {}),
        "opportunities": [
            dict(item) for item in list(state.get("opportunities") or [])
        ],
        "selection_counts": dict(state.get("selection_counts") or {}),
        "cycle_events": [
            dict(item) for item in list(state.get("cycle_events") or [])
        ],
    }


@with_storage()
def get_latest_uoa_state(
    *,
    db_target: str | None = None,
    label: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    run_record = storage.jobs.get_latest_live_collector_run(
        label=label, status="succeeded"
    )
    if run_record is None:
        raise ValueError("No completed live collector run was found")
    cycle_id = _cycle_id_from_run_payload(run_record)
    if cycle_id is None:
        raise ValueError("Live collector run is missing cycle_id")
    state = get_live_session_for_cycle(
        storage=storage,
        cycle_id=cycle_id,
        label=label,
    )
    return _build_uoa_state_payload(state)


@with_storage()
def get_uoa_state_for_cycle(
    *,
    db_target: str | None = None,
    cycle_id: str,
    label: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    state = get_live_session_for_cycle(
        storage=storage,
        cycle_id=cycle_id,
        label=label,
    )
    if state.get("job_run") is None:
        raise ValueError(
            f"No completed live collector run was found for cycle_id={cycle_id}"
        )
    return _build_uoa_state_payload(state)
