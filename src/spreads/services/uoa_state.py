from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from spreads.db.decorators import with_storage
from spreads.services.live_collector_health import (
    build_selection_summary,
    enrich_live_collector_job_run_payload,
    normalize_uoa_decisions_payload,
)
from spreads.services.opportunities import list_active_cycle_opportunity_rows
from spreads.services.selection_summary import live_selection_counts


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


def _collector_cycle_payload(
    collector_store: Any, *, cycle_id: str
) -> dict[str, Any] | None:
    cycle = collector_store.get_cycle(cycle_id)
    return None if cycle is None else dict(cycle)


def _build_uoa_state_payload(
    *,
    run_record: Mapping[str, Any],
    collector_store: Any,
    signal_store: Any,
) -> dict[str, Any]:
    run_payload = enrich_live_collector_job_run_payload(run_record)
    if run_payload is None:
        raise ValueError("Job run payload is unavailable")
    cycle_id = _cycle_id_from_run_payload(run_payload)
    if cycle_id is None:
        raise ValueError("Live collector run is missing cycle_id")
    cycle = _collector_cycle_payload(collector_store, cycle_id=cycle_id)
    opportunities = list_active_cycle_opportunity_rows(
        signal_store,
        cycle_id=cycle_id,
        pipeline_id=(
            None
            if cycle is None
            else str(cycle.get("pipeline_id") or f"pipeline:{str(cycle['label']).lower()}")
        ),
        market_date=None if cycle is None else str(cycle.get("session_date") or ""),
    )
    signal_schema_ready = bool(
        signal_store is not None
        and hasattr(signal_store, "schema_ready")
        and signal_store.schema_ready()
    )
    if not opportunities and not signal_schema_ready:
        opportunities = [dict(item) for item in collector_store.list_cycle_candidates(cycle_id)]
    selection_counts = live_selection_counts(opportunities)
    cycle_events = collector_store.list_cycle_events(cycle_id)
    return {
        "job_run": {
            "job_run_id": run_payload.get("job_run_id"),
            "job_key": run_payload.get("job_key"),
            "job_type": run_payload.get("job_type"),
            "status": run_payload.get("status"),
            "scheduled_for": run_payload.get("scheduled_for"),
            "started_at": run_payload.get("started_at"),
            "finished_at": run_payload.get("finished_at"),
            "session_id": run_payload.get("session_id"),
            "slot_at": run_payload.get("slot_at"),
            "worker_name": run_payload.get("worker_name"),
        },
        "cycle": cycle,
        "quote_capture": dict(run_payload.get("quote_capture") or {}),
        "trade_capture": dict(run_payload.get("trade_capture") or {}),
        "uoa_summary": dict(run_payload.get("uoa_summary") or {}),
        "uoa_quote_summary": dict(run_payload.get("uoa_quote_summary") or {}),
        "uoa_decisions": normalize_uoa_decisions_payload(
            run_payload.get("uoa_decisions")
        ),
        "selection_summary": (
            dict(run_payload.get("selection_summary") or {})
            if run_payload.get("selection_summary")
            else build_selection_summary(opportunities)
        ),
        "opportunities": [dict(item) for item in opportunities],
        "selection_counts": selection_counts,
        "cycle_events": [dict(item) for item in cycle_events],
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
    return _build_uoa_state_payload(
        run_record=run_record,
        collector_store=storage.collector,
        signal_store=storage.signals,
    )


@with_storage()
def get_uoa_state_for_cycle(
    *,
    db_target: str | None = None,
    cycle_id: str,
    label: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    run_record = storage.jobs.get_live_collector_run_by_cycle_id(
        cycle_id=cycle_id,
        label=label,
        status="succeeded",
    )
    if run_record is None:
        raise ValueError(
            f"No completed live collector run was found for cycle_id={cycle_id}"
        )
    return _build_uoa_state_payload(
        run_record=run_record,
        collector_store=storage.collector,
        signal_store=storage.signals,
    )
