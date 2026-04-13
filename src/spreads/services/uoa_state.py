from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from spreads.db.decorators import with_storage
from spreads.services.live_collector_health import enrich_live_collector_job_run_payload
from spreads.services.selection_terms import normalize_uoa_decision_state


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


def _normalize_uoa_root(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    decision_state = normalize_uoa_decision_state(payload.get("decision_state"))
    if decision_state is not None:
        payload["decision_state"] = decision_state
    return payload


def _normalize_uoa_decisions_payload(payload: Any) -> dict[str, Any]:
    source = {} if not isinstance(payload, Mapping) else dict(payload)
    overview = (
        {}
        if not isinstance(source.get("overview"), Mapping)
        else dict(source.get("overview"))
    )
    normalized_overview = {
        **overview,
        "monitor_count": overview.get("monitor_count", overview.get("watchlist_count")),
        "promotable_count": overview.get(
            "promotable_count", overview.get("board_count")
        ),
    }
    roots = [
        _normalize_uoa_root(item)
        for item in list(source.get("roots") or [])
        if isinstance(item, Mapping)
    ]
    top_monitor_roots = [
        _normalize_uoa_root(item)
        for item in list(
            source.get("top_monitor_roots", source.get("top_watchlist_roots")) or []
        )
        if isinstance(item, Mapping)
    ]
    top_promotable_roots = [
        _normalize_uoa_root(item)
        for item in list(
            source.get("top_promotable_roots", source.get("top_board_roots")) or []
        )
        if isinstance(item, Mapping)
    ]
    top_high_roots = [
        _normalize_uoa_root(item)
        for item in list(source.get("top_high_roots") or [])
        if isinstance(item, Mapping)
    ]
    return {
        **source,
        "overview": normalized_overview,
        "roots": roots,
        "top_monitor_roots": top_monitor_roots,
        "top_promotable_roots": top_promotable_roots,
        "top_high_roots": top_high_roots,
    }


def _build_uoa_state_payload(
    *,
    run_record: Mapping[str, Any],
    collector_store: Any,
) -> dict[str, Any]:
    run_payload = enrich_live_collector_job_run_payload(run_record)
    if run_payload is None:
        raise ValueError("Job run payload is unavailable")
    cycle_id = _cycle_id_from_run_payload(run_payload)
    if cycle_id is None:
        raise ValueError("Live collector run is missing cycle_id")
    cycle = _collector_cycle_payload(collector_store, cycle_id=cycle_id)
    opportunities = collector_store.list_cycle_candidates(cycle_id)
    selection_counts = {
        "promotable": 0,
        "monitor": 0,
    }
    for row in opportunities:
        if str(row.get("eligibility") or "live") != "live":
            continue
        selection_state = str(row.get("selection_state") or "")
        if selection_state in selection_counts:
            selection_counts[selection_state] += 1
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
        "uoa_decisions": _normalize_uoa_decisions_payload(
            run_payload.get("uoa_decisions")
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
        run_record=run_record, collector_store=storage.collector
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
        run_record=run_record, collector_store=storage.collector
    )
