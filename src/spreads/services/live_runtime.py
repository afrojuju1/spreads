from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from spreads.services.execution import normalize_execution_policy
from spreads.services.live_collector_health import (
    build_selection_summary,
    enrich_live_collector_job_run_payload,
    normalize_uoa_decisions_payload,
)
from spreads.services.live_pipelines import list_enabled_live_collector_pipelines
from spreads.services.live_recovery import (
    list_session_slot_health_by_session_id,
    load_session_slot_health,
)
from spreads.services.opportunities import list_active_cycle_opportunity_rows
from spreads.services.runtime_identity import (
    build_live_run_scope_id,
    build_pipeline_id,
    parse_pipeline_id,
    resolve_pipeline_policy_fields,
)
from spreads.services.selection_summary import live_selection_counts
from spreads.storage.serializers import parse_datetime


def list_latest_live_sessions(
    *,
    storage: Any,
    market_date: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    collector_store = storage.collector
    if not _collector_schema_ready(collector_store):
        return []

    job_store = storage.jobs
    signal_store = storage.signals
    pipeline_rows = _list_runtime_pipelines(job_store)
    if not pipeline_rows:
        pipeline_rows = [
            dict(row)
            for row in collector_store.list_pipelines(limit=max(limit, 1))
        ]
    latest_cycles = _latest_cycles_for_market_date(
        collector_store=collector_store,
        pipeline_rows=pipeline_rows,
        market_date=market_date,
    )
    latest_cycle_by_pipeline_id = {
        str(row["pipeline_id"]): row for row in latest_cycles
    }
    session_ids = [
        build_live_run_scope_id(str(row["label"]), str(row["market_date"]))
        for row in latest_cycles
    ]
    slot_health_by_session_id = list_session_slot_health_by_session_id(
        recovery_store=storage.recovery,
        session_ids=session_ids,
        session_date=market_date,
    )
    latest_runs = [
        enrich_live_collector_job_run_payload(row)
        for row in job_store.list_latest_runs_by_session_ids(
            session_ids=session_ids,
            job_type="live_collector",
        )
    ]
    latest_runs_by_session_id = {
        str(row["session_id"]): row for row in latest_runs if row.get("session_id")
    }
    candidate_counts_by_cycle_id = _candidate_counts_by_cycle_id(
        collector_store=collector_store,
        signal_store=signal_store,
        cycle_ids=[str(row["cycle_id"]) for row in latest_cycles],
    )

    sessions: list[dict[str, Any]] = []
    for pipeline in pipeline_rows:
        latest_cycle = latest_cycle_by_pipeline_id.get(str(pipeline["pipeline_id"]))
        if latest_cycle is None:
            continue
        session_id = build_live_run_scope_id(
            str(latest_cycle["label"]),
            str(latest_cycle["market_date"]),
        )
        summary_run = _load_summary_run(
            job_store,
            cycle_id=str(latest_cycle["cycle_id"]),
            label=str(latest_cycle["label"]),
        )
        selection_summary = (
            dict(summary_run.get("selection_summary") or {})
            if isinstance(summary_run, Mapping)
            and isinstance(summary_run.get("selection_summary"), Mapping)
            else None
        )
        sessions.append(
            {
                "pipeline": dict(pipeline),
                "cycle": dict(latest_cycle),
                "label": str(latest_cycle["label"]),
                "market_date": str(latest_cycle["market_date"]),
                "session_id": session_id,
                "latest_run": latest_runs_by_session_id.get(session_id),
                "job_run": _job_run_payload(summary_run),
                "selection_summary": selection_summary,
                "quote_capture": {}
                if summary_run is None
                else dict(summary_run.get("quote_capture") or {}),
                "trade_capture": {}
                if summary_run is None
                else dict(summary_run.get("trade_capture") or {}),
                "uoa_summary": {}
                if summary_run is None
                else dict(summary_run.get("uoa_summary") or {}),
                "uoa_quote_summary": {}
                if summary_run is None
                else dict(summary_run.get("uoa_quote_summary") or {}),
                "uoa_decisions": normalize_uoa_decisions_payload(
                    None if summary_run is None else summary_run.get("uoa_decisions")
                ),
                "slot_health": dict(slot_health_by_session_id.get(session_id) or {}),
                "candidate_counts": dict(
                    candidate_counts_by_cycle_id.get(str(latest_cycle["cycle_id"]), {})
                ),
            }
        )
    return sessions


def get_live_session(
    *,
    storage: Any,
    pipeline_id: str,
    market_date: str | None = None,
) -> dict[str, Any]:
    collector_store = storage.collector
    if not _collector_schema_ready(collector_store):
        raise ValueError(f"Unknown pipeline_id: {pipeline_id}")

    pipeline = _resolve_runtime_pipeline(
        collector_store=collector_store,
        job_store=storage.jobs,
        pipeline_id=pipeline_id,
    )
    if pipeline is None:
        raise ValueError(f"Unknown pipeline_id: {pipeline_id}")

    label = str(pipeline["label"])
    cycle = _resolve_cycle(
        collector_store=collector_store,
        label=label,
        market_date=market_date,
    )
    if cycle is None:
        raise ValueError(f"Unknown pipeline_id: {pipeline_id}")

    return _build_live_session_state(
        storage=storage,
        pipeline=dict(pipeline),
        cycle=dict(cycle),
    )


def get_live_session_for_cycle(
    *,
    storage: Any,
    cycle_id: str,
    label: str | None = None,
) -> dict[str, Any]:
    collector_store = storage.collector
    if not _collector_schema_ready(collector_store):
        raise ValueError(f"Unknown cycle_id: {cycle_id}")

    cycle = collector_store.get_cycle(cycle_id)
    if cycle is None:
        raise ValueError(f"Unknown cycle_id: {cycle_id}")
    cycle_payload = dict(cycle)
    resolved_label = label or str(cycle_payload.get("label") or "")
    if not resolved_label:
        raise ValueError(f"Unknown cycle_id: {cycle_id}")
    pipeline_id = str(
        cycle_payload.get("pipeline_id") or build_pipeline_id(resolved_label)
    )
    pipeline = _resolve_runtime_pipeline(
        collector_store=collector_store,
        job_store=storage.jobs,
        pipeline_id=pipeline_id,
    )
    if pipeline is None:
        pipeline = {
            "pipeline_id": pipeline_id,
            "label": resolved_label,
        }

    return _build_live_session_state(
        storage=storage,
        pipeline=dict(pipeline),
        cycle=cycle_payload,
    )


def _build_live_session_state(
    *,
    storage: Any,
    pipeline: Mapping[str, Any],
    cycle: Mapping[str, Any],
) -> dict[str, Any]:
    collector_store = storage.collector
    job_store = storage.jobs
    recovery_store = storage.recovery
    signal_store = storage.signals

    pipeline_id = str(
        cycle.get("pipeline_id") or pipeline.get("pipeline_id") or ""
    ) or build_pipeline_id(str(cycle["label"]))
    label = str(cycle["label"])
    market_date = str(cycle.get("market_date") or cycle.get("session_date") or "")
    cycle_id = str(cycle["cycle_id"])
    session_id = build_live_run_scope_id(label, market_date)

    slot_runs = _sort_session_runs(
        enrich_live_collector_job_run_payload(row)
        for row in job_store.list_job_runs(
            job_type="live_collector",
            session_id=session_id,
            limit=500,
        )
    )
    latest_run = slot_runs[0] if slot_runs else None
    summary_run = _load_summary_run(job_store, cycle_id=cycle_id, label=label)
    opportunities = _cycle_opportunity_payloads(
        collector_store,
        signal_store,
        pipeline_id=pipeline_id,
        market_date=market_date,
        cycle_id=cycle_id,
    )
    selection_counts = live_selection_counts(opportunities)
    candidate_counts = {
        "candidate_count": len(opportunities),
        "promotable": int(selection_counts.get("promotable") or 0),
        "monitor": int(selection_counts.get("monitor") or 0),
    }
    cycle_events = [
        dict(row) for row in list(collector_store.list_cycle_events(cycle_id) or [])
    ]
    run_payload = summary_run
    selection_summary = (
        None
        if run_payload is None
        else (
            dict(run_payload.get("selection_summary") or {})
            if isinstance(run_payload.get("selection_summary"), Mapping)
            else None
        )
    )
    if selection_summary is None:
        selection_summary = build_selection_summary(opportunities)
    recovery_slots = (
        []
        if not recovery_store.schema_ready()
        else [
            dict(row)
            for row in recovery_store.list_live_session_slots(
                session_id=session_id,
                limit=50,
            )
        ]
    )
    live_opportunities = [
        row
        for row in opportunities
        if str(row.get("eligibility") or "live") == "live"
    ]
    analysis_only_opportunities = [
        row
        for row in opportunities
        if str(row.get("eligibility") or "live") != "live"
    ]

    return {
        "pipeline": dict(pipeline),
        "cycle": dict(cycle),
        "label": label,
        "market_date": market_date,
        "session_id": session_id,
        "latest_run": latest_run,
        "job_run": _job_run_payload(run_payload),
        "slot_runs": slot_runs,
        "slot_health": load_session_slot_health(
            recovery_store=recovery_store,
            session_id=session_id,
        ),
        "recovery_slots": recovery_slots,
        "opportunities": opportunities,
        "live_opportunities": live_opportunities,
        "analysis_only_opportunities": analysis_only_opportunities,
        "candidate_counts": candidate_counts,
        "selection_counts": selection_counts,
        "selection_summary": selection_summary,
        "cycle_events": cycle_events,
        "quote_capture": {} if run_payload is None else dict(run_payload.get("quote_capture") or {}),
        "trade_capture": {} if run_payload is None else dict(run_payload.get("trade_capture") or {}),
        "uoa_summary": {} if run_payload is None else dict(run_payload.get("uoa_summary") or {}),
        "uoa_quote_summary": {} if run_payload is None else dict(run_payload.get("uoa_quote_summary") or {}),
        "uoa_decisions": normalize_uoa_decisions_payload(
            None if run_payload is None else run_payload.get("uoa_decisions")
        ),
    }


def _collector_schema_ready(collector_store: Any) -> bool:
    if hasattr(collector_store, "schema_ready"):
        return bool(collector_store.schema_ready())
    return bool(collector_store.pipeline_schema_ready())


def _runtime_pipeline_row(catalog_entry: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(catalog_entry.get("payload") or {})
    universe_label = str(payload.get("universe") or "")
    policy_fields = resolve_pipeline_policy_fields(
        profile=payload.get("profile"),
        universe_label=universe_label,
    )
    execution_policy = normalize_execution_policy(
        {
            "execution_policy": payload.get("execution_policy"),
            "risk_policy": payload.get("risk_policy"),
        }
    )
    return {
        "pipeline_id": str(catalog_entry["pipeline_id"]),
        "label": str(catalog_entry["label"]),
        "name": str(catalog_entry.get("label") or catalog_entry["pipeline_id"]),
        "source_job_key": catalog_entry.get("job_key"),
        "enabled": True,
        "universe_label": universe_label,
        "style_profile": str(policy_fields["style_profile"]),
        "default_horizon_intent": str(policy_fields["horizon_intent"]),
        "product_scope_json": {
            "product_class": str(policy_fields["product_class"]),
            "legacy_labels": [str(catalog_entry["label"])],
        },
        "policy_json": {
            "legacy_profile": payload.get("profile"),
            "strategy_mode": payload.get("strategy"),
            "greeks_source": payload.get("greeks_source"),
            "deployment_mode": execution_policy.get("deployment_mode"),
        },
        "updated_at": None,
    }


def _list_runtime_pipelines(job_store: Any) -> list[dict[str, Any]]:
    if not hasattr(job_store, "list_job_definitions"):
        return []
    definitions = job_store.list_job_definitions(
        enabled_only=True,
        job_type="live_collector",
    )
    return [
        _runtime_pipeline_row(row)
        for row in list_enabled_live_collector_pipelines(definitions)
    ]


def _resolve_runtime_pipeline(
    *,
    collector_store: Any,
    job_store: Any,
    pipeline_id: str,
) -> dict[str, Any] | None:
    parsed = parse_pipeline_id(pipeline_id)
    if parsed is None:
        return None
    pipeline = next(
        (
            row
            for row in _list_runtime_pipelines(job_store)
            if str(row["pipeline_id"]) == pipeline_id
        ),
        None,
    )
    if pipeline is None and hasattr(collector_store, "get_pipeline"):
        pipeline = collector_store.get_pipeline(pipeline_id)
    return None if pipeline is None else dict(pipeline)


def _resolve_cycle(
    *,
    collector_store: Any,
    label: str,
    market_date: str | None,
) -> dict[str, Any] | None:
    if market_date is None:
        cycle = collector_store.get_latest_cycle(label)
    else:
        cycles = collector_store.list_cycles(
            label,
            session_date=market_date,
            limit=1,
        )
        cycle = None if not cycles else cycles[0]
    return None if cycle is None else dict(cycle)


def _latest_cycles_for_market_date(
    *,
    collector_store: Any,
    pipeline_rows: list[Mapping[str, Any]],
    market_date: str | None,
) -> list[dict[str, Any]]:
    resolved_cycles: list[dict[str, Any]] = []
    for row in pipeline_rows:
        label = str(row.get("label") or "")
        if not label:
            continue
        latest_cycle = _resolve_cycle(
            collector_store=collector_store,
            label=label,
            market_date=market_date,
        )
        if latest_cycle is None:
            continue
        latest_cycle.setdefault("pipeline_id", str(row["pipeline_id"]))
        latest_cycle["market_date"] = str(
            latest_cycle.get("market_date") or latest_cycle.get("session_date") or ""
        )
        resolved_cycles.append(latest_cycle)
    return resolved_cycles


def _candidate_counts_by_cycle_id(
    *,
    collector_store: Any,
    signal_store: Any,
    cycle_ids: list[str],
) -> dict[str, dict[str, int]]:
    if (
        signal_store is not None
        and hasattr(signal_store, "schema_ready")
        and signal_store.schema_ready()
        and hasattr(signal_store, "count_active_cycle_opportunities_by_cycle_ids")
    ):
        return signal_store.count_active_cycle_opportunities_by_cycle_ids(cycle_ids)
    return collector_store.count_cycle_candidates_by_cycle_ids(cycle_ids)


def _cycle_opportunity_payloads(
    collector_store: Any,
    signal_store: Any,
    *,
    pipeline_id: str,
    market_date: str,
    cycle_id: str,
) -> list[dict[str, Any]]:
    signal_schema_ready = bool(
        signal_store is not None
        and hasattr(signal_store, "schema_ready")
        and signal_store.schema_ready()
    )
    opportunities = list_active_cycle_opportunity_rows(
        signal_store,
        cycle_id=cycle_id,
        pipeline_id=pipeline_id,
        market_date=market_date,
        limit=500,
    )
    if not opportunities and not signal_schema_ready:
        opportunities = [
            dict(candidate) for candidate in collector_store.list_cycle_candidates(cycle_id)
        ]
    return opportunities


def _load_summary_run(
    job_store: Any,
    *,
    cycle_id: str,
    label: str,
) -> dict[str, Any] | None:
    if not hasattr(job_store, "get_live_collector_run_by_cycle_id"):
        return None
    run_record = job_store.get_live_collector_run_by_cycle_id(
        cycle_id=cycle_id,
        label=label,
        status="succeeded",
    )
    return None if run_record is None else enrich_live_collector_job_run_payload(run_record)


def _job_run_payload(run_payload: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if run_payload is None:
        return None
    return {
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
    }


def _parse_sort_value(value: str | None):
    normalized = value.strip() if isinstance(value, str) else None
    parsed = parse_datetime(normalized) if normalized else None
    return parsed or parse_datetime("1970-01-01T00:00:00Z")


def _sort_session_runs(runs: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    enriched = [dict(run) for run in runs]
    enriched.sort(
        key=lambda run: _parse_sort_value(
            None
            if not (run.get("slot_at") or run.get("scheduled_for"))
            else str(run.get("slot_at") or run.get("scheduled_for"))
        ),
        reverse=True,
    )
    return enriched


__all__ = [
    "get_live_session",
    "get_live_session_for_cycle",
    "list_latest_live_sessions",
]
