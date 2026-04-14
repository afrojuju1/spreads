from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from spreads.db.decorators import with_storage
from spreads.services.analysis import build_session_summary
from spreads.services.control_plane import get_control_state_snapshot
from spreads.services.execution import list_session_execution_attempts
from spreads.services.execution_portfolio import build_session_execution_portfolio
from spreads.services.live_collector_health import (
    build_tradeability_summary,
    enrich_live_collector_job_run_payload,
)
from spreads.services.opportunity_replay import (
    OpportunityReplayLookupError,
    build_opportunity_replay,
    build_recent_opportunity_replay_batch,
)
from spreads.services.live_recovery import (
    list_session_slot_health_by_session_id,
    load_session_slot_health,
)
from spreads.services.risk_manager import (
    build_session_risk_snapshot,
    normalize_risk_policy,
)
from spreads.services.runtime_identity import build_live_run_scope_id, parse_pipeline_id
from spreads.storage.serializers import parse_datetime

DEFAULT_ANALYSIS_PROFIT_TARGET = 0.5
DEFAULT_ANALYSIS_STOP_MULTIPLE = 2.0
DEFAULT_PIPELINE_REPLAY_RECENT = 20


def _parse_sort_value(value: str | None):
    normalized = value.strip() if isinstance(value, str) else None
    parsed = parse_datetime(normalized) if normalized else None
    return parsed or parse_datetime("1970-01-01T00:00:00Z")


def _latest_activity_timestamp(*values: str | None) -> str | None:
    best_value: str | None = None
    best_timestamp = None
    for value in values:
        normalized = value.strip() if isinstance(value, str) else None
        if not normalized:
            continue
        parsed = parse_datetime(normalized)
        if parsed is None:
            continue
        if best_timestamp is None or parsed > best_timestamp:
            best_timestamp = parsed
            best_value = normalized
    return best_value


def _derive_runtime_status(
    *,
    latest_run: Mapping[str, Any] | None,
    latest_cycle: Mapping[str, Any] | None,
    slot_health: Mapping[str, Any] | None = None,
) -> str:
    gap_active = bool((slot_health or {}).get("gap_active"))
    if latest_run is None:
        status = "healthy" if latest_cycle is not None else "idle"
        return "degraded" if gap_active and status != "running" else status

    status = str(latest_run.get("status") or "idle")
    if status == "running":
        return "running"
    if status == "failed":
        return "failed"
    if status == "queued":
        return "idle"
    if status == "skipped":
        return "degraded"
    if status == "succeeded":
        capture_status = str(latest_run.get("capture_status") or "")
        status = "healthy" if capture_status == "healthy" else "degraded"
        return "degraded" if gap_active and status != "running" else status
    status = "idle"
    return "degraded" if gap_active and status != "running" else status


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


def _session_risk_policy(latest_run: Mapping[str, Any] | None) -> dict[str, Any]:
    if latest_run is None:
        return normalize_risk_policy(None)
    payload = latest_run.get("payload")
    if not isinstance(payload, Mapping):
        return normalize_risk_policy(None)
    raw_policy = payload.get("risk_policy")
    return normalize_risk_policy(raw_policy if isinstance(raw_policy, dict) else None)


def _session_live_action_gate(
    latest_run: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if latest_run is None:
        return None
    gate = latest_run.get("live_action_gate")
    if not isinstance(gate, Mapping):
        return None
    return dict(gate)


def _latest_auto_execution(
    latest_run: Mapping[str, Any] | None,
    *,
    slot_runs: Iterable[Mapping[str, Any]] | None = None,
) -> dict[str, Any] | None:
    candidates: list[Mapping[str, Any]] = []
    if latest_run is not None:
        candidates.append(latest_run)
    if slot_runs is not None:
        candidates.extend(slot_runs)
    for run in candidates:
        summary = run.get("auto_execution_summary")
        if isinstance(summary, Mapping):
            return dict(summary)
    return None


def _tradeability_fields(
    *,
    latest_run: Mapping[str, Any] | None,
    slot_health: Mapping[str, Any] | None,
    has_live_opportunities: bool,
    has_analysis_only_opportunities: bool = False,
) -> dict[str, Any]:
    tradeability = build_tradeability_summary(
        capture_status=None if latest_run is None else latest_run.get("capture_status"),
        live_action_gate=_session_live_action_gate(latest_run),
        slot_health=slot_health,
        has_live_opportunities=has_live_opportunities,
        has_analysis_only_opportunities=has_analysis_only_opportunities,
    )
    return {
        "tradeability": tradeability,
        "tradeability_state": tradeability["state"],
        "tradeability_reason": tradeability.get("reason_code"),
        "tradeability_message": tradeability.get("message"),
    }


def _cycle_opportunity_payloads(
    collector_store: Any,
    cycle_id: str,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    opportunities = [
        dict(candidate) for candidate in collector_store.list_cycle_candidates(cycle_id)
    ]
    live_counts = {
        "promotable": 0,
        "monitor": 0,
    }
    for row in opportunities:
        if str(row.get("eligibility") or "live") != "live":
            continue
        selection_state = str(row.get("selection_state") or "")
        if selection_state in live_counts:
            live_counts[selection_state] += 1
    return opportunities, live_counts


def _normalize_pipeline_replay_mode(value: str | None) -> str:
    normalized = value.strip().lower() if isinstance(value, str) else "none"
    if normalized not in {"none", "current", "recent", "both"}:
        raise ValueError(f"Unsupported include_replay mode: {value}")
    return normalized


def _resolve_pipeline_replay(
    *,
    db_target: str,
    label: str,
    market_date: str,
    include_replay: str,
    storage: Any,
) -> dict[str, Any] | None:
    normalized_mode = _normalize_pipeline_replay_mode(include_replay)
    if normalized_mode == "none":
        return None

    replay_payload: dict[str, Any] = {
        "include_replay": normalized_mode,
        "recent_limit": DEFAULT_PIPELINE_REPLAY_RECENT,
        "current": None,
        "recent": None,
        "warnings": [],
    }

    if normalized_mode in {"current", "both"}:
        try:
            replay_payload["current"] = build_opportunity_replay(
                db_target=db_target,
                label=label,
                session_date=market_date,
                storage=storage,
            )
        except OpportunityReplayLookupError as exc:
            replay_payload["warnings"].append(str(exc))

    if normalized_mode in {"recent", "both"}:
        try:
            replay_payload["recent"] = build_recent_opportunity_replay_batch(
                db_target=db_target,
                recent=DEFAULT_PIPELINE_REPLAY_RECENT,
                label=label,
                storage=storage,
            )
        except OpportunityReplayLookupError as exc:
            replay_payload["warnings"].append(str(exc))

    return replay_payload


def _reconciliation_snapshot(portfolio: Mapping[str, Any]) -> dict[str, Any]:
    positions = portfolio.get("positions")
    if not isinstance(positions, list) or not positions:
        return {
            "status": "clear",
            "note": "No open positions require reconciliation.",
        }

    open_positions = [
        position
        for position in positions
        if isinstance(position, Mapping)
        and str(position.get("position_status") or "") in {"open", "partial_close"}
    ]
    if not open_positions:
        return {
            "status": "clear",
            "note": "No open positions require reconciliation.",
        }

    mismatch_positions = [
        position
        for position in open_positions
        if str(position.get("reconciliation_status") or "") == "mismatch"
    ]
    if mismatch_positions:
        return {
            "status": "mismatch",
            "note": f"{len(mismatch_positions)} open position(s) have broker reconciliation mismatches.",
        }

    pending_positions = [
        position
        for position in open_positions
        if not position.get("last_reconciled_at")
    ]
    if pending_positions:
        return {
            "status": "pending",
            "note": f"{len(pending_positions)} open position(s) are waiting for broker reconciliation.",
        }
    return {
        "status": "matched",
        "note": "Open positions match the broker inventory snapshot.",
    }


def _is_default_analysis_request(*, profit_target: float, stop_multiple: float) -> bool:
    return (
        abs(float(profit_target) - DEFAULT_ANALYSIS_PROFIT_TARGET) < 1e-9
        and abs(float(stop_multiple) - DEFAULT_ANALYSIS_STOP_MULTIPLE) < 1e-9
    )


def _resolve_pipeline_analysis(
    *,
    analysis_run: Any,
    db_target: str,
    market_date: str,
    label: str,
    profit_target: float,
    stop_multiple: float,
    storage: Any,
) -> dict[str, Any]:
    analysis_run_payload = dict(analysis_run)
    stored_summary = analysis_run_payload.get("summary")
    if _is_default_analysis_request(
        profit_target=profit_target,
        stop_multiple=stop_multiple,
    ) and isinstance(stored_summary, Mapping):
        return {
            **dict(stored_summary),
            "diagnostics": analysis_run_payload.get("diagnostics"),
            "recommendations": analysis_run_payload.get("recommendations"),
            "report": analysis_run_payload.get("report_markdown"),
            "analysis_run": analysis_run_payload,
        }

    summary = build_session_summary(
        db_target=db_target,
        session_date=market_date,
        label=label,
        profit_target=profit_target,
        stop_multiple=stop_multiple,
        storage=storage,
    )
    return {
        **summary,
        "analysis_run": analysis_run_payload,
    }


def _latest_cycles_for_market_date(
    *,
    collector_store: Any,
    pipeline_rows: list[Mapping[str, Any]],
    market_date: str | None,
) -> list[dict[str, Any]]:
    if market_date is None:
        pipeline_ids = [str(row["pipeline_id"]) for row in pipeline_rows]
        return [
            dict(row)
            for row in collector_store.list_latest_cycles_by_pipeline_ids(pipeline_ids)
        ]

    resolved_cycles: list[dict[str, Any]] = []
    for row in pipeline_rows:
        latest_cycle = collector_store.get_latest_pipeline_cycle(
            str(row["pipeline_id"]),
            market_date=market_date,
        )
        if latest_cycle is not None:
            resolved_cycles.append(dict(latest_cycle))
    return resolved_cycles


def _serialize_pipeline_summary(
    *,
    pipeline: Mapping[str, Any],
    latest_cycle: Mapping[str, Any],
    latest_run: Mapping[str, Any] | None,
    slot_health: Mapping[str, Any],
    candidate_counts: dict[str, int],
    alert_count: int,
) -> dict[str, Any]:
    updated_at = _latest_activity_timestamp(
        None if latest_run is None else str(latest_run.get("finished_at") or ""),
        None if latest_run is None else str(latest_run.get("heartbeat_at") or ""),
        None if latest_run is None else str(latest_run.get("started_at") or ""),
        None
        if latest_run is None
        else str(latest_run.get("slot_at") or latest_run.get("scheduled_for") or ""),
        str(latest_cycle.get("generated_at") or ""),
        str(pipeline.get("updated_at") or ""),
    )
    tradeability_fields = _tradeability_fields(
        latest_run=latest_run,
        slot_health=slot_health,
        has_live_opportunities=bool(
            int(candidate_counts.get("promotable") or 0)
            or int(candidate_counts.get("monitor") or 0)
        ),
    )
    latest_auto_execution = _latest_auto_execution(latest_run)
    return {
        "pipeline_id": str(pipeline["pipeline_id"]),
        "label": str(pipeline["label"]),
        "name": pipeline.get("name"),
        "status": _derive_runtime_status(
            latest_run=latest_run,
            latest_cycle=latest_cycle,
            slot_health=slot_health,
        ),
        "latest_market_date": str(latest_cycle["market_date"]),
        "latest_slot_at": slot_health.get("latest_slot_at")
        or (None if latest_run is None else latest_run.get("slot_at")),
        "latest_slot_status": slot_health.get("latest_slot_status")
        or (None if latest_run is None else latest_run.get("status")),
        "latest_capture_status": None
        if latest_run is None
        else latest_run.get("capture_status"),
        "latest_auto_execution": latest_auto_execution,
        "latest_auto_execution_status": None
        if latest_auto_execution is None
        else latest_auto_execution.get("status"),
        "promotable_count": int(candidate_counts.get("promotable") or 0),
        "monitor_count": int(candidate_counts.get("monitor") or 0),
        "alert_count": int(alert_count or 0),
        "live_action_gate": _session_live_action_gate(latest_run),
        "gap_active": bool(slot_health.get("gap_active")),
        "recovery_state": slot_health.get("recovery_state"),
        "missed_slot_count": int(slot_health.get("missed_slot_count") or 0),
        "unrecoverable_slot_count": int(
            slot_health.get("unrecoverable_slot_count") or 0
        ),
        "latest_fresh_slot_at": slot_health.get("latest_fresh_slot_at"),
        "latest_resume_slot_at": slot_health.get("latest_resume_slot_at"),
        **tradeability_fields,
        "updated_at": updated_at,
        "style_profile": pipeline.get("style_profile"),
        "horizon_intent": pipeline.get("default_horizon_intent"),
        "product_scope": pipeline.get("product_scope_json"),
        "policy": pipeline.get("policy_json"),
    }


@with_storage()
def list_pipelines(
    *,
    db_target: str,
    limit: int = 100,
    market_date: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    collector_store = storage.collector
    if not collector_store.pipeline_schema_ready():
        return {"pipelines": []}

    job_store = storage.jobs
    alert_store = storage.alerts
    pipeline_rows = [
        dict(row) for row in collector_store.list_pipelines(limit=max(limit * 5, limit))
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
    candidate_counts_by_cycle_id = collector_store.count_cycle_candidates_by_cycle_ids(
        [str(row["cycle_id"]) for row in latest_cycles]
    )
    alert_counts = alert_store.count_alert_events_by_session_keys(
        [(str(row["market_date"]), str(row["label"])) for row in latest_cycles]
    )

    summaries: list[dict[str, Any]] = []
    for pipeline in pipeline_rows:
        latest_cycle = latest_cycle_by_pipeline_id.get(str(pipeline["pipeline_id"]))
        if latest_cycle is None:
            continue
        legacy_session_id = build_live_run_scope_id(
            str(latest_cycle["label"]),
            str(latest_cycle["market_date"]),
        )
        summaries.append(
            _serialize_pipeline_summary(
                pipeline=pipeline,
                latest_cycle=latest_cycle,
                latest_run=latest_runs_by_session_id.get(legacy_session_id),
                slot_health=dict(
                    slot_health_by_session_id.get(legacy_session_id) or {}
                ),
                candidate_counts=candidate_counts_by_cycle_id.get(
                    str(latest_cycle["cycle_id"]),
                    {},
                ),
                alert_count=int(
                    alert_counts.get(
                        (str(latest_cycle["market_date"]), str(latest_cycle["label"]))
                    )
                    or 0
                ),
            )
        )

    summaries.sort(
        key=lambda row: _parse_sort_value(
            None if not row.get("updated_at") else str(row["updated_at"])
        ),
        reverse=True,
    )
    return {"pipelines": summaries[:limit]}


@with_storage()
def get_pipeline_detail(
    *,
    db_target: str,
    pipeline_id: str,
    market_date: str | None,
    include_replay: str = "none",
    profit_target: float,
    stop_multiple: float,
    storage: Any | None = None,
) -> dict[str, Any]:
    parsed = parse_pipeline_id(pipeline_id)
    if parsed is None:
        raise ValueError(f"Unknown pipeline_id: {pipeline_id}")

    collector_store = storage.collector
    job_store = storage.jobs
    alert_store = storage.alerts
    post_market_store = storage.post_market
    execution_store = storage.execution
    recovery_store = storage.recovery
    risk_store = getattr(storage, "risk", None)
    signal_store = storage.signals

    if not collector_store.pipeline_schema_ready():
        raise ValueError(f"Unknown pipeline_id: {pipeline_id}")

    pipeline = collector_store.get_pipeline(pipeline_id)
    if pipeline is None:
        raise ValueError(f"Unknown pipeline_id: {pipeline_id}")

    latest_cycle = collector_store.get_latest_pipeline_cycle(
        pipeline_id,
        market_date=market_date,
    )
    if latest_cycle is None:
        raise ValueError(f"Unknown pipeline_id: {pipeline_id}")

    resolved_market_date = str(latest_cycle["market_date"])
    legacy_session_id = build_live_run_scope_id(
        str(latest_cycle["label"]),
        resolved_market_date,
    )

    slot_runs = _sort_session_runs(
        enrich_live_collector_job_run_payload(row)
        for row in job_store.list_job_runs(
            job_type="live_collector",
            session_id=legacy_session_id,
            limit=500,
        )
    )
    latest_run = slot_runs[0] if slot_runs else None

    current_cycle = None
    all_opportunities: list[dict[str, Any]] = []
    selection_counts = {"promotable": 0, "monitor": 0}
    all_opportunities, selection_counts = _cycle_opportunity_payloads(
        collector_store,
        str(latest_cycle["cycle_id"]),
    )
    if signal_store.schema_ready():
        active_opportunities = {
            int(row["source_candidate_id"]): dict(row)
            for row in signal_store.list_opportunities(
                pipeline_id=pipeline_id,
                market_date=resolved_market_date,
                limit=500,
            )
            if row.get("source_candidate_id") not in (None, "")
        }
        all_opportunities = [
            {
                **row,
                "opportunity_id": (
                    None
                    if row.get("candidate_id") in (None, "")
                    else (active_opportunities.get(int(row["candidate_id"])) or {}).get(
                        "opportunity_id"
                    )
                ),
                "pipeline_id": pipeline_id,
                "market_date": resolved_market_date,
            }
            for row in all_opportunities
        ]
    live_opportunities = [
        row
        for row in all_opportunities
        if str(row.get("eligibility") or "live") == "live"
    ]
    analysis_only_opportunities = [
        row
        for row in all_opportunities
        if str(row.get("eligibility") or "live") != "live"
    ]
    current_cycle = {
        **dict(latest_cycle),
        "opportunities": all_opportunities,
        "live_opportunities": live_opportunities,
        "analysis_only_opportunities": analysis_only_opportunities,
        "selection_counts": selection_counts,
        "legacy_session_id": legacy_session_id,
    }

    alerts = [
        dict(alert)
        for alert in alert_store.list_alert_events(
            session_date=resolved_market_date,
            label=str(latest_cycle["label"]),
            limit=200,
        )
    ]
    events = [
        dict(event)
        for event in collector_store.list_events(
            label=str(latest_cycle["label"]),
            session_date=resolved_market_date,
            limit=400,
            ascending=True,
        )
    ]

    analysis_run = post_market_store.get_latest_run(
        label=str(latest_cycle["label"]),
        session_date=resolved_market_date,
        succeeded_only=True,
    )
    analysis = None
    if analysis_run is not None:
        analysis = _resolve_pipeline_analysis(
            analysis_run=analysis_run,
            db_target=db_target,
            market_date=resolved_market_date,
            label=str(latest_cycle["label"]),
            profit_target=profit_target,
            stop_multiple=stop_multiple,
            storage=storage,
        )
    replay = _resolve_pipeline_replay(
        db_target=db_target,
        label=str(latest_cycle["label"]),
        market_date=resolved_market_date,
        include_replay=include_replay,
        storage=storage,
    )

    updated_at = _latest_activity_timestamp(
        None if latest_run is None else str(latest_run.get("finished_at") or ""),
        None if latest_run is None else str(latest_run.get("heartbeat_at") or ""),
        None if latest_run is None else str(latest_run.get("started_at") or ""),
        None
        if latest_run is None
        else str(latest_run.get("slot_at") or latest_run.get("scheduled_for") or ""),
        str(current_cycle.get("generated_at") or ""),
        str(pipeline.get("updated_at") or ""),
    )
    executions = list_session_execution_attempts(
        db_target=db_target,
        session_id=legacy_session_id,
        limit=50,
        execution_store=execution_store,
        storage=storage,
    )
    portfolio = build_session_execution_portfolio(
        db_target=db_target,
        session_id=legacy_session_id,
        executions=executions,
        execution_store=execution_store,
        storage=storage,
    )
    risk_snapshot = build_session_risk_snapshot(
        execution_store=execution_store,
        session_id=legacy_session_id,
        risk_policy=_session_risk_policy(latest_run),
    )
    risk_decisions = (
        []
        if risk_store is None or not risk_store.schema_ready()
        else [
            dict(row)
            for row in risk_store.list_risk_decisions(
                session_id=legacy_session_id,
                limit=100,
            )
        ]
    )
    reconciliation_snapshot = _reconciliation_snapshot(portfolio)
    control_snapshot = get_control_state_snapshot(storage=storage)
    live_action_gate = _session_live_action_gate(latest_run)
    slot_health = load_session_slot_health(
        recovery_store=recovery_store,
        session_id=legacy_session_id,
    )
    recovery_slots = (
        []
        if not recovery_store.schema_ready()
        else [
            dict(row)
            for row in recovery_store.list_live_session_slots(
                session_id=legacy_session_id,
                limit=50,
            )
        ]
    )
    cycles = [
        {
            **dict(row),
            "legacy_session_id": build_live_run_scope_id(
                str(row["label"]),
                str(row["market_date"]),
            ),
        }
        for row in collector_store.list_pipeline_cycles(
            pipeline_id=pipeline_id,
            limit=50,
        )
    ]
    tradeability_fields = _tradeability_fields(
        latest_run=latest_run,
        slot_health=slot_health,
        has_live_opportunities=bool(live_opportunities),
        has_analysis_only_opportunities=bool(analysis_only_opportunities),
    )
    latest_auto_execution = _latest_auto_execution(latest_run, slot_runs=slot_runs)

    return {
        "pipeline_id": pipeline_id,
        "market_date": resolved_market_date,
        "label": str(latest_cycle["label"]),
        "status": _derive_runtime_status(
            latest_run=latest_run,
            latest_cycle=current_cycle,
            slot_health=slot_health,
        ),
        "updated_at": updated_at,
        "risk_status": risk_snapshot["status"],
        "risk_note": risk_snapshot.get("note"),
        "reconciliation_status": reconciliation_snapshot["status"],
        "reconciliation_note": reconciliation_snapshot.get("note"),
        "latest_slot": latest_run,
        "slot_health": slot_health,
        "recovery_slots": recovery_slots,
        "live_action_gate": live_action_gate,
        "latest_auto_execution": latest_auto_execution,
        **tradeability_fields,
        "pipeline": dict(pipeline),
        "current_cycle": current_cycle,
        "cycles": cycles,
        "opportunities": live_opportunities,
        "analysis_only_opportunities": analysis_only_opportunities,
        "selection_counts": selection_counts,
        "slot_runs": slot_runs,
        "alerts": alerts,
        "events": events,
        "executions": executions,
        "risk_decisions": risk_decisions,
        "control": control_snapshot,
        "portfolio": portfolio,
        "analysis": analysis,
        "replay": replay,
    }


@with_storage()
def list_pipeline_cycles(
    *,
    db_target: str,
    pipeline_id: str,
    market_date: str | None = None,
    limit: int = 100,
    storage: Any | None = None,
) -> dict[str, Any]:
    collector_store = storage.collector
    if not collector_store.pipeline_schema_ready():
        return {"cycles": []}
    rows = [
        dict(row)
        for row in collector_store.list_pipeline_cycles(
            pipeline_id=pipeline_id,
            market_date=market_date,
            limit=limit,
        )
    ]
    return {"cycles": rows}
