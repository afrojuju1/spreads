from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from core.db.decorators import with_storage
from core.services.analysis import build_session_summary
from core.services.control_plane import get_control_state_snapshot
from core.services.execution import (
    list_session_execution_attempts,
    normalize_execution_policy,
)
from core.services.execution_portfolio import build_session_execution_portfolio
from core.services.live_collector_health.tradeability import (
    build_tradeability_summary,
)
from core.services.live_runtime import (
    get_live_session,
    list_latest_live_sessions,
)
from core.services.opportunity_replay import (
    OpportunityReplayLookupError,
    build_opportunity_replay,
    build_recent_opportunity_replay_batch,
)
from core.services.risk_manager import (
    build_session_risk_snapshot,
    normalize_risk_policy,
)
from core.services.runtime_identity import (
    build_live_run_scope_id,
    parse_pipeline_id,
)
from core.storage.serializers import parse_datetime

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
        if capture_status in {"healthy", "idle"}:
            status = "healthy" if capture_status == "healthy" else "idle"
        else:
            status = "degraded"
        return "degraded" if gap_active and status != "running" else status
    status = "idle"
    return "degraded" if gap_active and status != "running" else status


def _session_risk_policy(latest_run: Mapping[str, Any] | None) -> dict[str, Any]:
    if latest_run is None:
        return normalize_risk_policy(None)
    payload = latest_run.get("payload")
    if not isinstance(payload, Mapping):
        return normalize_risk_policy(None)
    raw_policy = payload.get("risk_policy")
    return normalize_risk_policy(raw_policy if isinstance(raw_policy, dict) else None)


def _session_execution_policy(latest_run: Mapping[str, Any] | None) -> dict[str, Any]:
    if latest_run is None:
        return normalize_execution_policy(None)
    payload = latest_run.get("payload")
    if not isinstance(payload, Mapping):
        return normalize_execution_policy(None)
    return normalize_execution_policy(
        {
            "execution_policy": payload.get("execution_policy"),
            "risk_policy": payload.get("risk_policy"),
        }
    )


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


def _collector_schema_ready(collector_store: Any) -> bool:
    if hasattr(collector_store, "schema_ready"):
        return bool(collector_store.schema_ready())
    return bool(collector_store.pipeline_schema_ready())


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


def _build_cycle_payload(
    *,
    pipeline_id: str,
    cycle: Mapping[str, Any],
    candidate_counts: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    counts = dict(candidate_counts or {})
    summary = {
        "candidate_count": int(counts.get("candidate_count") or 0),
        "promotable_count": int(counts.get("promotable") or 0),
        "monitor_count": int(counts.get("monitor") or 0),
        "failure_count": len(cycle.get("failures") or []),
        "event_count": 0,
    }
    return {
        **dict(cycle),
        "pipeline_id": pipeline_id,
        "market_date": str(cycle.get("market_date") or cycle.get("session_date") or ""),
        "strategy_mode": cycle.get("strategy_mode") or cycle.get("strategy"),
        "legacy_profile": cycle.get("legacy_profile") or cycle.get("profile"),
        "summary": summary,
        "summary_json": summary,
    }


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
    alert_store = storage.alerts
    live_sessions = list_latest_live_sessions(
        storage=storage,
        market_date=market_date,
        limit=max(limit * 5, limit),
    )
    alert_counts = alert_store.count_alert_events_by_session_keys(
        [
            (str(session["market_date"]), str(session["label"]))
            for session in live_sessions
        ]
    )

    summaries: list[dict[str, Any]] = []
    for session in live_sessions:
        summaries.append(
            _serialize_pipeline_summary(
                pipeline=session["pipeline"],
                latest_cycle=session["cycle"],
                latest_run=session.get("latest_run"),
                slot_health=dict(session.get("slot_health") or {}),
                candidate_counts=dict(session.get("candidate_counts") or {}),
                alert_count=int(
                    alert_counts.get(
                        (str(session["market_date"]), str(session["label"]))
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
    collector_store = storage.collector
    alert_store = storage.alerts
    post_market_store = storage.post_market
    execution_store = storage.execution
    risk_store = getattr(storage, "risk", None)
    signal_store = storage.signals

    live_session = get_live_session(
        storage=storage,
        pipeline_id=pipeline_id,
        market_date=market_date,
    )
    pipeline = dict(live_session["pipeline"])
    latest_cycle = dict(live_session["cycle"])
    label = str(live_session["label"])
    resolved_market_date = str(live_session["market_date"])
    legacy_session_id = str(live_session["session_id"])
    latest_run = live_session.get("latest_run")
    slot_runs = [dict(row) for row in list(live_session.get("slot_runs") or [])]
    all_opportunities = [
        dict(row) for row in list(live_session.get("opportunities") or [])
    ]
    live_opportunities = [
        dict(row) for row in list(live_session.get("live_opportunities") or [])
    ]
    analysis_only_opportunities = [
        dict(row)
        for row in list(live_session.get("analysis_only_opportunities") or [])
    ]
    selection_counts = dict(live_session.get("selection_counts") or {})
    candidate_counts = dict(live_session.get("candidate_counts") or {})
    automation_summary = dict(live_session.get("automation_summary") or {})
    current_cycle = {
        **_build_cycle_payload(
            pipeline_id=pipeline_id,
            cycle=latest_cycle,
            candidate_counts=candidate_counts,
        ),
        "opportunities": all_opportunities,
        "live_opportunities": live_opportunities,
        "analysis_only_opportunities": analysis_only_opportunities,
        "selection_counts": selection_counts,
        "promotable_count": int(selection_counts.get("promotable") or 0),
        "monitor_count": int(selection_counts.get("monitor") or 0),
        "legacy_session_id": legacy_session_id,
        "automation_summary": automation_summary,
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
            label=label,
            session_date=resolved_market_date,
            limit=400,
            ascending=True,
        )
    ]

    analysis_run = post_market_store.get_latest_run(
        label=label,
        session_date=resolved_market_date,
        succeeded_only=True,
    )
    analysis = None
    if analysis_run is not None:
        analysis = _resolve_pipeline_analysis(
            analysis_run=analysis_run,
            db_target=db_target,
            market_date=resolved_market_date,
            label=label,
            profit_target=profit_target,
            stop_multiple=stop_multiple,
            storage=storage,
        )
    replay = _resolve_pipeline_replay(
        db_target=db_target,
        label=label,
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
        execution_policy=_session_execution_policy(latest_run),
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
    slot_health = dict(live_session.get("slot_health") or {})
    recovery_slots = [
        dict(row) for row in list(live_session.get("recovery_slots") or [])
    ]
    cycle_rows = [dict(row) for row in collector_store.list_cycles(label, limit=50)]
    cycle_counts_by_cycle_id = _candidate_counts_by_cycle_id(
        collector_store=collector_store,
        signal_store=signal_store,
        cycle_ids=[str(row["cycle_id"]) for row in cycle_rows],
    )
    cycles = [
        {
            **_build_cycle_payload(
                pipeline_id=pipeline_id,
                cycle=row,
                candidate_counts=cycle_counts_by_cycle_id.get(str(row["cycle_id"])),
            ),
            "legacy_session_id": build_live_run_scope_id(
                str(row["label"]),
                str(row.get("session_date") or row.get("market_date")),
            ),
        }
        for row in cycle_rows
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
        "automation_summary": automation_summary,
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
    if not _collector_schema_ready(collector_store):
        return {"cycles": []}
    parsed = parse_pipeline_id(pipeline_id)
    if parsed is None:
        return {"cycles": []}
    signal_store = storage.signals
    rows = [
        dict(row)
        for row in collector_store.list_cycles(
            parsed["label"],
            session_date=market_date,
            limit=limit,
        )
    ]
    cycle_ids = [str(row["cycle_id"]) for row in rows]
    candidate_counts_by_cycle_id = _candidate_counts_by_cycle_id(
        collector_store=collector_store,
        signal_store=signal_store,
        cycle_ids=cycle_ids,
    )
    return {
        "cycles": [
            _build_cycle_payload(
                pipeline_id=pipeline_id,
                cycle=row,
                candidate_counts=candidate_counts_by_cycle_id.get(str(row["cycle_id"])),
            )
            for row in rows
        ]
    }
