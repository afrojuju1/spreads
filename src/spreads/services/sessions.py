from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from spreads.db.decorators import with_storage
from spreads.services.analysis import build_session_summary
from spreads.services.control_plane import get_control_state_snapshot
from spreads.services.execution import list_session_execution_attempts
from spreads.services.execution_portfolio import build_session_execution_portfolio
from spreads.services.live_collector_health import enrich_live_collector_job_run_payload
from spreads.services.live_pipelines import parse_live_session_id
from spreads.services.risk_manager import build_session_risk_snapshot, normalize_risk_policy
from spreads.storage.serializers import parse_datetime

DEFAULT_ANALYSIS_PROFIT_TARGET = 0.5
DEFAULT_ANALYSIS_STOP_MULTIPLE = 2.0


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


def _derive_session_status(
    *,
    latest_run: Mapping[str, Any] | None,
    latest_cycle: Mapping[str, Any] | None,
) -> str:
    if latest_run is None:
        return "healthy" if latest_cycle is not None else "idle"

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
        return "healthy" if capture_status == "healthy" else "degraded"
    return "idle"


def _sort_session_runs(runs: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    enriched = [dict(run) for run in runs]
    enriched.sort(
        key=lambda run: _parse_sort_value(
            None if not (run.get("slot_at") or run.get("scheduled_for")) else str(run.get("slot_at") or run.get("scheduled_for"))
        ),
        reverse=True,
    )
    return enriched


def _resolve_session_identity(
    session_id: str,
    *,
    collector_store: Any,
    job_store: Any,
) -> dict[str, str] | None:
    resolved = parse_live_session_id(session_id)
    if resolved is not None:
        return resolved

    latest_cycle = collector_store.get_latest_session_cycle(session_id)
    if latest_cycle is not None:
        return {
            "session_id": session_id,
            "label": str(latest_cycle["label"]),
            "session_date": str(latest_cycle["session_date"]),
        }

    session_runs = job_store.list_job_runs(job_type="live_collector", session_id=session_id, limit=1)
    if not session_runs:
        return None
    payload = session_runs[0]["payload"]
    label = payload.get("label")
    session_date = payload.get("session_date")
    if not isinstance(label, str) or not isinstance(session_date, str):
        return None
    return {
        "session_id": session_id,
        "label": label,
        "session_date": session_date,
    }


def _session_risk_policy(latest_run: Mapping[str, Any] | None) -> dict[str, Any]:
    if latest_run is None:
        return normalize_risk_policy(None)
    payload = latest_run.get("payload")
    if not isinstance(payload, Mapping):
        return normalize_risk_policy(None)
    raw_policy = payload.get("risk_policy")
    return normalize_risk_policy(raw_policy if isinstance(raw_policy, dict) else None)


def _cycle_opportunity_payloads(
    collector_store: Any,
    cycle_id: str,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    opportunities = [
        dict(candidate)
        for candidate in collector_store.list_cycle_candidates(cycle_id)
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


def _reconciliation_snapshot(portfolio: Mapping[str, Any]) -> dict[str, Any]:
    positions = portfolio.get("positions")
    if not isinstance(positions, list) or not positions:
        return {
            "status": "clear",
            "note": "No session positions are open for reconciliation.",
        }

    open_positions = [
        position
        for position in positions
        if isinstance(position, Mapping) and str(position.get("position_status") or "") in {"open", "partial_close"}
    ]
    if not open_positions:
        return {
            "status": "clear",
            "note": "No session positions are open for reconciliation.",
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


def _resolve_session_analysis(
    *,
    analysis_run: Any,
    db_target: str,
    session_date: str,
    label: str,
    profit_target: float,
    stop_multiple: float,
    storage: Any,
) -> dict[str, Any]:
    analysis_run_payload = dict(analysis_run)
    stored_summary = analysis_run_payload.get("summary")
    if _is_default_analysis_request(profit_target=profit_target, stop_multiple=stop_multiple) and isinstance(
        stored_summary,
        Mapping,
    ):
        return {
            **dict(stored_summary),
            "diagnostics": analysis_run_payload.get("diagnostics"),
            "recommendations": analysis_run_payload.get("recommendations"),
            "report": analysis_run_payload.get("report_markdown"),
            "analysis_run": analysis_run_payload,
        }

    summary = build_session_summary(
        db_target=db_target,
        session_date=session_date,
        label=label,
        profit_target=profit_target,
        stop_multiple=stop_multiple,
        storage=storage,
    )
    return {
        **summary,
        "analysis_run": analysis_run_payload,
    }


@with_storage()
def list_existing_sessions(
    *,
    db_target: str,
    limit: int = 100,
    session_date: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    collector_store = storage.collector
    job_store = storage.jobs
    alert_store = storage.alerts
    candidate_session_ids = set(job_store.list_session_ids(job_type="live_collector", limit=max(limit * 10, 200)))
    candidate_session_ids.update(
        collector_store.list_session_ids(session_date=session_date, limit=max(limit * 10, 200))
    )
    if not candidate_session_ids:
        return {"sessions": []}

    latest_cycles = collector_store.list_latest_cycles_by_session_ids(sorted(candidate_session_ids))
    latest_cycles_by_session_id = {
        str(cycle["session_id"]): dict(cycle)
        for cycle in latest_cycles
        if cycle.get("session_id")
    }
    latest_runs = [
        enrich_live_collector_job_run_payload(row)
        for row in job_store.list_latest_runs_by_session_ids(
            session_ids=sorted(candidate_session_ids),
            job_type="live_collector",
        )
    ]
    latest_runs_by_session_id = {
        str(run["session_id"]): run
        for run in latest_runs
        if run.get("session_id")
    }
    candidate_counts_by_cycle_id = collector_store.count_cycle_candidates_by_cycle_ids(
        [
            str(cycle["cycle_id"])
            for cycle in latest_cycles
        ]
    )

    resolved_sessions: list[dict[str, Any]] = []
    for session_id in candidate_session_ids:
        identity = parse_live_session_id(session_id)
        latest_cycle_payload = latest_cycles_by_session_id.get(session_id)
        latest_run = latest_runs_by_session_id.get(session_id)
        if identity is None and latest_cycle_payload is not None:
            identity = {
                "session_id": session_id,
                "label": str(latest_cycle_payload["label"]),
                "session_date": str(latest_cycle_payload["session_date"]),
            }
        if identity is None and latest_run is not None:
            payload = latest_run.get("payload")
            label = payload.get("label") if isinstance(payload, Mapping) else None
            resolved_session_date = payload.get("session_date") if isinstance(payload, Mapping) else None
            if isinstance(label, str) and isinstance(resolved_session_date, str):
                identity = {
                    "session_id": session_id,
                    "label": label,
                    "session_date": resolved_session_date,
                }
        if identity is None:
            continue
        if session_date and identity["session_date"] != session_date:
            continue
        resolved_sessions.append(
            {
                "session_id": session_id,
                "label": identity["label"],
                "session_date": identity["session_date"],
                "latest_run": latest_run,
                "latest_cycle": latest_cycle_payload,
            }
        )

    alert_counts = alert_store.count_alert_events_by_session_keys(
        [
            (str(row["session_date"]), str(row["label"]))
            for row in resolved_sessions
        ]
    )
    session_rows: list[dict[str, Any]] = []
    for row in resolved_sessions:
        latest_run = row["latest_run"]
        latest_cycle_payload = row["latest_cycle"]
        cycle_counts = (
            candidate_counts_by_cycle_id.get(str(latest_cycle_payload["cycle_id"]), {})
            if latest_cycle_payload is not None
            else {}
        )
        updated_at = _latest_activity_timestamp(
            None if latest_run is None else str(latest_run.get("finished_at") or ""),
            None if latest_run is None else str(latest_run.get("heartbeat_at") or ""),
            None if latest_run is None else str(latest_run.get("started_at") or ""),
            None if latest_run is None else str(latest_run.get("slot_at") or latest_run.get("scheduled_for") or ""),
            None if latest_cycle_payload is None else str(latest_cycle_payload.get("generated_at") or ""),
        )
        session_rows.append(
            {
                "session_id": row["session_id"],
                "label": row["label"],
                "session_date": row["session_date"],
                "status": _derive_session_status(latest_run=latest_run, latest_cycle=latest_cycle_payload),
                "latest_slot_at": None if latest_run is None else latest_run.get("slot_at"),
                "latest_slot_status": None if latest_run is None else latest_run.get("status"),
                "latest_capture_status": None if latest_run is None else latest_run.get("capture_status"),
                "websocket_quote_events_saved": 0
                if latest_run is None
                else int((latest_run.get("quote_capture") or {}).get("websocket_quote_events_saved") or 0),
                "baseline_quote_events_saved": 0
                if latest_run is None
                else int((latest_run.get("quote_capture") or {}).get("baseline_quote_events_saved") or 0),
                "recovery_quote_events_saved": 0
                if latest_run is None
                else int((latest_run.get("quote_capture") or {}).get("recovery_quote_events_saved") or 0),
                "promotable_count": int(cycle_counts.get("promotable") or 0),
                "monitor_count": int(cycle_counts.get("monitor") or 0),
                "alert_count": int(alert_counts.get((str(row["session_date"]), str(row["label"]))) or 0),
                "updated_at": updated_at,
            }
        )

    session_rows.sort(
        key=lambda row: _parse_sort_value(
            None if not row.get("updated_at") else str(row.get("updated_at"))
        ),
        reverse=True,
    )
    return {"sessions": session_rows[:limit]}


@with_storage()
def get_session_detail(
    *,
    db_target: str,
    session_id: str,
    profit_target: float,
    stop_multiple: float,
    storage: Any | None = None,
) -> dict[str, Any]:
    collector_store = storage.collector
    job_store = storage.jobs
    alert_store = storage.alerts
    post_market_store = storage.post_market
    execution_store = storage.execution
    risk_store = getattr(storage, "risk", None)
    identity = _resolve_session_identity(
        session_id,
        collector_store=collector_store,
        job_store=job_store,
    )
    if identity is None:
        raise ValueError(f"Unknown session_id: {session_id}")

    slot_runs = _sort_session_runs(
        enrich_live_collector_job_run_payload(row)
        for row in job_store.list_job_runs(
            job_type="live_collector",
            session_id=session_id,
            limit=500,
        )
    )
    latest_run = slot_runs[0] if slot_runs else None
    latest_cycle = collector_store.get_latest_session_cycle(session_id)
    if latest_run is None and latest_cycle is None:
        raise ValueError(f"Unknown session_id: {session_id}")
    current_cycle = None
    opportunities: list[dict[str, Any]] = []
    selection_counts = {"promotable": 0, "monitor": 0}
    if latest_cycle is not None:
        opportunities, selection_counts = _cycle_opportunity_payloads(
            collector_store, str(latest_cycle["cycle_id"])
        )
        current_cycle = {
            **latest_cycle,
            "opportunities": opportunities,
            "selection_counts": selection_counts,
        }

    alerts = [
        dict(alert)
        for alert in alert_store.list_alert_events(
            session_date=identity["session_date"],
            label=identity["label"],
            limit=200,
        )
    ]
    events = [
        dict(event)
        for event in collector_store.list_events(
            label=identity["label"],
            session_date=identity["session_date"],
            limit=400,
            ascending=True,
        )
    ]

    analysis_run = post_market_store.get_latest_run(
        label=identity["label"],
        session_date=identity["session_date"],
        succeeded_only=True,
    )
    analysis = None
    if analysis_run is not None:
        analysis = _resolve_session_analysis(
            analysis_run=analysis_run,
            db_target=db_target,
            session_date=identity["session_date"],
            label=identity["label"],
            profit_target=profit_target,
            stop_multiple=stop_multiple,
            storage=storage,
        )

    updated_at = _latest_activity_timestamp(
        None if latest_run is None else str(latest_run.get("finished_at") or ""),
        None if latest_run is None else str(latest_run.get("heartbeat_at") or ""),
        None if latest_run is None else str(latest_run.get("started_at") or ""),
        None if latest_run is None else str(latest_run.get("slot_at") or latest_run.get("scheduled_for") or ""),
        None if current_cycle is None else str(current_cycle.get("generated_at") or ""),
    )
    executions = list_session_execution_attempts(
        db_target=db_target,
        session_id=session_id,
        limit=50,
        execution_store=execution_store,
        storage=storage,
    )
    portfolio = build_session_execution_portfolio(
        db_target=db_target,
        session_id=session_id,
        executions=executions,
        execution_store=execution_store,
        storage=storage,
    )
    risk_snapshot = build_session_risk_snapshot(
        execution_store=execution_store,
        session_id=session_id,
        risk_policy=_session_risk_policy(latest_run),
    )
    risk_decisions = (
        []
        if risk_store is None or not risk_store.schema_ready()
        else [
            dict(row)
            for row in risk_store.list_risk_decisions(
                session_id=session_id,
                limit=100,
            )
        ]
    )
    reconciliation_snapshot = _reconciliation_snapshot(portfolio)
    control_snapshot = get_control_state_snapshot(storage=storage)

    return {
        "session_id": session_id,
        "label": identity["label"],
        "session_date": identity["session_date"],
        "status": _derive_session_status(latest_run=latest_run, latest_cycle=current_cycle),
        "updated_at": updated_at,
        "risk_status": risk_snapshot["status"],
        "risk_note": risk_snapshot.get("note"),
        "reconciliation_status": reconciliation_snapshot["status"],
        "reconciliation_note": reconciliation_snapshot.get("note"),
        "latest_slot": latest_run,
        "current_cycle": current_cycle,
        "opportunities": opportunities,
        "selection_counts": selection_counts,
        "slot_runs": slot_runs,
        "alerts": alerts,
        "events": events,
        "executions": executions,
        "risk_decisions": risk_decisions,
        "control": control_snapshot,
        "portfolio": portfolio,
        "analysis": analysis,
    }
