from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from spreads.services.analysis import build_session_summary
from spreads.services.execution import list_session_execution_attempts
from spreads.services.live_collector_health import enrich_live_collector_job_run_payload
from spreads.services.live_pipelines import parse_live_session_id
from spreads.storage.factory import (
    build_alert_repository,
    build_collector_repository,
    build_job_repository,
    build_post_market_repository,
)
from spreads.storage.serializers import parse_datetime


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


def list_existing_sessions(
    *,
    db_target: str,
    limit: int = 100,
    session_date: str | None = None,
) -> dict[str, Any]:
    collector_store = build_collector_repository(db_target)
    job_store = build_job_repository(db_target)
    alert_store = build_alert_repository(db_target)
    try:
        candidate_session_ids = set(job_store.list_session_ids(job_type="live_collector", limit=max(limit * 10, 200)))
        candidate_session_ids.update(
            collector_store.list_session_ids(session_date=session_date, limit=max(limit * 10, 200))
        )

        session_rows: list[dict[str, Any]] = []
        for session_id in candidate_session_ids:
            identity = _resolve_session_identity(
                session_id,
                collector_store=collector_store,
                job_store=job_store,
            )
            if identity is None:
                continue
            if session_date and identity["session_date"] != session_date:
                continue

            runs = _sort_session_runs(
                enrich_live_collector_job_run_payload(row.to_dict())
                for row in job_store.list_job_runs(
                    job_type="live_collector",
                    session_id=session_id,
                    limit=12,
                )
            )
            latest_run = runs[0] if runs else None
            latest_cycle = collector_store.get_latest_session_cycle(session_id)
            latest_cycle_payload = None if latest_cycle is None else latest_cycle.to_dict()

            board_count = 0
            watchlist_count = 0
            if latest_cycle is not None:
                board_count = len(
                    collector_store.list_cycle_candidates(latest_cycle["cycle_id"], bucket="board")
                )
                watchlist_count = len(
                    collector_store.list_cycle_candidates(latest_cycle["cycle_id"], bucket="watchlist")
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
                    "session_id": session_id,
                    "label": identity["label"],
                    "session_date": identity["session_date"],
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
                    "board_count": board_count,
                    "watchlist_count": watchlist_count,
                    "alert_count": alert_store.count_alert_events(
                        session_date=identity["session_date"],
                        label=identity["label"],
                    ),
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
    finally:
        alert_store.close()
        job_store.close()
        collector_store.close()


def get_session_detail(
    *,
    db_target: str,
    session_id: str,
    profit_target: float,
    stop_multiple: float,
) -> dict[str, Any]:
    collector_store = build_collector_repository(db_target)
    job_store = build_job_repository(db_target)
    alert_store = build_alert_repository(db_target)
    post_market_store = build_post_market_repository(db_target)
    try:
        identity = _resolve_session_identity(
            session_id,
            collector_store=collector_store,
            job_store=job_store,
        )
        if identity is None:
            raise ValueError(f"Unknown session_id: {session_id}")

        slot_runs = _sort_session_runs(
            enrich_live_collector_job_run_payload(row.to_dict())
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
        board_candidates: list[dict[str, Any]] = []
        watchlist_candidates: list[dict[str, Any]] = []
        if latest_cycle is not None:
            board_candidates = [
                candidate.to_dict()
                for candidate in collector_store.list_cycle_candidates(latest_cycle["cycle_id"], bucket="board")
            ]
            watchlist_candidates = [
                candidate.to_dict()
                for candidate in collector_store.list_cycle_candidates(latest_cycle["cycle_id"], bucket="watchlist")
            ]
            current_cycle = {
                **latest_cycle.to_dict(),
                "board_candidates": board_candidates,
                "watchlist_candidates": watchlist_candidates,
            }

        alerts = [
            alert.to_dict()
            for alert in alert_store.list_alert_events(
                session_date=identity["session_date"],
                label=identity["label"],
                limit=200,
            )
        ]
        events = [
            event.to_dict()
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
            summary = build_session_summary(
                db_target=db_target,
                session_date=identity["session_date"],
                label=identity["label"],
                profit_target=profit_target,
                stop_multiple=stop_multiple,
            )
            analysis = {
                **summary,
                "analysis_run": analysis_run.to_dict(),
            }

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
            limit=25,
        )

        return {
            "session_id": session_id,
            "label": identity["label"],
            "session_date": identity["session_date"],
            "status": _derive_session_status(latest_run=latest_run, latest_cycle=current_cycle),
            "updated_at": updated_at,
            "latest_slot": latest_run,
            "current_cycle": current_cycle,
            "board_candidates": board_candidates,
            "watchlist_candidates": watchlist_candidates,
            "slot_runs": slot_runs,
            "alerts": alerts,
            "events": events,
            "executions": executions,
            "analysis": analysis,
        }
    finally:
        post_market_store.close()
        alert_store.close()
        job_store.close()
        collector_store.close()
