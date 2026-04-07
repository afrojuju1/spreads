from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from fastapi import FastAPI, HTTPException, Query

from spreads.domain.profiles import UNIVERSE_PRESETS
from spreads.services.analysis import (
    build_signal_tuning,
    build_session_outcomes,
    build_session_summary,
    render_session_summary_markdown,
)
from spreads.jobs.orchestration import SCHEDULER_RUNTIME_LEASE_KEY, WORKER_RUNTIME_LEASE_PREFIX
from spreads.storage import (
    build_alert_repository,
    build_collector_repository,
    build_history_store,
    build_job_repository,
    default_database_url,
)

app = FastAPI(title="Spreads API", version="0.2.0")


def resolve_db(db: str | None) -> str:
    return db or default_database_url()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/universes")
def list_universes() -> dict[str, list[str]]:
    return {name: list(symbols) for name, symbols in UNIVERSE_PRESETS.items()}


@app.get("/live/{label}")
def get_live_snapshot(label: str, db: str | None = None) -> dict[str, Any]:
    store = build_collector_repository(resolve_db(db))
    try:
        cycle = store.get_latest_cycle(label)
        if cycle is None:
            raise HTTPException(status_code=404, detail="Live cycle not found")
        board = store.list_cycle_candidates(cycle["cycle_id"], bucket="board")
        watchlist = store.list_cycle_candidates(cycle["cycle_id"], bucket="watchlist")
        return {
            **cycle.to_dict(),
            "board_candidates": [candidate.to_dict() for candidate in board],
            "watchlist_candidates": [candidate.to_dict() for candidate in watchlist],
            "failures": cycle["failures"],
        }
    finally:
        store.close()


@app.get("/live/{label}/cycles")
def list_live_cycles(
    label: str,
    session_date: str | None = None,
    limit: int = Query(default=50, ge=1, le=1000),
    db: str | None = None,
) -> dict[str, Any]:
    store = build_collector_repository(resolve_db(db))
    try:
        cycles = store.list_cycles(label=label, session_date=session_date, limit=limit)
        return {"cycles": [cycle.to_dict() for cycle in cycles]}
    finally:
        store.close()


@app.get("/live/{label}/events")
def list_live_events(
    label: str,
    session_date: str | None = None,
    limit: int = Query(default=200, ge=1, le=5000),
    db: str | None = None,
) -> dict[str, Any]:
    store = build_collector_repository(resolve_db(db))
    try:
        resolved_session_date = session_date
        if resolved_session_date is None:
            cycle = store.get_latest_cycle(label)
            if cycle is None:
                raise HTTPException(status_code=404, detail="Live cycle not found")
            resolved_session_date = cycle["session_date"]
        events = store.list_events(
            label=label,
            session_date=resolved_session_date,
            limit=limit,
            ascending=True,
        )
        return {
            "label": label,
            "session_date": resolved_session_date,
            "events": [event.to_dict() for event in events],
        }
    finally:
        store.close()


@app.get("/history/runs")
def list_history_runs(
    symbol: str | None = None,
    strategy: str | None = None,
    limit: int = Query(default=25, ge=1, le=500),
    db: str | None = None,
) -> dict[str, Any]:
    store = build_history_store(resolve_db(db))
    try:
        return {
            "runs": [
                run.to_dict()
                for run in store.list_runs(
                    limit=limit,
                    symbol=symbol,
                    strategy=strategy,
                )
            ]
        }
    finally:
        store.close()


@app.get("/history/runs/{run_id}")
def get_history_run(run_id: str, db: str | None = None) -> dict[str, Any]:
    store = build_history_store(resolve_db(db))
    try:
        run = store.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return run.to_dict()
    finally:
        store.close()


@app.get("/history/runs/{run_id}/candidates")
def get_history_run_candidates(run_id: str, db: str | None = None) -> dict[str, Any]:
    store = build_history_store(resolve_db(db))
    try:
        run = store.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        candidates = store.list_candidates(run_id)
        return {
            "run_id": run_id,
            "candidates": [candidate.to_dict() for candidate in candidates],
        }
    finally:
        store.close()


@app.get("/sessions/{session_date}/{label}/outcomes")
def get_session_outcomes(
    session_date: str,
    label: str,
    replay_profit_target: float = Query(default=0.5, gt=0),
    replay_stop_multiple: float = Query(default=2.0, gt=0),
    db: str | None = None,
) -> dict[str, Any]:
    db_target = resolve_db(db)
    history_store = build_history_store(db_target)
    collector_store = build_collector_repository(db_target)
    try:
        return build_session_outcomes(
            history_store=history_store,
            collector_store=collector_store,
            session_date=session_date,
            label=label,
            profit_target=replay_profit_target,
            stop_multiple=replay_stop_multiple,
        )
    finally:
        collector_store.close()
        history_store.close()


@app.get("/sessions/{session_date}/{label}/summary")
def get_session_summary(
    session_date: str,
    label: str,
    replay_profit_target: float = Query(default=0.5, gt=0),
    replay_stop_multiple: float = Query(default=2.0, gt=0),
    db: str | None = None,
) -> dict[str, Any]:
    return build_session_summary(
        db_target=resolve_db(db),
        session_date=session_date,
        label=label,
        profit_target=replay_profit_target,
        stop_multiple=replay_stop_multiple,
    )


@app.get("/sessions/{session_date}/{label}/tuning")
def get_session_tuning(
    session_date: str,
    label: str,
    replay_profit_target: float = Query(default=0.5, gt=0),
    replay_stop_multiple: float = Query(default=2.0, gt=0),
    db: str | None = None,
) -> dict[str, Any]:
    db_target = resolve_db(db)
    history_store = build_history_store(db_target)
    collector_store = build_collector_repository(db_target)
    try:
        outcomes = build_session_outcomes(
            history_store=history_store,
            collector_store=collector_store,
            session_date=session_date,
            label=label,
            profit_target=replay_profit_target,
            stop_multiple=replay_stop_multiple,
        )
        return build_signal_tuning(outcomes)
    finally:
        collector_store.close()
        history_store.close()


@app.get("/alerts")
def list_alerts(
    session_date: str | None = None,
    label: str | None = None,
    symbol: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    db: str | None = None,
) -> dict[str, Any]:
    store = build_alert_repository(resolve_db(db))
    try:
        alerts = store.list_alert_events(
            session_date=session_date,
            label=label,
            symbol=symbol,
            limit=limit,
        )
        return {"alerts": [alert.to_dict() for alert in alerts]}
    finally:
        store.close()


@app.get("/alerts/latest")
def list_latest_alerts(
    limit: int = Query(default=25, ge=1, le=250),
    db: str | None = None,
) -> dict[str, Any]:
    store = build_alert_repository(resolve_db(db))
    try:
        alerts = store.list_alert_events(limit=limit)
        return {"alerts": [alert.to_dict() for alert in alerts]}
    finally:
        store.close()


@app.get("/alerts/{alert_id}")
def get_alert(alert_id: int, db: str | None = None) -> dict[str, Any]:
    store = build_alert_repository(resolve_db(db))
    try:
        alert = store.get_alert_event(alert_id)
        if alert is None:
            raise HTTPException(status_code=404, detail="Alert not found")
        return alert.to_dict()
    finally:
        store.close()


@app.get("/jobs")
def list_jobs(
    enabled_only: bool | None = Query(default=None),
    job_type: str | None = None,
    db: str | None = None,
) -> dict[str, Any]:
    store = build_job_repository(resolve_db(db))
    try:
        rows = store.list_job_definitions(enabled_only=enabled_only, job_type=job_type)
        return {"jobs": [row.to_dict() for row in rows]}
    finally:
        store.close()


@app.get("/jobs/runs")
def list_job_runs(
    job_key: str | None = None,
    job_type: str | None = None,
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    db: str | None = None,
) -> dict[str, Any]:
    store = build_job_repository(resolve_db(db))
    try:
        rows = store.list_job_runs(job_key=job_key, job_type=job_type, status=status, limit=limit)
        return {"job_runs": [row.to_dict() for row in rows]}
    finally:
        store.close()


@app.get("/jobs/runs/{job_run_id}")
def get_job_run(job_run_id: str, db: str | None = None) -> dict[str, Any]:
    store = build_job_repository(resolve_db(db))
    try:
        row = store.get_job_run(job_run_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Job run not found")
        return row.to_dict()
    finally:
        store.close()


@app.get("/jobs/health")
def get_jobs_health(db: str | None = None) -> dict[str, Any]:
    store = build_job_repository(resolve_db(db))
    try:
        definitions = store.list_job_definitions(enabled_only=True)
        running = store.list_job_runs(status="running", limit=100)
        queued = store.list_job_runs(status="queued", limit=100)
        scheduler = store.get_lease(SCHEDULER_RUNTIME_LEASE_KEY)
        workers = store.list_active_leases(prefix=WORKER_RUNTIME_LEASE_PREFIX)
        latest_collectors: dict[str, Any] = {}
        for definition in definitions:
            if definition["job_type"] != "live_collector":
                continue
            runs = store.list_job_runs(job_key=definition["job_key"], status="succeeded", limit=1)
            latest_collectors[definition["job_key"]] = None if not runs else runs[0].to_dict()
        return {
            "scheduler": None if scheduler is None else scheduler.to_dict(),
            "workers": [lease.to_dict() for lease in workers],
            "running_jobs": [row.to_dict() for row in running],
            "queued_jobs": [row.to_dict() for row in queued],
            "latest_successful_collectors": latest_collectors,
        }
    finally:
        store.close()


@app.get("/analysis/{session_date}/{label}")
def get_analysis_report(
    session_date: str,
    label: str,
    replay_profit_target: float = Query(default=0.5, gt=0),
    replay_stop_multiple: float = Query(default=2.0, gt=0),
    db: str | None = None,
) -> dict[str, Any]:
    summary = build_session_summary(
        db_target=resolve_db(db),
        session_date=session_date,
        label=label,
        profit_target=replay_profit_target,
        stop_multiple=replay_stop_multiple,
    )
    content = render_session_summary_markdown(summary)
    return {"session_date": session_date, "label": label, "content": content}
