from __future__ import annotations

import asyncio
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from arq import create_pool
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
import redis.asyncio as redis_async

from spreads.domain.profiles import UNIVERSE_PRESETS
from spreads.events import GLOBAL_EVENTS_CHANNEL, publish_global_event_async
from spreads.services.analysis import (
    build_signal_tuning,
    build_session_outcomes,
    build_session_summary,
    render_session_summary_markdown,
)
from spreads.services.generator import (
    build_generator_args,
    generate_symbol_ideas,
    generator_job_channel,
    generator_result_summary,
    list_generator_symbol_suggestions,
)
from spreads.jobs.orchestration import (
    SCHEDULER_RUNTIME_LEASE_KEY,
    WORKER_RUNTIME_LEASE_PREFIX,
    build_redis_settings,
    default_redis_url,
)
from spreads.storage import (
    build_alert_repository,
    build_collector_repository,
    build_generator_job_repository,
    build_history_store,
    build_job_repository,
    build_post_market_repository,
    default_database_url,
)

app = FastAPI(title="Spreads API", version="0.2.0")


class GeneratorRequest(BaseModel):
    symbol: str = Field(..., min_length=1)
    profile: str = Field(default="weekly")
    strategy: str = Field(default="combined")
    greeks_source: str = Field(default="auto")
    top: int = Field(default=5, ge=1, le=25)
    min_credit: float | None = Field(default=None, gt=0)
    short_delta_max: float | None = Field(default=None, gt=0)
    short_delta_target: float | None = Field(default=None, gt=0)
    allow_off_hours: bool = False


class GeneratorJobRequest(GeneratorRequest):
    pass


def resolve_db(db: str | None) -> str:
    return db or default_database_url()


def _generator_request_payload(payload: GeneratorRequest, *, db_target: str) -> dict[str, Any]:
    return {
        "history_db": db_target,
        "symbol": payload.symbol.upper(),
        "profile": payload.profile,
        "strategy": payload.strategy,
        "greeks_source": payload.greeks_source,
        "top": payload.top,
        "min_credit": payload.min_credit,
        "short_delta_max": payload.short_delta_max,
        "short_delta_target": payload.short_delta_target,
        "allow_off_hours": payload.allow_off_hours,
    }


def _generator_job_payload(job: Any, *, include_result: bool = True) -> dict[str, Any]:
    payload = job.to_dict()
    result = payload.get("result") if include_result else None
    payload["summary"] = generator_result_summary(result)
    if not include_result:
        payload.pop("result", None)
    return payload


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/universes")
def list_universes() -> dict[str, list[str]]:
    return {name: list(symbols) for name, symbols in UNIVERSE_PRESETS.items()}


@app.post("/generator/ideas")
def generate_ideas(payload: GeneratorRequest, db: str | None = None) -> dict[str, Any]:
    args = build_generator_args(_generator_request_payload(payload, db_target=resolve_db(db)))
    result = generate_symbol_ideas(args)
    result["allow_off_hours"] = payload.allow_off_hours
    return result


@app.get("/generator/symbols")
def list_generator_symbols(
    query: str = Query(default="", max_length=64),
    limit: int = Query(default=40, ge=1, le=200),
) -> dict[str, Any]:
    return list_generator_symbol_suggestions(query=query, limit=limit)


@app.post("/generator/jobs")
async def create_generator_job(payload: GeneratorJobRequest, db: str | None = None) -> dict[str, Any]:
    db_target = resolve_db(db)
    store = build_generator_job_repository(db_target)
    redis = await create_pool(build_redis_settings(default_redis_url()))
    try:
        generator_job_id = f"generator:{uuid4().hex}"
        request_payload = _generator_request_payload(payload, db_target=db_target)
        job = store.create_job(
            generator_job_id=generator_job_id,
            arq_job_id=generator_job_id,
            symbol=payload.symbol.upper(),
            created_at=datetime.now(UTC),
            request=request_payload,
            status="queued",
        )
        try:
            await redis.enqueue_job(
                "run_generator_job",
                generator_job_id,
                request_payload,
                _job_id=generator_job_id,
            )
        except Exception as exc:
            job = store.fail_job(
                generator_job_id=generator_job_id,
                finished_at=datetime.now(UTC),
                error_text=str(exc),
            )
            try:
                await publish_global_event_async(
                    redis,
                    topic="generator.job.updated",
                    entity_type="generator_job",
                    entity_id=generator_job_id,
                    payload=_generator_job_payload(job),
                )
            except Exception:
                pass
            raise
        try:
            await publish_global_event_async(
                redis,
                topic="generator.job.updated",
                entity_type="generator_job",
                entity_id=generator_job_id,
                payload=_generator_job_payload(job),
            )
        except Exception:
            pass
        return _generator_job_payload(job)
    finally:
        await redis.close()
        store.close()


@app.get("/generator/jobs")
def list_generator_jobs(
    symbol: str | None = None,
    status: str | None = None,
    limit: int = Query(default=12, ge=1, le=200),
    db: str | None = None,
) -> dict[str, Any]:
    store = build_generator_job_repository(resolve_db(db))
    try:
        symbol_filter = symbol.strip().upper() if symbol and symbol.strip() else None
        status_filter = status.strip().lower() if status and status.strip() else None
        if status_filter == "all":
            status_filter = None
        jobs = store.list_jobs(symbol=symbol_filter, status=status_filter, limit=limit)
        return {"jobs": [_generator_job_payload(job, include_result=False) for job in jobs]}
    finally:
        store.close()


@app.get("/generator/jobs/{generator_job_id}")
def get_generator_job(generator_job_id: str, db: str | None = None) -> dict[str, Any]:
    store = build_generator_job_repository(resolve_db(db))
    try:
        job = store.get_job(generator_job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Generator job not found")
        return _generator_job_payload(job)
    finally:
        store.close()


@app.websocket("/ws/events")
async def global_events_ws(websocket: WebSocket) -> None:
    redis = redis_async.from_url(default_redis_url(), decode_responses=True)
    pubsub = redis.pubsub()
    try:
        await websocket.accept()
        await pubsub.subscribe(GLOBAL_EVENTS_CHANNEL)
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message.get("type") == "message":
                payload = message["data"]
                if isinstance(payload, str):
                    await websocket.send_json(json.loads(payload))
                else:
                    await websocket.send_json(payload)
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        return
    finally:
        await pubsub.unsubscribe(GLOBAL_EVENTS_CHANNEL)
        await pubsub.aclose()
        await redis.aclose()


@app.websocket("/ws/generator/{generator_job_id}")
async def generator_job_ws(websocket: WebSocket, generator_job_id: str, db: str | None = None) -> None:
    db_target = resolve_db(db)
    store = build_generator_job_repository(db_target)
    redis = redis_async.from_url(default_redis_url(), decode_responses=True)
    pubsub = redis.pubsub()
    channel = generator_job_channel(generator_job_id)
    try:
        job = store.get_job(generator_job_id)
        await websocket.accept()
        if job is None:
            await websocket.send_json({"type": "error", "detail": "Generator job not found"})
            await websocket.close(code=4404)
            return
        await websocket.send_json({"type": "snapshot", "job": _generator_job_payload(job)})
        await pubsub.subscribe(channel)
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message.get("type") == "message":
                payload = message["data"]
                if isinstance(payload, str):
                    await websocket.send_json(json.loads(payload))
                else:
                    await websocket.send_json(payload)
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        return
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.aclose()
        await redis.aclose()
        store.close()


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


@app.get("/post-market/runs")
def list_post_market_runs(
    session_date: str | None = None,
    label: str | None = None,
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    db: str | None = None,
) -> dict[str, Any]:
    store = build_post_market_repository(resolve_db(db))
    try:
        rows = store.list_runs(session_date=session_date, label=label, status=status, limit=limit)
        return {"analysis_runs": [row.to_dict() for row in rows]}
    finally:
        store.close()


@app.get("/post-market/runs/{analysis_run_id}")
def get_post_market_run(analysis_run_id: str, db: str | None = None) -> dict[str, Any]:
    store = build_post_market_repository(resolve_db(db))
    try:
        row = store.get_run(analysis_run_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Post-market analysis run not found")
        return row.to_dict()
    finally:
        store.close()


@app.get("/post-market/{session_date}/{label}")
def get_post_market_analysis(session_date: str, label: str, db: str | None = None) -> dict[str, Any]:
    store = build_post_market_repository(resolve_db(db))
    try:
        row = store.get_latest_run(label=label, session_date=session_date)
        if row is None:
            raise HTTPException(status_code=404, detail="Post-market analysis not found")
        return row.to_dict()
    finally:
        store.close()
