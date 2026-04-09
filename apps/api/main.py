from __future__ import annotations

import asyncio
import json
import sys
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from arq import create_pool
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
import redis.asyncio as redis_async
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from spreads.db.core import first_model_row, get_model_row, list_model_rows, to_storage_row
from spreads.db.decorators import with_session
from spreads.domain.profiles import UNIVERSE_PRESETS
from spreads.events.bus import GLOBAL_EVENTS_CHANNEL, publish_global_event_async
from spreads.jobs.orchestration import (
    SCHEDULER_RUNTIME_LEASE_KEY,
    WORKER_RUNTIME_LEASE_PREFIX,
)
from spreads.runtime.config import default_database_url, default_redis_url
from spreads.runtime.redis import build_redis_settings
from spreads.services.account_state import get_account_overview as get_alpaca_account_overview
from spreads.services.analysis import (
    build_session_summary,
    resolve_date,
    render_session_summary_markdown,
)
from spreads.services.generator import (
    build_generator_args,
    generate_symbol_ideas,
    generator_job_channel,
    generator_result_summary,
    list_generator_symbol_suggestions,
)
from spreads.services.live_collector_health import enrich_live_collector_job_run_payload
from spreads.services.option_quote_capture import AlpacaOptionQuoteCaptureBroker
from spreads.services.operator_actions import (
    apply_generator_live_action,
    create_manual_generator_alert,
)
from spreads.services.execution import (
    EXECUTION_SCHEMA_MESSAGE,
    refresh_live_session_execution,
    submit_session_position_close,
    submit_live_session_execution,
)
from spreads.services.sessions import (
    DEFAULT_ANALYSIS_PROFIT_TARGET,
    DEFAULT_ANALYSIS_STOP_MULTIPLE,
    get_session_detail,
    list_existing_sessions,
)
from spreads.storage.alert_models import AlertEventModel
from spreads.storage.collector_models import (
    CollectorCycleCandidateModel,
    CollectorCycleEventModel,
    CollectorCycleModel,
)
from spreads.storage.factory import (
    build_generator_job_repository,
)
from spreads.storage.generator_job_models import GeneratorJobModel
from spreads.storage.job_models import JobDefinitionModel, JobLeaseModel, JobRunModel
from spreads.storage.models import ScanCandidateModel, ScanRunModel
from spreads.storage.post_market_models import PostMarketAnalysisRunModel

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.option_quote_capture_broker = AlpacaOptionQuoteCaptureBroker()
    try:
        yield
    finally:
        await app.state.option_quote_capture_broker.aclose()


app = FastAPI(title="Spreads API", version="0.2.0", lifespan=lifespan)


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


class GeneratorCandidateActionRequest(BaseModel):
    action: Literal["create_alert", "promote_live"]
    strategy: str = Field(..., min_length=1)
    short_symbol: str = Field(..., min_length=1)
    long_symbol: str = Field(..., min_length=1)
    live_label: str | None = Field(default=None, min_length=1)
    bucket: Literal["board", "watchlist"] | None = None


class OptionQuoteCaptureRequest(BaseModel):
    candidates: list[dict[str, Any]] = Field(default_factory=list)
    feed: Literal["opra", "indicative"] = "opra"
    duration_seconds: float = Field(default=20.0, gt=0, le=60.0)
    data_base_url: str | None = None


class SessionExecutionRequest(BaseModel):
    candidate_id: int = Field(..., gt=0)
    quantity: int | None = Field(default=None, ge=1, le=25)
    limit_price: float | None = Field(default=None, gt=0)


class SessionPositionCloseRequest(BaseModel):
    quantity: int | None = Field(default=None, ge=1, le=25)
    limit_price: float | None = Field(default=None, gt=0)


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


def _job_run_payload(run: Any) -> dict[str, Any]:
    return enrich_live_collector_job_run_payload(run.to_dict())


def _cycle_candidates_payload(
    session: Session,
    *,
    cycle: dict[str, Any],
    bucket: str,
) -> list[dict[str, Any]]:
    statement = (
        select(CollectorCycleCandidateModel)
        .where(CollectorCycleCandidateModel.cycle_id == str(cycle["cycle_id"]))
        .where(CollectorCycleCandidateModel.bucket == bucket)
        .order_by(CollectorCycleCandidateModel.position.asc())
    )
    return [
        to_storage_row(
            model,
            extra={
                "label": cycle["label"],
                "session_date": cycle["session_date"],
                "generated_at": cycle["generated_at"],
            },
        ).to_dict()
        for model in session.scalars(statement).all()
    ]


@with_session()
def _list_generator_job_rows(
    *,
    symbol: str | None,
    status: str | None,
    limit: int,
    db_target: str | None = None,
    session: Session | None = None,
) -> list[dict[str, Any]]:
    statement = select(GeneratorJobModel)
    if symbol:
        statement = statement.where(GeneratorJobModel.symbol == symbol.upper())
    if status:
        statement = statement.where(GeneratorJobModel.status == status)
    statement = statement.order_by(GeneratorJobModel.created_at.desc()).limit(limit)
    return [_generator_job_payload(row, include_result=False) for row in list_model_rows(session, statement)]


@with_session()
def _get_generator_job_row(
    *,
    generator_job_id: str,
    db_target: str | None = None,
    session: Session | None = None,
) -> Any | None:
    return get_model_row(session, GeneratorJobModel, generator_job_id)


@with_session()
def _get_live_snapshot_payload(
    *,
    label: str,
    db_target: str | None = None,
    session: Session | None = None,
) -> dict[str, Any] | None:
    cycle = first_model_row(
        session,
        select(CollectorCycleModel)
        .where(CollectorCycleModel.label == label)
        .order_by(CollectorCycleModel.generated_at.desc(), CollectorCycleModel.cycle_id.desc())
        .limit(1),
    )
    if cycle is None:
        return None
    cycle_payload = cycle.to_dict()
    cycle_payload["board_candidates"] = _cycle_candidates_payload(session, cycle=cycle_payload, bucket="board")
    cycle_payload["watchlist_candidates"] = _cycle_candidates_payload(session, cycle=cycle_payload, bucket="watchlist")
    cycle_payload["failures"] = list(cycle_payload.get("failures") or [])
    return cycle_payload


@with_session()
def _list_live_cycles_payload(
    *,
    label: str,
    session_date: str | None,
    limit: int,
    db_target: str | None = None,
    session: Session | None = None,
) -> list[dict[str, Any]]:
    statement = select(CollectorCycleModel).where(CollectorCycleModel.label == label)
    if session_date:
        statement = statement.where(CollectorCycleModel.session_date == date.fromisoformat(session_date))
    statement = statement.order_by(CollectorCycleModel.generated_at.desc()).limit(limit)
    return [row.to_dict() for row in list_model_rows(session, statement)]


@with_session()
def _list_live_events_payload(
    *,
    label: str,
    session_date: str | None,
    limit: int,
    db_target: str | None = None,
    session: Session | None = None,
) -> dict[str, Any] | None:
    resolved_session_date = session_date
    if resolved_session_date is None:
        cycle = first_model_row(
            session,
            select(CollectorCycleModel)
            .where(CollectorCycleModel.label == label)
            .order_by(CollectorCycleModel.generated_at.desc(), CollectorCycleModel.cycle_id.desc())
            .limit(1),
        )
        if cycle is None:
            return None
        resolved_session_date = str(cycle["session_date"])
    statement = (
        select(CollectorCycleEventModel)
        .where(CollectorCycleEventModel.label == label)
        .where(CollectorCycleEventModel.session_date == date.fromisoformat(resolved_session_date))
        .order_by(CollectorCycleEventModel.generated_at.asc(), CollectorCycleEventModel.event_id.asc())
        .limit(limit)
    )
    return {
        "label": label,
        "session_date": resolved_session_date,
        "events": [row.to_dict() for row in list_model_rows(session, statement)],
    }


@with_session()
def _list_history_runs_payload(
    *,
    symbol: str | None,
    strategy: str | None,
    limit: int,
    db_target: str | None = None,
    session: Session | None = None,
) -> list[dict[str, Any]]:
    statement = select(ScanRunModel)
    if symbol:
        statement = statement.where(ScanRunModel.symbol == symbol.upper())
    if strategy:
        statement = statement.where(ScanRunModel.strategy == strategy)
    statement = statement.order_by(ScanRunModel.generated_at.desc()).limit(limit)
    return [row.to_dict() for row in list_model_rows(session, statement)]


@with_session()
def _get_history_run_payload(
    *,
    run_id: str,
    db_target: str | None = None,
    session: Session | None = None,
) -> dict[str, Any] | None:
    row = get_model_row(session, ScanRunModel, run_id)
    return None if row is None else row.to_dict()


@with_session()
def _list_history_run_candidates_payload(
    *,
    run_id: str,
    db_target: str | None = None,
    session: Session | None = None,
) -> list[dict[str, Any]]:
    statement = (
        select(ScanCandidateModel)
        .where(ScanCandidateModel.run_id == run_id)
        .order_by(ScanCandidateModel.rank.asc())
    )
    return [row.to_dict() for row in list_model_rows(session, statement)]


@with_session()
def _list_alert_rows(
    *,
    session_date: str | None,
    label: str | None,
    symbol: str | None,
    limit: int,
    db_target: str | None = None,
    session: Session | None = None,
) -> list[dict[str, Any]]:
    statement = select(AlertEventModel)
    if session_date:
        statement = statement.where(AlertEventModel.session_date == date.fromisoformat(session_date))
    if label:
        statement = statement.where(AlertEventModel.label == label)
    if symbol:
        statement = statement.where(AlertEventModel.symbol == symbol.upper())
    statement = statement.order_by(AlertEventModel.created_at.desc(), AlertEventModel.alert_id.desc()).limit(limit)
    return [row.to_dict() for row in list_model_rows(session, statement)]


@with_session()
def _get_alert_row(
    *,
    alert_id: int,
    db_target: str | None = None,
    session: Session | None = None,
) -> dict[str, Any] | None:
    row = get_model_row(session, AlertEventModel, alert_id)
    return None if row is None else row.to_dict()


@with_session()
def _list_job_definition_rows(
    *,
    enabled_only: bool | None,
    job_type: str | None,
    db_target: str | None = None,
    session: Session | None = None,
) -> list[dict[str, Any]]:
    statement = select(JobDefinitionModel)
    if enabled_only is True:
        statement = statement.where(JobDefinitionModel.enabled.is_(True))
    elif enabled_only is False:
        statement = statement.where(JobDefinitionModel.enabled.is_(False))
    if job_type:
        statement = statement.where(JobDefinitionModel.job_type == job_type)
    statement = statement.order_by(JobDefinitionModel.job_key.asc())
    return [row.to_dict() for row in list_model_rows(session, statement)]


@with_session()
def _list_job_run_rows(
    *,
    job_key: str | None = None,
    job_type: str | None = None,
    status: str | None = None,
    session_id: str | None = None,
    limit: int = 100,
    db_target: str | None = None,
    session: Session | None = None,
) -> list[dict[str, Any]]:
    statement = select(JobRunModel)
    if job_key:
        statement = statement.where(JobRunModel.job_key == job_key)
    if job_type:
        statement = statement.where(JobRunModel.job_type == job_type)
    if status:
        statement = statement.where(JobRunModel.status == status)
    if session_id:
        statement = statement.where(JobRunModel.session_id == session_id)
    statement = statement.order_by(JobRunModel.scheduled_for.desc(), JobRunModel.job_run_id.desc()).limit(limit)
    return [_job_run_payload(row) for row in list_model_rows(session, statement)]


@with_session()
def _get_job_run_row(
    *,
    job_run_id: str,
    db_target: str | None = None,
    session: Session | None = None,
) -> dict[str, Any] | None:
    row = get_model_row(session, JobRunModel, job_run_id)
    return None if row is None else _job_run_payload(row)


@with_session()
def _get_jobs_health_payload(
    *,
    db_target: str | None = None,
    session: Session | None = None,
) -> dict[str, Any]:
    definitions = list_model_rows(
        session,
        select(JobDefinitionModel)
        .where(JobDefinitionModel.enabled.is_(True))
        .order_by(JobDefinitionModel.job_key.asc()),
    )
    running = list_model_rows(
        session,
        select(JobRunModel)
        .where(JobRunModel.status == "running")
        .order_by(JobRunModel.scheduled_for.desc(), JobRunModel.job_run_id.desc())
        .limit(100),
    )
    queued = list_model_rows(
        session,
        select(JobRunModel)
        .where(JobRunModel.status == "queued")
        .order_by(JobRunModel.scheduled_for.desc(), JobRunModel.job_run_id.desc())
        .limit(100),
    )
    scheduler = get_model_row(session, JobLeaseModel, SCHEDULER_RUNTIME_LEASE_KEY)
    workers = list_model_rows(
        session,
        select(JobLeaseModel)
        .where(JobLeaseModel.lease_key.like(f"{WORKER_RUNTIME_LEASE_PREFIX}%"))
        .where(JobLeaseModel.expires_at > func.now())
        .order_by(JobLeaseModel.expires_at.desc(), JobLeaseModel.lease_key.asc()),
    )
    collector_job_keys = [
        str(definition["job_key"])
        for definition in definitions
        if definition["job_type"] == "live_collector"
    ]
    latest_collectors = {job_key: None for job_key in collector_job_keys}
    if collector_job_keys:
        ranked_runs = (
            select(
                JobRunModel.job_run_id.label("job_run_id"),
                func.row_number()
                .over(
                    partition_by=JobRunModel.job_key,
                    order_by=(JobRunModel.scheduled_for.desc(), JobRunModel.job_run_id.desc()),
                )
                .label("run_rank"),
            )
            .where(JobRunModel.job_key.in_(collector_job_keys))
            .where(JobRunModel.status == "succeeded")
            .subquery()
        )
        latest_run_rows = list_model_rows(
            session,
            select(JobRunModel)
            .join(ranked_runs, JobRunModel.job_run_id == ranked_runs.c.job_run_id)
            .where(ranked_runs.c.run_rank == 1),
        )
        latest_collectors.update(
            {
                str(row["job_key"]): _job_run_payload(row)
                for row in latest_run_rows
            }
        )
    return {
        "scheduler": None if scheduler is None else scheduler.to_dict(),
        "workers": [row.to_dict() for row in workers],
        "running_jobs": [_job_run_payload(row) for row in running],
        "queued_jobs": [_job_run_payload(row) for row in queued],
        "latest_successful_collectors": latest_collectors,
    }


@with_session()
def _list_post_market_run_rows(
    *,
    session_date: str | None,
    label: str | None,
    status: str | None,
    limit: int,
    db_target: str | None = None,
    session: Session | None = None,
) -> list[dict[str, Any]]:
    statement = select(PostMarketAnalysisRunModel)
    if session_date:
        statement = statement.where(PostMarketAnalysisRunModel.session_date == date.fromisoformat(session_date))
    if label:
        statement = statement.where(PostMarketAnalysisRunModel.label == label)
    if status:
        statement = statement.where(PostMarketAnalysisRunModel.status == status)
    statement = statement.order_by(
        PostMarketAnalysisRunModel.completed_at.desc().nullslast(),
        PostMarketAnalysisRunModel.created_at.desc(),
        PostMarketAnalysisRunModel.analysis_run_id.desc(),
    ).limit(limit)
    return [row.to_dict() for row in list_model_rows(session, statement)]


@with_session()
def _get_post_market_run_row(
    *,
    analysis_run_id: str,
    db_target: str | None = None,
    session: Session | None = None,
) -> dict[str, Any] | None:
    row = get_model_row(session, PostMarketAnalysisRunModel, analysis_run_id)
    return None if row is None else row.to_dict()


@with_session()
def _get_latest_post_market_run_row(
    *,
    session_date: str,
    label: str,
    db_target: str | None = None,
    session: Session | None = None,
) -> dict[str, Any] | None:
    row = first_model_row(
        session,
        select(PostMarketAnalysisRunModel)
        .where(PostMarketAnalysisRunModel.label == label)
        .where(PostMarketAnalysisRunModel.session_date == date.fromisoformat(session_date))
        .order_by(
            PostMarketAnalysisRunModel.completed_at.desc().nullslast(),
            PostMarketAnalysisRunModel.created_at.desc(),
            PostMarketAnalysisRunModel.analysis_run_id.desc(),
        )
        .limit(1),
    )
    return None if row is None else row.to_dict()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/account/overview")
def get_account_overview(
    history_range: Literal["1D", "1W", "1M"] = Query(default="1D"),
) -> dict[str, Any]:
    try:
        return get_alpaca_account_overview(history_range=history_range)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/internal/market-data/option-quotes/capture")
async def capture_option_quotes(payload: OptionQuoteCaptureRequest, request: Request) -> dict[str, Any]:
    broker = request.app.state.option_quote_capture_broker
    try:
        quotes = await broker.capture_quote_records(
            candidates=list(payload.candidates),
            feed=payload.feed,
            duration_seconds=payload.duration_seconds,
            data_base_url=payload.data_base_url,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"quotes": quotes}


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
    symbol_filter = symbol.strip().upper() if symbol and symbol.strip() else None
    status_filter = status.strip().lower() if status and status.strip() else None
    if status_filter == "all":
        status_filter = None
    return {
        "jobs": _list_generator_job_rows(
            symbol=symbol_filter,
            status=status_filter,
            limit=limit,
            db_target=resolve_db(db),
        )
    }


@app.get("/generator/jobs/{generator_job_id}")
def get_generator_job(generator_job_id: str, db: str | None = None) -> dict[str, Any]:
    job = _get_generator_job_row(generator_job_id=generator_job_id, db_target=resolve_db(db))
    if job is None:
        raise HTTPException(status_code=404, detail="Generator job not found")
    return _generator_job_payload(job)


@app.post("/generator/jobs/{generator_job_id}/actions")
def apply_generator_job_action(
    generator_job_id: str,
    payload: GeneratorCandidateActionRequest,
    db: str | None = None,
) -> dict[str, Any]:
    db_target = resolve_db(db)
    store = build_generator_job_repository(db_target)
    try:
        job = store.get_job(generator_job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Generator job not found")
        try:
            if payload.action == "create_alert":
                if not payload.live_label:
                    raise HTTPException(status_code=400, detail="live_label is required to create an alert")
                return create_manual_generator_alert(
                    job=job,
                    live_label=payload.live_label,
                    strategy=payload.strategy,
                    short_symbol=payload.short_symbol,
                    long_symbol=payload.long_symbol,
                    db_target=db_target,
                )
            if not payload.live_label:
                raise HTTPException(status_code=400, detail="live_label is required to update live workflow")
            if payload.bucket is None:
                raise HTTPException(status_code=400, detail="bucket is required to update live workflow")
            return apply_generator_live_action(
                job=job,
                live_label=payload.live_label,
                bucket=payload.bucket,
                strategy=payload.strategy,
                short_symbol=payload.short_symbol,
                long_symbol=payload.long_symbol,
                db_target=db_target,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
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
    except asyncio.CancelledError:
        return
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
    except asyncio.CancelledError:
        return
    except WebSocketDisconnect:
        return
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.aclose()
        await redis.aclose()
        store.close()


@app.get("/live/{label}")
def get_live_snapshot(label: str, db: str | None = None) -> dict[str, Any]:
    payload = _get_live_snapshot_payload(label=label, db_target=resolve_db(db))
    if payload is None:
        raise HTTPException(status_code=404, detail="Live cycle not found")
    return payload


@app.get("/live/{label}/cycles")
def list_live_cycles(
    label: str,
    session_date: str | None = None,
    limit: int = Query(default=50, ge=1, le=1000),
    db: str | None = None,
) -> dict[str, Any]:
    return {
        "cycles": _list_live_cycles_payload(
            label=label,
            session_date=session_date,
            limit=limit,
            db_target=resolve_db(db),
        )
    }


@app.get("/live/{label}/events")
def list_live_events(
    label: str,
    session_date: str | None = None,
    limit: int = Query(default=200, ge=1, le=5000),
    db: str | None = None,
) -> dict[str, Any]:
    payload = _list_live_events_payload(
        label=label,
        session_date=session_date,
        limit=limit,
        db_target=resolve_db(db),
    )
    if payload is None:
        raise HTTPException(status_code=404, detail="Live cycle not found")
    return payload


@app.get("/history/runs")
def list_history_runs(
    symbol: str | None = None,
    strategy: str | None = None,
    limit: int = Query(default=25, ge=1, le=500),
    db: str | None = None,
) -> dict[str, Any]:
    return {
        "runs": _list_history_runs_payload(
            symbol=symbol,
            strategy=strategy,
            limit=limit,
            db_target=resolve_db(db),
        )
    }


@app.get("/history/runs/{run_id}")
def get_history_run(run_id: str, db: str | None = None) -> dict[str, Any]:
    run = _get_history_run_payload(run_id=run_id, db_target=resolve_db(db))
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


@app.get("/history/runs/{run_id}/candidates")
def get_history_run_candidates(run_id: str, db: str | None = None) -> dict[str, Any]:
    run = _get_history_run_payload(run_id=run_id, db_target=resolve_db(db))
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return {
        "run_id": run_id,
        "candidates": _list_history_run_candidates_payload(run_id=run_id, db_target=resolve_db(db)),
    }


@app.get("/sessions")
def list_sessions(
    session_date: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    db: str | None = None,
) -> dict[str, Any]:
    resolved_session_date = None if session_date is None else resolve_date(session_date)
    return list_existing_sessions(
        db_target=resolve_db(db),
        limit=limit,
        session_date=resolved_session_date,
    )


@app.get("/sessions/{session_id}")
def get_session(
    session_id: str,
    replay_profit_target: float = Query(default=0.5, gt=0),
    replay_stop_multiple: float = Query(default=2.0, gt=0),
    db: str | None = None,
) -> dict[str, Any]:
    try:
        return get_session_detail(
            db_target=resolve_db(db),
            session_id=session_id,
            profit_target=replay_profit_target,
            stop_multiple=replay_stop_multiple,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/sessions/{session_id}/executions")
def submit_session_execution(
    session_id: str,
    payload: SessionExecutionRequest,
    db: str | None = None,
) -> dict[str, Any]:
    try:
        return submit_live_session_execution(
            db_target=resolve_db(db),
            session_id=session_id,
            candidate_id=payload.candidate_id,
            quantity=payload.quantity,
            limit_price=payload.limit_price,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        if str(exc) == EXECUTION_SCHEMA_MESSAGE:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/sessions/{session_id}/positions/{session_position_id}/close")
def close_session_position(
    session_id: str,
    session_position_id: str,
    payload: SessionPositionCloseRequest,
    db: str | None = None,
) -> dict[str, Any]:
    try:
        return submit_session_position_close(
            db_target=resolve_db(db),
            session_id=session_id,
            session_position_id=session_position_id,
            quantity=payload.quantity,
            limit_price=payload.limit_price,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        if str(exc) == EXECUTION_SCHEMA_MESSAGE:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/sessions/{session_id}/executions/{execution_attempt_id}/refresh")
def refresh_session_execution(
    session_id: str,
    execution_attempt_id: str,
    db: str | None = None,
) -> dict[str, Any]:
    try:
        return refresh_live_session_execution(
            db_target=resolve_db(db),
            session_id=session_id,
            execution_attempt_id=execution_attempt_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        if str(exc) == EXECUTION_SCHEMA_MESSAGE:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/alerts")
def list_alerts(
    session_date: str | None = None,
    label: str | None = None,
    symbol: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    db: str | None = None,
) -> dict[str, Any]:
    return {
        "alerts": _list_alert_rows(
            session_date=session_date,
            label=label,
            symbol=symbol,
            limit=limit,
            db_target=resolve_db(db),
        )
    }


@app.get("/alerts/latest")
def list_latest_alerts(
    limit: int = Query(default=25, ge=1, le=250),
    db: str | None = None,
) -> dict[str, Any]:
    return {
        "alerts": _list_alert_rows(
            session_date=None,
            label=None,
            symbol=None,
            limit=limit,
            db_target=resolve_db(db),
        )
    }


@app.get("/alerts/{alert_id}")
def get_alert(alert_id: int, db: str | None = None) -> dict[str, Any]:
    alert = _get_alert_row(alert_id=alert_id, db_target=resolve_db(db))
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    return alert


@app.get("/jobs")
def list_jobs(
    enabled_only: bool | None = Query(default=None),
    job_type: str | None = None,
    db: str | None = None,
) -> dict[str, Any]:
    return {
        "jobs": _list_job_definition_rows(
            enabled_only=enabled_only,
            job_type=job_type,
            db_target=resolve_db(db),
        )
    }


@app.get("/jobs/runs")
def list_job_runs(
    job_key: str | None = None,
    job_type: str | None = None,
    status: str | None = None,
    session_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    db: str | None = None,
) -> dict[str, Any]:
    return {
        "job_runs": _list_job_run_rows(
            job_key=job_key,
            job_type=job_type,
            status=status,
            session_id=session_id,
            limit=limit,
            db_target=resolve_db(db),
        )
    }


@app.get("/jobs/runs/{job_run_id}")
def get_job_run(job_run_id: str, db: str | None = None) -> dict[str, Any]:
    row = _get_job_run_row(job_run_id=job_run_id, db_target=resolve_db(db))
    if row is None:
        raise HTTPException(status_code=404, detail="Job run not found")
    return row


@app.get("/jobs/health")
def get_jobs_health(db: str | None = None) -> dict[str, Any]:
    return _get_jobs_health_payload(db_target=resolve_db(db))


@app.get("/analysis/{session_date}/{label}")
def get_analysis_report(
    session_date: str,
    label: str,
    replay_profit_target: float = Query(default=0.5, gt=0),
    replay_stop_multiple: float = Query(default=2.0, gt=0),
    db: str | None = None,
) -> dict[str, Any]:
    content: str | None = None
    if (
        abs(float(replay_profit_target) - DEFAULT_ANALYSIS_PROFIT_TARGET) < 1e-9
        and abs(float(replay_stop_multiple) - DEFAULT_ANALYSIS_STOP_MULTIPLE) < 1e-9
    ):
        analysis_run = _get_latest_post_market_run_row(
            session_date=session_date,
            label=label,
            db_target=resolve_db(db),
        )
        if analysis_run is not None:
            stored_report = analysis_run.get("report_markdown")
            if isinstance(stored_report, str) and stored_report.strip():
                content = stored_report
            else:
                stored_summary = analysis_run.get("summary")
                if isinstance(stored_summary, dict):
                    content = render_session_summary_markdown(stored_summary)
    if content is None:
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
    return {
        "analysis_runs": _list_post_market_run_rows(
            session_date=session_date,
            label=label,
            status=status,
            limit=limit,
            db_target=resolve_db(db),
        )
    }


@app.get("/post-market/runs/{analysis_run_id}")
def get_post_market_run(analysis_run_id: str, db: str | None = None) -> dict[str, Any]:
    row = _get_post_market_run_row(analysis_run_id=analysis_run_id, db_target=resolve_db(db))
    if row is None:
        raise HTTPException(status_code=404, detail="Post-market analysis run not found")
    return row


@app.get("/post-market/{session_date}/{label}")
def get_post_market_analysis(session_date: str, label: str, db: str | None = None) -> dict[str, Any]:
    row = _get_latest_post_market_run_row(
        session_date=session_date,
        label=label,
        db_target=resolve_db(db),
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Post-market analysis not found")
    return row
