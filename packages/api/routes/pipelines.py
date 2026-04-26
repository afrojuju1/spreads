from __future__ import annotations

from fastapi import APIRouter, Query

from api.errors import bad_request_error, not_found_error, service_unavailable_error
from api.schemas.pipelines import PipelineRunRequest
from core.runtime.config import default_database_url
from core.services.discovery_sessions import (
    get_discovery_session_detail,
    list_discovery_session_cycles,
    list_discovery_sessions,
)
from core.services.market_dates import resolve_market_date
from core.services.pipelines import start_pipeline_run

router = APIRouter()


def _db_target(db: str | None) -> str:
    return db or default_database_url()


@router.get("/pipelines")
def list_pipelines_route(
    market_date: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    db: str | None = None,
) -> dict[str, object]:
    try:
        resolved_market_date = (
            None if market_date is None else resolve_market_date(market_date)
        )
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    return list_discovery_sessions(
        db_target=_db_target(db),
        limit=limit,
        market_date=resolved_market_date,
    )


@router.get("/pipelines/{pipeline_id}")
def get_pipeline_route(
    pipeline_id: str,
    market_date: str | None = None,
    cycle_id: str | None = None,
    db: str | None = None,
) -> dict[str, object]:
    try:
        resolved_market_date = (
            None if market_date is None else resolve_market_date(market_date)
        )
        return get_discovery_session_detail(
            db_target=_db_target(db),
            pipeline_id=pipeline_id,
            market_date=resolved_market_date,
            cycle_id=cycle_id,
        )
    except ValueError as exc:
        raise not_found_error(exc) from exc


@router.get("/pipelines/{pipeline_id}/cycles")
def list_pipeline_cycles_route(
    pipeline_id: str,
    market_date: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    db: str | None = None,
) -> dict[str, object]:
    try:
        resolved_market_date = (
            None if market_date is None else resolve_market_date(market_date)
        )
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    return list_discovery_session_cycles(
        db_target=_db_target(db),
        pipeline_id=pipeline_id,
        market_date=resolved_market_date,
        limit=limit,
    )


@router.post("/pipelines/run")
def start_pipeline_run_route(
    payload: PipelineRunRequest,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return start_pipeline_run(
            db_target=_db_target(db),
            symbol=payload.symbol,
            strategy_mode=payload.strategy_mode,
            strategy_family=payload.strategy_family,
        )
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    except RuntimeError as exc:
        raise service_unavailable_error(exc) from exc
