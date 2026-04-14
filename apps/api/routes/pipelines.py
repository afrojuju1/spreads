from __future__ import annotations

from fastapi import APIRouter, Query

from apps.api.errors import bad_request_error, not_found_error
from spreads.runtime.config import default_database_url
from spreads.services.analysis import resolve_date
from spreads.services.pipelines import (
    get_pipeline_detail,
    list_pipeline_cycles,
    list_pipelines,
)

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
        resolved_market_date = None if market_date is None else resolve_date(market_date)
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    return list_pipelines(
        db_target=_db_target(db),
        limit=limit,
        market_date=resolved_market_date,
    )


@router.get("/pipelines/{pipeline_id}")
def get_pipeline_route(
    pipeline_id: str,
    market_date: str | None = None,
    include_replay: str = Query(default="none"),
    replay_profit_target: float = Query(default=0.5, gt=0),
    replay_stop_multiple: float = Query(default=2.0, gt=0),
    db: str | None = None,
) -> dict[str, object]:
    try:
        resolved_market_date = None if market_date is None else resolve_date(market_date)
        resolved_include_replay = include_replay.strip().lower()
        if resolved_include_replay not in {"none", "current", "recent", "both"}:
            raise bad_request_error(
                ValueError(
                    "include_replay must be one of: none, current, recent, both."
                )
            )
        return get_pipeline_detail(
            db_target=_db_target(db),
            pipeline_id=pipeline_id,
            market_date=resolved_market_date,
            include_replay=resolved_include_replay,
            profit_target=replay_profit_target,
            stop_multiple=replay_stop_multiple,
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
        resolved_market_date = None if market_date is None else resolve_date(market_date)
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    return list_pipeline_cycles(
        db_target=_db_target(db),
        pipeline_id=pipeline_id,
        market_date=resolved_market_date,
        limit=limit,
    )
