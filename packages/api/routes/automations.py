from __future__ import annotations

from fastapi import APIRouter, Query

from api.errors import bad_request_error, not_found_error
from core.runtime.config import default_database_url
from core.services.automation_runtimes import (
    get_automation_runtime_detail,
    list_automation_runtimes,
)
from core.services.market_dates import resolve_market_date

router = APIRouter()


def _db_target(db: str | None) -> str:
    return db or default_database_url()


@router.get("/automations")
def list_automations_route(
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
    return list_automation_runtimes(
        db_target=_db_target(db),
        market_date=resolved_market_date,
        limit=limit,
    )


@router.get("/automations/{bot_id}/{automation_id}")
def get_automation_route(
    bot_id: str,
    automation_id: str,
    market_date: str | None = None,
    limit: int = Query(default=200, ge=1, le=500),
    db: str | None = None,
) -> dict[str, object]:
    try:
        resolved_market_date = (
            None if market_date is None else resolve_market_date(market_date)
        )
        return get_automation_runtime_detail(
            db_target=_db_target(db),
            bot_id=bot_id,
            automation_id=automation_id,
            market_date=resolved_market_date,
            limit=limit,
        )
    except ValueError as exc:
        raise not_found_error(exc) from exc
