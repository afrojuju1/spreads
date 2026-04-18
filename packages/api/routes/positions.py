from __future__ import annotations

from fastapi import APIRouter, Query

from api.errors import bad_request_error, execution_runtime_error, not_found_error
from api.schemas.positions import PositionCloseRequest
from core.runtime.config import default_database_url
from core.services.market_dates import resolve_market_date
from core.services.execution import submit_position_close_by_id
from core.services.positions import get_position_detail, list_positions

router = APIRouter()


def _db_target(db: str | None) -> str:
    return db or default_database_url()


@router.get("/positions")
def list_positions_route(
    pipeline_id: str | None = None,
    label: str | None = None,
    bot_id: str | None = None,
    automation_id: str | None = None,
    strategy_config_id: str | None = None,
    market_date: str | None = None,
    limit: int = Query(default=200, ge=1, le=500),
    db: str | None = None,
) -> dict[str, object]:
    try:
        resolved_market_date = (
            None if market_date is None else resolve_market_date(market_date)
        )
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    return list_positions(
        db_target=_db_target(db),
        pipeline_id=pipeline_id,
        label=label,
        market_date=resolved_market_date,
        bot_id=bot_id,
        automation_id=automation_id,
        strategy_config_id=strategy_config_id,
        limit=limit,
    )


@router.get("/positions/{position_id}")
def get_position_route(
    position_id: str,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return get_position_detail(
            db_target=_db_target(db),
            position_id=position_id,
        )
    except ValueError as exc:
        raise not_found_error(exc) from exc


@router.post("/positions/{position_id}/close")
def close_position_route(
    position_id: str,
    payload: PositionCloseRequest,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return submit_position_close_by_id(
            db_target=_db_target(db),
            position_id=position_id,
            quantity=payload.quantity,
            limit_price=payload.limit_price,
        )
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    except RuntimeError as exc:
        raise execution_runtime_error(exc) from exc
