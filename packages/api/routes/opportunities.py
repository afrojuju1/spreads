from __future__ import annotations

from fastapi import APIRouter, Query

from api.errors import bad_request_error, execution_runtime_error, not_found_error
from api.schemas.opportunities import OpportunityExecutionRequest
from core.runtime.config import default_database_url
from core.services.analysis import resolve_date
from core.services.execution import submit_opportunity_execution
from core.services.opportunities import get_opportunity_detail, list_opportunities

router = APIRouter()


def _db_target(db: str | None) -> str:
    return db or default_database_url()


@router.get("/opportunities")
def list_opportunities_route(
    pipeline_id: str | None = None,
    market_date: str | None = None,
    lifecycle_state: str | None = None,
    include_analysis_only: bool = False,
    limit: int = Query(default=200, ge=1, le=500),
    db: str | None = None,
) -> dict[str, object]:
    try:
        resolved_market_date = (
            None if market_date is None else resolve_date(market_date)
        )
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    return list_opportunities(
        db_target=_db_target(db),
        pipeline_id=pipeline_id,
        market_date=resolved_market_date,
        lifecycle_state=lifecycle_state,
        include_analysis_only=include_analysis_only,
        limit=limit,
    )


@router.get("/opportunities/{opportunity_id}")
def get_opportunity_route(
    opportunity_id: str,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return get_opportunity_detail(
            db_target=_db_target(db),
            opportunity_id=opportunity_id,
        )
    except ValueError as exc:
        raise not_found_error(exc) from exc


@router.post("/opportunities/{opportunity_id}/execute")
def submit_opportunity_execution_route(
    opportunity_id: str,
    payload: OpportunityExecutionRequest,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return submit_opportunity_execution(
            db_target=_db_target(db),
            opportunity_id=opportunity_id,
            quantity=payload.quantity,
            limit_price=payload.limit_price,
        )
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    except RuntimeError as exc:
        raise execution_runtime_error(exc) from exc
