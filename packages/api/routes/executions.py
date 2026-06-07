from __future__ import annotations

from fastapi import APIRouter

from api.errors import bad_request_error, execution_runtime_error
from api.schemas.executions import EquityOrderRequest
from core.runtime.config import default_database_url
from core.services.execution.direct_orders import submit_equity_order
from core.services.execution.runtimes import resolve_execution_runtime_capabilities
from core.services.execution.sync import cancel_execution_attempt, refresh_execution_attempt

router = APIRouter()


def _db_target(db: str | None) -> str:
    return db or default_database_url()


@router.get("/executions/runtimes")
def list_execution_runtimes_route() -> dict[str, object]:
    return resolve_execution_runtime_capabilities()


@router.post("/executions/equity-orders")
def submit_equity_order_route(
    payload: EquityOrderRequest,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return submit_equity_order(
            db_target=_db_target(db),
            symbol=payload.symbol,
            side=payload.side,
            quantity=payload.quantity,
            limit_price=payload.limit_price,
            time_in_force=payload.time_in_force,
            label=payload.label,
            market_date=payload.market_date,
            execution_runtime=payload.execution_runtime,
        )
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    except RuntimeError as exc:
        raise execution_runtime_error(exc) from exc


@router.post("/executions/{execution_attempt_id}/refresh")
def refresh_execution_route(
    execution_attempt_id: str,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return refresh_execution_attempt(
            db_target=_db_target(db),
            execution_attempt_id=execution_attempt_id,
        )
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    except RuntimeError as exc:
        raise execution_runtime_error(exc) from exc


@router.post("/executions/{execution_attempt_id}/cancel")
def cancel_execution_route(
    execution_attempt_id: str,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return cancel_execution_attempt(
            db_target=_db_target(db),
            execution_attempt_id=execution_attempt_id,
        )
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    except RuntimeError as exc:
        raise execution_runtime_error(exc) from exc
