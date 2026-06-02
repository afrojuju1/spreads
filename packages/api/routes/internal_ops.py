from __future__ import annotations

from fastapi import APIRouter

from api.errors import service_unavailable_error
from core.runtime.config import default_database_url
from core.services.ops import (
    build_finviz_direct_ledger,
    build_live_doctor,
    build_system_status,
    build_trading_health,
)

router = APIRouter()


def _db_target(db: str | None) -> str:
    return db or default_database_url()


@router.get("/internal/ops/status")
def get_internal_ops_status_route(
    db: str | None = None,
) -> dict[str, object]:
    try:
        return build_system_status(db_target=_db_target(db))
    except Exception as exc:
        raise service_unavailable_error(exc) from exc


@router.get("/internal/ops/trading")
def get_internal_ops_trading_route(
    db: str | None = None,
) -> dict[str, object]:
    try:
        return build_trading_health(db_target=_db_target(db))
    except Exception as exc:
        raise service_unavailable_error(exc) from exc


@router.get("/internal/ops/live-doctor")
def get_internal_ops_live_doctor_route(
    feed_id: str = "finviz_momentum",
    market_date: str | None = None,
    limit: int = 5,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return build_live_doctor(
            db_target=_db_target(db),
            feed_id=feed_id,
            market_date=market_date,
            limit=limit,
        )
    except Exception as exc:
        raise service_unavailable_error(exc) from exc


@router.get("/internal/ops/finviz-ledger")
def get_internal_ops_finviz_ledger_route(
    feed_id: str = "finviz_momentum",
    market_date: str | None = None,
    limit: int = 10,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return build_finviz_direct_ledger(
            db_target=_db_target(db),
            feed_id=feed_id,
            market_date=market_date,
            limit=limit,
        )
    except Exception as exc:
        raise service_unavailable_error(exc) from exc
