from __future__ import annotations

from fastapi import APIRouter

from api.errors import service_unavailable_error
from core.runtime.config import default_database_url
from core.services.ops import build_storage_ops_state, build_trading_ops_state

router = APIRouter()


def _db_target(db: str | None) -> str:
    return db or default_database_url()


@router.get("/internal/trading-ops/state")
def get_internal_trading_ops_state_route(
    market_date: str | None = None,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return build_trading_ops_state(
            db_target=_db_target(db),
            market_date=market_date,
        )
    except Exception as exc:
        raise service_unavailable_error(exc) from exc


@router.get("/internal/storage-ops/state")
def get_internal_storage_ops_state_route(
    db: str | None = None,
) -> dict[str, object]:
    try:
        return build_storage_ops_state(db_target=_db_target(db))
    except Exception as exc:
        raise service_unavailable_error(exc) from exc
