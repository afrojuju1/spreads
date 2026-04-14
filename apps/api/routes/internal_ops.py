from __future__ import annotations

from fastapi import APIRouter

from apps.api.errors import service_unavailable_error
from spreads.runtime.config import default_database_url
from spreads.services.ops_visibility import build_system_status, build_trading_health

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
