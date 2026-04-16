from __future__ import annotations

from fastapi import APIRouter

from api.errors import not_found_error
from core.runtime.config import default_database_url
from core.services.uoa_state import get_latest_uoa_state, get_uoa_state_for_cycle

router = APIRouter()


def _db_target(db: str | None) -> str:
    return db or default_database_url()


@router.get("/internal/uoa/state")
def get_internal_uoa_state_route(
    label: str | None = None,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return get_latest_uoa_state(
            db_target=_db_target(db),
            label=label,
        )
    except ValueError as exc:
        raise not_found_error(exc) from exc


@router.get("/internal/uoa/cycles/{cycle_id}")
def get_internal_uoa_cycle_state_route(
    cycle_id: str,
    label: str | None = None,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return get_uoa_state_for_cycle(
            db_target=_db_target(db),
            cycle_id=cycle_id,
            label=label,
        )
    except ValueError as exc:
        raise not_found_error(exc) from exc
