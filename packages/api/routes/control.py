from __future__ import annotations

from fastapi import APIRouter

from api.errors import bad_request_error, control_runtime_error
from api.schemas.control import ControlModeRequest
from core.runtime.config import default_database_url
from core.services.control_plane import get_control_state_snapshot, set_control_mode

router = APIRouter()


def _db_target(db: str | None) -> str:
    return db or default_database_url()


@router.get("/control/state")
def get_control_state_route(db: str | None = None) -> dict[str, object]:
    return get_control_state_snapshot(db_target=_db_target(db))


@router.post("/control/mode")
def update_control_mode_route(
    payload: ControlModeRequest,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return set_control_mode(
            db_target=_db_target(db),
            mode=payload.mode,
            reason_code=payload.reason_code,
            note=payload.note,
        )
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    except RuntimeError as exc:
        raise control_runtime_error(exc) from exc
