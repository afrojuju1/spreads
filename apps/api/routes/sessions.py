from __future__ import annotations

from fastapi import APIRouter, Query

from apps.api.errors import bad_request_error, not_found_error
from spreads.runtime.config import default_database_url
from spreads.services.analysis import resolve_date
from spreads.services.sessions import get_session_detail, list_existing_sessions

router = APIRouter()


def _db_target(db: str | None) -> str:
    return db or default_database_url()


@router.get("/sessions")
def list_sessions_route(
    session_date: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    db: str | None = None,
) -> dict[str, object]:
    try:
        resolved_session_date = None if session_date is None else resolve_date(session_date)
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    return list_existing_sessions(
        db_target=_db_target(db),
        limit=limit,
        session_date=resolved_session_date,
    )


@router.get("/sessions/{session_id}")
def get_session_route(
    session_id: str,
    replay_profit_target: float = Query(default=0.5, gt=0),
    replay_stop_multiple: float = Query(default=2.0, gt=0),
    db: str | None = None,
) -> dict[str, object]:
    try:
        return get_session_detail(
            db_target=_db_target(db),
            session_id=session_id,
            profit_target=replay_profit_target,
            stop_multiple=replay_stop_multiple,
        )
    except ValueError as exc:
        raise not_found_error(exc) from exc
