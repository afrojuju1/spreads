from __future__ import annotations

from fastapi import APIRouter, Query

from apps.api.errors import bad_request_error, execution_runtime_error, not_found_error
from apps.api.schemas.sessions import SessionExecutionRequest, SessionPositionCloseRequest
from spreads.runtime.config import default_database_url
from spreads.services.analysis import resolve_date
from spreads.services.execution import (
    refresh_live_session_execution,
    submit_live_session_execution,
    submit_session_position_close,
)
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


@router.post("/sessions/{session_id}/executions")
def submit_session_execution_route(
    session_id: str,
    payload: SessionExecutionRequest,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return submit_live_session_execution(
            db_target=_db_target(db),
            session_id=session_id,
            candidate_id=payload.candidate_id,
            quantity=payload.quantity,
            limit_price=payload.limit_price,
        )
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    except RuntimeError as exc:
        raise execution_runtime_error(exc) from exc


@router.post("/sessions/{session_id}/positions/{session_position_id}/close")
def close_session_position_route(
    session_id: str,
    session_position_id: str,
    payload: SessionPositionCloseRequest,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return submit_session_position_close(
            db_target=_db_target(db),
            session_id=session_id,
            session_position_id=session_position_id,
            quantity=payload.quantity,
            limit_price=payload.limit_price,
        )
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    except RuntimeError as exc:
        raise execution_runtime_error(exc) from exc


@router.post("/sessions/{session_id}/executions/{execution_attempt_id}/refresh")
def refresh_session_execution_route(
    session_id: str,
    execution_attempt_id: str,
    db: str | None = None,
) -> dict[str, object]:
    try:
        return refresh_live_session_execution(
            db_target=_db_target(db),
            session_id=session_id,
            execution_attempt_id=execution_attempt_id,
        )
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    except RuntimeError as exc:
        raise execution_runtime_error(exc) from exc
