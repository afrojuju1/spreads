from __future__ import annotations

from typing import Any

from core.services.pipelines import (
    get_pipeline_detail,
    list_pipeline_cycles,
    list_pipelines,
)


def list_discovery_sessions(
    *,
    db_target: str,
    market_date: str | None = None,
    limit: int = 100,
    storage: Any | None = None,
) -> dict[str, Any]:
    return list_pipelines(
        db_target=db_target,
        market_date=market_date,
        limit=limit,
        storage=storage,
    )


def get_discovery_session_detail(
    *,
    db_target: str,
    pipeline_id: str,
    market_date: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    return get_pipeline_detail(
        db_target=db_target,
        pipeline_id=pipeline_id,
        market_date=market_date,
        storage=storage,
    )


def list_discovery_session_cycles(
    *,
    db_target: str,
    pipeline_id: str,
    market_date: str | None = None,
    limit: int = 100,
    storage: Any | None = None,
) -> dict[str, Any]:
    return list_pipeline_cycles(
        db_target=db_target,
        pipeline_id=pipeline_id,
        market_date=market_date,
        limit=limit,
        storage=storage,
    )


__all__ = [
    "get_discovery_session_detail",
    "list_discovery_session_cycles",
    "list_discovery_sessions",
]
