from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from core.integrations.calendar_events.refresh import run_calendar_event_refresh
from core.jobs.contracts import RoutineExecutionContext, RoutineHandler, RoutineOutcome
from core.jobs.registry import CALENDAR_EVENT_REFRESH_JOB_TYPE, TICKER_SOURCE_JOB_TYPE
from core.runtime.config import default_redis_url
from core.services.sources.dispatch import persist_ticker_source_result, run_ticker_source
from core.storage.serializers import render_value
from core.value_coercion import as_mapping, as_text


def _ticker_source_projection(result: Mapping[str, Any]) -> dict[str, Any]:
    entries = [dict(item) for item in list(result.get("entries") or []) if isinstance(item, Mapping)]
    observations = list(result.get("observations") or [])
    degradation = as_mapping(result.get("degradation"))
    return dict(
        render_value(
            {
                "status": result.get("status"),
                "reason": degradation.get("reason"),
                "source_id": result.get("source_id"),
                "recipe": result.get("recipe"),
                "generated_at": result.get("generated_at"),
                "ticker_source_run_id": result.get("ticker_source_run_id"),
                "symbols": list(result.get("symbols") or []),
                "entries": entries[:25],
                "entry_count": len(entries),
                "observation_count": len(observations),
                "summary": result.get("summary"),
                "degradation": degradation,
                "persistence": result.get("persistence"),
            }
        )
    )


def _calendar_refresh_projection(result: Mapping[str, Any]) -> dict[str, Any]:
    return dict(
        render_value(
            {
                "status": result.get("status"),
                "refresh_id": result.get("refresh_id"),
                "window_start": result.get("window_start"),
                "window_end": result.get("window_end"),
                "provider_statuses": result.get("provider_statuses"),
                "records_upserted": result.get("records_upserted"),
                "records_by_source": result.get("records_by_source"),
                "initial_consensus_count": result.get("initial_consensus_count"),
                "final_consensus_count": result.get("final_consensus_count"),
                "final_conflict_count": result.get("final_conflict_count"),
                "finviz_enrichment_symbols": list(result.get("finviz_enrichment_symbols") or []),
                "providers": result.get("providers"),
            }
        )
    )


def _ticker_source(context: RoutineExecutionContext) -> RoutineOutcome:
    context.heartbeat()
    result = run_ticker_source(
        source_id=str(context.payload["source_id"]),
        recipe=str(context.payload["recipe"]),
        recipe_args=dict(context.payload.get("recipe_args") or {}),
    )
    persistence = persist_ticker_source_result(
        context.storage.engine_facts,
        source_id=str(context.payload["source_id"]),
        recipe=str(context.payload["recipe"]),
        job_run_id=context.job_run_id,
        result=result,
        config_hash=as_text(context.payload.get("declared_config_hash")),
    )
    enriched = {
        **result,
        "ticker_source_run_id": persistence.get("ticker_source_run_id"),
        "persistence": persistence,
    }
    projection = _ticker_source_projection(enriched)
    if result.get("status") in {"degraded", "skipped"}:
        return RoutineOutcome.skipped(projection)
    return RoutineOutcome.succeeded(projection)


def _calendar_event_refresh(context: RoutineExecutionContext) -> RoutineOutcome:
    context.heartbeat()
    result = run_calendar_event_refresh(
        refresh_id=str(context.payload["refresh_id"]),
        database_url=context.database_url,
        redis_url=str(context.payload.get("redis_url") or default_redis_url()),
        payload=dict(context.payload),
        heartbeat=context.heartbeat,
    )
    projection = _calendar_refresh_projection(result)
    if result.get("status") == "skipped":
        return RoutineOutcome.skipped(projection)
    return RoutineOutcome.succeeded(projection)


HANDLERS: Mapping[str, RoutineHandler] = MappingProxyType(
    {
        TICKER_SOURCE_JOB_TYPE: _ticker_source,
        CALENDAR_EVENT_REFRESH_JOB_TYPE: _calendar_event_refresh,
    }
)

__all__ = ["HANDLERS"]
