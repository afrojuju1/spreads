from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from core.events.bus import publish_global_event_async
from core.observability.logging import log_event
from core.services.discovery_run_health.enrichment import (
    enrich_discovery_run_job_run_payload,
)

DISCOVERY_RUN_STREAM_STALL_THRESHOLD = 2
DISCOVERY_RUN_SLOT_LAG_THRESHOLD = 2

logger = logging.getLogger(__name__)


async def _publish_job_run_event(ctx: dict[str, Any], run_record: Any) -> None:
    if run_record is None:
        return
    event_bus = ctx.get("event_bus")
    if event_bus is None:
        return
    try:
        payload = enrich_discovery_run_job_run_payload(run_record)
        await publish_global_event_async(
            event_bus,
            topic="job.run.updated",
            event_class="control_event",
            entity_type="job_run",
            entity_id=run_record["job_run_id"],
            payload=payload,
            timestamp=run_record.get("finished_at")
            or run_record.get("heartbeat_at")
            or run_record["scheduled_for"],
            source="worker",
            session_date=payload.get("session_date")
            if isinstance(payload.get("session_date"), str)
            else None,
            correlation_id=str(run_record["job_key"]),
        )
    except Exception:
        pass


def _parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00") if value.endswith("Z") else value
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _run_duration_seconds(run_payload: dict[str, Any]) -> float | None:
    started_at = _parse_utc(run_payload.get("started_at"))
    finished_at = _parse_utc(run_payload.get("finished_at"))
    if started_at is None or finished_at is None:
        return None
    duration = (finished_at - started_at).total_seconds()
    return round(duration, 3) if duration >= 0 else None


def _slot_lag_slots(run_payload: dict[str, Any]) -> int:
    slot_at = _parse_utc(run_payload.get("slot_at"))
    finished_at = _parse_utc(run_payload.get("finished_at"))
    interval_seconds = int(
        (run_payload.get("payload") or {}).get("interval_seconds") or 0
    )
    if slot_at is None or finished_at is None or interval_seconds <= 0:
        return 0
    elapsed_seconds = max((finished_at - slot_at).total_seconds(), 0.0)
    return max(int(elapsed_seconds // interval_seconds) - 1, 0)


def _count_consecutive_stream_zero_slots(
    job_store: Any,
    *,
    job_key: str,
    session_id: str,
) -> int:
    rows = job_store.list_job_runs(
        job_key=job_key,
        status="succeeded",
        session_id=session_id,
        limit=8,
    )
    consecutive = 0
    for row in rows:
        payload = enrich_discovery_run_job_run_payload(row)
        quote_capture = payload.get("quote_capture") or {}
        if int(quote_capture.get("expected_quote_symbol_count", 0)) <= 0:
            continue
        if int(quote_capture.get("stream_quote_events_saved", 0)) > 0:
            break
        consecutive += 1
    return consecutive


def _build_discovery_run_log_payload(
    run_payload: dict[str, Any],
    *,
    consecutive_stream_zero_slots: int,
    slot_lag_slots: int,
) -> dict[str, Any]:
    result = run_payload.get("result") or {}
    quote_capture = run_payload.get("quote_capture") or {}
    trade_capture = run_payload.get("trade_capture") or {}
    uoa_summary = run_payload.get("uoa_summary") or {}
    uoa_quote_summary = run_payload.get("uoa_quote_summary") or {}
    uoa_decisions = run_payload.get("uoa_decisions") or {}
    cycle_ids = result.get("cycle_ids") or []
    return {
        "event": "discovery_run_slot_completed",
        "job_run_id": run_payload["job_run_id"],
        "job_key": run_payload["job_key"],
        "label": result.get("label") or (run_payload.get("payload") or {}).get("label"),
        "session_id": run_payload.get("session_id"),
        "slot_at": run_payload.get("slot_at"),
        "cycle_id": None if not cycle_ids else cycle_ids[0],
        "worker_name": run_payload.get("worker_name"),
        "duration_seconds": _run_duration_seconds(run_payload),
        "promotable_opportunity_count": int(
            result.get("promotable_opportunity_count") or 0
        ),
        "monitor_opportunity_count": int(result.get("monitor_opportunity_count") or 0),
        "quote_capture": quote_capture,
        "trade_capture": trade_capture,
        "uoa_overview": dict(uoa_summary.get("overview") or {}),
        "uoa_quote_overview": dict(uoa_quote_summary.get("overview") or {}),
        "uoa_top_roots": [
            dict(item) for item in (uoa_summary.get("top_roots") or [])[:3]
        ],
        "uoa_top_contracts": [
            dict(item) for item in (uoa_summary.get("top_contracts") or [])[:3]
        ],
        "uoa_decision_overview": dict(uoa_decisions.get("overview") or {}),
        "uoa_promotable_roots": [
            dict(item) for item in (uoa_decisions.get("top_promotable_roots") or [])[:3]
        ],
        "uoa_high_roots": [
            dict(item) for item in (uoa_decisions.get("top_high_roots") or [])[:3]
        ],
        "consecutive_stream_zero_slots": consecutive_stream_zero_slots,
        "slot_lag_slots": slot_lag_slots,
    }


def _build_discovery_run_degradation(
    run_payload: dict[str, Any],
    *,
    consecutive_stream_zero_slots: int,
    slot_lag_slots: int,
) -> dict[str, Any] | None:
    quote_capture = run_payload.get("quote_capture") or {}
    expected_quote_symbol_count = int(
        quote_capture.get("expected_quote_symbol_count", 0) or 0
    )
    reasons: list[str] = []
    if (
        expected_quote_symbol_count > 0
        and int(quote_capture.get("total_quote_events_saved", 0)) == 0
    ):
        reasons.append("quote_capture_empty")
    if (
        expected_quote_symbol_count > 0
        and int(quote_capture.get("stream_quote_events_saved", 0)) == 0
        and consecutive_stream_zero_slots >= DISCOVERY_RUN_STREAM_STALL_THRESHOLD
    ):
        reasons.append("stream_capture_stalled")
    if slot_lag_slots >= DISCOVERY_RUN_SLOT_LAG_THRESHOLD:
        reasons.append("slot_lagging")
    if not reasons:
        return None
    result = run_payload.get("result") or {}
    cycle_ids = result.get("cycle_ids") or []
    return {
        "reasons": reasons,
        "job_run_id": run_payload["job_run_id"],
        "job_key": run_payload["job_key"],
        "label": result.get("label") or (run_payload.get("payload") or {}).get("label"),
        "session_id": run_payload.get("session_id"),
        "slot_at": run_payload.get("slot_at"),
        "cycle_id": None if not cycle_ids else cycle_ids[0],
        "capture_status": quote_capture.get("capture_status"),
        "quote_capture": quote_capture,
        "consecutive_stream_zero_slots": consecutive_stream_zero_slots,
        "slot_lag_slots": slot_lag_slots,
    }


async def _emit_discovery_run_observability(
    ctx: dict[str, Any], run_record: Any
) -> None:
    run_payload = enrich_discovery_run_job_run_payload(run_record)
    session_id = run_payload.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        return
    job_store = ctx["job_store"]
    consecutive_stream_zero_slots = await asyncio.to_thread(
        _count_consecutive_stream_zero_slots,
        job_store,
        job_key=str(run_payload["job_key"]),
        session_id=session_id,
    )
    slot_lag_slots = _slot_lag_slots(run_payload)
    log_payload = _build_discovery_run_log_payload(
        run_payload,
        consecutive_stream_zero_slots=consecutive_stream_zero_slots,
        slot_lag_slots=slot_lag_slots,
    )
    log_event(
        logger,
        logging.INFO,
        "discovery_run_slot_completed",
        **{
            key: value
            for key, value in log_payload.items()
            if key != "event"
        },
    )
    degradation = _build_discovery_run_degradation(
        run_payload,
        consecutive_stream_zero_slots=consecutive_stream_zero_slots,
        slot_lag_slots=slot_lag_slots,
    )
    if degradation is None:
        return
    log_event(
        logger,
        logging.WARNING,
        "discovery_run_slot_degraded",
        **degradation,
    )
    event_bus = ctx.get("event_bus")
    if event_bus is None:
        return
    try:
        await publish_global_event_async(
            event_bus,
            topic="live.discovery_run.degraded",
            event_class="control_event",
            entity_type="job_run",
            entity_id=run_payload["job_run_id"],
            payload=degradation,
            event_type="alert",
            timestamp=run_payload.get("finished_at") or run_payload.get("slot_at"),
            source="worker",
            session_date=run_payload.get("session_date")
            if isinstance(run_payload.get("session_date"), str)
            else None,
            correlation_id=session_id,
        )
    except Exception:
        pass
