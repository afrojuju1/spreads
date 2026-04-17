from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

from core.events.bus import publish_global_event_async
from core.services.live_collector_health.enrichment import (
    enrich_live_collector_job_run_payload,
)
from core.services.live_pipelines import build_live_run_scope_id

LIVE_COLLECTOR_STREAM_STALL_THRESHOLD = 2
LIVE_COLLECTOR_SLOT_LAG_THRESHOLD = 2


def _compact_single_analysis_result(
    result: dict[str, Any],
    *,
    include_report: bool = False,
) -> dict[str, Any]:
    summary = result["summary"]
    outcomes = summary["outcomes"]
    payload = {
        "session_date": result["session_date"],
        "label": result["label"],
        "cycle_count": summary["cycle_count"],
        "idea_count": outcomes["idea_count"],
        "counts_by_selection_state": outcomes["counts_by_selection_state"],
        "run_count": summary["run_overview"]["run_count"],
        "quote_event_count": summary["quote_overview"]["quote_event_count"],
        "event_count": summary["event_overview"]["event_count"],
    }
    if include_report:
        payload["report"] = result.get("report")
    return payload


def compact_analysis_result(
    result: dict[str, Any],
    *,
    include_report: bool = False,
) -> dict[str, Any]:
    if result.get("mode") == "planner":
        return {
            "mode": "planner",
            "session_date": result["session_date"],
            "expected_labels": list(result.get("expected_labels") or []),
            "realized_labels": list(result.get("realized_labels") or []),
            "runs": [
                _compact_single_analysis_result(item, include_report=include_report)
                for item in result.get("runs", [])
            ],
            "skipped_labels": [dict(item) for item in result.get("skipped_labels", [])],
            "failed_labels": [dict(item) for item in result.get("failed_labels", [])],
        }
    return _compact_single_analysis_result(result, include_report=include_report)


def _compact_single_post_market_result(result: dict[str, Any]) -> dict[str, Any]:
    diagnostics = result["diagnostics"]
    selection_state_performance = diagnostics["selection_state_performance"]
    return {
        "analysis_run_id": result["analysis_run_id"],
        "session_date": result["session_date"],
        "label": result["label"],
        "status": result["status"],
        "overall_verdict": diagnostics["overall_verdict"],
        "strength_count": len(diagnostics["strengths"]),
        "problem_count": len(diagnostics["problems"]),
        "recommendation_count": len(result["recommendations"]),
        "promotable_count": selection_state_performance["promotable"]["count"],
        "monitor_count": selection_state_performance["monitor"]["count"],
    }


def compact_post_market_result(result: dict[str, Any]) -> dict[str, Any]:
    if result.get("mode") == "planner":
        return {
            "mode": "planner",
            "session_date": result["session_date"],
            "expected_labels": list(result.get("expected_labels") or []),
            "realized_labels": list(result.get("realized_labels") or []),
            "runs": [
                _compact_single_post_market_result(item)
                for item in result.get("runs", [])
            ],
            "skipped_labels": [dict(item) for item in result.get("skipped_labels", [])],
            "failed_labels": [dict(item) for item in result.get("failed_labels", [])],
        }
    return _compact_single_post_market_result(result)


async def _publish_job_run_event(ctx: dict[str, Any], run_record: Any) -> None:
    if run_record is None:
        return
    event_bus = ctx.get("event_bus")
    if event_bus is None:
        return
    try:
        payload = enrich_live_collector_job_run_payload(run_record)
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


async def _publish_post_market_event(
    ctx: dict[str, Any],
    *,
    analysis_run_id: str,
    payload: dict[str, Any],
    timestamp: str | datetime | None = None,
) -> None:
    event_bus = ctx.get("event_bus")
    if event_bus is None:
        return
    session_id = None
    label = payload.get("label")
    session_date = payload.get("session_date")
    if (
        isinstance(label, str)
        and label
        and isinstance(session_date, str)
        and session_date
    ):
        session_id = build_live_run_scope_id(label, session_date)
    try:
        await publish_global_event_async(
            event_bus,
            topic="post_market.analysis.updated",
            event_class="analytics_event",
            entity_type="post_market_analysis",
            entity_id=analysis_run_id,
            payload={
                **payload,
                **({} if session_id is None else {"session_id": session_id}),
            },
            timestamp=timestamp,
            source="worker",
            session_date=session_date if isinstance(session_date, str) else None,
            correlation_id=session_id,
        )
    except Exception:
        pass


async def _publish_post_market_planner_events(
    ctx: dict[str, Any], result: dict[str, Any]
) -> None:
    for run in result.get("runs", []):
        await _publish_post_market_event(
            ctx,
            analysis_run_id=str(run["analysis_run_id"]),
            payload=run,
            timestamp=datetime.now(UTC),
        )
    for skipped in result.get("skipped_labels", []):
        await _publish_post_market_event(
            ctx,
            analysis_run_id=str(skipped["analysis_run_id"]),
            payload=skipped,
            timestamp=datetime.now(UTC),
        )


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
        payload = enrich_live_collector_job_run_payload(row)
        quote_capture = payload.get("quote_capture") or {}
        if int(quote_capture.get("expected_quote_symbol_count", 0)) <= 0:
            continue
        if int(quote_capture.get("stream_quote_events_saved", 0)) > 0:
            break
        consecutive += 1
    return consecutive


def _build_live_collector_log_payload(
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
        "event": "live_collector_slot_completed",
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


def _build_live_collector_degradation(
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
        and consecutive_stream_zero_slots >= LIVE_COLLECTOR_STREAM_STALL_THRESHOLD
    ):
        reasons.append("stream_capture_stalled")
    if slot_lag_slots >= LIVE_COLLECTOR_SLOT_LAG_THRESHOLD:
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


async def _emit_live_collector_observability(
    ctx: dict[str, Any], run_record: Any
) -> None:
    run_payload = enrich_live_collector_job_run_payload(run_record)
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
    log_payload = _build_live_collector_log_payload(
        run_payload,
        consecutive_stream_zero_slots=consecutive_stream_zero_slots,
        slot_lag_slots=slot_lag_slots,
    )
    print(json.dumps(log_payload, separators=(",", ":"), sort_keys=True), flush=True)
    degradation = _build_live_collector_degradation(
        run_payload,
        consecutive_stream_zero_slots=consecutive_stream_zero_slots,
        slot_lag_slots=slot_lag_slots,
    )
    if degradation is None:
        return
    print(
        json.dumps(
            {"event": "live_collector_slot_degraded", **degradation},
            separators=(",", ":"),
            sort_keys=True,
        ),
        flush=True,
    )
    event_bus = ctx.get("event_bus")
    if event_bus is None:
        return
    try:
        await publish_global_event_async(
            event_bus,
            topic="live.collector.degraded",
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
