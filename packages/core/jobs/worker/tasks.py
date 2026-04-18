from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.jobs.registry import (
    ALERT_DELIVERY_JOB_TYPE,
    ALERT_RECONCILE_JOB_TYPE,
    BROKER_SYNC_JOB_TYPE,
    COLLECTOR_RECOVERY_JOB_TYPE,
    EXECUTION_SUBMIT_JOB_TYPE,
    LIVE_COLLECTOR_JOB_TYPE,
    OPTIONS_AUTOMATION_EXECUTE_JOB_TYPE,
    OPTIONS_AUTOMATION_ENTRY_JOB_TYPE,
    OPTIONS_AUTOMATION_MANAGEMENT_JOB_TYPE,
    POSITION_EXIT_MANAGER_JOB_TYPE,
    POST_CLOSE_ANALYSIS_JOB_TYPE,
    POST_MARKET_ANALYSIS_JOB_TYPE,
)
from core.services.alert_delivery import (
    ALERT_DELIVERY_STALE_SECONDS,
    reconcile_alert_delivery,
    run_alert_delivery,
)
from core.services.broker_sync import run_broker_sync
from core.services.collections.config import build_collection_args
from core.services.collections.models import LiveTickContext
from core.services.collections.runtime import run_collection_tick
from core.services.decision_engine import run_entry_automation_decision
from core.services.execution import run_execution_submit
from core.services.execution_intents import dispatch_pending_execution_intents
from core.services.exit_manager import run_position_exit_manager
from core.services.live_recovery import (
    LIVE_SLOT_STATUS_MISSED,
    LIVE_SLOT_STATUS_RUNNING,
    LIVE_SLOT_STATUS_SUCCEEDED,
    build_slot_details_from_cycle_result,
    run_collector_recovery,
)
from core.services.post_market_analysis import (
    parse_args as parse_post_market_args,
    run_post_market_analysis,
)
from core.services.post_close.cli import build_analysis_args
from core.services.post_close.service import run_post_close_analysis
from core.services.strategy_positions import run_management_automation_decision

from .managed import ManagedJobFailure, _execute_managed_job
from .observability import (
    _publish_post_market_event,
    _publish_post_market_planner_events,
    compact_analysis_result,
    compact_post_market_result,
)
from .planner import run_post_close_analysis_targets, run_post_market_analysis_targets


async def _update_live_slot_status(
    ctx: dict[str, Any],
    *,
    payload: Mapping[str, Any],
    job_run_id: str,
    status: str,
    capture_status: str | None = None,
    recovery_note: str | None = None,
    slot_details: dict[str, Any] | None = None,
    queued_at: str | datetime | None = None,
    started_at: str | datetime | None = None,
    finished_at: str | datetime | None = None,
) -> None:
    recovery_store = ctx["storage"].recovery
    if not recovery_store.schema_ready():
        return
    session_id = payload.get("session_id")
    session_date = payload.get("session_date")
    label = payload.get("label")
    slot_at = payload.get("slot_at")
    job_key = payload.get("job_key")
    if not all(
        isinstance(value, str) and value
        for value in (session_id, session_date, label, slot_at, job_key)
    ):
        return
    await asyncio.to_thread(
        recovery_store.upsert_live_session_slot,
        job_key=str(job_key),
        session_id=str(session_id),
        session_date=str(session_date),
        label=str(label),
        slot_at=str(slot_at),
        scheduled_for=str(payload.get("scheduled_for") or slot_at),
        status=status,
        job_run_id=job_run_id,
        capture_status=capture_status,
        recovery_note=recovery_note,
        slot_details={} if slot_details is None else dict(slot_details),
        queued_at=None if queued_at is None else str(queued_at),
        started_at=None if started_at is None else str(started_at),
        finished_at=None if finished_at is None else str(finished_at),
        updated_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    )


async def run_broker_sync_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = str(payload.get("db") or ctx["database_url"])

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_broker_sync(
            db_target=database_url,
            history_range=str(payload.get("history_range", "1D")),
            activity_lookback_days=int(payload.get("activity_lookback_days", 1)),
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = BROKER_SYNC_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_collector_recovery_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = str(payload.get("db") or ctx["database_url"])

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_collector_recovery(
            db_target=database_url,
            storage=ctx["storage"],
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = COLLECTOR_RECOVERY_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_execution_submit_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = ctx["database_url"]

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_execution_submit(
            db_target=database_url,
            execution_attempt_id=str(payload["execution_attempt_id"]),
            heartbeat=heartbeat,
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = EXECUTION_SUBMIT_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_position_exit_manager_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = ctx["database_url"]

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_position_exit_manager(
            db_target=database_url,
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = POSITION_EXIT_MANAGER_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_options_automation_entry_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = ctx["database_url"]

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_entry_automation_decision(
            db_target=database_url,
            bot_id=str(payload["bot_id"]),
            automation_id=str(payload["automation_id"]),
            market_date=payload.get("market_date"),
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = OPTIONS_AUTOMATION_ENTRY_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_options_automation_management_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = ctx["database_url"]

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_management_automation_decision(
            db_target=database_url,
            bot_id=str(payload["bot_id"]),
            automation_id=str(payload["automation_id"]),
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = OPTIONS_AUTOMATION_MANAGEMENT_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_options_automation_execute_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = ctx["database_url"]

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return dispatch_pending_execution_intents(
            db_target=database_url,
            limit=int(payload.get("limit", 25) or 25),
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = OPTIONS_AUTOMATION_EXECUTE_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_alert_delivery_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_alert_delivery(
            alert_store=ctx["storage"].alerts,
            alert_id=int(payload["alert_id"]),
            delivery_job_run_id=job_run_id,
            worker_name=ctx["worker_name"],
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = ALERT_DELIVERY_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_alert_reconcile_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return reconcile_alert_delivery(
            alert_store=ctx["storage"].alerts,
            job_store=ctx["job_store"],
            limit=int(payload.get("limit", 200)),
            stale_after_seconds=int(
                payload.get("stale_after_seconds", ALERT_DELIVERY_STALE_SECONDS)
            ),
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = ALERT_RECONCILE_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_live_collector_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    async def on_running(run_record: Mapping[str, Any]) -> None:
        await _update_live_slot_status(
            ctx,
            payload=payload,
            job_run_id=str(run_record["job_run_id"]),
            status=LIVE_SLOT_STATUS_RUNNING,
            queued_at=run_record.get("scheduled_for"),
            started_at=run_record.get("started_at"),
        )

    async def on_completed(
        run_record: Mapping[str, Any], result: Mapping[str, Any]
    ) -> None:
        slot_status = LIVE_SLOT_STATUS_SUCCEEDED
        recovery_note = None
        capture_status = None
        slot_details = None
        if str(result.get("status") or "") == "completed":
            capture_status = str(
                (result.get("quote_capture") or {}).get("capture_status") or ""
            )
            slot_details = build_slot_details_from_cycle_result(result)
        elif str(result.get("slot_status") or "") == LIVE_SLOT_STATUS_MISSED:
            slot_status = LIVE_SLOT_STATUS_MISSED
            recovery_note = str(
                result.get("message")
                or result.get("reason")
                or "Live slot was skipped as stale."
            )
        else:
            slot_status = LIVE_SLOT_STATUS_MISSED
            recovery_note = str(
                result.get("reason") or "Live slot did not complete successfully."
            )
        await _update_live_slot_status(
            ctx,
            payload=payload,
            job_run_id=str(run_record["job_run_id"]),
            status=slot_status,
            capture_status=capture_status,
            recovery_note=recovery_note,
            slot_details=slot_details,
            started_at=run_record.get("started_at"),
            finished_at=run_record.get("finished_at"),
        )

    async def on_failed(
        run_record: Mapping[str, Any], partial_result: Mapping[str, Any] | None
    ) -> None:
        await _update_live_slot_status(
            ctx,
            payload=payload,
            job_run_id=str(run_record["job_run_id"]),
            status=LIVE_SLOT_STATUS_MISSED,
            recovery_note=(
                None
                if partial_result is None
                else str(
                    partial_result.get("reason")
                    or partial_result.get("message")
                    or "Live slot failed."
                )
            )
            or "Live slot failed before it could complete.",
            slot_details=None
            if partial_result is None
            else build_slot_details_from_cycle_result(partial_result),
            started_at=run_record.get("started_at"),
            finished_at=run_record.get("finished_at"),
        )

    def runner(heartbeat: Any) -> dict[str, Any]:
        args = build_collection_args(payload)
        session_id = payload.get("session_id")
        slot_at = payload.get("slot_at")
        if not isinstance(session_id, str) or not session_id:
            raise ValueError("live_collector payload is missing session_id")
        if not isinstance(slot_at, str) or not slot_at:
            raise ValueError("live_collector payload is missing slot_at")
        tick_context = LiveTickContext(
            job_run_id=job_run_id,
            session_id=session_id,
            slot_at=slot_at,
        )
        return run_collection_tick(
            args,
            tick_context=tick_context,
            heartbeat=heartbeat,
            emit_output=False,
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = LIVE_COLLECTOR_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
        on_running=on_running,
        on_completed=on_completed,
        on_failed=on_failed,
    )


async def run_post_close_analysis_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = ctx["database_url"]
    job_store = ctx["job_store"]

    def runner(heartbeat: Any) -> dict[str, Any]:
        if payload.get("label"):
            heartbeat()
            args = build_analysis_args(
                {
                    "db": database_url,
                    "date": payload.get("date", "today"),
                    "label": payload["label"],
                    "backtest_profit_target": payload.get(
                        "backtest_profit_target", 0.5
                    ),
                    "backtest_stop_multiple": payload.get(
                        "backtest_stop_multiple", 2.0
                    ),
                }
            )
            return run_post_close_analysis(args, emit_output=False)
        return run_post_close_analysis_targets(
            db_target=database_url,
            job_store=job_store,
            payload=payload,
            heartbeat=heartbeat,
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = POST_CLOSE_ANALYSIS_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: compact_analysis_result(
            result,
            include_report=bool(payload.get("include_report")),
        ),
    )


async def run_post_market_analysis_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = str(payload.get("db") or ctx["database_url"])
    job_store = ctx["job_store"]

    def runner(heartbeat: Any) -> dict[str, Any]:
        if payload.get("label"):
            heartbeat()
            args = parse_post_market_args(
                [
                    "--db",
                    database_url,
                    "--date",
                    str(payload.get("date", "today")),
                    "--label",
                    str(payload["label"]),
                    "--backtest-profit-target",
                    str(payload.get("backtest_profit_target", 0.5)),
                    "--backtest-stop-multiple",
                    str(payload.get("backtest_stop_multiple", 2.0)),
                ]
            )
            return run_post_market_analysis(
                args,
                emit_output=False,
                analysis_run_id=job_run_id,
                job_run_id=job_run_id,
            )
        return run_post_market_analysis_targets(
            db_target=database_url,
            job_store=job_store,
            parent_job_run_id=job_run_id,
            payload=payload,
            heartbeat=heartbeat,
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = POST_MARKET_ANALYSIS_JOB_TYPE
    try:
        result = await _execute_managed_job(
            ctx,
            job_key=job_key,
            job_run_id=job_run_id,
            arq_job_id=arq_job_id,
            payload=enriched_payload,
            runner=runner,
            compact_result=compact_post_market_result,
        )
        if result.get("mode") == "planner":
            await _publish_post_market_planner_events(ctx, result)
            return result
        await _publish_post_market_event(
            ctx,
            analysis_run_id=str(result["analysis_run_id"]),
            payload=result,
            timestamp=datetime.now(UTC),
        )
        return result
    except ManagedJobFailure as exc:
        partial_result = (
            compact_post_market_result(exc.result) if exc.result is not None else None
        )
        if partial_result is not None and partial_result.get("mode") == "planner":
            await _publish_post_market_planner_events(ctx, partial_result)
        await _publish_post_market_event(
            ctx,
            analysis_run_id=job_run_id,
            payload={
                "analysis_run_id": job_run_id,
                "session_date": payload.get("date", "today"),
                "status": "failed",
                "failed_labels": []
                if partial_result is None
                else partial_result.get("failed_labels", []),
            },
            timestamp=datetime.now(UTC),
        )
        raise
    except Exception:
        await _publish_post_market_event(
            ctx,
            analysis_run_id=job_run_id,
            payload={
                "analysis_run_id": job_run_id,
                "label": payload.get("label"),
                "session_date": payload.get("date", "today"),
                "status": "failed",
            },
            timestamp=datetime.now(UTC),
        )
        raise
