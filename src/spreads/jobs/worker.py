from __future__ import annotations

import asyncio
import json
import os
import socket
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as redis_async

from spreads.events import publish_global_event_async
from spreads.jobs.live_collector import build_collection_args, run_collection
from spreads.jobs.orchestration import (
    build_redis_settings,
    default_redis_url,
    singleton_lease_key,
    worker_runtime_lease_key,
)
from spreads.services.analysis import build_analysis_args, run_post_close_analysis
from spreads.services.generator import build_generator_args, generate_symbol_ideas, generator_job_channel
from spreads.services.post_market_analysis import parse_args as parse_post_market_args
from spreads.services.post_market_analysis import run_post_market_analysis
from spreads.storage import build_generator_job_repository, build_job_repository, default_database_url

WORKER_HEARTBEAT_SECONDS = 30
WORKER_LEASE_TTL_SECONDS = 90
JOB_LEASE_TTL_SECONDS = 600


def worker_name() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def compact_analysis_result(result: dict[str, Any]) -> dict[str, Any]:
    summary = result["summary"]
    outcomes = summary["outcomes"]
    return {
        "session_date": result["session_date"],
        "label": result["label"],
        "cycle_count": summary["cycle_count"],
        "idea_count": outcomes["idea_count"],
        "counts_by_bucket": outcomes["counts_by_bucket"],
        "run_count": summary["run_overview"]["run_count"],
        "quote_event_count": summary["quote_overview"]["quote_event_count"],
        "event_count": summary["event_overview"]["event_count"],
    }


def compact_post_market_result(result: dict[str, Any]) -> dict[str, Any]:
    diagnostics = result["diagnostics"]
    bucket_performance = diagnostics["bucket_performance"]
    return {
        "analysis_run_id": result["analysis_run_id"],
        "session_date": result["session_date"],
        "label": result["label"],
        "status": result["status"],
        "overall_verdict": diagnostics["overall_verdict"],
        "strength_count": len(diagnostics["strengths"]),
        "problem_count": len(diagnostics["problems"]),
        "recommendation_count": len(result["recommendations"]),
        "board_count": bucket_performance["board"]["count"],
        "watchlist_count": bucket_performance["watchlist"]["count"],
    }


async def _heartbeat_runtime(job_store: Any, runtime_owner: str) -> None:
    while True:
        await asyncio.to_thread(
            job_store.acquire_lease,
            lease_key=worker_runtime_lease_key(runtime_owner),
            owner=runtime_owner,
            expires_in_seconds=WORKER_LEASE_TTL_SECONDS,
            state={"kind": "worker"},
        )
        await asyncio.sleep(WORKER_HEARTBEAT_SECONDS)


async def startup(ctx: dict[str, Any]) -> None:
    ctx["database_url"] = default_database_url()
    ctx["redis_url"] = default_redis_url()
    ctx["worker_name"] = worker_name()
    ctx["job_store"] = build_job_repository(ctx["database_url"])
    ctx["generator_job_store"] = build_generator_job_repository(ctx["database_url"])
    ctx["event_bus"] = redis_async.from_url(ctx["redis_url"], decode_responses=True)
    ctx["runtime_heartbeat_task"] = asyncio.create_task(
        _heartbeat_runtime(ctx["job_store"], ctx["worker_name"])
    )


async def shutdown(ctx: dict[str, Any]) -> None:
    task = ctx.get("runtime_heartbeat_task")
    if task is not None:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    job_store = ctx.get("job_store")
    if job_store is not None:
        await asyncio.to_thread(
            job_store.release_lease,
            worker_runtime_lease_key(ctx["worker_name"]),
            owner=ctx["worker_name"],
        )
        await asyncio.to_thread(job_store.close)
    generator_job_store = ctx.get("generator_job_store")
    if generator_job_store is not None:
        await asyncio.to_thread(generator_job_store.close)
    event_bus = ctx.get("event_bus")
    if event_bus is not None:
        await event_bus.aclose()


async def _publish_generator_job_event(
    ctx: dict[str, Any],
    event_type: str,
    job_record: Any,
) -> None:
    event_bus = ctx.get("event_bus")
    if event_bus is None:
        return
    payload = {
        "type": event_type,
        "job": job_record.to_dict(),
    }
    await event_bus.publish(generator_job_channel(job_record["generator_job_id"]), json.dumps(payload))
    try:
        await publish_global_event_async(
            event_bus,
            topic="generator.job.updated",
            entity_type="generator_job",
            entity_id=job_record["generator_job_id"],
            payload=job_record.to_dict(),
            timestamp=job_record.get("finished_at") or job_record.get("started_at") or job_record["created_at"],
        )
    except Exception:
        pass


async def _publish_job_run_event(ctx: dict[str, Any], run_record: Any) -> None:
    event_bus = ctx.get("event_bus")
    if event_bus is None:
        return
    try:
        await publish_global_event_async(
            event_bus,
            topic="job.run.updated",
            entity_type="job_run",
            entity_id=run_record["job_run_id"],
            payload=run_record.to_dict(),
            timestamp=run_record.get("finished_at") or run_record.get("heartbeat_at") or run_record["scheduled_for"],
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
    try:
        await publish_global_event_async(
            event_bus,
            topic="post_market.analysis.updated",
            entity_type="post_market_analysis",
            entity_id=analysis_run_id,
            payload=payload,
            timestamp=timestamp,
        )
    except Exception:
        pass


async def _mark_running(job_store: Any, job_run_id: str, runtime_owner: str) -> None:
    now = datetime.now(UTC)
    run_record = await asyncio.to_thread(
        job_store.update_job_run_status,
        job_run_id=job_run_id,
        status="running",
        worker_name=runtime_owner,
        started_at=now,
        heartbeat_at=now,
    )
    return run_record


def _sync_job_heartbeat(
    job_store: Any,
    *,
    job_run_id: str,
    runtime_owner: str,
    lease_key: str | None,
) -> None:
    now = datetime.now(UTC)
    job_store.heartbeat_job_run(
        job_run_id=job_run_id,
        heartbeat_at=now,
        worker_name=runtime_owner,
    )
    if lease_key is not None:
        job_store.renew_lease(
            lease_key=lease_key,
            owner=job_run_id,
            expires_in_seconds=JOB_LEASE_TTL_SECONDS,
            state={"kind": "singleton_job"},
        )


async def _execute_managed_job(
    ctx: dict[str, Any],
    *,
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    runner: Any,
    compact_result: Any,
) -> dict[str, Any]:
    job_store = ctx["job_store"]
    runtime_owner = ctx["worker_name"]
    lease_key = None
    scope = payload.get("singleton_scope")
    job_type = payload.get("job_type")
    if scope and job_type:
        lease_key = singleton_lease_key(str(job_type), str(scope))
        acquired = await asyncio.to_thread(
            job_store.acquire_lease,
            lease_key=lease_key,
            owner=job_run_id,
            job_run_id=job_run_id,
            expires_in_seconds=JOB_LEASE_TTL_SECONDS,
            state={"kind": "singleton_job", "job_key": job_key},
        )
        if not acquired:
            result = {"status": "skipped", "reason": "singleton_lease_unavailable"}
            skipped_record = await asyncio.to_thread(
                job_store.update_job_run_status,
                job_run_id=job_run_id,
                status="skipped",
                worker_name=runtime_owner,
                finished_at=datetime.now(UTC),
                heartbeat_at=datetime.now(UTC),
                result=result,
            )
            await _publish_job_run_event(ctx, skipped_record)
            return result

    running_record = await _mark_running(job_store, job_run_id, runtime_owner)
    await _publish_job_run_event(ctx, running_record)
    try:
        result = await asyncio.to_thread(
            runner,
            lambda: _sync_job_heartbeat(
                job_store,
                job_run_id=job_run_id,
                runtime_owner=runtime_owner,
                lease_key=lease_key,
            ),
        )
        compact = compact_result(result)
        completed_record = await asyncio.to_thread(
            job_store.update_job_run_status,
            job_run_id=job_run_id,
            status="succeeded",
            worker_name=runtime_owner,
            finished_at=datetime.now(UTC),
            heartbeat_at=datetime.now(UTC),
            result=compact,
        )
        await _publish_job_run_event(ctx, completed_record)
        return compact
    except Exception as exc:
        failed_record = await asyncio.to_thread(
            job_store.update_job_run_status,
            job_run_id=job_run_id,
            status="failed",
            worker_name=runtime_owner,
            finished_at=datetime.now(UTC),
            heartbeat_at=datetime.now(UTC),
            error_text=str(exc),
        )
        await _publish_job_run_event(ctx, failed_record)
        raise
    finally:
        if lease_key is not None:
            await asyncio.to_thread(job_store.release_lease, lease_key, owner=job_run_id)


async def run_live_collector_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    def runner(heartbeat: Any) -> dict[str, Any]:
        args = build_collection_args(payload)
        return run_collection(args, heartbeat=heartbeat, emit_output=False)

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = "live_collector"
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=lambda result: result,
    )


async def run_post_close_analysis_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        args = build_analysis_args(
            {
                "db": default_database_url(),
                "date": payload.get("date", "today"),
                "label": payload["label"],
                "replay_profit_target": payload.get("replay_profit_target", 0.5),
                "replay_stop_multiple": payload.get("replay_stop_multiple", 2.0),
            }
        )
        return run_post_close_analysis(args, emit_output=False)

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = "post_close_analysis"
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=compact_analysis_result,
    )


async def run_post_market_analysis_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        args = parse_post_market_args(
            [
                "--db",
                default_database_url(),
                "--date",
                str(payload.get("date", "today")),
                "--label",
                str(payload["label"]),
                "--replay-profit-target",
                str(payload.get("replay_profit_target", 0.5)),
                "--replay-stop-multiple",
                str(payload.get("replay_stop_multiple", 2.0)),
            ]
        )
        return run_post_market_analysis(
            args,
            emit_output=False,
            analysis_run_id=job_run_id,
            job_run_id=job_run_id,
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = "post_market_analysis"
    try:
        result = await _execute_managed_job(
            ctx,
            job_key=job_key,
            job_run_id=job_run_id,
            payload=enriched_payload,
            runner=runner,
            compact_result=compact_post_market_result,
        )
        await _publish_post_market_event(
            ctx,
            analysis_run_id=str(result["analysis_run_id"]),
            payload=result,
            timestamp=datetime.now(UTC),
        )
        return result
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


async def run_generator_job(
    ctx: dict[str, Any],
    generator_job_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    generator_job_store = ctx["generator_job_store"]
    running_record = await asyncio.to_thread(
        generator_job_store.start_job,
        generator_job_id=generator_job_id,
        started_at=datetime.now(UTC),
    )
    await _publish_generator_job_event(ctx, "running", running_record)
    try:
        args = build_generator_args(payload)
        result = await asyncio.to_thread(generate_symbol_ideas, args)
        final_status = "succeeded" if result.get("status") == "ok" else "no_play"
        completed_record = await asyncio.to_thread(
            generator_job_store.complete_job,
            generator_job_id=generator_job_id,
            finished_at=datetime.now(UTC),
            status=final_status,
            result=result,
        )
        await _publish_generator_job_event(ctx, "completed", completed_record)
        return {
            "generator_job_id": generator_job_id,
            "status": final_status,
            "symbol": completed_record["symbol"],
        }
    except Exception as exc:
        failed_record = await asyncio.to_thread(
            generator_job_store.fail_job,
            generator_job_id=generator_job_id,
            finished_at=datetime.now(UTC),
            error_text=str(exc),
        )
        await _publish_generator_job_event(ctx, "failed", failed_record)
        raise


class WorkerSettings:
    functions = [run_live_collector_job, run_post_close_analysis_job, run_post_market_analysis_job, run_generator_job]
    redis_settings = build_redis_settings(default_redis_url())
    on_startup = startup
    on_shutdown = shutdown
    keep_result = 0
    job_timeout = 60 * 60
    max_jobs = 1
