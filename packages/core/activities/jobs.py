from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from temporalio import activity

from core.engine.outbox_publisher import publish_pending_engine_outbox
from core.integrations.calendar_events.refresh import run_calendar_event_refresh
from core.jobs.orchestration import due_job_payload, singleton_lease_key
from core.jobs.registry import (
    ALERT_DELIVERY_JOB_TYPE,
    ALERT_RECONCILE_JOB_TYPE,
    BROKER_SYNC_JOB_TYPE,
    CALENDAR_EVENT_REFRESH_JOB_TYPE,
    COMPANY_VALUATION_BOOTSTRAP_JOB_TYPE,
    COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_TYPE,
    COMPANY_VALUATION_SCREEN_MATERIALIZE_JOB_TYPE,
    ENGINE_OUTBOX_PUBLISH_JOB_TYPE,
    EXECUTION_LIFECYCLE_START_JOB_TYPE,
    TICKER_SOURCE_JOB_TYPE,
    TRADINGAGENTS_SCAN_JOB_TYPE,
    TRADING_STRATEGY_ENTRY_JOB_TYPE,
    TRADING_STRATEGY_MANAGE_JOB_TYPE,
)
from core.jobs.specs import get_declared_job_row
from core.runtime.config import default_database_url, default_redis_url
from core.services.alert_delivery import (
    ALERT_DELIVERY_STALE_SECONDS,
    reconcile_alert_delivery,
    run_alert_delivery,
)
from core.services.broker_sync import run_broker_sync
from core.services.company_valuation.bootstrap import (
    CompanyValuationBootstrapRequest,
    bootstrap_company_valuation,
)
from core.services.company_valuation.screening import materialize_company_valuation_screen
from core.services.company_valuation.unresolved import (
    ResolveUnresolvedInstitutionalPositionsRequest,
    resolve_unresolved_institutional_positions,
)
from core.services.execution_intents import start_pending_execution_lifecycle_workflows
from core.services.exit_manager import run_trading_strategy_manage
from core.services.sources.dispatch import persist_ticker_source_result, run_ticker_source
from core.services.trading_engine.strategy_runtime import run_trading_strategy_entry
from core.services.tradingagents_scan import run_tradingagents_scan
from core.storage.company_valuation_repository import CompanyValuationRepository
from core.storage.factory import build_storage_context
from core.storage.serializers import parse_date, parse_datetime, render_value
from core.value_coercion import coerce_int

JOB_LEASE_TTL_SECONDS = 600
TERMINAL_JOB_STATUSES = {"succeeded", "skipped", "failed"}


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _heartbeat(job_store: Any, *, job_run_id: str, orchestration_id: str, worker_name: str, lease_key: str | None) -> None:
    now = _utc_now()
    run_record = job_store.heartbeat_job_run(
        job_run_id=job_run_id,
        expected_orchestration_id=orchestration_id,
        heartbeat_at=now,
        worker_name=worker_name,
    )
    if run_record is None:
        raise RuntimeError(f"Job run {job_run_id} was superseded during Temporal execution.")
    if lease_key is not None:
        job_store.renew_lease(
            lease_key=lease_key,
            owner=job_run_id,
            expires_in_seconds=JOB_LEASE_TTL_SECONDS,
            state={"kind": "temporal_singleton_job", "orchestration_id": orchestration_id},
        )
    activity.heartbeat({"job_run_id": job_run_id, "heartbeat_at": now.isoformat()})


def _normalized_tickers(payload: Mapping[str, Any]) -> tuple[str, ...]:
    values = payload.get("tickers")
    if not isinstance(values, list):
        return ()
    return tuple(dict.fromkeys(str(value or "").upper().strip() for value in values if str(value or "").strip()))


def _compact_ticker_source_result(result: Mapping[str, Any]) -> dict[str, Any]:
    entries = [dict(item) for item in list(result.get("entries") or []) if isinstance(item, Mapping)]
    observations = list(result.get("observations") or [])
    return render_value(
        {
            "status": result.get("status"),
            "source_id": result.get("source_id"),
            "recipe": result.get("recipe"),
            "generated_at": result.get("generated_at"),
            "ticker_source_run_id": result.get("ticker_source_run_id"),
            "symbols": list(result.get("symbols") or []),
            "entries": entries[:25],
            "entry_count": len(entries),
            "observation_count": len(observations),
            "summary": result.get("summary"),
            "degradation": result.get("degradation"),
            "persistence": result.get("persistence"),
        }
    )


def _compact_calendar_event_refresh_result(result: Mapping[str, Any]) -> dict[str, Any]:
    return render_value(
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


def _compact_company_valuation_bootstrap_result(result: Mapping[str, Any]) -> dict[str, Any]:
    ticker_rows = list(result.get("ticker_results") or [])
    errors = [str(item) for item in list(result.get("errors") or [])]
    compact_rows = [
        {
            "ticker": row.get("ticker"),
            "error": row.get("error"),
            "quality_score": ((row.get("recompute") or {}).get("quality_score") if isinstance(row, Mapping) else None),
            "intrinsic_value_mid": ((row.get("recompute") or {}).get("intrinsic_value_mid") if isinstance(row, Mapping) else None),
            "valuation_gap": ((row.get("recompute") or {}).get("valuation_gap") if isinstance(row, Mapping) else None),
        }
        for row in ticker_rows[:25]
        if isinstance(row, Mapping)
    ]
    return render_value(
        {
            "status": result.get("status"),
            "started_at": result.get("started_at"),
            "completed_at": result.get("completed_at"),
            "tickers": list(result.get("tickers") or []),
            "ticker_count": len(list(result.get("tickers") or [])),
            "ticker_results": compact_rows,
            "ticker_result_count": len(ticker_rows),
            "screening": result.get("screening"),
            "universe_bootstrap": result.get("universe_bootstrap"),
            "treasury_curve": result.get("treasury_curve"),
            "errors": errors[:25],
            "error_count": len(errors),
        }
    )


def _run_job(
    *,
    job_type: str,
    payload: dict[str, Any],
    job_run_id: str,
    storage: Any,
    heartbeat: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    database_url = str(payload.get("db") or storage.database_url or default_database_url())
    if job_type == BROKER_SYNC_JOB_TYPE:
        heartbeat()
        result = run_broker_sync(
            db_target=database_url,
            history_range=str(payload.get("history_range", "1D")),
            activity_lookback_days=int(payload.get("activity_lookback_days", 1)),
        )
        return result, render_value(result)
    if job_type == TRADING_STRATEGY_ENTRY_JOB_TYPE:
        heartbeat()
        result = run_trading_strategy_entry(
            db_target=database_url,
            trading_strategy_id=str(payload["trading_strategy_id"]),
            market_date=payload.get("market_date"),
            planner_job_run_id=job_run_id,
        )
        return result, render_value(result)
    if job_type == TRADING_STRATEGY_MANAGE_JOB_TYPE:
        heartbeat()
        result = run_trading_strategy_manage(
            db_target=database_url,
            storage=storage,
            trading_strategy_id=str(payload["trading_strategy_id"]),
        )
        return result, render_value(result)
    if job_type == EXECUTION_LIFECYCLE_START_JOB_TYPE:
        heartbeat()
        result = start_pending_execution_lifecycle_workflows(
            db_target=database_url,
            limit=int(payload.get("limit", 25) or 25),
        )
        return result, render_value(result)
    if job_type == ENGINE_OUTBOX_PUBLISH_JOB_TYPE:
        heartbeat()
        result = asyncio.run(
            publish_pending_engine_outbox(
                repository=storage.engine_events,
                nats_url=payload.get("nats_url"),
                limit=int(payload.get("limit", 100) or 100),
            )
        )
        return result, render_value(result)
    if job_type == ALERT_DELIVERY_JOB_TYPE:
        heartbeat()
        result = run_alert_delivery(
            alert_store=storage.alerts,
            alert_id=int(payload["alert_id"]),
            delivery_job_run_id=job_run_id,
            worker_name="temporal",
        )
        return result, result
    if job_type == ALERT_RECONCILE_JOB_TYPE:
        heartbeat()
        result = reconcile_alert_delivery(
            alert_store=storage.alerts,
            job_store=storage.jobs,
            limit=int(payload.get("limit", 200)),
            stale_after_seconds=int(payload.get("stale_after_seconds", ALERT_DELIVERY_STALE_SECONDS)),
        )
        return result, result
    if job_type == TICKER_SOURCE_JOB_TYPE:
        heartbeat()
        result = run_ticker_source(
            source_id=str(payload["source_id"]),
            recipe=str(payload["recipe"]),
            recipe_args=dict(payload.get("recipe_args") or {}),
        )
        persistence = persist_ticker_source_result(
            storage.engine_facts,
            source_id=str(payload["source_id"]),
            recipe=str(payload["recipe"]),
            job_run_id=job_run_id,
            result=result,
        )
        enriched = {
            **result,
            "ticker_source_run_id": persistence.get("ticker_source_run_id"),
            "persistence": persistence,
        }
        return enriched, _compact_ticker_source_result(enriched)
    if job_type == CALENDAR_EVENT_REFRESH_JOB_TYPE:
        heartbeat()
        result = run_calendar_event_refresh(
            refresh_id=str(payload["refresh_id"]),
            database_url=database_url,
            redis_url=str(payload.get("redis_url") or default_redis_url()),
            payload=payload,
            heartbeat=heartbeat,
        )
        return result, _compact_calendar_event_refresh_result(result)
    if job_type == TRADINGAGENTS_SCAN_JOB_TYPE:
        heartbeat()
        result = run_tradingagents_scan(
            storage=storage,
            job_store=storage.jobs,
            job_run_id=job_run_id,
            payload=payload,
            heartbeat=heartbeat,
        )
        return result, render_value(result)
    if job_type == COMPANY_VALUATION_BOOTSTRAP_JOB_TYPE:
        heartbeat()
        result = bootstrap_company_valuation(
            CompanyValuationBootstrapRequest(
                tickers=_normalized_tickers(payload),
                as_of=parse_datetime(payload.get("as_of")),
                bootstrap_universe=bool(payload.get("bootstrap_universe", False)),
                universe_limit=coerce_int(payload.get("universe_limit")),
                refresh_treasury=bool(payload.get("refresh_treasury", True)),
                treasury_curve_date=(None if payload.get("treasury_curve_date") in (None, "") else parse_date(str(payload["treasury_curve_date"]))),
                refresh_filings=bool(payload.get("refresh_filings", True)),
                filings_since=parse_datetime(payload.get("filings_since")),
                filings_until=parse_datetime(payload.get("filings_until")),
                refresh_insiders=bool(payload.get("refresh_insiders", True)),
                refresh_beneficial_ownership=bool(payload.get("refresh_beneficial_ownership", True)),
                ownership_since=parse_datetime(payload.get("ownership_since")),
                ownership_until=parse_datetime(payload.get("ownership_until")),
                refresh_market_inputs=bool(payload.get("refresh_market_inputs", True)),
                recompute=bool(payload.get("recompute", True)),
                materialize_screen=bool(payload.get("materialize_screen", True)),
                continue_on_error=bool(payload.get("continue_on_error", True)),
                config_root=(None if payload.get("config_root") in (None, "") else str(payload["config_root"])),
            ),
            repository=CompanyValuationRepository(database_url),
            heartbeat=heartbeat,
        ).to_payload()
        return result, _compact_company_valuation_bootstrap_result(result)
    if job_type == COMPANY_VALUATION_SCREEN_MATERIALIZE_JOB_TYPE:
        heartbeat()
        result = materialize_company_valuation_screen(
            as_of=parse_datetime(payload.get("as_of")),
            template_id=(None if payload.get("template_id") in (None, "") else str(payload["template_id"])),
            tickers=_normalized_tickers(payload) or None,
            issuer_limit=coerce_int(payload.get("issuer_limit")),
            supported_only=bool(payload.get("supported_only", True)),
            stressed_operator_only=bool(payload.get("stressed_operator_only", False)),
            repository=CompanyValuationRepository(database_url),
            config_root=(None if payload.get("config_root") in (None, "") else str(payload["config_root"])),
            heartbeat=heartbeat,
        ).to_payload()
        return result, render_value(result)
    if job_type == COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_TYPE:
        heartbeat()
        result = resolve_unresolved_institutional_positions(
            ResolveUnresolvedInstitutionalPositionsRequest(
                report_period=(None if payload.get("report_period") in (None, "") else parse_date(str(payload["report_period"]))),
                limit_rows=int(payload.get("limit_rows", 20000) or 20000),
                batch_cusips=int(payload.get("batch_cusips", 50) or 50),
                max_attempts=int(payload.get("max_attempts", 5) or 5),
            ),
            repository=CompanyValuationRepository(database_url),
            heartbeat=heartbeat,
        ).to_payload()
        return result, render_value(result)
    raise RuntimeError(f"Unsupported Temporal job type: {job_type}")


def _scheduled_payload(request: dict[str, Any]) -> tuple[str, datetime, dict[str, Any], dict[str, Any]] | None:
    job_key = str(request.get("job_key") or "").strip()
    if not job_key:
        raise ValueError("Temporal scheduled job request requires job_key")
    definition = get_declared_job_row(job_key)
    if definition is None or not bool(definition.get("enabled")):
        return None
    observed_at = parse_datetime(request.get("scheduled_for")) or _utc_now()
    due = due_job_payload(definition, now=observed_at)
    if due is None:
        return None
    job_run_id, scheduled_for, payload = due
    return str(job_run_id), scheduled_for, dict(payload), dict(definition)


@activity.defn(name="run_scheduled_job_activity")
def run_scheduled_job_activity(request: dict[str, Any]) -> dict[str, Any]:
    info = activity.info()
    base_orchestration_id = str(request.get("orchestration_id") or info.workflow_id)
    orchestration_id = base_orchestration_id
    storage = build_storage_context(default_database_url())
    job_store = storage.jobs
    lease_key: str | None = None
    job_run_id: str | None = None
    try:
        if request.get("adhoc"):
            orchestration_id = base_orchestration_id
            job_run_id = str(request["job_run_id"])
            scheduled_for = parse_datetime(request.get("scheduled_for")) or _utc_now()
            payload = dict(request.get("payload") or {})
            job_key = str(request["job_key"])
            job_type = str(request["job_type"])
            singleton_scope = payload.get("singleton_scope")
            job_store.create_job_run(
                job_run_id=job_run_id,
                job_key=job_key,
                orchestration_id=orchestration_id,
                job_type=job_type,
                status="queued",
                scheduled_for=scheduled_for,
                session_id=payload.get("session_id") if isinstance(payload.get("session_id"), str) else None,
                payload=payload,
            )
        else:
            scheduled = _scheduled_payload(request)
            if scheduled is None:
                return {"status": "skipped", "reason": "not_due_or_disabled", "job_key": request.get("job_key")}
            job_run_id, scheduled_for, payload, definition = scheduled
            orchestration_id = f"{base_orchestration_id}:{job_run_id}"
            job_key = str(definition["job_key"])
            job_type = str(definition["job_type"])
            singleton_scope = definition.get("singleton_scope")
            row, created = job_store.create_job_run(
                job_run_id=job_run_id,
                job_key=job_key,
                orchestration_id=orchestration_id,
                job_type=job_type,
                status="queued",
                scheduled_for=scheduled_for,
                payload=payload,
            )
            if not created and str(row.get("status") or "") in TERMINAL_JOB_STATUSES:
                return {"status": "skipped", "reason": "job_run_already_terminal", "job_run_id": job_run_id}

        if singleton_scope:
            lease_key = singleton_lease_key(job_type, str(singleton_scope))
            acquired = job_store.acquire_lease(
                lease_key=lease_key,
                owner=job_run_id,
                job_run_id=job_run_id,
                expires_in_seconds=JOB_LEASE_TTL_SECONDS,
                state={"kind": "temporal_singleton_job", "job_key": job_key, "orchestration_id": orchestration_id},
            )
            if not acquired:
                result = {"status": "skipped", "reason": "singleton_lease_unavailable"}
                job_store.update_job_run_status(
                    job_run_id=job_run_id,
                    status="skipped",
                    expected_orchestration_id=orchestration_id,
                    worker_name=info.worker_identity,
                    finished_at=_utc_now(),
                    heartbeat_at=_utc_now(),
                    result=result,
                )
                return result

        job_store.update_job_run_status(
            job_run_id=job_run_id,
            status="running",
            expected_orchestration_id=orchestration_id,
            worker_name=info.worker_identity,
            started_at=_utc_now(),
            heartbeat_at=_utc_now(),
        )

        def heartbeat() -> None:
            _heartbeat(
                job_store,
                job_run_id=job_run_id or "",
                orchestration_id=orchestration_id,
                worker_name=info.worker_identity,
                lease_key=lease_key,
            )

        result, compact = _run_job(
            job_type=job_type,
            payload=payload,
            job_run_id=job_run_id,
            storage=storage,
            heartbeat=heartbeat,
        )
        final_status = "skipped" if isinstance(result, dict) and result.get("status") == "skipped" else "succeeded"
        completed = job_store.update_job_run_status(
            job_run_id=job_run_id,
            status=final_status,
            expected_orchestration_id=orchestration_id,
            worker_name=info.worker_identity,
            finished_at=_utc_now(),
            heartbeat_at=_utc_now(),
            result=compact,
        )
        if completed is None:
            raise RuntimeError(f"Job run {job_run_id} was superseded before completion.")
        return compact
    except Exception as exc:
        if job_run_id:
            job_store.update_job_run_status(
                job_run_id=job_run_id,
                status="failed",
                expected_orchestration_id=orchestration_id,
                worker_name=info.worker_identity,
                finished_at=_utc_now(),
                heartbeat_at=_utc_now(),
                error_text=str(exc),
            )
        raise
    finally:
        if lease_key is not None and job_run_id is not None:
            job_store.release_lease(lease_key, owner=job_run_id)
        storage.close()
