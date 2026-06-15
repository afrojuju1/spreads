from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.jobs.registry import (
    ALERT_DELIVERY_JOB_TYPE,
    ALERT_RECONCILE_JOB_TYPE,
    BROKER_SYNC_JOB_TYPE,
    CALENDAR_EVENT_REFRESH_JOB_TYPE,
    COMPANY_VALUATION_BOOTSTRAP_JOB_TYPE,
    COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_TYPE,
    COMPANY_VALUATION_SCREEN_MATERIALIZE_JOB_TYPE,
    EXECUTION_INTENT_DISPATCH_JOB_TYPE,
    EXECUTION_SUBMIT_JOB_TYPE,
    TICKER_SOURCE_JOB_TYPE,
    TRADINGAGENTS_SCAN_JOB_TYPE,
    TRADING_STRATEGY_ENTRY_JOB_TYPE,
    TRADING_STRATEGY_MANAGE_JOB_TYPE,
)
from core.integrations.calendar_events.refresh import run_calendar_event_refresh
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
from core.services.company_valuation.screening import (
    materialize_company_valuation_screen,
)
from core.services.company_valuation.unresolved import (
    ResolveUnresolvedInstitutionalPositionsRequest,
    resolve_unresolved_institutional_positions,
)
from core.services.execution.submit import run_execution_submit
from core.services.execution_intents import dispatch_pending_execution_intents
from core.services.exit_manager import run_trading_strategy_manage
from core.services.ticker_sources import persist_ticker_source_result, run_ticker_source
from core.services.trading_engine.strategy_runtime import run_trading_strategy_entry
from core.services.tradingagents_scan import run_tradingagents_scan
from core.storage.company_valuation_repository import CompanyValuationRepository
from core.storage.serializers import parse_date, parse_datetime, render_value

from .managed import _execute_managed_job


def _normalized_tickers(payload: Mapping[str, Any]) -> tuple[str, ...]:
    values = payload.get("tickers")
    if not isinstance(values, list):
        return ()
    return tuple(dict.fromkeys(str(value or "").upper().strip() for value in values if str(value or "").strip()))


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _compact_company_valuation_bootstrap_result(
    result: Mapping[str, Any],
) -> dict[str, Any]:
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
        compact_result=render_value,
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
        compact_result=render_value,
    )


async def run_trading_strategy_entry_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = ctx["database_url"]

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_trading_strategy_entry(
            db_target=database_url,
            trading_strategy_id=str(payload["trading_strategy_id"]),
            market_date=payload.get("market_date"),
            planner_job_run_id=job_run_id,
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = TRADING_STRATEGY_ENTRY_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=render_value,
    )


async def run_trading_strategy_manage_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = ctx["database_url"]

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_trading_strategy_manage(
            db_target=database_url,
            storage=ctx["storage"],
            trading_strategy_id=str(payload["trading_strategy_id"]),
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = TRADING_STRATEGY_MANAGE_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=render_value,
    )


async def run_execution_intent_dispatch_job(
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
    enriched_payload["job_type"] = EXECUTION_INTENT_DISPATCH_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=render_value,
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
            stale_after_seconds=int(payload.get("stale_after_seconds", ALERT_DELIVERY_STALE_SECONDS)),
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


async def run_ticker_source_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        result = run_ticker_source(
            source_id=str(payload["source_id"]),
            recipe=str(payload["recipe"]),
            recipe_args=dict(payload.get("recipe_args") or {}),
        )
        persistence = persist_ticker_source_result(
            ctx["storage"].engine_facts,
            source_id=str(payload["source_id"]),
            recipe=str(payload["recipe"]),
            job_run_id=job_run_id,
            result=result,
        )
        return {
            **result,
            "ticker_source_run_id": persistence.get("ticker_source_run_id"),
            "persistence": persistence,
        }

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = TICKER_SOURCE_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=_compact_ticker_source_result,
    )


async def run_calendar_event_refresh_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_calendar_event_refresh(
            refresh_id=str(payload["refresh_id"]),
            database_url=ctx["database_url"],
            redis_url=ctx["redis_url"],
            payload=payload,
            heartbeat=heartbeat,
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = CALENDAR_EVENT_REFRESH_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=_compact_calendar_event_refresh_result,
    )


async def run_tradingagents_scan_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        return run_tradingagents_scan(
            storage=ctx["storage"],
            job_store=ctx["job_store"],
            job_run_id=job_run_id,
            payload=payload,
            heartbeat=heartbeat,
        )

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = TRADINGAGENTS_SCAN_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=render_value,
    )


async def run_company_valuation_bootstrap_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = str(payload.get("db") or ctx["database_url"])

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        result = bootstrap_company_valuation(
            CompanyValuationBootstrapRequest(
                tickers=_normalized_tickers(payload),
                as_of=parse_datetime(payload.get("as_of")),
                bootstrap_universe=bool(payload.get("bootstrap_universe", False)),
                universe_limit=_optional_int(payload.get("universe_limit")),
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
        )
        return result.to_payload()

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = COMPANY_VALUATION_BOOTSTRAP_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=_compact_company_valuation_bootstrap_result,
    )


async def run_company_valuation_screen_materialize_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = str(payload.get("db") or ctx["database_url"])

    def runner(heartbeat: Any) -> dict[str, Any]:
        heartbeat()
        result = materialize_company_valuation_screen(
            as_of=parse_datetime(payload.get("as_of")),
            template_id=(None if payload.get("template_id") in (None, "") else str(payload["template_id"])),
            tickers=_normalized_tickers(payload) or None,
            issuer_limit=_optional_int(payload.get("issuer_limit")),
            supported_only=bool(payload.get("supported_only", True)),
            stressed_operator_only=bool(payload.get("stressed_operator_only", False)),
            repository=CompanyValuationRepository(database_url),
            config_root=(None if payload.get("config_root") in (None, "") else str(payload["config_root"])),
            heartbeat=heartbeat,
        )
        return result.to_payload()

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = COMPANY_VALUATION_SCREEN_MATERIALIZE_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=render_value,
    )


async def run_company_valuation_resolve_unresolved_job(
    ctx: dict[str, Any],
    job_key: str,
    job_run_id: str,
    payload: dict[str, Any],
    arq_job_id: str,
) -> dict[str, Any]:
    database_url = str(payload.get("db") or ctx["database_url"])

    def runner(heartbeat: Any) -> dict[str, Any]:
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
        )
        return result.to_payload()

    enriched_payload = dict(payload)
    enriched_payload["job_type"] = COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_TYPE
    return await _execute_managed_job(
        ctx,
        job_key=job_key,
        job_run_id=job_run_id,
        arq_job_id=arq_job_id,
        payload=enriched_payload,
        runner=runner,
        compact_result=render_value,
    )
