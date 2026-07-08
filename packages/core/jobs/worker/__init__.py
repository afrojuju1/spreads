from __future__ import annotations

from core.jobs.registry import (
    DATA_QUEUE_NAME,
    RESEARCH_QUEUE_NAME,
    RUNTIME_QUEUE_NAME,
    VALUATION_QUEUE_NAME,
)
from core.runtime.config import default_redis_url
from core.runtime.redis import build_redis_settings

from .lifecycle import (
    data_startup,
    research_startup,
    runtime_startup,
    shutdown,
    valuation_startup,
)
from .managed import ManagedJobFailure, SupersededJobRun
from .tasks import (
    run_alert_delivery_job,
    run_alert_reconcile_job,
    run_broker_sync_job,
    run_calendar_event_refresh_job,
    run_company_valuation_bootstrap_job,
    run_company_valuation_resolve_unresolved_job,
    run_company_valuation_screen_materialize_job,
    run_execution_lifecycle_start_job,
    run_execution_submit_job,
    run_ticker_source_job,
    run_tradingagents_scan_job,
    run_trading_strategy_entry_job,
    run_trading_strategy_manage_job,
)


class RuntimeWorkerSettings:
    functions = [
        run_broker_sync_job,
        run_execution_submit_job,
        run_trading_strategy_entry_job,
        run_trading_strategy_manage_job,
        run_execution_lifecycle_start_job,
        run_alert_delivery_job,
        run_alert_reconcile_job,
    ]
    queue_name = RUNTIME_QUEUE_NAME
    redis_settings = build_redis_settings(default_redis_url())
    on_startup = runtime_startup
    on_shutdown = shutdown
    keep_result = 0
    log_results = False
    job_timeout = 8 * 60 * 60
    max_jobs = 4


class DataWorkerSettings:
    functions = [run_ticker_source_job, run_calendar_event_refresh_job]
    queue_name = DATA_QUEUE_NAME
    redis_settings = build_redis_settings(default_redis_url())
    on_startup = data_startup
    on_shutdown = shutdown
    keep_result = 0
    log_results = False
    job_timeout = 8 * 60 * 60
    max_jobs = 1


class ValuationWorkerSettings:
    functions = [
        run_company_valuation_bootstrap_job,
        run_company_valuation_screen_materialize_job,
        run_company_valuation_resolve_unresolved_job,
    ]
    queue_name = VALUATION_QUEUE_NAME
    redis_settings = build_redis_settings(default_redis_url())
    on_startup = valuation_startup
    on_shutdown = shutdown
    keep_result = 0
    log_results = False
    job_timeout = 8 * 60 * 60
    max_jobs = 1


class ResearchWorkerSettings:
    functions = [
        run_tradingagents_scan_job,
    ]
    queue_name = RESEARCH_QUEUE_NAME
    redis_settings = build_redis_settings(default_redis_url())
    on_startup = research_startup
    on_shutdown = shutdown
    keep_result = 0
    log_results = False
    job_timeout = 8 * 60 * 60
    max_jobs = 1


WorkerSettings = RuntimeWorkerSettings


__all__ = [
    "DataWorkerSettings",
    "ManagedJobFailure",
    "ResearchWorkerSettings",
    "RuntimeWorkerSettings",
    "SupersededJobRun",
    "ValuationWorkerSettings",
    "WorkerSettings",
    "run_alert_delivery_job",
    "run_alert_reconcile_job",
    "run_broker_sync_job",
    "run_calendar_event_refresh_job",
    "run_company_valuation_bootstrap_job",
    "run_company_valuation_resolve_unresolved_job",
    "run_company_valuation_screen_materialize_job",
    "run_execution_lifecycle_start_job",
    "run_execution_submit_job",
    "run_ticker_source_job",
    "run_tradingagents_scan_job",
    "run_trading_strategy_entry_job",
    "run_trading_strategy_manage_job",
]
