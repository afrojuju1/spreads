from __future__ import annotations

from core.jobs.registry import DISCOVERY_QUEUE_NAME, RUNTIME_QUEUE_NAME
from core.runtime.config import default_redis_url
from core.runtime.redis import build_redis_settings

from .lifecycle import discovery_startup, runtime_startup, shutdown
from .managed import ManagedJobFailure, SupersededJobRun
from .tasks import (
    run_alert_delivery_job,
    run_alert_reconcile_job,
    run_broker_sync_job,
    run_collector_recovery_job,
    run_execution_submit_job,
    run_live_collector_job,
    run_options_automation_entry_job,
    run_options_automation_execute_job,
    run_options_automation_management_job,
    run_position_exit_manager_job,
    run_post_close_analysis_job,
    run_post_market_analysis_job,
)


class RuntimeWorkerSettings:
    functions = [
        run_broker_sync_job,
        run_collector_recovery_job,
        run_execution_submit_job,
        run_options_automation_entry_job,
        run_options_automation_management_job,
        run_options_automation_execute_job,
        run_alert_delivery_job,
        run_alert_reconcile_job,
        run_position_exit_manager_job,
        run_post_close_analysis_job,
        run_post_market_analysis_job,
    ]
    queue_name = RUNTIME_QUEUE_NAME
    redis_settings = build_redis_settings(default_redis_url())
    on_startup = runtime_startup
    on_shutdown = shutdown
    keep_result = 0
    job_timeout = 8 * 60 * 60
    max_jobs = 4


class DiscoveryWorkerSettings:
    functions = [
        run_live_collector_job,
    ]
    queue_name = DISCOVERY_QUEUE_NAME
    redis_settings = build_redis_settings(default_redis_url())
    on_startup = discovery_startup
    on_shutdown = shutdown
    keep_result = 0
    job_timeout = 8 * 60 * 60
    max_jobs = 1


WorkerSettings = RuntimeWorkerSettings


__all__ = [
    "DiscoveryWorkerSettings",
    "ManagedJobFailure",
    "RuntimeWorkerSettings",
    "SupersededJobRun",
    "WorkerSettings",
    "run_alert_delivery_job",
    "run_alert_reconcile_job",
    "run_broker_sync_job",
    "run_collector_recovery_job",
    "run_execution_submit_job",
    "run_live_collector_job",
    "run_options_automation_entry_job",
    "run_options_automation_execute_job",
    "run_options_automation_management_job",
    "run_position_exit_manager_job",
    "run_post_close_analysis_job",
    "run_post_market_analysis_job",
]
