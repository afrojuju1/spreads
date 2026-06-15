from __future__ import annotations

from dataclasses import dataclass

RUNTIME_QUEUE_NAME = "arq:queue:runtime"
DATA_QUEUE_NAME = "arq:queue:data"
VALUATION_QUEUE_NAME = "arq:queue:valuation"
RESEARCH_QUEUE_NAME = "arq:queue:research"

BROKER_SYNC_JOB_TYPE = "broker_sync"
EXECUTION_SUBMIT_JOB_TYPE = "execution_submit"
ALERT_DELIVERY_JOB_TYPE = "alert_delivery"
ALERT_RECONCILE_JOB_TYPE = "alert_reconcile"
TICKER_SOURCE_JOB_TYPE = "ticker_source"
CALENDAR_EVENT_REFRESH_JOB_TYPE = "calendar_event_refresh"
TRADINGAGENTS_SCAN_JOB_TYPE = "tradingagents_scan"
TRADING_STRATEGY_ENTRY_JOB_TYPE = "trading_strategy_entry"
TRADING_STRATEGY_MANAGE_JOB_TYPE = "trading_strategy_manage"
EXECUTION_INTENT_DISPATCH_JOB_TYPE = "execution_intent_dispatch"
COMPANY_VALUATION_BOOTSTRAP_JOB_TYPE = "company_valuation_bootstrap"
COMPANY_VALUATION_SCREEN_MATERIALIZE_JOB_TYPE = "company_valuation_screen_materialize"
COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_TYPE = "company_valuation_resolve_unresolved"

EXECUTION_SUBMIT_ADHOC_JOB_KEY = "execution_submit:adhoc"
ALERT_DELIVERY_ADHOC_JOB_KEY = "alert_delivery:adhoc"
ALERT_RECONCILE_JOB_KEY = "alert_reconcile:scheduled"
EXECUTION_INTENT_DISPATCH_ADHOC_JOB_KEY = "execution_intent_dispatch:adhoc"
COMPANY_VALUATION_BOOTSTRAP_ADHOC_JOB_KEY = "company_valuation_bootstrap:adhoc"
COMPANY_VALUATION_SCREEN_MATERIALIZE_ADHOC_JOB_KEY = "company_valuation_screen_materialize:adhoc"
COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_KEY = "company_valuation_resolve_unresolved:global"


@dataclass(frozen=True)
class JobSpec:
    job_type: str
    task_name: str
    queue_name: str


@dataclass(frozen=True)
class WorkerLaneSpec:
    settings_name: str
    queue_name: str
    task_names: tuple[str, ...]
    max_jobs: int = 1


JOB_SPECS = {
    spec.job_type: spec
    for spec in (
        JobSpec(
            job_type=BROKER_SYNC_JOB_TYPE,
            task_name="run_broker_sync_job",
            queue_name=RUNTIME_QUEUE_NAME,
        ),
        JobSpec(
            job_type=EXECUTION_SUBMIT_JOB_TYPE,
            task_name="run_execution_submit_job",
            queue_name=RUNTIME_QUEUE_NAME,
        ),
        JobSpec(
            job_type=ALERT_DELIVERY_JOB_TYPE,
            task_name="run_alert_delivery_job",
            queue_name=RUNTIME_QUEUE_NAME,
        ),
        JobSpec(
            job_type=ALERT_RECONCILE_JOB_TYPE,
            task_name="run_alert_reconcile_job",
            queue_name=RUNTIME_QUEUE_NAME,
        ),
        JobSpec(
            job_type=TICKER_SOURCE_JOB_TYPE,
            task_name="run_ticker_source_job",
            queue_name=DATA_QUEUE_NAME,
        ),
        JobSpec(
            job_type=CALENDAR_EVENT_REFRESH_JOB_TYPE,
            task_name="run_calendar_event_refresh_job",
            queue_name=DATA_QUEUE_NAME,
        ),
        JobSpec(
            job_type=TRADINGAGENTS_SCAN_JOB_TYPE,
            task_name="run_tradingagents_scan_job",
            queue_name=RESEARCH_QUEUE_NAME,
        ),
        JobSpec(
            job_type=TRADING_STRATEGY_ENTRY_JOB_TYPE,
            task_name="run_trading_strategy_entry_job",
            queue_name=RUNTIME_QUEUE_NAME,
        ),
        JobSpec(
            job_type=TRADING_STRATEGY_MANAGE_JOB_TYPE,
            task_name="run_trading_strategy_manage_job",
            queue_name=RUNTIME_QUEUE_NAME,
        ),
        JobSpec(
            job_type=EXECUTION_INTENT_DISPATCH_JOB_TYPE,
            task_name="run_execution_intent_dispatch_job",
            queue_name=RUNTIME_QUEUE_NAME,
        ),
        JobSpec(
            job_type=COMPANY_VALUATION_BOOTSTRAP_JOB_TYPE,
            task_name="run_company_valuation_bootstrap_job",
            queue_name=VALUATION_QUEUE_NAME,
        ),
        JobSpec(
            job_type=COMPANY_VALUATION_SCREEN_MATERIALIZE_JOB_TYPE,
            task_name="run_company_valuation_screen_materialize_job",
            queue_name=VALUATION_QUEUE_NAME,
        ),
        JobSpec(
            job_type=COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_TYPE,
            task_name="run_company_valuation_resolve_unresolved_job",
            queue_name=VALUATION_QUEUE_NAME,
        ),
    )
}

WORKER_LANES = (
    WorkerLaneSpec(
        settings_name="RuntimeWorkerSettings",
        queue_name=RUNTIME_QUEUE_NAME,
        task_names=(
            JOB_SPECS[BROKER_SYNC_JOB_TYPE].task_name,
            JOB_SPECS[EXECUTION_SUBMIT_JOB_TYPE].task_name,
            JOB_SPECS[ALERT_DELIVERY_JOB_TYPE].task_name,
            JOB_SPECS[ALERT_RECONCILE_JOB_TYPE].task_name,
            JOB_SPECS[TRADING_STRATEGY_ENTRY_JOB_TYPE].task_name,
            JOB_SPECS[TRADING_STRATEGY_MANAGE_JOB_TYPE].task_name,
            JOB_SPECS[EXECUTION_INTENT_DISPATCH_JOB_TYPE].task_name,
        ),
        max_jobs=4,
    ),
    WorkerLaneSpec(
        settings_name="DataWorkerSettings",
        queue_name=DATA_QUEUE_NAME,
        task_names=(
            JOB_SPECS[TICKER_SOURCE_JOB_TYPE].task_name,
            JOB_SPECS[CALENDAR_EVENT_REFRESH_JOB_TYPE].task_name,
        ),
    ),
    WorkerLaneSpec(
        settings_name="ValuationWorkerSettings",
        queue_name=VALUATION_QUEUE_NAME,
        task_names=(
            JOB_SPECS[COMPANY_VALUATION_BOOTSTRAP_JOB_TYPE].task_name,
            JOB_SPECS[COMPANY_VALUATION_SCREEN_MATERIALIZE_JOB_TYPE].task_name,
            JOB_SPECS[COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_TYPE].task_name,
        ),
    ),
    WorkerLaneSpec(
        settings_name="ResearchWorkerSettings",
        queue_name=RESEARCH_QUEUE_NAME,
        task_names=(JOB_SPECS[TRADINGAGENTS_SCAN_JOB_TYPE].task_name,),
    ),
)


def get_job_spec(job_type: str) -> JobSpec | None:
    return JOB_SPECS.get(job_type)


def get_task_name_for_job_type(job_type: str) -> str | None:
    spec = get_job_spec(job_type)
    return None if spec is None else spec.task_name


def get_queue_name_for_job_type(job_type: str) -> str | None:
    spec = get_job_spec(job_type)
    return None if spec is None else spec.queue_name
