from __future__ import annotations

from dataclasses import dataclass

RUNTIME_QUEUE_NAME = "arq:queue:runtime"
DISCOVERY_QUEUE_NAME = "arq:queue:discovery"
VALUATION_QUEUE_NAME = "arq:queue:valuation"
RESEARCH_QUEUE_NAME = "arq:queue:research"

BROKER_SYNC_JOB_TYPE = "broker_sync"
EXECUTION_SUBMIT_JOB_TYPE = "execution_submit"
ALERT_DELIVERY_JOB_TYPE = "alert_delivery"
ALERT_RECONCILE_JOB_TYPE = "alert_reconcile"
DISCOVERY_RUN_JOB_TYPE = "discovery_run"
SYMBOL_FEED_JOB_TYPE = "symbol_feed"
TRADINGAGENTS_SCAN_JOB_TYPE = "tradingagents_scan"
FINVIZ_DIRECT_TRADING_JOB_TYPE = "finviz_direct_trading"
POSITION_EXIT_MANAGER_JOB_TYPE = "position_exit_manager"
DISCOVERY_RECOVERY_JOB_TYPE = "discovery_recovery"
OPTIONS_AUTOMATION_ENTRY_JOB_TYPE = "options_automation_entry"
OPTIONS_AUTOMATION_EXECUTE_JOB_TYPE = "options_automation_execute"
COMPANY_VALUATION_BOOTSTRAP_JOB_TYPE = "company_valuation_bootstrap"
COMPANY_VALUATION_SCREEN_MATERIALIZE_JOB_TYPE = (
    "company_valuation_screen_materialize"
)
COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_TYPE = (
    "company_valuation_resolve_unresolved"
)

EXECUTION_SUBMIT_ADHOC_JOB_KEY = "execution_submit:adhoc"
ALERT_DELIVERY_ADHOC_JOB_KEY = "alert_delivery:adhoc"
ALERT_RECONCILE_JOB_KEY = "alert_reconcile:scheduled"
DISCOVERY_RECOVERY_JOB_KEY = "discovery_recovery:global"
OPTIONS_AUTOMATION_ENTRY_ADHOC_JOB_KEY = "options_automation_entry:adhoc"
OPTIONS_AUTOMATION_EXECUTE_ADHOC_JOB_KEY = "options_automation_execute:adhoc"
COMPANY_VALUATION_BOOTSTRAP_ADHOC_JOB_KEY = "company_valuation_bootstrap:adhoc"
COMPANY_VALUATION_SCREEN_MATERIALIZE_ADHOC_JOB_KEY = (
    "company_valuation_screen_materialize:adhoc"
)
COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_KEY = (
    "company_valuation_resolve_unresolved:global"
)


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
            job_type=POSITION_EXIT_MANAGER_JOB_TYPE,
            task_name="run_position_exit_manager_job",
            queue_name=RUNTIME_QUEUE_NAME,
        ),
        JobSpec(
            job_type=DISCOVERY_RUN_JOB_TYPE,
            task_name="run_discovery_run_job",
            queue_name=DISCOVERY_QUEUE_NAME,
        ),
        JobSpec(
            job_type=SYMBOL_FEED_JOB_TYPE,
            task_name="run_symbol_feed_job",
            queue_name=DISCOVERY_QUEUE_NAME,
        ),
        JobSpec(
            job_type=TRADINGAGENTS_SCAN_JOB_TYPE,
            task_name="run_tradingagents_scan_job",
            queue_name=RESEARCH_QUEUE_NAME,
        ),
        JobSpec(
            job_type=FINVIZ_DIRECT_TRADING_JOB_TYPE,
            task_name="run_finviz_direct_trading_job",
            queue_name=RUNTIME_QUEUE_NAME,
        ),
        JobSpec(
            job_type=DISCOVERY_RECOVERY_JOB_TYPE,
            task_name="run_discovery_recovery_job",
            queue_name=RUNTIME_QUEUE_NAME,
        ),
        JobSpec(
            job_type=OPTIONS_AUTOMATION_ENTRY_JOB_TYPE,
            task_name="run_options_automation_entry_job",
            queue_name=RUNTIME_QUEUE_NAME,
        ),
        JobSpec(
            job_type=OPTIONS_AUTOMATION_EXECUTE_JOB_TYPE,
            task_name="run_options_automation_execute_job",
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
            JOB_SPECS[POSITION_EXIT_MANAGER_JOB_TYPE].task_name,
            JOB_SPECS[DISCOVERY_RECOVERY_JOB_TYPE].task_name,
            JOB_SPECS[OPTIONS_AUTOMATION_ENTRY_JOB_TYPE].task_name,
            JOB_SPECS[OPTIONS_AUTOMATION_EXECUTE_JOB_TYPE].task_name,
            JOB_SPECS[FINVIZ_DIRECT_TRADING_JOB_TYPE].task_name,
        ),
        max_jobs=4,
    ),
    WorkerLaneSpec(
        settings_name="DiscoveryWorkerSettings",
        queue_name=DISCOVERY_QUEUE_NAME,
        task_names=(
            JOB_SPECS[DISCOVERY_RUN_JOB_TYPE].task_name,
            JOB_SPECS[SYMBOL_FEED_JOB_TYPE].task_name,
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
        task_names=(
            JOB_SPECS[TRADINGAGENTS_SCAN_JOB_TYPE].task_name,
        ),
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
