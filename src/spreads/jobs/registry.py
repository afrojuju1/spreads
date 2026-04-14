from __future__ import annotations

from dataclasses import dataclass

MAIN_QUEUE_NAME = "arq:queue:fast"
COLLECTOR_QUEUE_NAME = "arq:queue:collector"

BROKER_SYNC_JOB_TYPE = "broker_sync"
EXECUTION_SUBMIT_JOB_TYPE = "execution_submit"
ALERT_DELIVERY_JOB_TYPE = "alert_delivery"
ALERT_RECONCILE_JOB_TYPE = "alert_reconcile"
LIVE_COLLECTOR_JOB_TYPE = "live_collector"
POST_CLOSE_ANALYSIS_JOB_TYPE = "post_close_analysis"
POST_MARKET_ANALYSIS_JOB_TYPE = "post_market_analysis"
SESSION_EXIT_MANAGER_JOB_TYPE = "session_exit_manager"
COLLECTOR_RECOVERY_JOB_TYPE = "collector_recovery"

EXECUTION_SUBMIT_ADHOC_JOB_KEY = "execution_submit:adhoc"
ALERT_DELIVERY_ADHOC_JOB_KEY = "alert_delivery:adhoc"
ALERT_RECONCILE_JOB_KEY = "alert_reconcile:scheduled"
POST_CLOSE_ANALYSIS_ADHOC_JOB_KEY = "post_close_analysis:adhoc"
COLLECTOR_RECOVERY_JOB_KEY = "collector_recovery:global"


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
            queue_name=MAIN_QUEUE_NAME,
        ),
        JobSpec(
            job_type=EXECUTION_SUBMIT_JOB_TYPE,
            task_name="run_execution_submit_job",
            queue_name=MAIN_QUEUE_NAME,
        ),
        JobSpec(
            job_type=ALERT_DELIVERY_JOB_TYPE,
            task_name="run_alert_delivery_job",
            queue_name=MAIN_QUEUE_NAME,
        ),
        JobSpec(
            job_type=ALERT_RECONCILE_JOB_TYPE,
            task_name="run_alert_reconcile_job",
            queue_name=MAIN_QUEUE_NAME,
        ),
        JobSpec(
            job_type=SESSION_EXIT_MANAGER_JOB_TYPE,
            task_name="run_session_exit_manager_job",
            queue_name=MAIN_QUEUE_NAME,
        ),
        JobSpec(
            job_type=LIVE_COLLECTOR_JOB_TYPE,
            task_name="run_live_collector_job",
            queue_name=COLLECTOR_QUEUE_NAME,
        ),
        JobSpec(
            job_type=POST_CLOSE_ANALYSIS_JOB_TYPE,
            task_name="run_post_close_analysis_job",
            queue_name=MAIN_QUEUE_NAME,
        ),
        JobSpec(
            job_type=POST_MARKET_ANALYSIS_JOB_TYPE,
            task_name="run_post_market_analysis_job",
            queue_name=MAIN_QUEUE_NAME,
        ),
        JobSpec(
            job_type=COLLECTOR_RECOVERY_JOB_TYPE,
            task_name="run_collector_recovery_job",
            queue_name=MAIN_QUEUE_NAME,
        ),
    )
}

WORKER_LANES = (
    WorkerLaneSpec(
        settings_name="MainWorkerSettings",
        queue_name=MAIN_QUEUE_NAME,
        task_names=(
            JOB_SPECS[BROKER_SYNC_JOB_TYPE].task_name,
            JOB_SPECS[EXECUTION_SUBMIT_JOB_TYPE].task_name,
            JOB_SPECS[ALERT_DELIVERY_JOB_TYPE].task_name,
            JOB_SPECS[ALERT_RECONCILE_JOB_TYPE].task_name,
            JOB_SPECS[SESSION_EXIT_MANAGER_JOB_TYPE].task_name,
            JOB_SPECS[POST_CLOSE_ANALYSIS_JOB_TYPE].task_name,
            JOB_SPECS[POST_MARKET_ANALYSIS_JOB_TYPE].task_name,
            JOB_SPECS[COLLECTOR_RECOVERY_JOB_TYPE].task_name,
        ),
        max_jobs=4,
    ),
    WorkerLaneSpec(
        settings_name="CollectorWorkerSettings",
        queue_name=COLLECTOR_QUEUE_NAME,
        task_names=(JOB_SPECS[LIVE_COLLECTOR_JOB_TYPE].task_name,),
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
