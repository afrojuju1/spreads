from __future__ import annotations

from dataclasses import dataclass

LIFECYCLE_WORKFLOW_LANE = "lifecycle"
RUNTIME_WORKFLOW_LANE = "runtime"
DATA_WORKFLOW_LANE = "data"
MAINTENANCE_WORKFLOW_LANE = "maintenance"
CAPTURE_WORKFLOW_LANE = "capture"
VALUATION_WORKFLOW_LANE = "valuation"
RESEARCH_WORKFLOW_LANE = "research"

BROKER_SYNC_JOB_TYPE = "broker_sync"
ENGINE_OUTBOX_PUBLISH_JOB_TYPE = "engine_outbox_publish"
EXECUTION_LIFECYCLE_START_JOB_TYPE = "execution_lifecycle_start"
ALERT_DELIVERY_JOB_TYPE = "alert_delivery"
ALERT_RECONCILE_JOB_TYPE = "alert_reconcile"
TICKER_SOURCE_JOB_TYPE = "ticker_source"
CALENDAR_EVENT_REFRESH_JOB_TYPE = "calendar_event_refresh"
TRADINGAGENTS_SCAN_JOB_TYPE = "tradingagents_scan"
TRADING_STRATEGY_ENTRY_JOB_TYPE = "trading_strategy_entry"
TRADING_STRATEGY_MANAGE_JOB_TYPE = "trading_strategy_manage"
COMPANY_VALUATION_BOOTSTRAP_JOB_TYPE = "company_valuation_bootstrap"
COMPANY_VALUATION_SCREEN_MATERIALIZE_JOB_TYPE = "company_valuation_screen_materialize"
COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_TYPE = "company_valuation_resolve_unresolved"
ROUTINE_SCHEDULE_RECONCILE_JOB_TYPE = "routine_schedule_reconcile"
POSTGRES_BACKUP_JOB_TYPE = "postgres_backup"
OPS_HEALTH_SNAPSHOT_JOB_TYPE = "ops_health_snapshot"
OPS_LOG_RETENTION_JOB_TYPE = "ops_log_retention"

EXECUTION_LIFECYCLE_START_ADHOC_JOB_KEY = "execution_lifecycle_start:adhoc"
ENGINE_OUTBOX_PUBLISH_JOB_KEY = "engine_outbox_publish:global"
ALERT_DELIVERY_ADHOC_JOB_KEY = "alert_delivery:adhoc"
ALERT_RECONCILE_JOB_KEY = "alert_reconcile:scheduled"
COMPANY_VALUATION_BOOTSTRAP_ADHOC_JOB_KEY = "company_valuation_bootstrap:adhoc"
COMPANY_VALUATION_SCREEN_MATERIALIZE_ADHOC_JOB_KEY = "company_valuation_screen_materialize:adhoc"
COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_KEY = "company_valuation_resolve_unresolved:global"


@dataclass(frozen=True)
class JobSpec:
    job_type: str
    workflow_lane: str
    activity_maximum_attempts: int = 3


@dataclass(frozen=True)
class WorkflowLaneSpec:
    lane: str
    required_for_trading: bool = False
    required_for_deploy: bool = True
    optional: bool = False
    max_concurrency: int = 1

    @property
    def job_types(self) -> tuple[str, ...]:
        return get_job_types_for_lane(self.lane)


JOB_SPECS = {
    spec.job_type: spec
    for spec in (
        JobSpec(BROKER_SYNC_JOB_TYPE, RUNTIME_WORKFLOW_LANE),
        JobSpec(ENGINE_OUTBOX_PUBLISH_JOB_TYPE, RUNTIME_WORKFLOW_LANE),
        JobSpec(ALERT_DELIVERY_JOB_TYPE, RUNTIME_WORKFLOW_LANE, activity_maximum_attempts=1),
        JobSpec(ALERT_RECONCILE_JOB_TYPE, RUNTIME_WORKFLOW_LANE),
        JobSpec(TICKER_SOURCE_JOB_TYPE, DATA_WORKFLOW_LANE),
        JobSpec(CALENDAR_EVENT_REFRESH_JOB_TYPE, DATA_WORKFLOW_LANE),
        JobSpec(TRADINGAGENTS_SCAN_JOB_TYPE, RESEARCH_WORKFLOW_LANE),
        JobSpec(TRADING_STRATEGY_ENTRY_JOB_TYPE, RUNTIME_WORKFLOW_LANE),
        JobSpec(TRADING_STRATEGY_MANAGE_JOB_TYPE, RUNTIME_WORKFLOW_LANE),
        JobSpec(EXECUTION_LIFECYCLE_START_JOB_TYPE, RUNTIME_WORKFLOW_LANE),
        JobSpec(COMPANY_VALUATION_BOOTSTRAP_JOB_TYPE, VALUATION_WORKFLOW_LANE),
        JobSpec(
            COMPANY_VALUATION_SCREEN_MATERIALIZE_JOB_TYPE,
            VALUATION_WORKFLOW_LANE,
        ),
        JobSpec(
            COMPANY_VALUATION_RESOLVE_UNRESOLVED_JOB_TYPE,
            VALUATION_WORKFLOW_LANE,
        ),
        JobSpec(ROUTINE_SCHEDULE_RECONCILE_JOB_TYPE, MAINTENANCE_WORKFLOW_LANE),
        JobSpec(POSTGRES_BACKUP_JOB_TYPE, MAINTENANCE_WORKFLOW_LANE),
        JobSpec(OPS_HEALTH_SNAPSHOT_JOB_TYPE, MAINTENANCE_WORKFLOW_LANE),
        JobSpec(OPS_LOG_RETENTION_JOB_TYPE, MAINTENANCE_WORKFLOW_LANE),
    )
}

WORKFLOW_LANES = (
    WorkflowLaneSpec(
        lane=LIFECYCLE_WORKFLOW_LANE,
        required_for_trading=True,
        max_concurrency=4,
    ),
    WorkflowLaneSpec(
        lane=RUNTIME_WORKFLOW_LANE,
        required_for_trading=True,
        max_concurrency=4,
    ),
    WorkflowLaneSpec(
        lane=DATA_WORKFLOW_LANE,
        required_for_trading=True,
    ),
    WorkflowLaneSpec(
        lane=MAINTENANCE_WORKFLOW_LANE,
        required_for_trading=False,
    ),
    WorkflowLaneSpec(
        lane=CAPTURE_WORKFLOW_LANE,
        required_for_trading=True,
        max_concurrency=1,
    ),
    WorkflowLaneSpec(
        lane=VALUATION_WORKFLOW_LANE,
        required_for_deploy=False,
        optional=True,
    ),
    WorkflowLaneSpec(
        lane=RESEARCH_WORKFLOW_LANE,
        required_for_deploy=False,
        optional=True,
    ),
)


def get_job_spec(job_type: str) -> JobSpec | None:
    return JOB_SPECS.get(job_type)


def get_workflow_lane_for_job_type(job_type: str) -> str | None:
    spec = get_job_spec(job_type)
    return None if spec is None else spec.workflow_lane


def get_job_types_for_lane(lane: str) -> tuple[str, ...]:
    normalized = str(lane or "").strip().lower()
    return tuple(spec.job_type for spec in JOB_SPECS.values() if spec.workflow_lane == normalized)


def get_workflow_lane(lane: str) -> WorkflowLaneSpec | None:
    normalized = str(lane or "").strip().lower()
    return next((spec for spec in WORKFLOW_LANES if spec.lane == normalized), None)


__all__ = [
    "DATA_WORKFLOW_LANE",
    "CAPTURE_WORKFLOW_LANE",
    "JOB_SPECS",
    "LIFECYCLE_WORKFLOW_LANE",
    "MAINTENANCE_WORKFLOW_LANE",
    "RESEARCH_WORKFLOW_LANE",
    "RUNTIME_WORKFLOW_LANE",
    "VALUATION_WORKFLOW_LANE",
    "WORKFLOW_LANES",
    "JobSpec",
    "WorkflowLaneSpec",
    "get_job_spec",
    "get_job_types_for_lane",
    "get_workflow_lane",
    "get_workflow_lane_for_job_type",
]
