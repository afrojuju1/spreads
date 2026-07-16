from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy, SearchAttributeKey

from core.jobs.contracts import build_ad_hoc_job_run_id
from core.workflow_runtime.wire import require_temporal_payload_budget

TEMPORAL_SCHEDULED_START_TIME = SearchAttributeKey.for_datetime("TemporalScheduledStartTime")


@workflow.defn
class ScheduledJobWorkflow:
    @workflow.run
    async def run(self, request: dict[str, Any]) -> dict[str, Any]:
        payload = dict(request)
        info = workflow.info()
        scheduled_for = info.typed_search_attributes.get(TEMPORAL_SCHEDULED_START_TIME) or workflow.now()
        payload.setdefault("scheduled_for", scheduled_for.isoformat().replace("+00:00", "Z"))
        payload.setdefault("workflow_id", info.workflow_id)
        payload.setdefault("orchestration_id", info.run_id)
        if bool(payload.get("adhoc")):
            payload["job_run_id"] = build_ad_hoc_job_run_id(
                str(payload.get("job_key") or ""),
                info.run_id,
            )
        retry_config = payload.get("activity_retry")
        retry_values = dict(retry_config) if isinstance(retry_config, dict) else {}
        require_temporal_payload_budget(payload, label="Routine Activity input")
        return await workflow.execute_activity(
            "run_scheduled_job_activity",
            payload,
            start_to_close_timeout=timedelta(hours=4),
            schedule_to_close_timeout=timedelta(hours=8),
            heartbeat_timeout=timedelta(minutes=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=5),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(minutes=1),
                maximum_attempts=max(int(retry_values.get("maximum_attempts", 3)), 1),
            ),
        )
