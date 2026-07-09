from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow


@workflow.defn
class ScheduledJobWorkflow:
    @workflow.run
    async def run(self, request: dict[str, Any]) -> dict[str, Any]:
        payload = dict(request)
        payload.setdefault("scheduled_for", workflow.now().isoformat().replace("+00:00", "Z"))
        payload.setdefault("orchestration_id", workflow.info().workflow_id)
        return await workflow.execute_activity(
            "run_scheduled_job_activity",
            payload,
            start_to_close_timeout=timedelta(hours=8),
            heartbeat_timeout=timedelta(minutes=10),
        )
