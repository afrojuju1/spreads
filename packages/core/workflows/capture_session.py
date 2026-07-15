from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow


@workflow.defn
class CaptureSessionWorkflow:
    @workflow.run
    async def run(self, request: dict[str, Any]) -> dict[str, Any]:
        return await workflow.execute_activity(
            "run_capture_session_activity",
            dict(request),
            start_to_close_timeout=timedelta(days=3650),
            heartbeat_timeout=timedelta(minutes=2),
        )
