from __future__ import annotations

from typing import Any

from core.events.bus import publish_global_event_async


async def _publish_job_run_event(ctx: dict[str, Any], run_record: Any) -> None:
    if run_record is None:
        return
    event_bus = ctx.get("event_bus")
    if event_bus is None:
        return
    try:
        payload = dict(run_record)
        await publish_global_event_async(
            event_bus,
            topic="job.run.updated",
            event_class="control_event",
            entity_type="job_run",
            entity_id=run_record["job_run_id"],
            payload=payload,
            timestamp=run_record.get("finished_at") or run_record.get("heartbeat_at") or run_record["scheduled_for"],
            source="worker",
            session_date=payload.get("session_date") if isinstance(payload.get("session_date"), str) else None,
            correlation_id=str(run_record["job_key"]),
        )
    except Exception:
        pass
