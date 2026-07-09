from __future__ import annotations

import argparse
import asyncio
from datetime import timedelta
import json
import re
from typing import Any

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleState,
    ScheduleUpdate,
)

from core.jobs.registry import get_task_queue_name_for_job_type
from core.jobs.specs import list_declared_job_rows
from core.runtime.config import default_temporal_address, default_temporal_namespace
from core.workflows.scheduled_job import ScheduledJobWorkflow


def schedule_id_for_job_key(job_key: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_.-]+", "-", str(job_key).strip()).strip("-")
    return f"spreads-job-{normalized}"


def _schedule_interval(definition: dict[str, Any]) -> timedelta:
    schedule_type = str(definition.get("schedule_type") or "")
    schedule = dict(definition.get("schedule") or {})
    if schedule_type == "interval_minutes":
        return timedelta(minutes=max(int(schedule.get("minutes", 1) or 1), 1))
    return timedelta(minutes=1)


def build_temporal_schedule(definition: dict[str, Any]) -> tuple[str, Schedule]:
    job_key = str(definition["job_key"])
    job_type = str(definition["job_type"])
    task_queue = get_task_queue_name_for_job_type(job_type)
    if task_queue is None:
        raise RuntimeError(f"Job type is not registered for Temporal scheduling: {job_type}")
    schedule_id = schedule_id_for_job_key(job_key)
    return schedule_id, Schedule(
        action=ScheduleActionStartWorkflow(
            ScheduledJobWorkflow.run,
            {"job_key": job_key},
            id=schedule_id,
            task_queue=task_queue,
        ),
        spec=ScheduleSpec(
            intervals=[ScheduleIntervalSpec(every=_schedule_interval(definition))],
            time_zone_name="America/New_York",
        ),
        policy=SchedulePolicy(
            overlap=ScheduleOverlapPolicy.SKIP,
            catchup_window=timedelta(minutes=5),
        ),
        state=ScheduleState(
            paused=not bool(definition.get("enabled")),
            note=f"Spreads declared job {job_key}",
        ),
    )


async def reconcile_temporal_schedules(
    *,
    client: Client | None,
    dry_run: bool = False,
    enabled_only: bool | None = None,
) -> dict[str, Any]:
    rows = list_declared_job_rows(enabled_only=enabled_only)
    planned: list[dict[str, Any]] = []
    created = 0
    updated = 0
    for definition in rows:
        schedule_id, schedule = build_temporal_schedule(definition)
        planned.append(
            {
                "schedule_id": schedule_id,
                "job_key": definition.get("job_key"),
                "job_type": definition.get("job_type"),
                "task_queue": get_task_queue_name_for_job_type(str(definition.get("job_type") or "")),
                "enabled": bool(definition.get("enabled")),
            }
        )
        if dry_run:
            continue
        if client is None:
            raise RuntimeError("Temporal client is required when dry_run is false.")
        handle = client.get_schedule_handle(schedule_id)
        try:
            await handle.describe()
        except Exception:
            await client.create_schedule(schedule_id, schedule)
            created += 1
            continue

        async def updater(_: Any) -> ScheduleUpdate:
            return ScheduleUpdate(schedule)

        await handle.update(updater)
        updated += 1
    return {
        "status": "ok",
        "dry_run": dry_run,
        "planned": planned,
        "planned_count": len(planned),
        "created_count": created,
        "updated_count": updated,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconcile Spreads declared jobs to Temporal schedules.")
    parser.add_argument("--address", default=default_temporal_address(), help="Temporal address.")
    parser.add_argument("--namespace", default=default_temporal_namespace(), help="Temporal namespace.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned schedules without mutating Temporal.")
    parser.add_argument("--enabled-only", action="store_true", help="Only reconcile enabled declared jobs.")
    return parser.parse_args(argv)


async def _main_async(args: argparse.Namespace) -> int:
    client = None if bool(args.dry_run) else await Client.connect(args.address, namespace=args.namespace)
    result = await reconcile_temporal_schedules(
        client=client,
        dry_run=bool(args.dry_run),
        enabled_only=True if bool(args.enabled_only) else None,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_main_async(parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
