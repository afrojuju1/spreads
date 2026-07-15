from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime
import hashlib
import json
from typing import Any

from temporalio.client import Client, ScheduleUpdate

from core.db import open_storage
from core.jobs.registry import ROUTINE_SCHEDULE_RECONCILE_JOB_TYPE, get_job_spec, get_workflow_lane_for_job_type
from core.jobs.specs import list_declared_job_rows
from core.runtime.config import default_database_url, default_workflow_address, default_workflow_namespace
from core.workflow_runtime.provider import (
    PROVIDER_NAME,
    ROUTINE_WORKFLOW_PREFIX,
    build_provider_schedule,
    connect_provider,
    provider_queue_for_lane,
    routine_workflow_id,
)

ROUTINE_RECONCILIATION_JOB_KEY = "workflow_runtime:routine_schedules"
ROUTINE_SCHEDULE_PREFIX = ROUTINE_WORKFLOW_PREFIX
RETIRED_SCHEDULE_PREFIX = "spreads-job-"


def routine_config_hash(rows: list[dict[str, Any]]) -> str:
    payload = [
        {
            "job_key": row.get("job_key"),
            "config_hash": row.get("config_hash"),
            "enabled": bool(row.get("enabled")),
            "schedule_type": row.get("schedule_type"),
            "schedule": row.get("schedule"),
            "activity_maximum_attempts": (
                job_spec.activity_maximum_attempts
                if (job_spec := get_job_spec(str(row.get("job_type") or ""))) is not None
                else None
            ),
        }
        for row in rows
    ]
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


async def _existing_schedule_ids(client: Client) -> set[str]:
    schedules = await client.list_schedules()
    return {str(item.id) async for item in schedules}


async def reconcile_routine_schedules(
    *,
    client: Client | None,
    dry_run: bool = False,
    enabled_only: bool | None = None,
) -> dict[str, Any]:
    rows = list_declared_job_rows(enabled_only=enabled_only)
    planned: list[dict[str, Any]] = []
    desired_ids: set[str] = set()
    created = 0
    updated = 0
    deleted = 0
    existing_ids = set() if dry_run else await _existing_schedule_ids(client) if client is not None else set()
    for definition in rows:
        schedule_id = routine_workflow_id(str(definition["job_key"]))
        schedule_type = str(definition.get("schedule_type") or "manual")
        lane = get_workflow_lane_for_job_type(str(definition.get("job_type") or ""))
        if lane is None:
            raise RuntimeError(f"Routine {definition['job_key']} has no workflow lane")
        planned_row = {
            "routine_id": definition.get("job_key"),
            "routine_type": definition.get("job_type"),
            "schedule_id": None if schedule_type == "manual" else schedule_id,
            "schedule_type": schedule_type,
            "workflow_lane": lane,
            "enabled": bool(definition.get("enabled")),
            "provider": {
                "name": PROVIDER_NAME,
                "queue": provider_queue_for_lane(lane),
            },
        }
        planned.append(planned_row)
        if schedule_type == "manual":
            continue
        desired_ids.add(schedule_id)
        if dry_run:
            build_provider_schedule(definition, schedule_id=schedule_id)
            continue
        if client is None:
            raise RuntimeError("Workflow provider client is required when dry_run is false")
        schedule = build_provider_schedule(definition, schedule_id=schedule_id)
        handle = client.get_schedule_handle(schedule_id)
        if schedule_id not in existing_ids:
            await client.create_schedule(schedule_id, schedule)
            created += 1
            continue

        async def updater(_: Any, *, replacement: Any = schedule) -> ScheduleUpdate:
            return ScheduleUpdate(replacement)

        await handle.update(updater)
        updated += 1
    if not dry_run and client is not None:
        stale_ids = {
            schedule_id
            for schedule_id in existing_ids
            if schedule_id.startswith((ROUTINE_SCHEDULE_PREFIX, RETIRED_SCHEDULE_PREFIX)) and schedule_id not in desired_ids
        }
        for schedule_id in sorted(stale_ids):
            await client.get_schedule_handle(schedule_id).delete()
            deleted += 1
    reconciled_at = datetime.now(UTC)
    return {
        "status": "ok",
        "dry_run": dry_run,
        "reconciled_at": reconciled_at.isoformat().replace("+00:00", "Z"),
        "config_hash": routine_config_hash(rows),
        "planned": planned,
        "planned_count": len(planned),
        "created_count": created,
        "updated_count": updated,
        "deleted_count": deleted,
        "provider": PROVIDER_NAME,
    }


def record_reconciliation(result: dict[str, Any]) -> None:
    reconciled_at = datetime.fromisoformat(str(result["reconciled_at"]).replace("Z", "+00:00"))
    job_run_id = f"{ROUTINE_RECONCILIATION_JOB_KEY}:{reconciled_at.strftime('%Y%m%dT%H%M%SZ')}"
    with open_storage(default_database_url()) as storage:
        if not storage.jobs.schema_ready():
            return
        storage.jobs.create_job_run(
            job_run_id=job_run_id,
            job_key=ROUTINE_RECONCILIATION_JOB_KEY,
            orchestration_id=None,
            job_type=ROUTINE_SCHEDULE_RECONCILE_JOB_TYPE,
            status="succeeded",
            scheduled_for=reconciled_at,
            payload={"config_hash": result["config_hash"], "provider": result["provider"]},
            worker_name="routine-schedules",
            result=result,
        )
        storage.jobs.update_job_run_status(
            job_run_id=job_run_id,
            status="succeeded",
            finished_at=reconciled_at,
            result=result,
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconcile declared Spreads routine schedules.")
    parser.add_argument("--address", default=default_workflow_address(), help="Workflow provider address.")
    parser.add_argument("--namespace", default=default_workflow_namespace(), help="Workflow provider namespace.")
    parser.add_argument("--dry-run", action="store_true", help="Render schedules without mutating the provider.")
    parser.add_argument("--enabled-only", action="store_true", help="Only reconcile enabled routines.")
    return parser.parse_args(argv)


async def _main_async(args: argparse.Namespace) -> int:
    client = None if bool(args.dry_run) else await connect_provider(address=args.address, namespace=args.namespace)
    result = await reconcile_routine_schedules(
        client=client,
        dry_run=bool(args.dry_run),
        enabled_only=True if bool(args.enabled_only) else None,
    )
    if not args.dry_run:
        record_reconciliation(result)
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_main_async(parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
