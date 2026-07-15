from __future__ import annotations

import argparse
import json
import time
from typing import Any

from core.services.ops.jobs.state import build_jobs_compact_state


def workflow_runtime_verification() -> dict[str, Any]:
    jobs = build_jobs_compact_state(limit=10)
    details = dict(jobs.get("details") or {})
    schedules = dict(details.get("routine_schedules") or {})
    lanes = [dict(row) for row in details.get("workflow_lanes") or [] if isinstance(row, dict)]
    blocked_required_lanes = [
        str(row.get("lane"))
        for row in lanes
        if bool(row.get("required_for_deploy")) and str(row.get("status") or "unknown") != "healthy"
    ]
    blocked_enabled_optional_lanes = [
        str(row.get("lane"))
        for row in lanes
        if bool(row.get("optional")) and str(row.get("status") or "unknown") == "blocked"
    ]
    schedule_status = str(schedules.get("status") or "unknown")
    healthy = not blocked_required_lanes and not blocked_enabled_optional_lanes and schedule_status == "healthy"
    return {
        "status": "healthy" if healthy else "blocked",
        "routine_schedule_status": schedule_status,
        "blocked_required_workflow_lanes": blocked_required_lanes,
        "blocked_enabled_optional_workflow_lanes": blocked_enabled_optional_lanes,
        "workflow_lanes": lanes,
        "routine_schedules": schedules,
    }


def wait_for_workflow_runtime(*, wait_seconds: int) -> dict[str, Any]:
    deadline = time.monotonic() + max(int(wait_seconds), 0)
    while True:
        payload = workflow_runtime_verification()
        if payload["status"] == "healthy" or time.monotonic() >= deadline:
            return payload
        time.sleep(2)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify required workflow lanes and routine schedules.")
    parser.add_argument("--wait-seconds", type=int, default=0, help="Wait for worker pollers to become healthy.")
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = wait_for_workflow_runtime(wait_seconds=max(int(args.wait_seconds), 0))
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
    else:
        print(f"Workflow runtime: {result['status']}")
        print(f"Routine schedules: {result['routine_schedule_status']}")
        for row in result["workflow_lanes"]:
            print(f"{row.get('lane')}: {row.get('status')} ({row.get('poller_count', 0)} pollers)")
    return 0 if result["status"] == "healthy" else 1


if __name__ == "__main__":
    raise SystemExit(main())
