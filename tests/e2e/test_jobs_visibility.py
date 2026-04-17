from __future__ import annotations

from datetime import UTC, datetime, timedelta
import unittest

from core.jobs.orchestration import (
    SCHEDULER_RUNTIME_LEASE_KEY,
    WORKER_RUNTIME_LEASE_PREFIX,
)
from core.services.ops_visibility import build_jobs_overview


class JobsVisibilityTests(unittest.TestCase):
    def test_build_jobs_overview_reports_worker_lanes(self) -> None:
        now = datetime.now(UTC)

        class _JobStore:
            def schema_ready(self) -> bool:
                return True

            def list_job_definitions(self, **_: object) -> list[dict[str, object]]:
                return [
                    {
                        "job_key": "collector",
                        "job_type": "live_collector",
                        "enabled": True,
                        "schedule_type": "interval_minutes",
                        "schedule": {"minutes": 1},
                        "payload": {},
                        "singleton_scope": None,
                    },
                    {
                        "job_key": "entry",
                        "job_type": "options_automation_entry",
                        "enabled": True,
                        "schedule_type": "interval_minutes",
                        "schedule": {"minutes": 1},
                        "payload": {},
                        "singleton_scope": None,
                    },
                ]

            def list_latest_runs_by_job_keys(
                self, **_: object
            ) -> list[dict[str, object]]:
                return []

            def list_job_runs(
                self, *, status: str | None = None, **_: object
            ) -> list[dict[str, object]]:
                if status == "queued":
                    return [
                        {
                            "job_run_id": "queued-collector",
                            "job_type": "live_collector",
                            "status": "queued",
                            "scheduled_for": now.isoformat(),
                        }
                    ]
                if status == "running":
                    return [
                        {
                            "job_run_id": "running-entry",
                            "job_type": "options_automation_entry",
                            "status": "running",
                            "scheduled_for": now.isoformat(),
                            "started_at": now.isoformat(),
                            "heartbeat_at": now.isoformat(),
                            "worker_name": "worker-main-1",
                        }
                    ]
                return []

            def get_lease(self, lease_key: str) -> dict[str, object] | None:
                if lease_key != SCHEDULER_RUNTIME_LEASE_KEY:
                    return None
                return {
                    "lease_key": lease_key,
                    "owner": "scheduler",
                    "expires_at": (now + timedelta(minutes=1)).isoformat(),
                    "job_run_id": None,
                }

            def list_active_leases(
                self, *, prefix: str | None = None
            ) -> list[dict[str, object]]:
                if prefix != WORKER_RUNTIME_LEASE_PREFIX:
                    return []
                return [
                    {
                        "lease_key": f"{WORKER_RUNTIME_LEASE_PREFIX}worker-main-1",
                        "owner": "worker-main-1",
                        "expires_at": (now + timedelta(minutes=1)).isoformat(),
                        "lease_state": {
                            "kind": "worker",
                            "lane": "main",
                            "settings_name": "MainWorkerSettings",
                            "queue_name": "arq:queue:fast",
                        },
                    },
                    {
                        "lease_key": f"{WORKER_RUNTIME_LEASE_PREFIX}worker-collector-1",
                        "owner": "worker-collector-1",
                        "expires_at": (now + timedelta(minutes=1)).isoformat(),
                        "lease_state": {
                            "kind": "worker",
                            "lane": "collector",
                            "settings_name": "CollectorWorkerSettings",
                            "queue_name": "arq:queue:collector",
                        },
                    },
                ]

        class _Storage:
            def __init__(self) -> None:
                self.jobs = _JobStore()

        payload = build_jobs_overview(storage=_Storage())
        lane_rows = list(payload["details"]["worker_lanes"])
        self.assertEqual(len(lane_rows), 2)
        lanes = {row["lane"]: row for row in lane_rows}
        self.assertEqual(lanes["main"]["running_job_count"], 1)
        self.assertEqual(lanes["collector"]["queued_job_count"], 1)
        self.assertEqual(lanes["main"]["active_worker_count"], 1)
        self.assertEqual(lanes["collector"]["active_worker_count"], 1)


if __name__ == "__main__":
    unittest.main()
