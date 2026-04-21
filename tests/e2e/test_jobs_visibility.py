from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from core.jobs.registry import ALERT_DELIVERY_ADHOC_JOB_KEY
from core.jobs.seed import default_job_definitions
from core.jobs.orchestration import (
    SCHEDULER_RUNTIME_LEASE_KEY,
    WORKER_RUNTIME_LEASE_PREFIX,
)
from core.services.ops import build_jobs_overview


class JobsVisibilityTests(unittest.TestCase):
    def test_default_job_definitions_include_alert_delivery_adhoc(self) -> None:
        self.assertTrue(
            any(
                row.get("job_key") == ALERT_DELIVERY_ADHOC_JOB_KEY
                for row in default_job_definitions()
            )
        )

    def test_build_jobs_overview_reports_worker_lanes(self) -> None:
        now = datetime.now(UTC)
        definitions = [
            {
                "job_key": "discovery_run:test",
                "job_type": "discovery_run",
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

        class _JobStore:
            def schema_ready(self) -> bool:
                return True

            def list_job_definitions(self, **_: object) -> list[dict[str, object]]:
                return list(definitions)

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
                            "job_run_id": "queued-stale-runtime",
                            "job_type": "options_automation_execute",
                            "status": "queued",
                            "scheduled_for": (now - timedelta(minutes=30)).isoformat(),
                        },
                        {
                            "job_run_id": "queued-discovery-run",
                            "job_type": "discovery_run",
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
                            "worker_name": "worker-runtime-1",
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
                        "lease_key": f"{WORKER_RUNTIME_LEASE_PREFIX}worker-runtime-1",
                        "owner": "worker-runtime-1",
                        "expires_at": (now + timedelta(minutes=1)).isoformat(),
                        "lease_state": {
                            "kind": "worker",
                            "lane": "runtime",
                            "settings_name": "RuntimeWorkerSettings",
                            "queue_name": "arq:queue:runtime",
                        },
                    },
                    {
                        "lease_key": f"{WORKER_RUNTIME_LEASE_PREFIX}worker-discovery-1",
                        "owner": "worker-discovery-1",
                        "expires_at": (now + timedelta(minutes=1)).isoformat(),
                        "lease_state": {
                            "kind": "worker",
                            "lane": "discovery",
                            "settings_name": "DiscoveryWorkerSettings",
                            "queue_name": "arq:queue:discovery",
                        },
                    },
                ]

        class _Storage:
            def __init__(self) -> None:
                self.jobs = _JobStore()

        with patch(
            "core.services.ops.jobs.default_job_definitions",
            return_value=list(definitions),
        ):
            payload = build_jobs_overview(storage=_Storage())
        lane_rows = list(payload["details"]["worker_lanes"])
        self.assertEqual(len(lane_rows), 2)
        lanes = {row["lane"]: row for row in lane_rows}
        self.assertEqual(lanes["runtime"]["running_job_count"], 1)
        self.assertEqual(lanes["discovery"]["queued_job_count"], 1)
        self.assertEqual(lanes["runtime"]["queued_job_count"], 0)
        self.assertEqual(lanes["runtime"]["active_worker_count"], 1)
        self.assertEqual(lanes["discovery"]["active_worker_count"], 1)
        self.assertEqual(payload["summary"]["stale_queued_job_count"], 1)
        self.assertEqual(payload["summary"]["seed_drift_count"], 0)

    def test_build_jobs_overview_ignores_benign_skipped_runs_in_status(self) -> None:
        now = datetime.now(UTC)
        definitions = [
            {
                "job_key": "discovery_recovery:global",
                "job_type": "discovery_recovery",
                "enabled": True,
                "schedule_type": "interval_minutes",
                "schedule": {"minutes": 1},
                "payload": {"singleton_scope": "global"},
                "singleton_scope": "global",
            }
        ]

        class _JobStore:
            def schema_ready(self) -> bool:
                return True

            def list_job_definitions(self, **_: object) -> list[dict[str, object]]:
                return list(definitions)

            def list_latest_runs_by_job_keys(
                self, **_: object
            ) -> list[dict[str, object]]:
                return [
                    {
                        "job_run_id": "discovery_recovery:start:1",
                        "job_key": "discovery_recovery:global",
                        "job_type": "discovery_recovery",
                        "status": "skipped",
                        "scheduled_for": now.isoformat(),
                        "finished_at": now.isoformat(),
                        "heartbeat_at": now.isoformat(),
                        "payload": {"singleton_scope": "global"},
                        "result": {
                            "status": "skipped",
                            "reason": "singleton_lease_unavailable",
                        },
                    }
                ]

            def list_job_runs(
                self, *, status: str | None = None, **_: object
            ) -> list[dict[str, object]]:
                if status in {"queued", "running"}:
                    return []
                return [
                    {
                        "job_run_id": "discovery_recovery:start:1",
                        "job_key": "discovery_recovery:global",
                        "job_type": "discovery_recovery",
                        "status": "skipped",
                        "scheduled_for": now.isoformat(),
                        "finished_at": now.isoformat(),
                        "heartbeat_at": now.isoformat(),
                        "payload": {"singleton_scope": "global"},
                        "result": {
                            "status": "skipped",
                            "reason": "singleton_lease_unavailable",
                        },
                    }
                ]

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
                        "lease_key": f"{WORKER_RUNTIME_LEASE_PREFIX}worker-runtime-1",
                        "owner": "worker-runtime-1",
                        "expires_at": (now + timedelta(minutes=1)).isoformat(),
                        "lease_state": {
                            "kind": "worker",
                            "lane": "runtime",
                            "settings_name": "RuntimeWorkerSettings",
                            "queue_name": "arq:queue:runtime",
                        },
                    },
                    {
                        "lease_key": f"{WORKER_RUNTIME_LEASE_PREFIX}worker-discovery-1",
                        "owner": "worker-discovery-1",
                        "expires_at": (now + timedelta(minutes=1)).isoformat(),
                        "lease_state": {
                            "kind": "worker",
                            "lane": "discovery",
                            "settings_name": "DiscoveryWorkerSettings",
                            "queue_name": "arq:queue:discovery",
                        },
                    }
                ]

        class _Storage:
            def __init__(self) -> None:
                self.jobs = _JobStore()

        with patch(
            "core.services.ops.jobs.default_job_definitions",
            return_value=list(definitions),
        ):
            payload = build_jobs_overview(storage=_Storage())

        self.assertEqual(payload["status"], "healthy")
        self.assertEqual(payload["summary"]["status_counts"], {"skipped": 1})
        self.assertEqual(payload["summary"]["operator_status_counts"], {"healthy": 1})
        self.assertEqual(payload["attention"], [])

    def test_build_jobs_overview_reports_seed_drift(self) -> None:
        now = datetime.now(UTC)
        live_definitions = [
            {
                "job_key": "discovery_run:test",
                "job_type": "discovery_run",
                "enabled": True,
                "schedule_type": "interval_minutes",
                "schedule": {"minutes": 2},
                "payload": {},
                "singleton_scope": None,
            },
            {
                "job_key": "extra-job",
                "job_type": "options_automation_entry",
                "enabled": False,
                "schedule_type": "manual",
                "schedule": {},
                "payload": {},
                "singleton_scope": None,
            },
        ]
        canonical_definitions = [
            {
                "job_key": "discovery_run:test",
                "job_type": "discovery_run",
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

        class _JobStore:
            def schema_ready(self) -> bool:
                return True

            def list_job_definitions(self, **_: object) -> list[dict[str, object]]:
                return list(live_definitions)

            def list_latest_runs_by_job_keys(
                self, **_: object
            ) -> list[dict[str, object]]:
                return []

            def list_job_runs(
                self, *, status: str | None = None, **_: object
            ) -> list[dict[str, object]]:
                if status in {"queued", "running"}:
                    return []
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
                        "lease_key": f"{WORKER_RUNTIME_LEASE_PREFIX}worker-runtime-1",
                        "owner": "worker-runtime-1",
                        "expires_at": (now + timedelta(minutes=1)).isoformat(),
                        "lease_state": {
                            "kind": "worker",
                            "lane": "runtime",
                            "settings_name": "RuntimeWorkerSettings",
                            "queue_name": "arq:queue:runtime",
                        },
                    },
                    {
                        "lease_key": f"{WORKER_RUNTIME_LEASE_PREFIX}worker-discovery-1",
                        "owner": "worker-discovery-1",
                        "expires_at": (now + timedelta(minutes=1)).isoformat(),
                        "lease_state": {
                            "kind": "worker",
                            "lane": "discovery",
                            "settings_name": "DiscoveryWorkerSettings",
                            "queue_name": "arq:queue:discovery",
                        },
                    },
                ]

        class _Storage:
            def __init__(self) -> None:
                self.jobs = _JobStore()

        with patch(
            "core.services.ops.jobs.default_job_definitions",
            return_value=list(canonical_definitions),
        ):
            payload = build_jobs_overview(storage=_Storage())

        self.assertEqual(payload["status"], "degraded")
        self.assertEqual(payload["summary"]["seed_drift_count"], 3)
        self.assertEqual(payload["summary"]["seed_missing_count"], 1)
        self.assertEqual(payload["summary"]["seed_extra_count"], 1)
        self.assertEqual(payload["summary"]["seed_mismatched_count"], 1)
        self.assertEqual(payload["details"]["seed_drift"]["missing"], [{"job_key": "entry"}])
        self.assertEqual(
            payload["details"]["seed_drift"]["extra"],
            [{"job_key": "extra-job"}],
        )
        self.assertEqual(
            payload["details"]["seed_drift"]["mismatched"],
            [{"job_key": "discovery_run:test", "fields": ["schedule"]}],
        )
        self.assertTrue(
            any(
                item.get("code") == "job_definition_seed_drift"
                for item in payload["attention"]
            )
        )


if __name__ == "__main__":
    unittest.main()
