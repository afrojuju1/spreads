from __future__ import annotations

import asyncio
import unittest
from datetime import UTC, datetime
from unittest.mock import patch

from spreads.jobs.scheduler import _reconcile_live_collector_jobs
from spreads.jobs.orchestration import isoformat_utc
from spreads.services.live_recovery import LIVE_SLOT_STATUS_MISSED


class _JobStore:
    def __init__(self, *, old_slot: datetime) -> None:
        self.definition = {
            "job_key": "live_collector:test",
            "job_type": "live_collector",
            "payload": {
                "interval_seconds": 60,
            },
        }
        self.runs: dict[str, dict[str, object]] = {
            "live_collector:test:20260415T143000Z": {
                "job_run_id": "live_collector:test:20260415T143000Z",
                "job_key": "live_collector:test",
                "job_type": "live_collector",
                "status": "queued",
                "scheduled_for": old_slot,
                "slot_at": old_slot,
                "session_id": "live:test:2026-04-15",
                "retry_count": 0,
                "payload": {
                    "job_key": "live_collector:test",
                    "job_type": "live_collector",
                    "label": "test",
                    "session_id": "live:test:2026-04-15",
                    "session_date": "2026-04-15",
                    "scheduled_for": isoformat_utc(old_slot),
                    "slot_at": isoformat_utc(old_slot),
                },
                "arq_job_id": "live_collector:test:20260415T143000Z",
            }
        }
        self.created_runs: list[dict[str, object]] = []

    def list_job_definitions(self, **_: object) -> list[dict[str, object]]:
        return [dict(self.definition)]

    def list_job_runs(self, *, job_key: str, session_id: str, limit: int = 1, **_: object) -> list[dict[str, object]]:
        del limit
        rows = [
            dict(row)
            for row in self.runs.values()
            if row["job_key"] == job_key and row["session_id"] == session_id
        ]
        rows.sort(
            key=lambda row: (row["scheduled_for"], row["job_run_id"]),
            reverse=True,
        )
        return rows

    def get_job_run_for_slot(
        self,
        *,
        job_key: str,
        session_id: str,
        slot_at: datetime,
    ) -> dict[str, object] | None:
        for row in self.runs.values():
            if (
                row["job_key"] == job_key
                and row["session_id"] == session_id
                and row["slot_at"] == slot_at
            ):
                return dict(row)
        return None

    def create_job_run(self, **kwargs: object) -> tuple[dict[str, object], bool]:
        record = dict(kwargs)
        self.runs[str(record["job_run_id"])] = record
        self.created_runs.append(record)
        return dict(record), True

    def requeue_job_run(
        self,
        *,
        job_run_id: str,
        arq_job_id: str,
        payload: dict[str, object] | None = None,
    ) -> dict[str, object]:
        row = self.runs[job_run_id]
        row["arq_job_id"] = arq_job_id
        row["retry_count"] = int(row.get("retry_count", 0)) + 1
        row["status"] = "queued"
        if payload is not None:
            row["payload"] = dict(payload)
        return dict(row)

    def update_job_run_status(
        self,
        *,
        job_run_id: str,
        status: str,
        expected_arq_job_id: str | None = None,
        finished_at: datetime | None = None,
        error_text: str | None = None,
        **_: object,
    ) -> dict[str, object] | None:
        row = self.runs[job_run_id]
        if expected_arq_job_id is not None and row["arq_job_id"] != expected_arq_job_id:
            return None
        row["status"] = status
        if finished_at is not None:
            row["finished_at"] = finished_at
        if error_text is not None:
            row["error_text"] = error_text
        return dict(row)


class _RecoveryStore:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], dict[str, object]] = {}

    def ensure_live_session_slots(self, **_: object) -> None:
        return None

    def get_live_session_slot(
        self,
        *,
        session_id: str,
        slot_at: datetime,
    ) -> dict[str, object] | None:
        return self.rows.get((session_id, isoformat_utc(slot_at)))

    def upsert_live_session_slot(self, **kwargs: object) -> dict[str, object]:
        row = dict(kwargs)
        self.rows[(str(row["session_id"]), str(row["slot_at"]))] = row
        return row


class SchedulerCoalescingTests(unittest.TestCase):
    def test_scheduler_coalesces_stale_queued_slot_to_latest_slot(self) -> None:
        old_slot = datetime(2026, 4, 15, 14, 30, tzinfo=UTC)
        current_slot = datetime(2026, 4, 15, 14, 31, tzinfo=UTC)
        job_store = _JobStore(old_slot=old_slot)
        recovery_store = _RecoveryStore()

        async def run_test() -> dict[str, object]:
            with patch(
                "spreads.jobs.scheduler.resolve_live_tick_plan",
                return_value={
                    "label": "test",
                    "session_id": "live:test:2026-04-15",
                    "session_date": "2026-04-15",
                    "interval_seconds": 60,
                    "slots": [old_slot, current_slot],
                    "current_slot": current_slot,
                    "payload": {"interval_seconds": 60},
                },
            ), patch(
                "spreads.jobs.scheduler._live_run_active",
                return_value=True,
            ), patch(
                "spreads.jobs.scheduler._enqueue_job_run",
                return_value=True,
            ), patch(
                "spreads.jobs.scheduler._enqueue_collector_recovery_if_needed",
                return_value=None,
            ), patch(
                "spreads.jobs.scheduler._publish_job_run_update",
                return_value=None,
            ):
                return await _reconcile_live_collector_jobs(
                    job_store,
                    recovery_store,
                    object(),
                    now=datetime(2026, 4, 15, 14, 31, 5, tzinfo=UTC),
                )

        result = asyncio.run(run_test())

        old_run = job_store.runs["live_collector:test:20260415T143000Z"]
        self.assertEqual(old_run["status"], "skipped")
        self.assertNotEqual(
            old_run["arq_job_id"],
            "live_collector:test:20260415T143000Z",
        )
        current_run = next(
            row
            for row in job_store.created_runs
            if row["slot_at"] == current_slot
        )
        self.assertEqual(current_run["status"], "queued")
        old_slot_record = recovery_store.rows[
            ("live:test:2026-04-15", "2026-04-15T14:30:00Z")
        ]
        self.assertEqual(old_slot_record["status"], LIVE_SLOT_STATUS_MISSED)
        self.assertIn(str(current_run["job_run_id"]), result["enqueued"])


if __name__ == "__main__":
    unittest.main()
