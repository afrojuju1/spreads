from __future__ import annotations

import unittest

from core.backtest import compare_backtest_payloads


class BacktestComparePayloadTests(unittest.TestCase):
    def test_compare_replay_range_payloads_tracks_config_roots_and_nested_counts(self) -> None:
        left_payload = {
            "status": "completed",
            "source": "alpaca",
            "config_root": "/tmp/before",
            "target": {
                "bot_id": "bot-1",
                "automation_id": "auto-1",
                "start_date": "2026-04-10",
                "end_date": "2026-04-23",
                "cycle_limit": 500,
                "sample_mode": "eod",
                "fidelity": "reduced",
                "config_root": "/tmp/before",
            },
            "summary": {
                "cycle_count": 10,
                "candidate_count": 40,
                "cycle_status_counts": {"selected": 3, "blocked": 7},
            },
            "cycles": [],
        }
        right_payload = {
            "status": "completed",
            "source": "alpaca",
            "config_root": "/tmp/after",
            "target": {
                "bot_id": "bot-1",
                "automation_id": "auto-1",
                "start_date": "2026-04-10",
                "end_date": "2026-04-23",
                "cycle_limit": 500,
                "sample_mode": "eod",
                "fidelity": "reduced",
                "config_root": "/tmp/after",
            },
            "summary": {
                "cycle_count": 12,
                "candidate_count": 55,
                "cycle_status_counts": {"selected": 4, "blocked": 8},
            },
            "cycles": [],
        }

        comparison = compare_backtest_payloads(
            left_payload=left_payload,
            right_payload=right_payload,
        )

        self.assertEqual(comparison.kind, "compare")
        self.assertEqual(comparison.params["comparison_type"], "replay_range")
        self.assertEqual(comparison.params["left_config_root"], "/tmp/before")
        self.assertEqual(comparison.params["right_config_root"], "/tmp/after")
        self.assertEqual(comparison.comparison_metrics["cycle_count"]["delta"], -2.0)
        self.assertEqual(
            comparison.comparison_metrics["candidate_count"]["delta"],
            -15.0,
        )
        self.assertEqual(
            comparison.comparison_metrics["cycle_status_counts.selected"]["delta"],
            -1.0,
        )
        self.assertEqual(comparison.comparison_metrics["source"]["left"], "alpaca")
        self.assertEqual(comparison.left_target.automation_id, "auto-1")

    def test_compare_rejects_mixed_export_types(self) -> None:
        run_payload = {
            "id": "run:left",
            "kind": "run",
            "status": "completed",
            "engine_name": "backtest",
            "engine_version": "v1",
            "created_at": "2026-04-23T12:00:00+00:00",
            "started_at": "2026-04-23T12:00:00+00:00",
            "completed_at": "2026-04-23T12:00:00+00:00",
            "target": {
                "bot_id": "bot-1",
                "automation_id": "auto-1",
                "strategy_id": "strategy-1",
            },
            "aggregate": {
                "session_count": 1,
                "fidelity": "high",
                "realized_pnl": 10.0,
            },
            "sessions": [],
        }
        replay_range_payload = {
            "status": "completed",
            "source": "stored",
            "target": {
                "bot_id": "bot-1",
                "automation_id": "auto-1",
                "start_date": "2026-04-10",
                "end_date": "2026-04-23",
                "cycle_limit": 500,
            },
            "summary": {"cycle_count": 10},
            "cycles": [],
        }

        with self.assertRaisesRegex(
            ValueError,
            "Cannot compare run exports against replay_range exports",
        ):
            compare_backtest_payloads(
                left_payload=run_payload,
                right_payload=replay_range_payload,
            )


if __name__ == "__main__":
    unittest.main()
