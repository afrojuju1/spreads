from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from typer.testing import CliRunner

from core.cli.main import app


class RuntimeCliTests(unittest.TestCase):
    def test_pipelines_detail_renders_text_mode_with_no_color(self) -> None:
        runner = CliRunner()
        payload = {
            "pipeline_id": "pipeline:demo",
            "label": "explore_demo",
            "market_date": "2026-04-21",
            "status": "healthy",
            "updated_at": "2026-04-21T20:05:03Z",
            "tradeability_state": "ready",
            "risk_status": "ok",
            "reconciliation_status": "clear",
            "quote_capture": {"capture_status": "healthy"},
            "trade_capture": {"capture_status": "idle"},
            "current_cycle": {
                "cycle_id": "cycle-1",
                "generated_at": "2026-04-21T20:05:03Z",
                "job_run_id": "job-run-1",
                "strategy": "call_credit",
                "profile": "weekly",
                "universe_label": "liquid_index_etfs",
                "promotable_count": 0,
                "monitor_count": 0,
                "resolved_ranking_policy": {
                    "by_strategy_family": {
                        "call_credit_spread": {
                            "min_probability_of_profit": 0.60,
                            "min_expected_value_dollars": 10.0,
                            "min_slippage_adjusted_expected_value_dollars": 8.0,
                            "max_entry_slippage_dollars": 12.0,
                            "min_model_implied_volatility": 0.18,
                        }
                    }
                },
                "ranking_policy_gate_summary": {
                    "status_counts": {"passed": 1, "blocked": 2},
                    "blocker_counts": {
                        "expected_value_dollars_below_floor": 2,
                    },
                },
                "raw_candidate_summary": {
                    "candidate_count": 1,
                    "symbol_counts": {"QQQ": 1},
                    "strategy_counts": {"call_credit": 1},
                    "resolved_ranking_policy": {
                        "by_strategy_family": {
                            "call_credit_spread": {
                                "min_probability_of_profit": 0.60,
                            }
                        }
                    },
                    "ranking_policy_gate_summary": {
                        "status_counts": {"passed": 1, "blocked": 2},
                        "blocker_counts": {
                            "expected_value_dollars_below_floor": 2,
                        },
                    },
                    "top_candidates": [
                        {
                            "underlying_symbol": "QQQ",
                            "strategy": "call_credit",
                            "expiration_date": "2026-04-30",
                            "symbol_path": "QQQ260430C00663000 / QQQ260430C00667000",
                            "quality_score": 51.9,
                            "midpoint_credit": 0.78,
                            "return_on_risk": 0.24,
                            "setup_status": "neutral",
                            "ranking_policy_status": "passed",
                            "ranking_policy_blockers": [],
                            "probability_of_profit": 0.863,
                            "breakeven_touch_probability": 0.276,
                            "expected_value_dollars": 23.22,
                            "slippage_adjusted_expected_value_dollars": 14.22,
                            "entry_slippage_dollars": 9.0,
                            "model_implied_volatility": 0.204,
                        }
                    ],
                    "top_blocked_candidates": [
                        {
                            "underlying_symbol": "QQQ",
                            "strategy": "call_credit",
                            "expiration_date": "2026-04-30",
                            "symbol_path": "QQQ260430C00663000 / QQQ260430C00667000",
                            "quality_score": 48.5,
                            "midpoint_credit": 0.72,
                            "return_on_risk": 0.21,
                            "setup_status": "neutral",
                            "ranking_policy_status": "blocked",
                            "ranking_policy_blockers": [
                                "expected_value_dollars_below_floor"
                            ],
                            "probability_of_profit": 0.84,
                            "breakeven_touch_probability": 0.29,
                            "expected_value_dollars": 6.2,
                            "slippage_adjusted_expected_value_dollars": 3.1,
                            "entry_slippage_dollars": 4.0,
                            "model_implied_volatility": 0.20,
                        }
                    ],
                },
            },
        }

        with patch(
            "core.cli.runtime.get_discovery_session_detail",
            return_value=payload,
        ):
            result = runner.invoke(
                app,
                [
                    "pipelines",
                    "pipeline:demo",
                    "--date",
                    "2026-04-21",
                    "--no-color",
                ],
            )

        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        self.assertIn("Pipeline", result.stdout)
        self.assertIn("Current Cycle", result.stdout)
        self.assertIn("Raw Candidate Summary", result.stdout)
        self.assertIn("Blocked Exemplars", result.stdout)

    def test_audit_json_output_is_parseable(self) -> None:
        runner = CliRunner()
        payload = {
            "status": "degraded",
            "generated_at": "2026-04-21T20:03:53Z",
            "summary": {
                "view": "discovery_audit",
                "pipeline_id": "pipeline:demo",
            },
            "attention": [
                {
                    "severity": "medium",
                    "code": "audit_risk_decisions_blocked",
                    "message": (
                        "5 risk decision(s) were blocked by policy and this "
                        "message is intentionally long enough to catch any line wrapping."
                    ),
                }
            ],
            "details": {
                "view": "discovery_audit",
                "current_cycle": {"generated_at": "2026-04-21T20:00:51Z"},
            },
        }

        with patch("core.cli.ops.build_audit_view", return_value=payload):
            result = runner.invoke(
                app,
                [
                    "audit",
                    "pipeline:demo",
                    "--date",
                    "2026-04-21",
                    "--json",
                    "--no-color",
                ],
            )

        self.assertEqual(result.exit_code, 1, msg=result.stdout)
        parsed = json.loads(result.stdout)
        self.assertEqual(parsed["status"], "degraded")
        self.assertEqual(
            parsed["attention"][0]["code"],
            "audit_risk_decisions_blocked",
        )


if __name__ == "__main__":
    unittest.main()
