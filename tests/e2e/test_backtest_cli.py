from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from typer.testing import CliRunner

from core.cli.main import app
from core.domain.backtest_models import (
    BacktestAggregate,
    BacktestRun,
    BacktestSessionSummary,
    BacktestTarget,
)


def _build_run(*, run_id: str, automation_id: str, fidelity: str) -> BacktestRun:
    now = datetime(2026, 4, 17, 15, 0, tzinfo=UTC)
    return BacktestRun(
        id=run_id,
        kind="run",
        status="completed",
        engine_name="backtest",
        engine_version="v1",
        created_at=now,
        started_at=now,
        completed_at=now,
        target=BacktestTarget(
            bot_id="bot-1",
            automation_id=automation_id,
            strategy_id="strategy-1",
        ),
        aggregate=BacktestAggregate(
            session_count=1,
            fidelity=fidelity,
            fidelity_counts={fidelity: 1},
            modeled_selected_count=1,
            modeled_fill_count=1,
            modeled_position_count=1,
            modeled_closed_count=1,
            modeled_realized_pnl=25.0,
            realized_pnl=10.0,
        ),
        sessions=[
            BacktestSessionSummary(
                session_date="2026-04-16",
                automation_run_id="auto-run-1",
                fidelity=fidelity,
                modeled_selected_opportunity_id="opp-1",
                modeled_fill_state="filled",
                modeled_exit_state="closed",
                modeled_realized_pnl=25.0,
                realized_pnl=10.0,
            )
        ],
    )


class BacktestCliTests(unittest.TestCase):
    def test_backtest_run_writes_renamed_artifacts_and_json_output(self) -> None:
        runner = CliRunner()
        fake_run = _build_run(
            run_id="run:cli-test",
            automation_id="auto-1",
            fidelity="medium",
        )
        with TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir).resolve()
            output_root = repo_root / "outputs" / "backtests"
            with (
                patch("core.cli.backtest.REPO_ROOT", repo_root),
                patch("core.cli.backtest.BACKTEST_OUTPUT_ROOT", output_root),
                patch(
                    "core.cli.backtest.build_backtest_run",
                    return_value=fake_run,
                ) as build_backtest_run_mock,
                patch(
                    "core.cli.backtest.render_json_payload",
                    side_effect=lambda _console, payload: print(json.dumps(payload)),
                ),
            ):
                result = runner.invoke(
                    app,
                    [
                        "backtest",
                        "run",
                        "--bot-id",
                        "bot-1",
                        "--automation-id",
                        "auto-1",
                        "--json",
                        "--no-color",
                    ],
                )

            self.assertEqual(result.exit_code, 0, msg=result.stdout)
            build_backtest_run_mock.assert_called_once_with(
                db_target="",
                bot_id="bot-1",
                automation_id="auto-1",
                config_root=None,
                start_date=None,
                end_date=None,
                limit=30,
            )
            payload = json.loads(result.stdout)
            self.assertEqual(payload["kind"], "run")
            self.assertEqual(payload["engine_name"], "backtest")
            self.assertTrue(payload["output_root"].startswith("outputs/backtests/run/"))
            self.assertEqual(payload["aggregate"]["fidelity"], "medium")
            summary_path = repo_root / payload["artifact_paths"]["summary_json"]
            sessions_path = repo_root / payload["artifact_paths"]["sessions_csv"]
            self.assertTrue(summary_path.exists())
            self.assertTrue(sessions_path.exists())

    def test_backtest_run_accepts_alternate_config_root(self) -> None:
        runner = CliRunner()
        fake_run = _build_run(
            run_id="run:cli-config-root",
            automation_id="auto-1",
            fidelity="high",
        )
        with TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir).resolve()
            output_root = repo_root / "outputs" / "backtests"
            config_root = repo_root / "alt-config"
            config_root.mkdir(parents=True, exist_ok=True)
            with (
                patch("core.cli.backtest.REPO_ROOT", repo_root),
                patch("core.cli.backtest.BACKTEST_OUTPUT_ROOT", output_root),
                patch(
                    "core.cli.backtest.build_backtest_run",
                    return_value=fake_run,
                ) as build_backtest_run_mock,
            ):
                result = runner.invoke(
                    app,
                    [
                        "backtest",
                        "run",
                        "--bot-id",
                        "bot-1",
                        "--automation-id",
                        "auto-1",
                        "--config-root",
                        str(config_root),
                        "--no-color",
                    ],
                )

            self.assertEqual(result.exit_code, 0, msg=result.stdout)
            build_backtest_run_mock.assert_called_once_with(
                db_target="",
                bot_id="bot-1",
                automation_id="auto-1",
                config_root=str(config_root),
                start_date=None,
                end_date=None,
                limit=30,
            )

    def test_backtest_compare_reads_run_payloads(self) -> None:
        runner = CliRunner()
        left = _build_run(run_id="run:left", automation_id="left", fidelity="high")
        right = _build_run(run_id="run:right", automation_id="right", fidelity="reduced")
        with TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir).resolve()
            output_root = repo_root / "outputs" / "backtests"
            left_json = repo_root / "left.json"
            right_json = repo_root / "right.json"
            left_json.write_text(json.dumps(left.to_payload()))
            right_json.write_text(json.dumps(right.to_payload()))
            with (
                patch("core.cli.backtest.REPO_ROOT", repo_root),
                patch("core.cli.backtest.BACKTEST_OUTPUT_ROOT", output_root),
                patch(
                    "core.cli.backtest.render_json_payload",
                    side_effect=lambda _console, payload: print(json.dumps(payload)),
                ),
            ):
                result = runner.invoke(
                    app,
                    [
                        "backtest",
                        "compare",
                        "--left-json",
                        str(left_json),
                        "--right-json",
                        str(right_json),
                        "--json",
                        "--no-color",
                    ],
                )

            self.assertEqual(result.exit_code, 0, msg=result.stdout)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["kind"], "compare")
            self.assertEqual(payload["engine_name"], "backtest")
            self.assertEqual(payload["metrics"]["fidelity"]["left"], "high")
            self.assertEqual(payload["metrics"]["fidelity"]["right"], "reduced")

    def test_backtest_compare_reads_replay_range_payloads(self) -> None:
        runner = CliRunner()
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
        with TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir).resolve()
            output_root = repo_root / "outputs" / "backtests"
            left_json = repo_root / "left-replay-range.json"
            right_json = repo_root / "right-replay-range.json"
            left_json.write_text(json.dumps(left_payload))
            right_json.write_text(json.dumps(right_payload))
            with (
                patch("core.cli.backtest.REPO_ROOT", repo_root),
                patch("core.cli.backtest.BACKTEST_OUTPUT_ROOT", output_root),
                patch(
                    "core.cli.backtest.render_json_payload",
                    side_effect=lambda _console, payload: print(json.dumps(payload)),
                ),
            ):
                result = runner.invoke(
                    app,
                    [
                        "backtest",
                        "compare",
                        "--left-json",
                        str(left_json),
                        "--right-json",
                        str(right_json),
                        "--json",
                        "--no-color",
                    ],
                )

            self.assertEqual(result.exit_code, 0, msg=result.stdout)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["kind"], "compare")
            self.assertEqual(payload["params"]["comparison_type"], "replay_range")
            self.assertEqual(payload["params"]["left_config_root"], "/tmp/before")
            self.assertEqual(payload["params"]["right_config_root"], "/tmp/after")
            self.assertEqual(payload["metrics"]["cycle_count"]["delta"], -2.0)
            self.assertEqual(
                payload["metrics"]["cycle_status_counts.selected"]["delta"],
                -1.0,
            )

    def test_backtest_replay_accepts_alternate_config_root(self) -> None:
        runner = CliRunner()
        fake_payload = {
            "status": "completed",
            "run": {"run_id": "run:cli-replay"},
            "summary": {},
            "stored_top": [],
            "replayed_top": [],
            "stored_only": [],
            "replayed_only": [],
            "rank_changes": [],
            "field_drifts": [],
        }
        with TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir).resolve()
            output_root = repo_root / "outputs" / "backtests"
            config_root = repo_root / "alt-config"
            config_root.mkdir(parents=True, exist_ok=True)
            with (
                patch("core.cli.backtest.REPO_ROOT", repo_root),
                patch("core.cli.backtest.BACKTEST_OUTPUT_ROOT", output_root),
                patch(
                    "core.cli.backtest.build_replay_payload",
                    return_value=fake_payload,
                ) as build_replay_payload_mock,
                patch(
                    "core.cli.backtest.render_json_payload",
                    side_effect=lambda _console, payload: print(json.dumps(payload)),
                ),
            ):
                result = runner.invoke(
                    app,
                    [
                        "backtest",
                        "replay",
                        "--run-id",
                        "run:cli-replay",
                        "--config-root",
                        str(config_root),
                        "--json",
                        "--no-color",
                    ],
                )

            self.assertEqual(result.exit_code, 0, msg=result.stdout)
            build_replay_payload_mock.assert_called_once_with(
                db_target="",
                run_id="run:cli-replay",
                symbol=None,
                strategy=None,
                config_root=str(config_root),
                calendar_confidence_policy=None,
                latest=False,
            )
            payload = json.loads(result.stdout)
            self.assertEqual(payload["run"]["run_id"], "run:cli-replay")
            replay_summaries = list(
                output_root.glob("replay/runs/config-*/run:cli-replay/summary.json")
            )
            self.assertEqual(len(replay_summaries), 1)
            self.assertTrue(replay_summaries[0].exists())

    def test_backtest_replay_range_accepts_alternate_config_root(self) -> None:
        runner = CliRunner()
        fake_payload = {
            "status": "completed",
            "source": "stored",
            "target": {},
            "summary": {},
            "cycles": [],
        }
        with TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir).resolve()
            output_root = repo_root / "outputs" / "backtests"
            config_root = repo_root / "alt-config"
            config_root.mkdir(parents=True, exist_ok=True)
            with (
                patch("core.cli.backtest.REPO_ROOT", repo_root),
                patch("core.cli.backtest.BACKTEST_OUTPUT_ROOT", output_root),
                patch(
                    "core.cli.backtest.build_replay_range_payload",
                    return_value=fake_payload,
                ) as build_replay_range_payload_mock,
                patch(
                    "core.cli.backtest.render_json_payload",
                    side_effect=lambda _console, payload: print(json.dumps(payload)),
                ),
            ):
                result = runner.invoke(
                    app,
                    [
                        "backtest",
                        "replay-range",
                        "--bot-id",
                        "bot-1",
                        "--automation-id",
                        "auto-1",
                        "--start-date",
                        "2026-04-20",
                        "--end-date",
                        "2026-04-23",
                        "--config-root",
                        str(config_root),
                        "--json",
                        "--no-color",
                    ],
                )

            self.assertEqual(result.exit_code, 0, msg=result.stdout)
            build_replay_range_payload_mock.assert_called_once_with(
                db_target="",
                bot_id="bot-1",
                automation_id="auto-1",
                start_date="2026-04-20",
                end_date="2026-04-23",
                limit=500,
                source="stored",
                config_root=str(config_root),
                sample_mode="intraday",
                calendar_confidence_policy=None,
            )
            payload = json.loads(result.stdout)
            self.assertEqual(payload["source"], "stored")
            replay_range_summaries = list(
                output_root.glob(
                    "replay/ranges/config-*/bot-1/auto-1/2026-04-20_2026-04-23/summary.json"
                )
            )
            replay_range_cycles = list(
                output_root.glob(
                    "replay/ranges/config-*/bot-1/auto-1/2026-04-20_2026-04-23/cycles.csv"
                )
            )
            self.assertEqual(len(replay_range_summaries), 1)
            self.assertEqual(len(replay_range_cycles), 1)

    def test_backtest_replay_accepts_calendar_confidence_policy_override(self) -> None:
        runner = CliRunner()
        fake_payload = {
            "status": "completed",
            "run": {"run_id": "run:cli-replay"},
            "summary": {},
            "stored_top": [],
            "replayed_top": [],
            "stored_only": [],
            "replayed_only": [],
            "rank_changes": [],
            "field_drifts": [],
        }
        with TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir).resolve()
            output_root = repo_root / "outputs" / "backtests"
            with (
                patch("core.cli.backtest.REPO_ROOT", repo_root),
                patch("core.cli.backtest.BACKTEST_OUTPUT_ROOT", output_root),
                patch(
                    "core.cli.backtest.build_replay_payload",
                    return_value=fake_payload,
                ) as build_replay_payload_mock,
                patch(
                    "core.cli.backtest.render_json_payload",
                    side_effect=lambda _console, payload: print(json.dumps(payload)),
                ),
            ):
                result = runner.invoke(
                    app,
                    [
                        "backtest",
                        "replay",
                        "--run-id",
                        "run:cli-replay",
                        "--calendar-confidence-policy",
                        "consensus",
                        "--json",
                        "--no-color",
                    ],
                )

            self.assertEqual(result.exit_code, 0, msg=result.stdout)
            build_replay_payload_mock.assert_called_once_with(
                db_target="",
                run_id="run:cli-replay",
                symbol=None,
                strategy=None,
                config_root=None,
                calendar_confidence_policy="consensus",
                latest=False,
            )
            replay_summaries = list(
                output_root.glob(
                    "replay/runs/calendar-confidence-consensus/run:cli-replay/summary.json"
                )
            )
            self.assertEqual(len(replay_summaries), 1)

    def test_backtest_replay_range_accepts_calendar_confidence_policy_override(self) -> None:
        runner = CliRunner()
        fake_payload = {
            "status": "completed",
            "source": "alpaca",
            "target": {},
            "summary": {},
            "cycles": [],
        }
        with TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir).resolve()
            output_root = repo_root / "outputs" / "backtests"
            with (
                patch("core.cli.backtest.REPO_ROOT", repo_root),
                patch("core.cli.backtest.BACKTEST_OUTPUT_ROOT", output_root),
                patch(
                    "core.cli.backtest.build_replay_range_payload",
                    return_value=fake_payload,
                ) as build_replay_range_payload_mock,
                patch(
                    "core.cli.backtest.render_json_payload",
                    side_effect=lambda _console, payload: print(json.dumps(payload)),
                ),
            ):
                result = runner.invoke(
                    app,
                    [
                        "backtest",
                        "replay-range",
                        "--bot-id",
                        "bot-1",
                        "--automation-id",
                        "auto-1",
                        "--start-date",
                        "2026-04-20",
                        "--end-date",
                        "2026-04-23",
                        "--source",
                        "alpaca",
                        "--sample-mode",
                        "eod",
                        "--calendar-confidence-policy",
                        "consensus",
                        "--json",
                        "--no-color",
                    ],
                )

            self.assertEqual(result.exit_code, 0, msg=result.stdout)
            build_replay_range_payload_mock.assert_called_once_with(
                db_target="",
                bot_id="bot-1",
                automation_id="auto-1",
                start_date="2026-04-20",
                end_date="2026-04-23",
                limit=500,
                source="alpaca",
                config_root=None,
                sample_mode="eod",
                calendar_confidence_policy="consensus",
            )
            replay_range_summaries = list(
                output_root.glob(
                    "replay/ranges/alpaca/eod/calendar-confidence-consensus/bot-1/auto-1/2026-04-20_2026-04-23/summary.json"
                )
            )
            self.assertEqual(len(replay_range_summaries), 1)

    def test_replay_command_no_longer_exists(self) -> None:
        runner = CliRunner()
        result = runner.invoke(app, ["replay"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("No such command", result.stderr)


if __name__ == "__main__":
    unittest.main()
