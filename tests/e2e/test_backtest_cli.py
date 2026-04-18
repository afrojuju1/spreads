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
                patch("core.cli.backtest.build_backtest_run", return_value=fake_run),
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
            payload = json.loads(result.stdout)
            self.assertEqual(payload["kind"], "run")
            self.assertEqual(payload["engine_name"], "backtest")
            self.assertTrue(payload["output_root"].startswith("outputs/backtests/run/"))
            self.assertEqual(payload["aggregate"]["fidelity"], "medium")
            summary_path = repo_root / payload["artifact_paths"]["summary_json"]
            sessions_path = repo_root / payload["artifact_paths"]["sessions_csv"]
            self.assertTrue(summary_path.exists())
            self.assertTrue(sessions_path.exists())

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

    def test_replay_command_no_longer_exists(self) -> None:
        runner = CliRunner()
        result = runner.invoke(app, ["replay"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("No such command", result.stderr)


if __name__ == "__main__":
    unittest.main()
