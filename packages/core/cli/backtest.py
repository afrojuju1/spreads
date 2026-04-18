from __future__ import annotations

import csv
from dataclasses import replace
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import typer

from core.backtest import build_backtest_run, compare_backtest_runs
from core.cli.ops_render import build_console, render_json_payload
from core.domain.backtest_models import (
    BacktestArtifact,
    BacktestRun,
    BacktestTarget,
    new_backtest_artifact_id,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
BACKTEST_OUTPUT_ROOT = REPO_ROOT / "outputs" / "backtests"


def _write_json_export(path: str, payload: dict[str, Any]) -> None:
    output_path = Path(path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, default=str) + "\n")


def _write_csv_export(path: str, rows: list[dict[str, Any]]) -> None:
    output_path = Path(path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("")
        return
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with output_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _flatten_sessions(run: BacktestRun) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    target = run.target
    for session_model in run.sessions:
        session = session_model.to_payload()
        session["bot_id"] = None if target is None else target.bot_id
        session["automation_id"] = None if target is None else target.automation_id
        session["strategy_id"] = None if target is None else target.strategy_id
        session["modeled_position_underlying"] = (
            (session.get("modeled_position") or {}).get("underlying_symbol")
            if isinstance(session.get("modeled_position"), dict)
            else None
        )
        session["modeled_position_entry_credit"] = (
            (session.get("modeled_position") or {}).get("entry_credit")
            if isinstance(session.get("modeled_position"), dict)
            else None
        )
        session.pop("top_opportunities", None)
        session.pop("modeled_position", None)
        rows.append(session)
    return rows


def _read_json_payload(path: str) -> dict[str, Any]:
    return dict(json.loads(Path(path).expanduser().read_text()))


def _read_backtest_run(path: str) -> BacktestRun:
    return BacktestRun.from_payload(_read_json_payload(path))


def _run_output_dir(*, bot_id: str, automation_id: str) -> Path:
    return BACKTEST_OUTPUT_ROOT / "run" / bot_id / automation_id


def _compare_output_path() -> Path:
    return BACKTEST_OUTPUT_ROOT / "compare" / "latest.json"


def _relative_repo_path(path: Path) -> str:
    resolved = path.expanduser().resolve()
    try:
        return str(resolved.relative_to(REPO_ROOT))
    except ValueError:
        return str(resolved)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact_from_path(
    *,
    run_id: str,
    artifact_type: str,
    artifact_role: str,
    file_format: str,
    path: Path,
    created_at: datetime,
    compute_metadata: bool = True,
) -> BacktestArtifact:
    resolved = path.expanduser().resolve()
    can_stat = compute_metadata and resolved.exists()
    return BacktestArtifact(
        id=new_backtest_artifact_id(),
        run_id=run_id,
        artifact_type=artifact_type,
        artifact_role=artifact_role,
        file_format=file_format,
        path=_relative_repo_path(resolved),
        created_at=created_at,
        size_bytes=resolved.stat().st_size if can_stat else None,
        sha256=_file_sha256(resolved) if can_stat else None,
    )


def _write_run_artifacts(
    *,
    output_dir: Path,
    run: BacktestRun,
    export_json: str | None,
    export_csv: str | None,
) -> BacktestRun:
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = run.id
    run_dir = output_dir / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "summary.json"
    sessions_path = output_dir / "sessions.csv"
    run_summary_path = run_dir / "summary.json"
    run_sessions_path = run_dir / "sessions.csv"
    session_rows = _flatten_sessions(run)
    _write_csv_export(str(sessions_path), session_rows)
    _write_csv_export(str(run_sessions_path), session_rows)
    artifacts = [
        _artifact_from_path(
            run_id=run_id,
            artifact_type="summary_json",
            artifact_role="latest",
            file_format="json",
            path=summary_path,
            created_at=run.completed_at or run.started_at,
            compute_metadata=False,
        ),
        _artifact_from_path(
            run_id=run_id,
            artifact_type="sessions_csv",
            artifact_role="latest",
            file_format="csv",
            path=sessions_path,
            created_at=run.completed_at or run.started_at,
        ),
        _artifact_from_path(
            run_id=run_id,
            artifact_type="summary_json",
            artifact_role="run",
            file_format="json",
            path=run_summary_path,
            created_at=run.completed_at or run.started_at,
            compute_metadata=False,
        ),
        _artifact_from_path(
            run_id=run_id,
            artifact_type="sessions_csv",
            artifact_role="run",
            file_format="csv",
            path=run_sessions_path,
            created_at=run.completed_at or run.started_at,
        ),
    ]
    run_with_artifacts = replace(
        run,
        output_root=_relative_repo_path(output_dir),
        artifacts=artifacts,
    )
    payload = run_with_artifacts.to_payload()
    _write_json_export(str(summary_path), payload)
    _write_json_export(str(run_summary_path), payload)
    if export_json:
        _write_json_export(export_json, payload)
    if export_csv:
        _write_csv_export(export_csv, session_rows)
    return run_with_artifacts


def _write_compare_artifacts(*, run: BacktestRun) -> BacktestRun:
    comparison_output_path = _compare_output_path()
    comparison_run_path = BACKTEST_OUTPUT_ROOT / "compare" / "runs" / f"{run.id}.json"
    comparison_run_path.parent.mkdir(parents=True, exist_ok=True)
    artifacts = [
        _artifact_from_path(
            run_id=run.id,
            artifact_type="comparison_json",
            artifact_role="latest",
            file_format="json",
            path=comparison_output_path,
            created_at=run.completed_at or run.started_at,
            compute_metadata=False,
        ),
        _artifact_from_path(
            run_id=run.id,
            artifact_type="comparison_json",
            artifact_role="run",
            file_format="json",
            path=comparison_run_path,
            created_at=run.completed_at or run.started_at,
            compute_metadata=False,
        ),
    ]
    run_with_artifacts = replace(
        run,
        output_root=_relative_repo_path(BACKTEST_OUTPUT_ROOT / "compare"),
        artifacts=artifacts,
    )
    payload = run_with_artifacts.to_payload()
    _write_json_export(str(comparison_output_path), payload)
    _write_json_export(str(comparison_run_path), payload)
    return run_with_artifacts


def _render_run_text(run: BacktestRun) -> str:
    target = BacktestTarget() if run.target is None else run.target
    aggregate = run.aggregate.to_payload() if run.aggregate is not None else {}
    artifacts = run.artifact_paths
    sessions = [session.to_payload() for session in run.sessions]
    lines = [
        f"Backtest run: {target.bot_id} / {target.automation_id} / {target.strategy_id}",
        f"Sessions {aggregate.get('session_count')} | fidelity {aggregate.get('fidelity')} | modeled selections {aggregate.get('modeled_selected_count')} | actual selections {aggregate.get('actual_selected_count')} | match rate {aggregate.get('selection_match_rate')}",
        f"Modeled fills {aggregate.get('modeled_fill_count')} | modeled closed {aggregate.get('modeled_closed_count')} | modeled realized pnl {aggregate.get('modeled_realized_pnl')} | modeled unrealized pnl {aggregate.get('modeled_unrealized_pnl')}",
        f"Actual positions {aggregate.get('position_count')} | realized pnl {aggregate.get('realized_pnl')} | unrealized pnl {aggregate.get('unrealized_pnl')}",
        *(
            []
            if not artifacts
            else [
                f"Artifacts: {artifacts.get('output_root')}",
                f"- summary {artifacts.get('summary_json')}",
                f"- sessions {artifacts.get('sessions_csv')}",
                f"- run {artifacts.get('run_dir')}",
            ]
        ),
        "",
        "Sessions:",
    ]
    for row in sessions[:20]:
        lines.append(
            "- "
            f"{row.get('session_date')} | fidelity {row.get('fidelity') or 'unsupported'} | opportunities {row.get('opportunity_count')} | modeled {row.get('modeled_selected_opportunity_id') or 'n/a'} | actual {row.get('actual_selected_opportunity_id') or 'n/a'} | modeled_fill {row.get('modeled_fill_state') or 'n/a'} | match {row.get('selection_match')} | positions {row.get('position_count')} | realized {row.get('realized_pnl')}"
        )
    return "\n".join(lines)


def _render_compare_text(run: BacktestRun) -> str:
    artifacts = run.artifact_paths
    left = BacktestTarget() if run.left_target is None else run.left_target
    right = BacktestTarget() if run.right_target is None else run.right_target
    lines = [
        f"Compare: {left.automation_id} vs {right.automation_id}",
        *(
            []
            if not artifacts.get("comparison_json")
            else [
                f"Artifact: {artifacts.get('comparison_json')}",
                f"Run: {artifacts.get('comparison_run_json')}",
            ]
        ),
        "",
        "Metrics:",
    ]
    for key, values in dict(run.comparison_metrics).items():
        lines.append(
            f"- {key}: left {values.get('left')} | right {values.get('right')} | delta {values.get('delta')}"
        )
    return "\n".join(lines)


backtest_app = typer.Typer(
    add_completion=False,
    help="Run historical backtests over config-owned automation runtime data.",
)


@backtest_app.command(
    "run", help="Backtest over automation runs and scoped opportunities."
)
def run_backtest_command(
    bot_id: str = typer.Option(..., "--bot-id", help="Target bot id."),
    automation_id: str = typer.Option(
        ..., "--automation-id", help="Target automation id."
    ),
    start_date: str | None = typer.Option(
        None, "--start-date", help="Start date YYYY-MM-DD."
    ),
    end_date: str | None = typer.Option(
        None, "--end-date", help="End date YYYY-MM-DD."
    ),
    limit: int = typer.Option(30, "--limit", help="Maximum sessions to include."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    export_json: str | None = typer.Option(
        None, "--export-json", help="Write payload to JSON file."
    ),
    export_csv: str | None = typer.Option(
        None, "--export-csv", help="Write session rows to CSV file."
    ),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    run = build_backtest_run(
        db_target=db or "",
        bot_id=bot_id,
        automation_id=automation_id,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    run = _write_run_artifacts(
        output_dir=_run_output_dir(bot_id=bot_id, automation_id=automation_id),
        run=run,
        export_json=export_json,
        export_csv=export_csv,
    )
    if json_output:
        render_json_payload(build_console(no_color=no_color), run.to_payload())
        return
    console = build_console(no_color=no_color)
    console.print(_render_run_text(run))


@backtest_app.command(
    "compare", help="Compare two exported backtest run payloads."
)
def compare_backtest_command(
    left_json: str = typer.Option(
        ..., "--left-json", help="Left backtest JSON export."
    ),
    right_json: str = typer.Option(
        ..., "--right-json", help="Right backtest JSON export."
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    run = compare_backtest_runs(
        left_run=_read_backtest_run(left_json),
        right_run=_read_backtest_run(right_json),
    )
    run = _write_compare_artifacts(run=run)
    if json_output:
        render_json_payload(build_console(no_color=no_color), run.to_payload())
        return
    console = build_console(no_color=no_color)
    console.print(_render_compare_text(run))


def main() -> None:
    backtest_app()


__all__ = ["backtest_app", "main"]
