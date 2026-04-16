from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import typer

from core.cli.ops_render import build_console, render_json_payload
from core.services.bootstrap_backtest import (
    build_bootstrap_backtest,
    compare_bootstrap_backtests,
)


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


def _flatten_sessions(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    target = dict(payload.get("target") or {})
    for row in list(payload.get("sessions") or []):
        session = dict(row)
        session["bot_id"] = target.get("bot_id")
        session["automation_id"] = target.get("automation_id")
        session["strategy_id"] = target.get("strategy_id")
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


def _render_bootstrap_text(payload: dict[str, Any]) -> str:
    target = dict(payload.get("target") or {})
    aggregate = dict(payload.get("aggregate") or {})
    sessions = list(payload.get("sessions") or [])
    lines = [
        f"Bootstrap backtest: {target.get('bot_id')} / {target.get('automation_id')} / {target.get('strategy_id')}",
        f"Sessions {aggregate.get('session_count')} | modeled selections {aggregate.get('modeled_selected_count')} | actual selections {aggregate.get('actual_selected_count')} | match rate {aggregate.get('selection_match_rate')}",
        f"Positions {aggregate.get('position_count')} | realized pnl {aggregate.get('realized_pnl')} | unrealized pnl {aggregate.get('unrealized_pnl')}",
        "",
        "Sessions:",
    ]
    for row in sessions[:20]:
        lines.append(
            "- "
            f"{row.get('session_date')} | opportunities {row.get('opportunity_count')} | modeled {row.get('modeled_selected_opportunity_id') or 'n/a'} | actual {row.get('actual_selected_opportunity_id') or 'n/a'} | modeled_fill {row.get('modeled_fill_state') or 'n/a'} | match {row.get('selection_match')} | positions {row.get('position_count')} | realized {row.get('realized_pnl')}"
        )
    return "\n".join(lines)


def _render_compare_text(payload: dict[str, Any]) -> str:
    lines = [
        f"Compare: {payload.get('left', {}).get('automation_id')} vs {payload.get('right', {}).get('automation_id')}",
        "",
        "Metrics:",
    ]
    for key, values in dict(payload.get("metrics") or {}).items():
        lines.append(
            f"- {key}: left {values.get('left')} | right {values.get('right')} | delta {values.get('delta')}"
        )
    return "\n".join(lines)


backtest_app = typer.Typer(
    add_completion=False,
    help="Run bootstrap backtest workflows over config-owned automation runtime data.",
)


@backtest_app.command(
    "bootstrap", help="Backtest over automation runs and scoped opportunities."
)
def bootstrap_backtest_command(
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
    payload = build_bootstrap_backtest(
        db_target=db or "",
        bot_id=bot_id,
        automation_id=automation_id,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    if export_json:
        _write_json_export(export_json, payload)
    if export_csv:
        _write_csv_export(export_csv, _flatten_sessions(payload))
    if json_output:
        render_json_payload(build_console(no_color=no_color), payload)
        return
    console = build_console(no_color=no_color)
    console.print(_render_bootstrap_text(payload))


@backtest_app.command(
    "compare", help="Compare two exported bootstrap backtest payloads."
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
    payload = compare_bootstrap_backtests(
        left_payload=_read_json_payload(left_json),
        right_payload=_read_json_payload(right_json),
    )
    if json_output:
        render_json_payload(build_console(no_color=no_color), payload)
        return
    console = build_console(no_color=no_color)
    console.print(_render_compare_text(payload))


def main() -> None:
    backtest_app()


__all__ = ["backtest_app", "main"]
