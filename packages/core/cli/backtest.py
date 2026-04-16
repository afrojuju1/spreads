from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer

from core.cli.ops_render import build_console, render_json_payload
from core.services.bootstrap_backtest import build_bootstrap_backtest


def _write_json_export(path: str, payload: dict[str, Any]) -> None:
    output_path = Path(path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, default=str) + "\n")


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
            f"{row.get('session_date')} | opportunities {row.get('opportunity_count')} | modeled {row.get('modeled_selected_opportunity_id') or 'n/a'} | actual {row.get('actual_selected_opportunity_id') or 'n/a'} | match {row.get('selection_match')} | positions {row.get('position_count')} | realized {row.get('realized_pnl')}"
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
    if json_output:
        render_json_payload(build_console(no_color=no_color), payload)
        return
    console = build_console(no_color=no_color)
    console.print(_render_bootstrap_text(payload))


def main() -> None:
    backtest_app()


__all__ = ["backtest_app", "main"]
