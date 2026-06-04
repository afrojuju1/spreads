from __future__ import annotations

import json
from typing import Any

import typer
from rich.table import Table

from core.cli.ops_render import (
    build_console,
    render_json_payload,
)
from core.services.execution.runtimes import resolve_execution_runtime_capabilities
from core.services.positions import get_position_detail, list_positions


def _print_payload(payload: dict[str, Any], *, json_output: bool) -> None:
    if json_output:
        typer.echo(json.dumps(payload, indent=2, default=str))
        return
    typer.echo(json.dumps(payload, indent=2, default=str))


def _render_execution_runtimes(payload: dict[str, Any], *, no_color: bool) -> None:
    console = build_console(no_color=no_color)
    table = Table(title="Execution Runtimes", header_style="bold")
    table.add_column("Runtime")
    table.add_column("Status")
    table.add_column("Entries", justify="right")
    table.add_column("Families")
    table.add_column("Capabilities")
    for row in payload.get("runtimes") or []:
        if not isinstance(row, dict):
            continue
        families = row.get("strategy_families")
        family_text = "-"
        if isinstance(families, dict) and families:
            family_text = ", ".join(f"{name} {count}" for name, count in sorted(families.items()))
        capabilities = [
            str(item.get("name")) for item in row.get("capabilities") or [] if isinstance(item, dict) and item.get("status") != "unsupported"
        ]
        table.add_row(
            str(row.get("runtime") or "-"),
            str(row.get("status") or "-"),
            str(row.get("entry_strategy_count") or 0),
            family_text,
            ", ".join(capabilities) or "-",
        )
    console.print(table)


def execution_runtimes_command(
    environment: str | None = typer.Option(
        None,
        "--env",
        help="Run this command against a named deploy target.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    payload = resolve_execution_runtime_capabilities()
    if json_output:
        console = build_console(no_color=no_color)
        render_json_payload(console, payload)
        return
    _render_execution_runtimes(payload, no_color=no_color)


def positions_command(
    position_id: str | None = typer.Argument(None, help="Position id to inspect."),
    environment: str | None = typer.Option(
        None,
        "--env",
        help="Run this command against a named deploy target.",
    ),
    trading_strategy_id: str | None = typer.Option(
        None,
        "--trading-strategy-id",
        help="Optional trading strategy owner filter.",
    ),
    date: str | None = typer.Option(None, "--date", help="Optional market date."),
    limit: int = typer.Option(50, "--limit", help="Maximum positions to list."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    payload = (
        list_positions(
            db_target=db,
            market_date=date,
            trading_strategy_id=trading_strategy_id,
            limit=limit,
        )
        if position_id is None
        else get_position_detail(db_target=db, position_id=position_id)
    )
    _print_payload(payload, json_output=json_output)
