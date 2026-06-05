from __future__ import annotations

import json
from typing import Any

import typer
from rich.panel import Panel
from rich.table import Table

from core.cli.ops_render import build_console, render_json_payload
from core.services.retention import (
    build_retention_status,
    prune_retained_data,
    retention_defaults,
)

retention_app = typer.Typer(
    add_completion=False,
    help="Maintain high-volume tick partitions.",
    no_args_is_help=True,
)


def _render_prune_summary(payload: dict[str, Any]) -> None:
    console = build_console(no_color=True)
    console.print(json.dumps(payload, sort_keys=True))


def _format_bytes(value: Any) -> str:
    if value is None:
        return "-"
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} TB"


def _render_value(value: Any) -> str:
    if value is None:
        return "-"
    return str(value)


def _render_status(payload: dict[str, Any], *, no_color: bool) -> None:
    console = build_console(no_color=no_color)
    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Overall", _render_value(payload.get("status")))
    overview.add_row("Generated", _render_value(payload.get("generated_at")))
    overview.add_row(
        "Latest Run",
        (f"{_render_value(summary.get('latest_run_status'))} @ " f"{_render_value(summary.get('latest_run_at'))}"),
    )
    overview.add_row(
        "Latest Maintenance",
        (
            f"created {_render_value(summary.get('latest_created_partition_count'))} | "
            f"expired {_render_value(summary.get('latest_expired_partition_count'))} | "
            f"dropped {_render_value(summary.get('latest_dropped_partition_count'))}"
        ),
    )
    overview.add_row("Partition Ready", "yes" if summary.get("partition_ready") else "no")
    overview.add_row(
        "Future Coverage",
        f"{_render_value(summary.get('future_partition_days'))}/{_render_value(summary.get('required_future_partition_days'))} days",
    )
    overview.add_row("Schedule", _render_value(summary.get("schedule")))
    overview.add_row("Retention Log", _render_value(summary.get("retention_log_path")))
    console.print(Panel(overview, title="Retention Status"))

    tables = list(details.get("tables") or [])
    if tables:
        table = Table(title="Tick Partitions", header_style="bold")
        table.add_column("Name")
        table.add_column("Retention", justify="right")
        table.add_column("Partitions", justify="right")
        table.add_column("Current")
        table.add_column("Future", justify="right")
        table.add_column("Rows Est.", justify="right")
        table.add_column("Size", justify="right")
        for row in tables:
            table.add_row(
                _render_value(row.get("name")),
                f"{_render_value(row.get('retention_days'))}d",
                _render_value(row.get("partition_count")),
                "ready" if row.get("current_partition_ready") else "missing",
                f"{_render_value(row.get('future_partition_days'))}/{_render_value(row.get('required_future_partition_days'))}",
                _render_value(row.get("estimated_live_rows")),
                _format_bytes(row.get("total_size_bytes")),
            )
        console.print(table)

    maintenance = dict(details.get("maintenance") or {})
    console.print(
        Panel(
            _render_value(maintenance.get("lock_profile")),
            title="Partition Maintenance",
        )
    )


@retention_app.command("status", help="Show quote/trade tick partition health.")
def status_command(
    environment: str | None = typer.Option(
        None,
        "--env",
        help="Run this command against a named deploy target.",
    ),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    include_pending_counts: bool = typer.Option(
        False,
        "--include-pending-counts",
        help="Count rows currently older than retention cutoffs.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    try:
        payload = build_retention_status(
            db_target=db,
            include_pending_counts=include_pending_counts,
        )
    except Exception as exc:
        typer.secho(f"Retention status failed: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(2) from None

    if json_output:
        render_json_payload(build_console(no_color=True), payload)
    else:
        _render_status(payload, no_color=no_color)


@retention_app.command("prune", help="Create future tick partitions and drop expired tick partitions.")
def prune_command(
    environment: str | None = typer.Option(
        None,
        "--env",
        help="Run this command against a named deploy target.",
    ),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    execute: bool = typer.Option(
        False,
        "--execute",
        help="Apply deletes. Omit for a dry run.",
    ),
    option_quote_tick_days: int | None = typer.Option(
        None,
        "--option-quote-tick-days",
        help="Retention days for option_quote_ticks.",
    ),
    option_trade_tick_days: int | None = typer.Option(
        None,
        "--option-trade-tick-days",
        help="Retention days for option_trade_ticks.",
    ),
    future_partition_days: int | None = typer.Option(
        None,
        "--future-partition-days",
        help="Future calendar days to keep precreated.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    defaults = retention_defaults()
    try:
        payload = prune_retained_data(
            db_target=db,
            dry_run=not execute,
            option_quote_tick_days=option_quote_tick_days or defaults["option_quote_tick_days"],
            option_trade_tick_days=option_trade_tick_days or defaults["option_trade_tick_days"],
            future_partition_days=future_partition_days or defaults["future_partition_days"],
        )
    except Exception as exc:
        typer.secho(f"Retention prune failed: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(2) from None

    if json_output:
        render_json_payload(build_console(no_color=True), payload)
    else:
        _render_prune_summary(payload)
