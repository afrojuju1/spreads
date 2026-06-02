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
    help="Prune high-volume retained runtime data.",
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
        (
            f"{_render_value(summary.get('latest_run_status'))} @ "
            f"{_render_value(summary.get('latest_run_at'))}"
        ),
    )
    overview.add_row(
        "Latest Prune",
        (
            f"matched {_render_value(summary.get('latest_matching_count'))} | "
            f"deleted {_render_value(summary.get('latest_deleted_count'))}"
        ),
    )
    overview.add_row(
        "Vacuum Full",
        (
            "pending "
            f"{_render_value(', '.join(summary.get('vacuum_full_pending_tables') or []))}"
            if summary.get("vacuum_full_pending")
            else "not pending"
        ),
    )
    overview.add_row("Schedule", _render_value(summary.get("schedule")))
    overview.add_row("Retention Log", _render_value(summary.get("retention_log_path")))
    console.print(Panel(overview, title="Retention Status"))

    tables = list(details.get("tables") or [])
    if tables:
        table = Table(title="High-Volume Retention Tables", header_style="bold")
        table.add_column("Name")
        table.add_column("Retention", justify="right")
        table.add_column("Retained Range")
        table.add_column("Rows Est.", justify="right")
        table.add_column("Dead Est.", justify="right")
        table.add_column("Size", justify="right")
        table.add_column("Latest Deleted", justify="right")
        table.add_column("Vacuum")
        for row in tables:
            latest_prune = (
                row.get("latest_prune") if isinstance(row.get("latest_prune"), dict) else {}
            )
            vacuum_full = (
                row.get("vacuum_full") if isinstance(row.get("vacuum_full"), dict) else {}
            )
            table.add_row(
                _render_value(row.get("name")),
                f"{_render_value(row.get('retention_days'))}d",
                f"{_render_value(row.get('retained_from'))} -> {_render_value(row.get('retained_to'))}",
                _render_value(row.get("estimated_live_rows")),
                _render_value(row.get("estimated_dead_rows")),
                _format_bytes(row.get("total_size_bytes")),
                _render_value(latest_prune.get("deleted_count")),
                "pending" if vacuum_full.get("pending") else "ok",
            )
        console.print(table)

    maintenance = dict(details.get("maintenance") or {})
    console.print(
        Panel(
            _render_value(maintenance.get("lock_profile")),
            title=f"Maintenance Runbook: {_render_value(maintenance.get('vacuum_full_runbook'))}",
        )
    )


@retention_app.command("status", help="Show quote/trade/event retention health.")
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


@retention_app.command("prune", help="Prune retained quote/trade/event rows.")
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
    option_quote_days: int | None = typer.Option(
        None,
        "--option-quote-days",
        help="Retention days for option_quote_events.",
    ),
    option_trade_days: int | None = typer.Option(
        None,
        "--option-trade-days",
        help="Retention days for option_trade_events.",
    ),
    event_log_market_days: int | None = typer.Option(
        None,
        "--event-log-market-days",
        help="Retention days for market_event rows in event_log.",
    ),
    event_log_control_days: int | None = typer.Option(
        None,
        "--event-log-control-days",
        help="Retention days for non-market rows in event_log.",
    ),
    batch_size: int | None = typer.Option(
        None,
        "--batch-size",
        help="Maximum rows to delete per table batch.",
    ),
    max_batches: int | None = typer.Option(
        None,
        "--max-batches",
        help="Maximum delete batches per table.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    defaults = retention_defaults()
    try:
        payload = prune_retained_data(
            db_target=db,
            dry_run=not execute,
            option_quote_days=option_quote_days or defaults["option_quote_days"],
            option_trade_days=option_trade_days or defaults["option_trade_days"],
            event_log_market_days=event_log_market_days
            or defaults["event_log_market_days"],
            event_log_control_days=event_log_control_days
            or defaults["event_log_control_days"],
            batch_size=batch_size or defaults["batch_size"],
            max_batches=max_batches or defaults["max_batches"],
        )
    except Exception as exc:
        typer.secho(f"Retention prune failed: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(2) from None

    if json_output:
        render_json_payload(build_console(no_color=True), payload)
    else:
        _render_prune_summary(payload)
