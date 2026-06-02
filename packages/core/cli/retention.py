from __future__ import annotations

import json
from typing import Any

import typer

from core.cli.ops_render import build_console, render_json_payload
from core.services.retention import prune_retained_data, retention_defaults

retention_app = typer.Typer(
    add_completion=False,
    help="Prune high-volume retained runtime data.",
    no_args_is_help=True,
)


def _render_prune_summary(payload: dict[str, Any]) -> None:
    console = build_console(no_color=True)
    console.print(json.dumps(payload, sort_keys=True))


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
