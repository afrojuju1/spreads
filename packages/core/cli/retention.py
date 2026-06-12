from __future__ import annotations

import json
from typing import Any

import typer

from core.cli.command_harness import run_payload_command
from core.cli.ops_render import build_console
from core.services.retention import (
    prune_retained_data,
    retention_defaults,
)

maintenance_app = typer.Typer(
    add_completion=False,
    help="Run explicit maintenance tasks.",
    no_args_is_help=True,
)

retention_maintenance_app = typer.Typer(
    add_completion=False,
    help="Maintain high-volume tick partitions.",
    no_args_is_help=True,
)


def _render_prune_summary(payload: dict[str, Any]) -> None:
    console = build_console(no_color=True)
    console.print(json.dumps(payload, sort_keys=True))


@retention_maintenance_app.command("prune", help="Create future tick partitions and drop expired tick partitions.")
def prune_command(
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
    run_payload_command(
        builder=lambda: prune_retained_data(
            db_target=db,
            dry_run=not execute,
            option_quote_tick_days=option_quote_tick_days or defaults["option_quote_tick_days"],
            option_trade_tick_days=option_trade_tick_days or defaults["option_trade_tick_days"],
            future_partition_days=future_partition_days or defaults["future_partition_days"],
        ),
        renderer=_render_prune_summary,
        json_output=json_output,
        no_color=True,
        error_prefix="Retention prune failed",
    )


maintenance_app.add_typer(retention_maintenance_app, name="retention")


__all__ = ["maintenance_app", "prune_command"]
