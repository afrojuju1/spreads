from __future__ import annotations

import json
from pathlib import Path

import typer

from core.services.config_validation import validate_trading_strategy_config

config_app = typer.Typer(add_completion=False, help="Validate config-driven trading models.")


@config_app.command("validate", help="Validate ticker source and trading strategy config.")
def validate_command(
    config_root: str | None = typer.Option(
        None,
        "--config-root",
        help="Optional config root override.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    payload = validate_trading_strategy_config(config_root=None if config_root is None else Path(config_root))
    if json_output:
        typer.echo(json.dumps(payload, indent=2, default=str))
        return
    typer.echo(
        "\n".join(
            [
                "status: ok",
                f"trading_strategies: {payload['trading_strategy_count']}",
                f"active_trading_strategies: {payload['active_trading_strategy_count']}",
                f"entry_routines: {payload['entry_routine_count']}",
                f"management_routines: {payload['management_routine_count']}",
                f"sources: {payload['source_counts']}",
                f"trade_structures: {payload['trade_structure_counts']}",
            ]
        )
    )


__all__ = ["config_app", "validate_command"]
