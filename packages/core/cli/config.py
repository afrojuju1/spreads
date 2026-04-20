from __future__ import annotations

import json
from pathlib import Path

import typer

from core.services.config_validation import validate_options_automation_config

config_app = typer.Typer(
    add_completion=False,
    help="Validate config-driven options automation models.",
)


@config_app.command("validate", help="Validate strategy, automation, and bot config.")
def validate_command(
    config_root: str | None = typer.Option(
        None,
        "--config-root",
        help="Optional config root override.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    payload = validate_options_automation_config(
        config_root=None if config_root is None else Path(config_root)
    )
    if json_output:
        typer.echo(json.dumps(payload, indent=2, default=str))
        return
    typer.echo(
        "\n".join(
            [
                "status: ok",
                f"strategies: {payload['strategy_count']}",
                f"automations: {payload['automation_count']}",
                f"bots: {payload['bot_count']}",
                f"active_bots: {payload['active_bot_count']}",
                f"collector_scopes: {payload['collector_scope_count']}",
            ]
        )
    )


__all__ = ["config_app", "validate_command"]
