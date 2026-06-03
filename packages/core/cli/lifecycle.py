from __future__ import annotations

import json

import typer

from core.services.lifecycle_schema import build_lifecycle_schema_summary

lifecycle_app = typer.Typer(
    add_completion=False,
    help="Inspect the target trading lifecycle schema.",
    no_args_is_help=True,
)


@lifecycle_app.command("schema", help="Show target trading lifecycle tables and states.")
def lifecycle_schema_command(
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    payload = build_lifecycle_schema_summary()
    if json_output:
        typer.echo(json.dumps(payload, indent=2, default=str))
        return

    typer.echo("status: target_schema_defined")
    typer.echo("posture: breaking_rewrite")
    typer.echo("live_writers_cut_over: false")
    typer.echo("tables:")
    for row in payload["tables"]:
        typer.echo(f"  - {row['name']}: {row['role']}")
    typer.echo("states:")
    for row in payload["states"]:
        typer.echo(f"  - {row['object_type']}: {', '.join(row['states'])}")


__all__ = ["lifecycle_app", "lifecycle_schema_command"]
