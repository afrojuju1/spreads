from __future__ import annotations

import typer
from typing import Any

from core.cli.command_harness import (
    run_visibility_command,
    validate_positive_limit,
)
from core.cli.ops_render import (
    render_job_lanes_view,
    render_jobs_view,
    render_storage_ops_state,
    render_trading_ops_state,
)
from core.services.ops import (
    OpsLookupError,
    build_job_lanes_overview,
    build_job_run_view,
    build_jobs_overview,
    build_storage_ops_state,
    build_trading_ops_state,
)


def _run_ops_visibility_command(**kwargs: Any) -> None:
    run_visibility_command(
        handled_error_types=(OpsLookupError,),
        **kwargs,
    )


def _jobs_payload(
    *,
    db: str | None,
    job_type: str | None,
    status: str | None,
    limit: int,
) -> dict[str, Any]:
    return build_jobs_overview(
        db_target=db,
        job_type=job_type,
        status=status,
        limit=validate_positive_limit(limit, option_name="--limit"),
    )


ops_app = typer.Typer(
    add_completion=False,
    help="Inspect canonical operator state.",
    no_args_is_help=True,
)


@ops_app.command("state", help="Show canonical live trading operator state.")
def trading_ops_state_command(
    market_date: str | None = typer.Option(
        None,
        "--date",
        help="Market date to inspect. Defaults to today in New York.",
    ),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(None, "--watch", help="Refresh every N seconds."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    run_trading_ops_state_visibility(
        market_date=market_date,
        db=db,
        json_output=json_output,
        watch=watch,
        no_color=no_color,
    )


def run_trading_ops_state_visibility(
    *,
    market_date: str | None,
    db: str | None,
    json_output: bool,
    watch: float | None,
    no_color: bool,
) -> None:
    _run_ops_visibility_command(
        builder=lambda: build_trading_ops_state(db_target=db, market_date=market_date),
        renderer=render_trading_ops_state,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )


@ops_app.command("storage", help="Show canonical storage and retention operator state.")
def storage_ops_state_command(
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(None, "--watch", help="Refresh every N seconds."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    run_storage_ops_state_visibility(
        db=db,
        json_output=json_output,
        watch=watch,
        no_color=no_color,
    )


def run_storage_ops_state_visibility(
    *,
    db: str | None,
    json_output: bool,
    watch: float | None,
    no_color: bool,
) -> None:
    _run_ops_visibility_command(
        builder=lambda: build_storage_ops_state(db_target=db),
        renderer=render_storage_ops_state,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )


jobs_app = typer.Typer(
    add_completion=False,
    help="Inspect job definitions and job runs.",
    invoke_without_command=True,
    no_args_is_help=False,
)


@jobs_app.callback(invoke_without_command=True)
def jobs_command(
    ctx: typer.Context,
    environment: str | None = typer.Option(
        None,
        "--env",
        help="Run this command against a named deploy target.",
    ),
    job_type: str | None = typer.Option(None, "--job-type", help="Filter runs and definitions by job type."),
    status: str | None = typer.Option(None, "--status", help="Filter runs by status."),
    limit: int = typer.Option(25, "--limit", help="Maximum job runs to list."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(None, "--watch", help="Refresh every N seconds."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    if ctx.invoked_subcommand is not None:
        return
    _run_ops_visibility_command(
        builder=lambda: _jobs_payload(
            db=db,
            job_type=job_type,
            status=status,
            limit=limit,
        ),
        renderer=render_jobs_view,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )


@jobs_app.command("run", help="Inspect one job run.")
def jobs_run_command(
    job_run_id: str = typer.Argument(..., help="Job run id to inspect."),
    environment: str | None = typer.Option(
        None,
        "--env",
        help="Run this command against a named deploy target.",
    ),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(None, "--watch", help="Refresh every N seconds."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    _run_ops_visibility_command(
        builder=lambda: build_job_run_view(
            db_target=db,
            job_run_id=job_run_id,
        ),
        renderer=render_jobs_view,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )


@jobs_app.command("lanes", help="Inspect worker lanes.")
def jobs_lanes_command(
    environment: str | None = typer.Option(
        None,
        "--env",
        help="Run this command against a named deploy target.",
    ),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(None, "--watch", help="Refresh every N seconds."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    _run_ops_visibility_command(
        builder=lambda: build_job_lanes_overview(db_target=db),
        renderer=render_job_lanes_view,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )
