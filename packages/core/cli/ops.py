from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import typer

from core.cli.ops_render import (
    build_console,
    render_job_lanes_view,
    render_jobs_view,
    render_json_payload,
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


def _exit_code_for_status(status: str | None) -> int:
    normalized = str(status or "unknown").strip().lower()
    if normalized in {"healthy", "idle"}:
        return 0
    if normalized in {"degraded", "unknown"}:
        return 1
    if normalized in {"blocked", "halted"}:
        return 2
    return 3


def _validate_watch_interval(value: float | None) -> float | None:
    if value is None:
        return None
    if value <= 0:
        raise ValueError("--watch must be greater than 0.")
    return value


def _validate_limit(value: int, *, option_name: str = "--limit") -> int:
    if value <= 0:
        raise ValueError(f"{option_name} must be greater than 0.")
    return value


def _render_loop(
    *,
    builder: Callable[[], dict[str, Any]],
    renderer: Callable[[Any, dict[str, Any]], None],
    json_output: bool,
    watch_seconds: float | None,
    no_color: bool,
) -> None:
    watch_interval = _validate_watch_interval(watch_seconds)
    console = build_console(no_color=no_color)
    payload: dict[str, Any] | None = None

    while True:
        if watch_interval is not None:
            console.clear()
        payload = builder()
        if json_output:
            render_json_payload(console, payload)
        else:
            renderer(console, payload)
        if watch_interval is None:
            raise typer.Exit(_exit_code_for_status(payload.get("status")))
        try:
            time.sleep(watch_interval)
        except KeyboardInterrupt:
            raise typer.Exit(_exit_code_for_status(None if payload is None else payload.get("status"))) from None


def _run_visibility_command(
    *,
    builder: Callable[[], dict[str, Any]],
    renderer: Callable[[Any, dict[str, Any]], None],
    json_output: bool,
    watch_seconds: float | None,
    no_color: bool,
) -> None:
    try:
        _render_loop(
            builder=builder,
            renderer=renderer,
            json_output=json_output,
            watch_seconds=watch_seconds,
            no_color=no_color,
        )
    except OpsLookupError as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(3) from None
    except ValueError as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(3) from None
    except typer.Exit:
        raise
    except KeyboardInterrupt:
        raise typer.Exit(130) from None
    except Exception as exc:
        typer.secho(f"Command failed: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(2) from None


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
    _run_visibility_command(
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
    _run_visibility_command(
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
    try:
        resolved_limit = _validate_limit(limit, option_name="--limit")
    except ValueError as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(3) from None
    _run_visibility_command(
        builder=lambda: build_jobs_overview(
            db_target=db,
            job_type=job_type,
            status=status,
            limit=resolved_limit,
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
    _run_visibility_command(
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
    _run_visibility_command(
        builder=lambda: build_job_lanes_overview(db_target=db),
        renderer=render_job_lanes_view,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )
