from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import typer

from spreads.cli.ops_render import (
    build_console,
    render_audit_view,
    render_jobs_view,
    render_json_payload,
    render_system_status,
    render_trading_health,
    render_uoa_view,
)
from spreads.services.ops_visibility import (
    OpsLookupError,
    build_audit_view,
    build_job_run_view,
    build_jobs_overview,
    build_system_status,
    build_trading_health,
    build_uoa_cycle_view,
    build_uoa_overview,
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
            raise typer.Exit(
                _exit_code_for_status(
                    None if payload is None else payload.get("status")
                )
            ) from None


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


def status_command(
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(
        None, "--watch", help="Refresh every N seconds."
    ),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    _run_visibility_command(
        builder=lambda: build_system_status(db_target=db),
        renderer=render_system_status,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )


def trading_command(
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(
        None, "--watch", help="Refresh every N seconds."
    ),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    _run_visibility_command(
        builder=lambda: build_trading_health(db_target=db),
        renderer=render_trading_health,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )


def audit_command(
    session_id: str = typer.Argument(..., help="Session id to audit."),
    timeline_limit: int = typer.Option(
        120,
        "--timeline-limit",
        help="Maximum timeline items to return.",
    ),
    event_scan_limit: int = typer.Option(
        5000,
        "--event-scan-limit",
        help="Maximum events to scan while building the replay.",
    ),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(
        None, "--watch", help="Refresh every N seconds."
    ),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    try:
        resolved_timeline_limit = _validate_limit(
            timeline_limit,
            option_name="--timeline-limit",
        )
        resolved_event_scan_limit = _validate_limit(
            event_scan_limit,
            option_name="--event-scan-limit",
        )
    except ValueError as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(3) from None
    _run_visibility_command(
        builder=lambda: build_audit_view(
            session_id=session_id,
            db_target=db,
            timeline_limit=resolved_timeline_limit,
            event_scan_limit=resolved_event_scan_limit,
        ),
        renderer=render_audit_view,
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
    job_type: str | None = typer.Option(
        None, "--job-type", help="Filter runs and definitions by job type."
    ),
    status: str | None = typer.Option(None, "--status", help="Filter runs by status."),
    limit: int = typer.Option(25, "--limit", help="Maximum job runs to list."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(
        None, "--watch", help="Refresh every N seconds."
    ),
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
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(
        None, "--watch", help="Refresh every N seconds."
    ),
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


uoa_app = typer.Typer(
    add_completion=False,
    help="Inspect UOA activity and cycle state.",
    invoke_without_command=True,
    no_args_is_help=False,
)


@uoa_app.callback(invoke_without_command=True)
def uoa_command(
    ctx: typer.Context,
    label: str | None = typer.Option(
        None, "--label", help="Filter to one collector label."
    ),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(
        None, "--watch", help="Refresh every N seconds."
    ),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    if ctx.invoked_subcommand is not None:
        return
    _run_visibility_command(
        builder=lambda: build_uoa_overview(
            db_target=db,
            label=label,
        ),
        renderer=render_uoa_view,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )


@uoa_app.command("cycle", help="Inspect one UOA cycle.")
def uoa_cycle_command(
    cycle_id: str = typer.Argument(..., help="Collector cycle id to inspect."),
    label: str | None = typer.Option(
        None, "--label", help="Filter to one collector label."
    ),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(
        None, "--watch", help="Refresh every N seconds."
    ),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    _run_visibility_command(
        builder=lambda: build_uoa_cycle_view(
            cycle_id=cycle_id,
            db_target=db,
            label=label,
        ),
        renderer=render_uoa_view,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )
