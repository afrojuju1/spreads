from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import typer

from spreads.cli.ops_render import (
    build_console,
    render_json_payload,
    render_sessions_view,
    render_system_status,
    render_trading_health,
)
from spreads.services.ops_visibility import (
    OpsLookupError,
    build_sessions_view,
    build_system_status,
    build_trading_health,
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


def _validate_limit(value: int) -> int:
    if value <= 0:
        raise ValueError("--limit must be greater than 0.")
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


def status_command(
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(None, "--watch", help="Refresh every N seconds."),
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
    watch: float | None = typer.Option(None, "--watch", help="Refresh every N seconds."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    _run_visibility_command(
        builder=lambda: build_trading_health(db_target=db),
        renderer=render_trading_health,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )


def sessions_command(
    session_id: str | None = typer.Argument(None, help="Session id to inspect."),
    date: str | None = typer.Option(None, "--date", help="Filter list mode to one session date."),
    limit: int = typer.Option(25, "--limit", help="Maximum sessions to list."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(None, "--watch", help="Refresh every N seconds."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    try:
        if session_id is not None and date is not None:
            raise ValueError("--date cannot be used with a session id.")
        resolved_limit = _validate_limit(limit)
    except ValueError as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(3) from None
    _run_visibility_command(
        builder=lambda: build_sessions_view(
            db_target=db,
            session_id=session_id,
            session_date=date,
            limit=resolved_limit,
        ),
        renderer=render_sessions_view,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )
