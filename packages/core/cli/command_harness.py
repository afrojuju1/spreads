from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import typer

from core.cli.ops_render import build_console, render_json_payload

PayloadBuilder = Callable[[], dict[str, Any]]
PayloadRenderer = Callable[[Any, dict[str, Any]], None]
PlainRenderer = Callable[[dict[str, Any]], None]
PassthroughEntrypoint = Callable[[list[str] | None], int]


def exit_code_for_status(status: str | None) -> int:
    normalized = str(status or "unknown").strip().lower()
    if normalized in {"healthy", "idle"}:
        return 0
    if normalized in {"degraded", "unknown"}:
        return 1
    if normalized in {"blocked", "halted"}:
        return 2
    return 3


def validate_watch_interval(value: float | None) -> float | None:
    if value is None:
        return None
    if value <= 0:
        raise ValueError("--watch must be greater than 0.")
    return value


def validate_positive_limit(value: int, *, option_name: str = "--limit") -> int:
    if value <= 0:
        raise ValueError(f"{option_name} must be greater than 0.")
    return value


def print_command_error(message: str) -> None:
    typer.secho(message, err=True, fg=typer.colors.RED)


def render_payload(
    payload: dict[str, Any],
    *,
    renderer: PlainRenderer,
    json_output: bool,
    no_color: bool,
) -> None:
    if json_output:
        render_json_payload(build_console(no_color=True), payload)
    else:
        renderer(payload)


def _render_loop(
    *,
    builder: PayloadBuilder,
    renderer: PayloadRenderer,
    json_output: bool,
    watch_seconds: float | None,
    no_color: bool,
) -> None:
    watch_interval = validate_watch_interval(watch_seconds)
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
            raise typer.Exit(exit_code_for_status(payload.get("status")))
        try:
            time.sleep(watch_interval)
        except KeyboardInterrupt:
            raise typer.Exit(exit_code_for_status(None if payload is None else payload.get("status"))) from None


def run_visibility_command(
    *,
    builder: PayloadBuilder,
    renderer: PayloadRenderer,
    json_output: bool,
    watch_seconds: float | None,
    no_color: bool,
    handled_error_types: tuple[type[BaseException], ...] = (),
    error_prefix: str = "Command failed",
) -> None:
    handled_errors = (ValueError, *handled_error_types)
    try:
        _render_loop(
            builder=builder,
            renderer=renderer,
            json_output=json_output,
            watch_seconds=watch_seconds,
            no_color=no_color,
        )
    except handled_errors as exc:
        print_command_error(str(exc))
        raise typer.Exit(3) from None
    except typer.Exit:
        raise
    except KeyboardInterrupt:
        raise typer.Exit(130) from None
    except Exception as exc:
        print_command_error(f"{error_prefix}: {exc}")
        raise typer.Exit(2) from None


def run_payload_command(
    *,
    builder: PayloadBuilder,
    renderer: PlainRenderer,
    json_output: bool,
    no_color: bool,
    error_prefix: str = "Command failed",
    error_exit_code: int = 2,
) -> None:
    try:
        payload = builder()
    except typer.Exit:
        raise
    except KeyboardInterrupt:
        raise typer.Exit(130) from None
    except Exception as exc:
        print_command_error(f"{error_prefix}: {exc}")
        raise typer.Exit(error_exit_code) from None
    render_payload(payload, renderer=renderer, json_output=json_output, no_color=no_color)


def run_passthrough(
    *,
    ctx: typer.Context,
    entrypoint: PassthroughEntrypoint,
) -> None:
    try:
        code = entrypoint(list(ctx.args))
    except SystemExit as exc:
        raw_code = exc.code
        if raw_code in (None, 0):
            code = 0
        elif isinstance(raw_code, int):
            code = raw_code
        else:
            code = 1
    raise typer.Exit(code)
