from __future__ import annotations

from typing import Any, Callable

import typer
from rich.panel import Panel
from rich.table import Table

from core.cli.ops_render import build_console, render_json_payload
from core.services.execution.sync import (
    cancel_execution_attempt,
    inspect_execution_attempt,
    refresh_execution_attempt,
)

execution_app = typer.Typer(
    add_completion=False,
    help="Inspect, refresh, or cancel execution attempts.",
    no_args_is_help=True,
)


def _render_value(value: Any) -> str:
    if value is None or value == "":
        return "-"
    return str(value)


def _short_id(value: Any, *, length: int = 18) -> str:
    text = _render_value(value)
    if text == "-":
        return text
    if len(text) <= length:
        return text
    return "..." + text[-max(length - 3, 0) :]


def _render_time(value: Any) -> str:
    text = _render_value(value)
    return text[:19] if text != "-" else text


def _result_exit_code(payload: dict[str, Any]) -> int:
    if str(payload.get("action") or "") == "cancel" and not bool(payload.get("changed")):
        return 0
    return 0


def _render_attempt_result(payload: dict[str, Any], *, json_output: bool, no_color: bool) -> None:
    if json_output:
        render_json_payload(build_console(no_color=True), payload)
        return

    console = build_console(no_color=no_color)
    summary = dict(payload.get("summary") or {})
    linked_intent = dict(payload.get("linked_intent") or {})
    attempt = dict(payload.get("attempt") or {})

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Action", _render_value(payload.get("action")))
    overview.add_row("Changed", "yes" if payload.get("changed") else "no")
    overview.add_row("Message", _render_value(payload.get("message")))
    overview.add_row("Attempt", _render_value(summary.get("execution_attempt_id")))
    overview.add_row("Status", _render_value(summary.get("status")))
    overview.add_row("Lifecycle", f"{_render_value(summary.get('lifecycle_phase'))} | next {_render_value(summary.get('next_action'))}")
    overview.add_row("Strategy", _render_value(summary.get("trading_strategy_id")))
    overview.add_row("Underlying", _render_value(summary.get("underlying_symbol")))
    overview.add_row("Intent", _render_value(summary.get("trade_intent")))
    overview.add_row("Context", _render_value(summary.get("attempt_context")))
    overview.add_row("Broker Order", _render_value(summary.get("broker_order_id")))
    overview.add_row("Client Order", _render_value(summary.get("client_order_id")))
    overview.add_row("Rows", f"orders {_render_value(summary.get('order_count'))} | fills {_render_value(summary.get('fill_count'))}")
    if linked_intent:
        overview.add_row(
            "Linked Intent",
            (f"{_render_value(linked_intent.get('state'))} | " f"{_render_value(linked_intent.get('execution_intent_id'))}"),
        )
    if summary.get("error_text"):
        overview.add_row("Error", _render_value(summary.get("error_text")))
    console.print(Panel(overview, title="Execution Attempt"))

    orders = [row for row in attempt.get("orders") or [] if isinstance(row, dict)]
    if orders:
        table = Table(title="Orders", header_style="bold")
        table.add_column("Broker Order")
        table.add_column("Status")
        table.add_column("Side")
        table.add_column("Symbol")
        table.add_column("Qty", justify="right")
        table.add_column("Filled", justify="right")
        table.add_column("Updated")
        for row in orders[:8]:
            table.add_row(
                _short_id(row.get("broker_order_id"), length=24),
                _render_value(row.get("order_status")),
                _render_value(row.get("side") or row.get("leg_side")),
                _render_value(row.get("symbol") or row.get("leg_symbol")),
                _render_value(row.get("quantity")),
                _render_value(row.get("filled_qty")),
                _render_time(row.get("updated_at")),
            )
        console.print(table)

    fills = [row for row in attempt.get("fills") or [] if isinstance(row, dict)]
    if fills:
        table = Table(title="Fills", header_style="bold")
        table.add_column("Fill")
        table.add_column("Order")
        table.add_column("Side")
        table.add_column("Symbol")
        table.add_column("Qty", justify="right")
        table.add_column("Price", justify="right")
        table.add_column("Filled")
        for row in fills[:8]:
            table.add_row(
                _short_id(row.get("broker_fill_id"), length=20),
                _short_id(row.get("broker_order_id"), length=20),
                _render_value(row.get("side")),
                _render_value(row.get("symbol")),
                _render_value(row.get("quantity")),
                _render_value(row.get("price")),
                _render_time(row.get("filled_at")),
            )
        console.print(table)


def _run_attempt_action(
    runner: Callable[..., dict[str, Any]],
    *,
    execution_attempt_id: str,
    db: str | None,
    json_output: bool,
    no_color: bool,
) -> None:
    try:
        payload = runner(
            db_target=db,
            execution_attempt_id=execution_attempt_id,
        )
    except (RuntimeError, ValueError) as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(2) from None
    _render_attempt_result(payload, json_output=json_output, no_color=no_color)
    raise typer.Exit(_result_exit_code(payload))


@execution_app.command("inspect", help="Inspect an execution_attempt by id without broker mutation.")
def execution_inspect_command(
    execution_attempt_id: str = typer.Argument(..., help="Execution attempt id."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI color."),
) -> None:
    _run_attempt_action(
        inspect_execution_attempt,
        execution_attempt_id=execution_attempt_id,
        db=db,
        json_output=json_output,
        no_color=no_color,
    )


@execution_app.command("refresh", help="Refresh broker/order/fill state for an execution_attempt by id.")
def execution_refresh_command(
    execution_attempt_id: str = typer.Argument(..., help="Execution attempt id."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI color."),
) -> None:
    _run_attempt_action(
        refresh_execution_attempt,
        execution_attempt_id=execution_attempt_id,
        db=db,
        json_output=json_output,
        no_color=no_color,
    )


@execution_app.command("cancel", help="Cancel an open execution_attempt by id; terminal attempts return changed=false.")
def execution_cancel_command(
    execution_attempt_id: str = typer.Argument(..., help="Execution attempt id."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI color."),
) -> None:
    _run_attempt_action(
        cancel_execution_attempt,
        execution_attempt_id=execution_attempt_id,
        db=db,
        json_output=json_output,
        no_color=no_color,
    )


__all__ = [
    "execution_app",
    "execution_cancel_command",
    "execution_inspect_command",
    "execution_refresh_command",
]
