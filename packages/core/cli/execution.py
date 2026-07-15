from __future__ import annotations

import re
from typing import Any, Callable

import typer
from rich.panel import Panel
from rich.table import Table

from core.cli.ops_render import build_console, render_json_payload
from core.services.execution.activity import list_execution_activity
from core.services.execution.sync import (
    inspect_execution_attempt,
)
from core.cli.runtime import execution_runtimes_command, positions_command

execution_app = typer.Typer(
    add_completion=False,
    help="Inspect execution attempts, positions, and runtime capabilities.",
    no_args_is_help=True,
)

OPTION_SYMBOL_RE = re.compile(r"^([A-Z]+)(\d{6})([CP])(\d{8})$")


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


def _render_clock(value: Any) -> str:
    text = _render_time(value)
    return text[11:19] if len(text) >= 19 and "T" in text else text


def _render_counts(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "-"
    return ", ".join(f"{key}:{count}" for key, count in sorted(value.items()))


def _render_strategy_counts(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "-"
    return ", ".join(f"{_compact_strategy(key)}:{count}" for key, count in sorted(value.items()))


def _render_activity_range(summary: dict[str, Any]) -> str:
    first = _render_clock(summary.get("first_activity_at"))
    latest = _render_clock(summary.get("latest_activity_at"))
    if first == "-" and latest == "-":
        return "-"
    if first == latest:
        return first
    return f"{first} -> {latest}"


def _compact_strategy(value: Any) -> str:
    text = _render_value(value)
    if text == "-":
        return text
    replacements = (
        ("short_dated_index_", "idx_"),
        ("short_dated_earnings_", "earn_"),
        ("short_dated_etf_", "etf_"),
        ("momentum_long_calls", "momentum_calls"),
    )
    for old, new in replacements:
        text = text.replace(old, new)
    return text


def _compact_position_intent(value: Any) -> str:
    mapping = {
        "buy_to_open": "BTO",
        "sell_to_open": "STO",
        "buy_to_close": "BTC",
        "sell_to_close": "STC",
    }
    text = _render_value(value)
    return mapping.get(text, text)


def _compact_status(value: Any) -> str:
    mapping = {
        "filled": "fill",
        "canceled": "cxl",
        "cancelled": "cxl",
        "rejected": "rej",
        "expired": "exp",
    }
    text = _render_value(value)
    return mapping.get(text, text)


def _compact_number(value: Any) -> str:
    if value is None or value == "":
        return "-"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:g}"
    text = str(value)
    try:
        return f"{float(text):g}"
    except ValueError:
        return text


def _money(value: Any) -> str:
    number = _compact_number(value)
    if number == "-":
        return number
    if number.startswith("-"):
        return f"-${number.removeprefix('-')}"
    return f"${number}"


def _compact_symbol(value: Any) -> str:
    text = _render_value(value)
    if text == "-":
        return text
    match = OPTION_SYMBOL_RE.match(text)
    if match is None:
        return text
    root, raw_date, option_type, raw_strike = match.groups()
    strike = int(raw_strike) / 1000
    strike_text = f"{strike:g}"
    return f"{root}{strike_text}{option_type} {raw_date[2:4]}/{raw_date[4:6]}"


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


def _render_execution_activity(payload: dict[str, Any], *, json_output: bool, no_color: bool) -> None:
    if json_output:
        render_json_payload(build_console(no_color=True), payload)
        return

    console = build_console(no_color=no_color)
    summary = dict(payload.get("summary") or {})
    attempts = [row for row in payload.get("attempts") or [] if isinstance(row, dict)]

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Status", f"{_render_value(summary.get('operator_status'))} | {_render_activity_range(summary)}")
    overview.add_row("Date", _render_value(payload.get("activity_date")))
    if payload.get("trading_strategy_id"):
        overview.add_row("Filter", _render_value(payload.get("trading_strategy_id")))
    overview.add_row(
        "Attempts",
        (
            f"{_render_value(summary.get('attempt_count'))} total | "
            f"{_render_value(summary.get('terminal_attempt_count'))} terminal | "
            f"{_render_value(summary.get('open_attempt_count'))} open | "
            f"{_render_value(summary.get('error_attempt_count'))} errors"
        ),
    )
    overview.add_row(
        "Broker",
        (
            f"{_render_value(summary.get('order_count'))} orders "
            f"({_render_value(summary.get('parent_order_count'))} parent / {_render_value(summary.get('leg_order_count'))} leg) | "
            f"{_render_value(summary.get('fill_count'))} fills"
        ),
    )
    overview.add_row("Closes", f"{_render_value(summary.get('position_close_count'))} closes | {_money(summary.get('realized_pnl'))} realized")
    overview.add_row("Strategies", _render_strategy_counts(summary.get("strategy_counts")))
    overview.add_row("Mix", f"intents {_render_counts(summary.get('intent_counts'))} | statuses {_render_counts(summary.get('status_counts'))}")
    cross_day_count = int(summary.get("cross_market_date_attempt_count") or 0)
    if cross_day_count:
        overview.add_row("Scope", f"{cross_day_count} prior-session attempt(s) included")
    console.print(Panel(overview, title=f"Execution Activity {payload.get('activity_date') or ''}".rstrip()))

    if not attempts:
        console.print("No execution attempts found.")
        return

    attempt_table = Table(title="Attempts", header_style="bold")
    attempt_table.add_column("#")
    attempt_table.add_column("Requested")
    attempt_table.add_column("Strategy")
    attempt_table.add_column("Intent")
    attempt_table.add_column("Status")
    attempt_table.add_column("Root")
    attempt_table.add_column("Limit", justify="right")
    attempt_table.add_column("Rows", justify="right")
    attempt_table.add_column("PnL", justify="right")
    attempt_table.add_column("ID")
    attempt_index = {str(row.get("execution_attempt_id")): str(index) for index, row in enumerate(attempts, start=1)}
    for index, row in enumerate(attempts, start=1):
        attempt_table.add_row(
            str(index),
            _render_clock(row.get("requested_at")),
            _compact_strategy(row.get("trading_strategy_id")),
            _render_value(row.get("trade_intent")),
            _render_value(row.get("status")),
            _render_value(row.get("underlying_symbol")),
            _render_value(row.get("limit_price")),
            f"o{_render_value(row.get('order_count'))}/f{_render_value(row.get('fill_count'))}",
            _render_value(row.get("realized_pnl")),
            _short_id(row.get("execution_attempt_id"), length=12),
        )
    console.print(attempt_table)

    order_rows = [order for attempt in attempts for order in attempt.get("orders") or [] if isinstance(order, dict)]
    if order_rows:
        order_table = Table(title="Orders", header_style="bold")
        order_table.add_column("#")
        order_table.add_column("Kind")
        order_table.add_column("St")
        order_table.add_column("Intent")
        order_table.add_column("Symbol", no_wrap=True)
        order_table.add_column("Qty", justify="right")
        order_table.add_column("Limit", justify="right")
        order_table.add_column("Filled", justify="right")
        order_table.add_column("Time")
        for row in order_rows:
            order_table.add_row(
                attempt_index.get(str(row.get("execution_attempt_id")), "?"),
                "parent" if row.get("parent_broker_order_id") is None else "leg",
                _compact_status(row.get("order_status")),
                _compact_position_intent(row.get("position_intent")),
                _compact_symbol(row.get("symbol") or row.get("leg_symbol")),
                _compact_number(row.get("quantity")),
                _compact_number(row.get("limit_price")),
                f"{_compact_number(row.get('filled_qty'))}@{_compact_number(row.get('filled_avg_price'))}",
                _render_clock(row.get("updated_at")),
            )
        console.print(order_table)

    fill_rows = [fill for attempt in attempts for fill in attempt.get("fills") or [] if isinstance(fill, dict)]
    if fill_rows:
        fill_table = Table(title="Fills", header_style="bold")
        fill_table.add_column("#")
        fill_table.add_column("Side")
        fill_table.add_column("Symbol", no_wrap=True)
        fill_table.add_column("Qty", justify="right")
        fill_table.add_column("Price", justify="right")
        fill_table.add_column("Filled")
        for row in fill_rows:
            fill_table.add_row(
                attempt_index.get(str(row.get("execution_attempt_id")), "?"),
                _render_value(row.get("side")),
                _compact_symbol(row.get("symbol")),
                _compact_number(row.get("quantity")),
                _compact_number(row.get("price")),
                _render_time(row.get("filled_at")),
            )
        console.print(fill_table)


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


@execution_app.command("list", help="List execution attempts, orders, and fills for one activity date.")
def execution_list_command(
    activity_date: str | None = typer.Option(None, "--date", help="Activity date in YYYY-MM-DD. Defaults to today in New York."),
    trading_strategy_id: str | None = typer.Option(None, "--trading-strategy-id", help="Optional trading strategy owner filter."),
    limit: int = typer.Option(100, "--limit", help="Maximum attempts to list."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI color."),
) -> None:
    try:
        payload = list_execution_activity(
            db_target=db,
            activity_date=activity_date,
            trading_strategy_id=trading_strategy_id,
            limit=limit,
        )
    except (RuntimeError, ValueError) as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(2) from None
    _render_execution_activity(payload, json_output=json_output, no_color=no_color)
    raise typer.Exit(0)


execution_app.command("positions", help="List positions or inspect one position.")(positions_command)
execution_app.command("runtimes", help="Show execution runtime capabilities.")(execution_runtimes_command)


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


__all__ = [
    "execution_app",
    "execution_inspect_command",
    "execution_list_command",
    "execution_runtimes_command",
    "positions_command",
]
