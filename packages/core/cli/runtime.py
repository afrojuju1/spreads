from __future__ import annotations

import json
from typing import Any

import typer
from rich.panel import Panel
from rich.table import Table

from core.cli.command_harness import run_passthrough
from core.cli.ops_render import (
    build_console,
    render_json_payload,
)
from core.services.execution.runtimes import resolve_execution_runtime_capabilities
from core.services.positions import get_position_detail, list_positions

PASSTHROUGH_CONTEXT_SETTINGS = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
    "help_option_names": [],
}

runtime_app = typer.Typer(
    add_completion=False,
    help="Run internal runtime processes.",
    no_args_is_help=True,
)


def _render_value(value: Any) -> str:
    if value is None:
        return "-"
    return str(value)


def _render_money(value: Any) -> str:
    if value is None:
        return "-"
    return f"${float(value):,.2f}"


def _truncate(value: Any, *, length: int = 32) -> str:
    text = _render_value(value)
    if len(text) <= length:
        return text
    return text[: max(length - 3, 0)].rstrip() + "..."


def _short_id(value: Any) -> str:
    text = _render_value(value)
    if text == "-":
        return text
    tail = text.rsplit(":", 1)[-1]
    return tail[-8:] if len(tail) > 8 else tail


def _date_part(value: Any) -> str:
    text = _render_value(value)
    if text == "-":
        return text
    return text[:10]


def _render_positions(payload: dict[str, Any], *, no_color: bool) -> None:
    console = build_console(no_color=no_color)
    summary = dict(payload.get("summary") or {})
    positions = [row for row in payload.get("positions") or [] if isinstance(row, dict)]

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Positions", _render_value(summary.get("position_count")))
    overview.add_row(
        "Open/Closed",
        f"{_render_value(summary.get('open_position_count'))} open | {_render_value(summary.get('closed_position_count'))} closed",
    )
    overview.add_row("Market Date", _render_value(summary.get("market_date")))
    overview.add_row("Strategy", _render_value(summary.get("trading_strategy_id")))
    close_lifecycle = summary.get("close_lifecycle") if isinstance(summary.get("close_lifecycle"), dict) else {}
    if close_lifecycle:
        overview.add_row(
            "Live Close Work",
            (
                f"{_render_value(close_lifecycle.get('live_action_status') or close_lifecycle.get('status'))} | "
                f"pending {_render_value(close_lifecycle.get('pending_close_intent_count'))} | "
                f"active {_render_value(close_lifecycle.get('active_close_attempt_count'))} | "
                f"failed {_render_value(close_lifecycle.get('failed_close_attempt_count'))}"
            ),
        )
        overview.add_row(
            "Close Accounting",
            (
                f"attempts {_render_value(close_lifecycle.get('accounting_close_attempt_count'))} | "
                f"intents {_render_value(close_lifecycle.get('accounting_close_intent_count'))} | "
                f"missing decisions {_render_value(close_lifecycle.get('accounting_missing_close_decision_count'))}"
            ),
        )
    console.print(Panel(overview, title="Positions"))

    if not positions:
        return

    table = Table(title="Position Rows", header_style="bold")
    table.add_column("ID")
    table.add_column("Sym")
    table.add_column("Strategy")
    table.add_column("Status")
    table.add_column("Qty", justify="right")
    table.add_column("Entry", justify="right")
    table.add_column("Net PnL", justify="right")
    table.add_column("Opened")
    table.add_column("Exit")
    for row in positions:
        table.add_row(
            _short_id(row.get("position_id")),
            _render_value(row.get("underlying_symbol") or row.get("root_symbol")),
            _truncate(row.get("trading_strategy_id") or row.get("strategy"), length=20),
            _render_value(row.get("position_status") or row.get("status")),
            _render_value(row.get("remaining_quantity")),
            _render_money(row.get("entry_value")),
            _render_money(row.get("net_pnl")),
            _date_part(row.get("opened_at")),
            _truncate(row.get("last_exit_reason"), length=18),
        )
    console.print(table)


def _render_position_detail(payload: dict[str, Any], *, no_color: bool) -> None:
    console = build_console(no_color=no_color)
    overview = Table.grid(padding=(0, 2))
    overview.add_row("Position", _render_value(payload.get("position_id")))
    overview.add_row("Strategy", _render_value(payload.get("trading_strategy_id") or payload.get("strategy")))
    overview.add_row("Underlying", _render_value(payload.get("underlying_symbol") or payload.get("root_symbol")))
    overview.add_row("Status", _render_value(payload.get("position_status") or payload.get("status")))
    overview.add_row("Quantity", f"{_render_value(payload.get('remaining_quantity'))} remaining")
    overview.add_row("Entry", _render_money(payload.get("entry_value")))
    overview.add_row("Net PnL", _render_money(payload.get("net_pnl")))
    overview.add_row("Opened", _render_value(payload.get("opened_at")))
    overview.add_row("Closed", _render_value(payload.get("closed_at")))
    overview.add_row("Last Exit", _render_value(payload.get("last_exit_reason")))
    overview.add_row("Reconciliation", _render_value(payload.get("reconciliation_status")))
    console.print(Panel(overview, title="Position Detail"))

    legs = [row for row in payload.get("legs") or [] if isinstance(row, dict)]
    if legs:
        table = Table(title="Legs", header_style="bold")
        table.add_column("Role")
        table.add_column("Side")
        table.add_column("Symbol")
        table.add_column("Intent")
        table.add_column("Strike", justify="right")
        table.add_column("Expiration")
        for leg in legs:
            table.add_row(
                _render_value(leg.get("role")),
                _render_value(leg.get("side")),
                _render_value(leg.get("symbol")),
                _render_value(leg.get("position_intent")),
                _render_value(leg.get("strike")),
                _render_value(leg.get("expiration_date")),
            )
        console.print(table)

    closes = [row for row in payload.get("closes") or [] if isinstance(row, dict)]
    if closes:
        table = Table(title="Closes", header_style="bold")
        table.add_column("Attempt")
        table.add_column("Qty", justify="right")
        table.add_column("Exit", justify="right")
        table.add_column("PnL", justify="right")
        table.add_column("Closed")
        for row in closes:
            table.add_row(
                _truncate(row.get("execution_attempt_id"), length=28),
                _render_value(row.get("closed_quantity")),
                _render_money(row.get("exit_value")),
                _render_money(row.get("realized_pnl")),
                _truncate(row.get("closed_at"), length=22),
            )
        console.print(table)


def _render_execution_runtimes(payload: dict[str, Any], *, no_color: bool) -> None:
    console = build_console(no_color=no_color)
    table = Table(title="Execution Runtimes", header_style="bold")
    table.add_column("Runtime")
    table.add_column("Status")
    table.add_column("Entries", justify="right")
    table.add_column("Families")
    table.add_column("Capabilities")
    for row in payload.get("runtimes") or []:
        if not isinstance(row, dict):
            continue
        families = row.get("strategy_families")
        family_text = "-"
        if isinstance(families, dict) and families:
            family_text = ", ".join(f"{name} {count}" for name, count in sorted(families.items()))
        capabilities = [
            str(item.get("name")) for item in row.get("capabilities") or [] if isinstance(item, dict) and item.get("status") != "unsupported"
        ]
        table.add_row(
            str(row.get("runtime") or "-"),
            str(row.get("status") or "-"),
            str(row.get("entry_strategy_count") or 0),
            family_text,
            ", ".join(capabilities) or "-",
        )
    console.print(table)


def execution_runtimes_command(
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    payload = resolve_execution_runtime_capabilities()
    if json_output:
        console = build_console(no_color=no_color)
        render_json_payload(console, payload)
        return
    _render_execution_runtimes(payload, no_color=no_color)


def positions_command(
    position_id: str | None = typer.Argument(None, help="Position id to inspect."),
    trading_strategy_id: str | None = typer.Option(
        None,
        "--trading-strategy-id",
        help="Optional trading strategy owner filter.",
    ),
    date: str | None = typer.Option(None, "--date", help="Optional market date."),
    limit: int = typer.Option(50, "--limit", help="Maximum positions to list."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    payload = (
        list_positions(
            db_target=db,
            market_date=date,
            trading_strategy_id=trading_strategy_id,
            limit=limit,
        )
        if position_id is None
        else get_position_detail(db_target=db, position_id=position_id)
    )
    if json_output:
        typer.echo(json.dumps(payload, indent=2, default=str))
        return
    if position_id is None:
        _render_positions(payload, no_color=no_color)
    else:
        _render_position_detail(payload, no_color=no_color)


@runtime_app.command(
    "scheduler",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Run the ARQ scheduler loop.",
)
def scheduler_command(ctx: typer.Context) -> None:
    from core.jobs.scheduler import main as scheduler_main

    run_passthrough(ctx=ctx, entrypoint=scheduler_main)


@runtime_app.command(
    "market-recorder",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Run the market recorder loop.",
)
def market_recorder_command(ctx: typer.Context) -> None:
    from core.services.market_recorder import main as market_recorder_main

    run_passthrough(ctx=ctx, entrypoint=market_recorder_main)
