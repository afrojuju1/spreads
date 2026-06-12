from __future__ import annotations

from typing import Any

import typer
from rich.panel import Panel
from rich.table import Table

from core.cli.ops_render import build_console, render_json_payload
from core.services.paper_lifecycle_smoke import (
    DEFAULT_AUTO_SELECT_MAX_DTE,
    DEFAULT_AUTO_SELECT_MIN_DTE,
    DEFAULT_MAX_DEBIT_DOLLARS,
    DEFAULT_TTL_MINUTES,
    create_synthetic_paper_close_smoke,
    create_synthetic_paper_open_smoke,
    inspect_synthetic_paper_smoke,
)
from core.services.trading_engine.strategy_runtime import run_trading_strategy_entry_observation

lifecycle_app = typer.Typer(
    add_completion=False,
    help="Run explicit trading lifecycle workflows.",
    no_args_is_help=True,
)
paper_smoke_app = typer.Typer(
    add_completion=False,
    help="Run synthetic_validation paper lifecycle smoke checks.",
    no_args_is_help=True,
)


def _render_value(value: Any) -> str:
    if value in (None, ""):
        return "-"
    return str(value)


def _render_strategy_observation_result(payload: dict[str, Any], *, no_color: bool) -> None:
    console = build_console(no_color=no_color)
    candidate_generation = payload.get("candidate_generation") if isinstance(payload.get("candidate_generation"), dict) else {}
    candidate_build = candidate_generation.get("candidate_build") if isinstance(candidate_generation.get("candidate_build"), dict) else {}
    strategy_run = candidate_generation.get("strategy_run") if isinstance(candidate_generation.get("strategy_run"), dict) else {}
    engine_facts = candidate_generation.get("engine_facts") if isinstance(candidate_generation.get("engine_facts"), dict) else {}

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Status", _render_value(payload.get("status")))
    overview.add_row("Strategy", _render_value(payload.get("trading_strategy_id")))
    overview.add_row("Market Date", _render_value(payload.get("market_date")))
    overview.add_row("Run Mode", _render_value(payload.get("entry_run_mode")))
    overview.add_row("Provenance", _render_value(payload.get("validation_provenance")))
    overview.add_row("Signals", _render_value(payload.get("signal_count")))
    overview.add_row("Decisions", _render_value(payload.get("decision_count")))
    overview.add_row("Admissions", _render_value(payload.get("admission_count")))
    overview.add_row("Execution Intent", _render_value(payload.get("execution_intent_id")))
    console.print(Panel(overview, title="Strategy Observation"))

    table = Table(title="Observation Evidence", header_style="bold")
    table.add_column("Metric")
    table.add_column("Value")
    table.add_row("Strategy Run", _render_value(strategy_run.get("strategy_run_id")))
    table.add_row("Candidate Run", _render_value(engine_facts.get("candidate_run_id")))
    table.add_row("Candidates", _render_value(candidate_build.get("candidate_count") or engine_facts.get("trade_candidate_count")))
    table.add_row("Trade Signals", _render_value(engine_facts.get("trade_signal_count")))
    table.add_row("Selected Decisions", _render_value(len(list(payload.get("selected_decision_ids") or []))))
    table.add_row("Reason", _render_value(payload.get("reason") or candidate_generation.get("reason")))
    console.print(table)


def _render_smoke_payload(payload: dict[str, Any], *, json_output: bool, no_color: bool) -> None:
    if json_output:
        render_json_payload(build_console(no_color=True), payload)
        return
    typer.echo(f"status: {payload.get('status')}")
    typer.echo(f"action: {payload.get('action', '-')}")
    typer.echo(f"intent: {payload.get('execution_intent_id', '-')}")
    if payload.get("run_id"):
        typer.echo(f"run: {payload.get('run_id')}")
    blockers = [row for row in payload.get("blockers") or [] if isinstance(row, dict)]
    if blockers:
        typer.echo("blockers:")
        for row in blockers:
            typer.echo(f"  - {row.get('code')}: {row.get('message')}")
    request = payload.get("request") if isinstance(payload.get("request"), dict) else {}
    if request:
        symbol = request.get("symbol") or request.get("position_id")
        if symbol:
            typer.echo(f"target: {symbol}")
        if request.get("limit_price") is not None:
            typer.echo(f"limit_price: {request.get('limit_price')}")
    dispatch = payload.get("dispatch") if isinstance(payload.get("dispatch"), dict) else None
    if dispatch:
        typer.echo(f"dispatch: {dispatch.get('status')} {dispatch.get('job_run_id', '')}".rstrip())
    if no_color:
        return


def _exit_if_blocked(payload: dict[str, Any]) -> None:
    if payload.get("status") == "blocked":
        raise typer.Exit(2)


@lifecycle_app.command(
    "observe-strategy",
    help="Run one authored strategy entry routine and persist observation-only analysis evidence.",
)
def observe_strategy_command(
    trading_strategy_id: str = typer.Argument(..., help="Authored trading strategy id to observe."),
    market_date: str | None = typer.Option(None, "--date", help="Market date for the observation run. Defaults to today."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    ignore_schedule: bool = typer.Option(False, "--ignore-schedule", help="Run even when the strategy entry routine is outside its schedule window."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    payload = run_trading_strategy_entry_observation(
        db_target=db,
        trading_strategy_id=trading_strategy_id,
        market_date=market_date,
        respect_schedule=not ignore_schedule,
    )
    if json_output:
        render_json_payload(build_console(no_color=True), payload)
        return
    _render_strategy_observation_result(payload, no_color=no_color)


@paper_smoke_app.command("open", help="Preview or create a synthetic paper open lifecycle run.")
def paper_smoke_open_command(
    execute: bool = typer.Option(False, "--execute", help="Create the intent and request dispatch. Defaults to preview."),
    auto_select: bool = typer.Option(False, "--auto-select", help="Select a quoted allowed contract under the debit cap."),
    underlying_symbol: str | None = typer.Option(None, "--underlying-symbol", help="Underlying symbol for the option contract."),
    contract_symbol: str | None = typer.Option(None, "--contract-symbol", help="Exact option contract symbol."),
    expiration_date: str | None = typer.Option(None, "--expiration-date", help="Option expiration date."),
    option_type: str = typer.Option("call", "--option-type", help="Option type: call or put."),
    strike: float | None = typer.Option(None, "--strike", help="Option strike."),
    quantity: int = typer.Option(1, "--quantity", help="Contract quantity."),
    limit_price: float | None = typer.Option(None, "--limit-price", help="Limit debit per contract."),
    max_debit_dollars: float = typer.Option(DEFAULT_MAX_DEBIT_DOLLARS, "--max-debit", help="Maximum total debit dollars."),
    ttl_minutes: int = typer.Option(DEFAULT_TTL_MINUTES, "--ttl-minutes", help="Intent TTL in minutes."),
    allow_underlying: list[str] | None = typer.Option(None, "--allow-underlying", help="Allowed underlying symbol. Repeatable or comma-separated."),
    allow_contract: list[str] | None = typer.Option(None, "--allow-contract", help="Allowed exact contract symbol. Repeatable or comma-separated."),
    auto_select_min_dte: int = typer.Option(DEFAULT_AUTO_SELECT_MIN_DTE, "--auto-select-min-dte", help="Auto-select minimum DTE."),
    auto_select_max_dte: int = typer.Option(DEFAULT_AUTO_SELECT_MAX_DTE, "--auto-select-max-dte", help="Auto-select maximum DTE."),
    request_dispatch: bool = typer.Option(True, "--request-dispatch/--no-request-dispatch", help="Queue dispatch after creating the intent."),
    calendar_name: str = typer.Option("NYSE", "--calendar", help="Market calendar for market-hours guard."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI color."),
) -> None:
    payload = create_synthetic_paper_open_smoke(
        db_target=db,
        execute=execute,
        auto_select=auto_select,
        underlying_symbol=underlying_symbol,
        contract_symbol=contract_symbol,
        expiration_date=expiration_date,
        option_type=option_type,
        strike=strike,
        quantity=quantity,
        limit_price=limit_price,
        max_debit_dollars=max_debit_dollars,
        ttl_minutes=ttl_minutes,
        allow_underlyings=allow_underlying,
        allow_contracts=allow_contract,
        calendar_name=calendar_name,
        auto_select_min_dte=auto_select_min_dte,
        auto_select_max_dte=auto_select_max_dte,
        request_dispatch=request_dispatch,
    )
    _render_smoke_payload(payload, json_output=json_output, no_color=no_color)
    _exit_if_blocked(payload)


@paper_smoke_app.command("close", help="Preview or create a synthetic paper close lifecycle run.")
def paper_smoke_close_command(
    position_id: str = typer.Argument(..., help="Synthetic_validation position id to close."),
    execute: bool = typer.Option(False, "--execute", help="Create the close intent and request dispatch. Defaults to preview."),
    limit_price: float | None = typer.Option(None, "--limit-price", help="Optional close limit price."),
    ttl_minutes: int = typer.Option(DEFAULT_TTL_MINUTES, "--ttl-minutes", help="Intent TTL in minutes."),
    request_dispatch: bool = typer.Option(True, "--request-dispatch/--no-request-dispatch", help="Queue dispatch after creating the intent."),
    calendar_name: str = typer.Option("NYSE", "--calendar", help="Market calendar for market-hours guard."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI color."),
) -> None:
    payload = create_synthetic_paper_close_smoke(
        db_target=db,
        execute=execute,
        position_id=position_id,
        limit_price=limit_price,
        ttl_minutes=ttl_minutes,
        calendar_name=calendar_name,
        request_dispatch=request_dispatch,
    )
    _render_smoke_payload(payload, json_output=json_output, no_color=no_color)
    _exit_if_blocked(payload)


@paper_smoke_app.command("status", help="Inspect synthetic paper lifecycle evidence for an intent.")
def paper_smoke_status_command(
    execution_intent_id: str = typer.Argument(..., help="Execution intent id or synthetic run id."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI color."),
) -> None:
    payload = inspect_synthetic_paper_smoke(
        db_target=db,
        execution_intent_id=execution_intent_id,
    )
    if json_output:
        render_json_payload(build_console(no_color=True), payload)
        return
    typer.echo(f"status: {payload.get('status')}")
    typer.echo(f"intent: {payload.get('execution_intent_id')}")
    typer.echo(f"provenance: {payload.get('validation_provenance')}")
    checks = payload.get("lifecycle_checks") if isinstance(payload.get("lifecycle_checks"), dict) else {}
    if checks:
        typer.echo("checks:")
        for key, value in checks.items():
            typer.echo(f"  - {key}: {'yes' if value else 'no'}")
    if no_color:
        return


lifecycle_app.add_typer(paper_smoke_app, name="paper-smoke")


__all__ = [
    "lifecycle_app",
    "observe_strategy_command",
    "paper_smoke_open_command",
    "paper_smoke_close_command",
    "paper_smoke_status_command",
]
