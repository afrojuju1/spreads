from __future__ import annotations

import json
from typing import Any

import typer

from core.cli.ops_render import build_console, render_json_payload
from core.services.lifecycle_schema import build_lifecycle_schema_summary
from core.services.paper_lifecycle_smoke import (
    DEFAULT_AUTO_SELECT_MAX_DTE,
    DEFAULT_AUTO_SELECT_MIN_DTE,
    DEFAULT_MAX_DEBIT_DOLLARS,
    DEFAULT_TTL_MINUTES,
    create_synthetic_paper_close_smoke,
    create_synthetic_paper_open_smoke,
    inspect_synthetic_paper_smoke,
)

lifecycle_app = typer.Typer(
    add_completion=False,
    help="Inspect the target trading lifecycle schema.",
    no_args_is_help=True,
)
paper_smoke_app = typer.Typer(
    add_completion=False,
    help="Run synthetic_validation paper lifecycle smoke checks.",
    no_args_is_help=True,
)


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
    "lifecycle_schema_command",
    "paper_smoke_open_command",
    "paper_smoke_close_command",
    "paper_smoke_status_command",
]
