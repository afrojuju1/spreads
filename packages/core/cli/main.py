from __future__ import annotations

from collections.abc import Callable

import typer

from core.cli.backtest import backtest_app
from core.cli.deploy import deploy_app
from core.cli.config import config_app
from core.cli.ops import (
    audit_command,
    jobs_app,
    status_command,
    trading_command,
    uoa_app,
)
from core.cli.runtime import (
    automations_command,
    opportunities_command,
    pipelines_command,
    positions_command,
)
from core.jobs.scheduler import main as scheduler_main
from core.services.alpaca_research import main as research_alpaca_main
from core.services.discovery_runs.runtime import main as discover_main
from core.services.market_recorder import main as market_recorder_main
from core.services.scanners.service import main as scan_main

PASSTHROUGH_CONTEXT_SETTINGS = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
    "help_option_names": [],
}

app = typer.Typer(
    add_completion=True,
    help="Spreads operator CLI.",
    no_args_is_help=True,
)


def _run_passthrough(
    *,
    ctx: typer.Context,
    entrypoint: Callable[[list[str] | None], int],
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


app.command("status", help="Show system and runtime health.")(status_command)
app.command("trading", help="Show live trading safety and readiness.")(trading_command)
app.command(
    "pipelines",
    help="List discovery sessions or inspect one pipeline compatibility view.",
)(
    pipelines_command
)
app.command("automations", help="List bot automation runtimes or inspect one.")(
    automations_command
)
app.command("opportunities", help="List opportunities or inspect one opportunity.")(
    opportunities_command
)
app.command("positions", help="List positions or inspect one position.")(
    positions_command
)
app.command("audit", help="Audit one pipeline date for operator investigation.")(
    audit_command
)
app.add_typer(jobs_app, name="jobs")
app.add_typer(backtest_app, name="backtest")
app.add_typer(config_app, name="config")
app.add_typer(deploy_app, name="deploy")
app.add_typer(uoa_app, name="uoa")


@app.command(
    "scan",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Run the spread scanner.",
)
def scan_command(ctx: typer.Context) -> None:
    _run_passthrough(ctx=ctx, entrypoint=scan_main)


@app.command(
    "discover",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Run a live discovery run session.",
)
def discover_command(ctx: typer.Context) -> None:
    _run_passthrough(ctx=ctx, entrypoint=discover_main)


research_app = typer.Typer(
    add_completion=False,
    help="Run research-oriented diagnostics and reports.",
)


@research_app.command(
    "alpaca",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Inspect Alpaca capability coverage for spreads.",
)
def research_alpaca_command(ctx: typer.Context) -> None:
    _run_passthrough(ctx=ctx, entrypoint=research_alpaca_main)


app.add_typer(research_app, name="research")


@app.command(
    "scheduler",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Run the ARQ scheduler loop.",
)
def scheduler_command(ctx: typer.Context) -> None:
    _run_passthrough(ctx=ctx, entrypoint=scheduler_main)


@app.command(
    "market-recorder",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Run the recovery market recorder loop.",
)
def market_recorder_command(ctx: typer.Context) -> None:
    _run_passthrough(ctx=ctx, entrypoint=market_recorder_main)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
