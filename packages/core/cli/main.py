from __future__ import annotations

import sys
from collections.abc import Callable

import typer

from core.cli.backtest import backtest_app
from core.cli.company_valuation import company_valuation_app
from core.cli.config import config_app
from core.cli.deploy import deploy_app
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
from core.services.deployments import (
    DeploymentConfigError,
    get_deploy_target,
    run_target_spreads_command,
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

TARGETABLE_ROOT_COMMANDS = {
    "status",
    "trading",
    "pipelines",
    "automations",
    "opportunities",
    "positions",
    "audit",
    "jobs",
    "uoa",
}


def _has_option(argv: list[str], option_name: str) -> bool:
    return any(raw == option_name or raw.startswith(f"{option_name}=") for raw in argv)


def _strip_target_env_option(argv: list[str]) -> tuple[str | None, list[str]]:
    environment: str | None = None
    filtered: list[str] = []
    index = 0
    while index < len(argv):
        raw = argv[index]
        if raw == "--env":
            if index + 1 >= len(argv):
                raise ValueError("--env requires a target name.")
            environment = str(argv[index + 1]).strip() or None
            index += 2
            continue
        if raw.startswith("--env="):
            environment = raw.split("=", 1)[1].strip() or None
            index += 1
            continue
        filtered.append(raw)
        index += 1
    return environment, filtered


def _root_command(argv: list[str]) -> str | None:
    for raw in argv:
        if raw.startswith("-"):
            continue
        return raw
    return None


def _maybe_run_target_command(argv: list[str]) -> int | None:
    if any(raw in {"-h", "--help"} for raw in argv):
        return None
    environment, filtered = _strip_target_env_option(argv)
    if environment is None:
        return None
    if _has_option(filtered, "--db"):
        raise ValueError("Do not use --db with --env. Target the environment directly.")
    root_command = _root_command(filtered)
    if root_command not in TARGETABLE_ROOT_COMMANDS:
        return None
    target = get_deploy_target(environment)
    return run_target_spreads_command(target, filtered)


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
app.add_typer(company_valuation_app, name="company-valuation")
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
    try:
        target_code = _maybe_run_target_command(list(sys.argv[1:]))
    except (DeploymentConfigError, ValueError) as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise SystemExit(3) from None
    except OSError as exc:
        typer.secho(f"Command failed: {exc}", err=True, fg=typer.colors.RED)
        raise SystemExit(2) from None
    if target_code is not None:
        raise SystemExit(target_code)
    app()


if __name__ == "__main__":
    main()
