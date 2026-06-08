from __future__ import annotations

import sys
from collections.abc import Callable

import typer

from core.cli.command_harness import run_passthrough
from core.cli.company_valuation import company_valuation_app
from core.cli.config import config_app
from core.cli.deploy import deploy_app
from core.cli.lifecycle import lifecycle_app
from core.cli.ops import (
    jobs_app,
    ops_app,
    run_storage_ops_state_visibility,
    run_trading_ops_state_visibility,
)
from core.cli.market_intel import market_intel_app
from core.cli.retention import retention_app
from core.cli.runtime import (
    execution_runtimes_command,
    positions_command,
)
from core.services.deployments import (
    DeploymentConfigError,
    get_deploy_target,
    logs_deploy_target,
    run_target_spreads_command,
)

PASSTHROUGH_CONTEXT_SETTINGS = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
}

app = typer.Typer(
    add_completion=True,
    help="Spreads operator CLI.",
    no_args_is_help=True,
)

TARGETABLE_ROOT_COMMANDS = {
    "ops",
    "positions",
    "execution-runtimes",
    "jobs",
    "market-intel",
    "retention",
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


def _targeted_args(
    command_name: str,
    *,
    json_output: bool,
    watch: float | None,
    no_color: bool,
    market_date: str | None = None,
) -> list[str]:
    args = [command_name]
    if market_date is not None:
        args.extend(["--date", market_date])
    if json_output:
        args.append("--json")
    if watch is not None:
        args.extend(["--watch", str(watch)])
    if no_color:
        args.append("--no-color")
    return args


def _run_target_or_local(
    *,
    environment: str | None,
    db: str | None,
    target_args: list[str],
    local_runner: Callable[[], None],
) -> None:
    if environment is None:
        local_runner()
        return
    if db is not None:
        typer.secho("Do not use --db with --env. Target the environment directly.", err=True, fg=typer.colors.RED)
        raise typer.Exit(3)
    try:
        code = run_target_spreads_command(get_deploy_target(environment), target_args)
    except (DeploymentConfigError, OSError, RuntimeError) as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(3) from None
    raise typer.Exit(code)


def _run_trading_visibility_alias(
    command_name: str,
    *,
    environment: str | None,
    market_date: str | None,
    db: str | None,
    json_output: bool,
    watch: float | None,
    no_color: bool,
) -> None:
    _run_target_or_local(
        environment=environment,
        db=db,
        target_args=_targeted_args(
            command_name,
            market_date=market_date,
            json_output=json_output,
            watch=watch,
            no_color=no_color,
        ),
        local_runner=lambda: run_trading_ops_state_visibility(
            market_date=market_date,
            db=db,
            json_output=json_output,
            watch=watch,
            no_color=no_color,
        ),
    )


app.add_typer(ops_app, name="ops")


@app.command("status", help="Show canonical live trading operator status.")
def status_command(
    environment: str | None = typer.Option(None, "--env", "--target", help="Run against a named deploy target."),
    market_date: str | None = typer.Option(None, "--date", help="Market date to inspect. Defaults to today in New York."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(None, "--watch", help="Refresh every N seconds."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    _run_trading_visibility_alias(
        "status",
        environment=environment,
        market_date=market_date,
        db=db,
        json_output=json_output,
        watch=watch,
        no_color=no_color,
    )


@app.command("trading", help="Show canonical live trading operator state.")
def trading_command(
    environment: str | None = typer.Option(None, "--env", "--target", help="Run against a named deploy target."),
    market_date: str | None = typer.Option(None, "--date", help="Market date to inspect. Defaults to today in New York."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(None, "--watch", help="Refresh every N seconds."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    _run_trading_visibility_alias(
        "trading",
        environment=environment,
        market_date=market_date,
        db=db,
        json_output=json_output,
        watch=watch,
        no_color=no_color,
    )


@app.command("storage", help="Show canonical storage and retention operator state.")
def storage_command(
    environment: str | None = typer.Option(None, "--env", "--target", help="Run against a named deploy target."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(None, "--watch", help="Refresh every N seconds."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    _run_target_or_local(
        environment=environment,
        db=db,
        target_args=_targeted_args(
            "storage",
            json_output=json_output,
            watch=watch,
            no_color=no_color,
        ),
        local_runner=lambda: run_storage_ops_state_visibility(
            db=db,
            json_output=json_output,
            watch=watch,
            no_color=no_color,
        ),
    )


app.command("positions", help="List positions or inspect one position.")(positions_command)
app.command("execution-runtimes", help="Show execution runtime capabilities.")(execution_runtimes_command)
app.add_typer(jobs_app, name="jobs")
app.add_typer(company_valuation_app, name="company-valuation")
app.add_typer(config_app, name="config")
app.add_typer(deploy_app, name="deploy")
app.add_typer(lifecycle_app, name="lifecycle")
app.add_typer(retention_app, name="retention")
app.add_typer(market_intel_app, name="market-intel")


@app.command("logs", help="Stream deployment logs for one target.")
def logs_command(
    environment: str = typer.Option(
        ...,
        "--env",
        "--target",
        help="Deployment target name.",
    ),
    services: list[str] = typer.Argument(
        [],
        help="Optional compose service names. Defaults to all services.",
    ),
    since: str | None = typer.Option(
        "5m",
        "--since",
        help="Show logs since this duration or timestamp.",
    ),
    tail: int | None = typer.Option(
        200,
        "--tail",
        help="Maximum recent log lines per service.",
    ),
    follow: bool = typer.Option(
        False,
        "--follow",
        "-f",
        help="Follow logs until interrupted.",
    ),
) -> None:
    try:
        code = logs_deploy_target(
            get_deploy_target(environment),
            services=list(services),
            since=since,
            tail=tail,
            follow=follow,
        )
    except (DeploymentConfigError, OSError, RuntimeError) as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(3) from None
    raise typer.Exit(code)


@app.command(
    "scheduler",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Run the ARQ scheduler loop.",
)
def scheduler_command(ctx: typer.Context) -> None:
    from core.jobs.scheduler import main as scheduler_main

    run_passthrough(ctx=ctx, entrypoint=scheduler_main)


@app.command(
    "market-recorder",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Run the recovery market recorder loop.",
)
def market_recorder_command(ctx: typer.Context) -> None:
    from core.services.market_recorder import main as market_recorder_main

    run_passthrough(ctx=ctx, entrypoint=market_recorder_main)


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
