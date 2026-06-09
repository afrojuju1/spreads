from __future__ import annotations

import sys

import typer

from core.cli.command_harness import run_passthrough
from core.cli.company_valuation import company_valuation_app
from core.cli.config import config_app
from core.cli.deploy import deploy_app
from core.cli.execution import execution_app
from core.cli.lifecycle import lifecycle_app
from core.cli.ops import jobs_app, ops_app
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
    "execution",
    "jobs",
    "lifecycle",
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


app.add_typer(ops_app, name="ops")


app.command("positions", help="List positions or inspect one position.")(positions_command)
app.command("execution-runtimes", help="Show execution runtime capabilities.")(execution_runtimes_command)
app.add_typer(execution_app, name="execution")
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
