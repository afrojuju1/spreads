from __future__ import annotations

from pathlib import Path
from typing import Any

import typer

from core.cli.command_harness import print_command_error, render_json_value, run_action_command
from core.services.deployments import (
    DeploymentConfigError,
    bootstrap_remote_target,
    deploy_target_payload,
    exec_spreads_command,
    get_deploy_target,
    install_systemd_service,
    list_deploy_targets,
    logs_deploy_target,
    render_deploy_env_file,
    render_prod_compose,
    restart_deploy_target_services,
    run_target_spreads_command,
    start_deploy_target,
    status_deploy_target,
    stop_deploy_target,
    sync_deploy_target,
)

deploy_app = typer.Typer(
    add_completion=False,
    help="Manage private single-host spreads deployment targets.",
)

PASSTHROUGH_CONTEXT_SETTINGS = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
}


def _resolve_target(environment: str) -> Any:
    return get_deploy_target(environment)


def _write_text_output(text: str, output: str | None) -> None:
    if output is None:
        typer.echo(text.rstrip())
        return
    path = Path(output)
    path.write_text(text)
    typer.echo(f"Wrote {path}")


def _run_deploy_action(action: Any) -> None:
    run_action_command(
        action,
        handled_error_types=(DeploymentConfigError, OSError, RuntimeError),
        error_exit_code=3,
    )


def _normalized_passthrough_args(args: list[str]) -> list[str]:
    if args and args[0] == "--":
        return list(args[1:])
    return list(args)


def _has_option(argv: list[str], option_name: str) -> bool:
    return any(raw == option_name or raw.startswith(f"{option_name}=") for raw in argv)


@deploy_app.command("targets", help="List configured deployment targets.")
def list_targets_command(
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    def action() -> None:
        targets = list_deploy_targets()
        if json_output:
            render_json_value([deploy_target_payload(target) for target in targets], indent=2)
            return
        for target in targets:
            location = target.ssh_host if target.ssh_host else "local"
            typer.echo(f"{target.name}: {target.mode} ({location})")

    _run_deploy_action(action)


@deploy_app.command("show", help="Show one deployment target definition.")
def show_target_command(
    environment: str = typer.Option(
        "local",
        "--env",
        "--target",
        help="Deployment target name.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    def action() -> None:
        payload = deploy_target_payload(_resolve_target(environment))
        if json_output:
            render_json_value(payload, indent=2)
            return
        for key, value in payload.items():
            typer.echo(f"{key}: {value}")

    _run_deploy_action(action)


@deploy_app.command("render-env", help="Render the deploy env file for one target.")
def render_env_command(
    environment: str = typer.Option(
        "local",
        "--env",
        "--target",
        help="Deployment target name.",
    ),
    output: str | None = typer.Option(
        None,
        "--output",
        help="Optional file path to write instead of stdout.",
    ),
    require_secrets: bool = typer.Option(
        False,
        "--require-secrets",
        help="Fail if required secrets are missing.",
    ),
) -> None:
    def action() -> None:
        text = render_deploy_env_file(
            _resolve_target(environment),
            require_secrets=require_secrets,
        )
        _write_text_output(text, output)

    _run_deploy_action(action)


@deploy_app.command(
    "render-compose",
    help="Render the prod compose file with one target's resolved values.",
)
def render_compose_command(
    environment: str = typer.Option(
        "local",
        "--env",
        "--target",
        help="Deployment target name.",
    ),
    output: str | None = typer.Option(
        None,
        "--output",
        help="Optional file path to write instead of stdout.",
    ),
) -> None:
    def action() -> None:
        text = render_prod_compose(_resolve_target(environment))
        _write_text_output(text, output)

    _run_deploy_action(action)


@deploy_app.command("bootstrap", help="Install Docker and host prerequisites over SSH.")
def bootstrap_command(
    environment: str = typer.Option(
        ...,
        "--env",
        "--target",
        help="Deployment target name.",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        help="Execute the remote bootstrap commands.",
    ),
) -> None:
    if not yes:
        print_command_error("Refusing to bootstrap without --yes.")
        raise typer.Exit(3)
    _run_deploy_action(lambda: bootstrap_remote_target(_resolve_target(environment)))


@deploy_app.command("sync", help="Sync the repo and deploy env file to one SSH target.")
def sync_command(
    environment: str = typer.Option(
        ...,
        "--env",
        "--target",
        help="Deployment target name.",
    ),
    allow_placeholders: bool = typer.Option(
        False,
        "--allow-placeholders",
        help="Allow replace-me secrets in the generated deploy env file.",
    ),
) -> None:
    _run_deploy_action(
        lambda: sync_deploy_target(
            _resolve_target(environment),
            require_secrets=not allow_placeholders,
        )
    )


@deploy_app.command("up", help="Bring one deployment target up with docker compose.")
def up_command(
    environment: str = typer.Option(
        "local",
        "--env",
        "--target",
        help="Deployment target name.",
    ),
    build: bool = typer.Option(True, "--build/--no-build", help="Build images on up."),
    sync: bool = typer.Option(
        True,
        "--sync/--no-sync",
        help="Sync the repo first for ssh targets.",
    ),
    allow_placeholders: bool = typer.Option(
        False,
        "--allow-placeholders",
        help="Allow replace-me secrets in the generated deploy env file.",
    ),
) -> None:
    _run_deploy_action(
        lambda: start_deploy_target(
            _resolve_target(environment),
            build=build,
            sync=sync,
            require_secrets=not allow_placeholders,
        )
    )


@deploy_app.command("down", help="Stop one deployment target with docker compose.")
def down_command(
    environment: str = typer.Option(
        "local",
        "--env",
        "--target",
        help="Deployment target name.",
    ),
) -> None:
    _run_deploy_action(lambda: stop_deploy_target(_resolve_target(environment)))


@deploy_app.command("status", help="Show docker compose status for one target.")
def status_command(
    environment: str = typer.Option(
        "local",
        "--env",
        "--target",
        help="Deployment target name.",
    ),
) -> None:
    _run_deploy_action(lambda: status_deploy_target(_resolve_target(environment)))


@deploy_app.command(
    "exec",
    help="Run a spreads CLI command on the deployed target checkout.",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
)
def exec_command(
    ctx: typer.Context,
    environment: str = typer.Option(
        ...,
        "--env",
        "--target",
        help="Deployment target name.",
    ),
) -> None:
    args = _normalized_passthrough_args(list(ctx.args))
    if not args:
        print_command_error(
            "Provide a spreads CLI command after `spreads deploy exec --env <target> --`.",
        )
        raise typer.Exit(3)
    if _has_option(args, "--db"):
        print_command_error(
            "Do not use --db with deploy targets. Run the command through the named deploy target instead.",
        )
        raise typer.Exit(3)

    def action() -> None:
        code = exec_spreads_command(_resolve_target(environment), args)
        raise typer.Exit(code)

    _run_deploy_action(action)


@deploy_app.command("logs", help="Stream docker compose logs for one deployment target.")
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
    def action() -> None:
        code = logs_deploy_target(
            _resolve_target(environment),
            services=list(services),
            since=since,
            tail=tail,
            follow=follow,
        )
        raise typer.Exit(code)

    _run_deploy_action(action)


@deploy_app.command("restart", help="Restart one or more compose services on a target.")
def restart_command(
    environment: str = typer.Option(
        ...,
        "--env",
        "--target",
        help="Deployment target name.",
    ),
    services: list[str] = typer.Argument(
        ...,
        help="Compose service names to restart.",
    ),
) -> None:
    def action() -> None:
        code = restart_deploy_target_services(
            _resolve_target(environment),
            services=list(services),
        )
        raise typer.Exit(code)

    _run_deploy_action(action)


@deploy_app.command("health", help="Run the standard operator health checks on a target.")
def health_command(
    environment: str = typer.Option(
        ...,
        "--env",
        "--target",
        help="Deployment target name.",
    ),
) -> None:
    def action() -> None:
        target = _resolve_target(environment)
        commands: list[tuple[str, list[str]]] = [
            ("Compose Status", ["deploy", "status", "--env", target.name]),
            ("Trading Ops State", ["ops", "state", "--json"]),
            ("Jobs", ["jobs", "--json"]),
            ("Storage Ops State", ["ops", "storage", "--json"]),
        ]
        exit_code = 0
        for title, args in commands:
            typer.echo(f"\n=== {title} ({target.name}) ===")
            if args[:2] == ["deploy", "status"]:
                status_deploy_target(target)
                continue
            code = run_target_spreads_command(target, args)
            exit_code = max(exit_code, code)
        raise typer.Exit(exit_code)

    _run_deploy_action(action)


@deploy_app.command(
    "install-service",
    help="Install and enable a reboot-safe systemd unit on one SSH target.",
)
def install_service_command(
    environment: str = typer.Option(
        ...,
        "--env",
        "--target",
        help="Deployment target name.",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        help="Execute the remote systemd installation.",
    ),
) -> None:
    if not yes:
        print_command_error("Refusing to install the systemd unit without --yes.")
        raise typer.Exit(3)
    _run_deploy_action(lambda: install_systemd_service(_resolve_target(environment)))


__all__ = ["deploy_app"]
