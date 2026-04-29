from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer

from core.services.deployments import (
    DeploymentConfigError,
    bootstrap_remote_target,
    deploy_target_payload,
    get_deploy_target,
    install_systemd_service,
    list_deploy_targets,
    render_deploy_env_file,
    render_prod_compose,
    start_deploy_target,
    status_deploy_target,
    stop_deploy_target,
    sync_deploy_target,
)

deploy_app = typer.Typer(
    add_completion=False,
    help="Manage private single-host spreads deployment targets.",
)


def _resolve_target(environment: str) -> Any:
    return get_deploy_target(environment)


def _write_text_output(text: str, output: str | None) -> None:
    if output is None:
        typer.echo(text.rstrip())
        return
    path = Path(output)
    path.write_text(text)
    typer.echo(f"Wrote {path}")


def _handle_deploy_error(exc: Exception) -> None:
    typer.secho(str(exc), err=True, fg=typer.colors.RED)
    raise typer.Exit(3) from None


@deploy_app.command("targets", help="List configured deployment targets.")
def list_targets_command(
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    targets = list_deploy_targets()
    if json_output:
        typer.echo(
            json.dumps([deploy_target_payload(target) for target in targets], indent=2)
        )
        return
    for target in targets:
        location = target.ssh_host if target.ssh_host else "local"
        typer.echo(f"{target.name}: {target.mode} ({location})")


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
    try:
        payload = deploy_target_payload(_resolve_target(environment))
    except DeploymentConfigError as exc:
        _handle_deploy_error(exc)
    if json_output:
        typer.echo(json.dumps(payload, indent=2))
        return
    for key, value in payload.items():
        typer.echo(f"{key}: {value}")


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
    try:
        text = render_deploy_env_file(
            _resolve_target(environment),
            require_secrets=require_secrets,
        )
        _write_text_output(text, output)
    except DeploymentConfigError as exc:
        _handle_deploy_error(exc)


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
    try:
        text = render_prod_compose(_resolve_target(environment))
        _write_text_output(text, output)
    except DeploymentConfigError as exc:
        _handle_deploy_error(exc)


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
        typer.secho("Refusing to bootstrap without --yes.", err=True, fg=typer.colors.RED)
        raise typer.Exit(3)
    try:
        bootstrap_remote_target(_resolve_target(environment))
    except (DeploymentConfigError, OSError, RuntimeError) as exc:
        _handle_deploy_error(exc)


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
    try:
        sync_deploy_target(
            _resolve_target(environment),
            require_secrets=not allow_placeholders,
        )
    except (DeploymentConfigError, OSError, RuntimeError) as exc:
        _handle_deploy_error(exc)


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
    try:
        start_deploy_target(
            _resolve_target(environment),
            build=build,
            sync=sync,
            require_secrets=not allow_placeholders,
        )
    except (DeploymentConfigError, OSError, RuntimeError) as exc:
        _handle_deploy_error(exc)


@deploy_app.command("down", help="Stop one deployment target with docker compose.")
def down_command(
    environment: str = typer.Option(
        "local",
        "--env",
        "--target",
        help="Deployment target name.",
    ),
) -> None:
    try:
        stop_deploy_target(_resolve_target(environment))
    except (DeploymentConfigError, OSError, RuntimeError) as exc:
        _handle_deploy_error(exc)


@deploy_app.command("status", help="Show docker compose status for one target.")
def status_command(
    environment: str = typer.Option(
        "local",
        "--env",
        "--target",
        help="Deployment target name.",
    ),
) -> None:
    try:
        status_deploy_target(_resolve_target(environment))
    except (DeploymentConfigError, OSError, RuntimeError) as exc:
        _handle_deploy_error(exc)


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
        typer.secho(
            "Refusing to install the systemd unit without --yes.",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(3)
    try:
        install_systemd_service(_resolve_target(environment))
    except (DeploymentConfigError, OSError, RuntimeError) as exc:
        _handle_deploy_error(exc)


__all__ = ["deploy_app"]
