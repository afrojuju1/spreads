from __future__ import annotations

import shlex

from core.services.deployment_commands import (
    _can_run_target_locally,
    _compose_base_args,
    _compose_down_args,
    _compose_ps_args,
    _compose_routine_reconcile_args,
    _compose_runtime_verify_args,
    _compose_up_args,
    _ensure_local_env_file,
    _local_target_command_env,
    _remote_shell_command,
    _render_shell_command,
    _run_command,
    _run_passthrough_command,
    _run_target_compose_command,
)
from core.services.deployment_remote import sync_deploy_target
from core.services.deployment_targets import DeployTarget


def start_deploy_target(
    target: DeployTarget,
    *,
    build: bool = True,
    sync: bool = True,
    require_secrets: bool = True,
) -> None:
    if target.is_remote and not _can_run_target_locally(target):
        if sync:
            sync_deploy_target(target, require_secrets=require_secrets)
        command = " ".join(shlex.quote(part) for part in _compose_up_args(target, build=build))
        _run_command(_remote_shell_command(target, command))
        reconcile_command = " ".join(shlex.quote(part) for part in _compose_routine_reconcile_args(target))
        _run_command(_remote_shell_command(target, reconcile_command))
        verify_command = " ".join(shlex.quote(part) for part in _compose_runtime_verify_args(target))
        _run_command(_remote_shell_command(target, verify_command))
        return
    _ensure_local_env_file(target, require_secrets=require_secrets)
    _run_command(_compose_up_args(target, build=build), cwd=target.deploy_path)
    _run_command(_compose_routine_reconcile_args(target), cwd=target.deploy_path)
    _run_command(_compose_runtime_verify_args(target), cwd=target.deploy_path)


def stop_deploy_target(target: DeployTarget) -> None:
    args = _compose_down_args(target)
    if not target.is_remote:
        _ensure_local_env_file(target, require_secrets=False)
        _run_command(args, cwd=target.deploy_path)
        return
    code = _run_target_compose_command(target, args)
    if code:
        raise RuntimeError(f"Command failed with exit {code}: {_render_shell_command(args)}")


def status_deploy_target(target: DeployTarget) -> None:
    args = _compose_ps_args(target)
    if not target.is_remote:
        _ensure_local_env_file(target, require_secrets=False)
        _run_command(args, cwd=target.deploy_path)
        return
    code = _run_target_compose_command(target, args)
    if code:
        raise RuntimeError(f"Command failed with exit {code}: {_render_shell_command(args)}")


def run_target_spreads_command(target: DeployTarget, cli_args: list[str]) -> int:
    command = ["uv", "run", "spreads", *cli_args]
    if target.is_remote and not _can_run_target_locally(target):
        return _run_passthrough_command(_remote_shell_command(target, _render_shell_command(command)))
    cwd = target.deploy_path
    return _run_passthrough_command(
        command,
        cwd=cwd,
        env=_local_target_command_env(target, require_secrets=False),
    )


def exec_spreads_command(target: DeployTarget, cli_args: list[str]) -> int:
    command = ["uv", "run", "spreads", *cli_args]
    if target.is_remote and not _can_run_target_locally(target):
        return _run_passthrough_command(_remote_shell_command(target, _render_shell_command(command)))
    return _run_passthrough_command(
        command,
        cwd=target.deploy_path,
        env=_local_target_command_env(target, require_secrets=False),
    )


def logs_deploy_target(
    target: DeployTarget,
    *,
    services: list[str],
    since: str | None = None,
    tail: int | None = None,
    follow: bool = False,
) -> int:
    args = _compose_base_args(target) + ["logs"]
    if follow:
        args.append("--follow")
    if since:
        args.extend(["--since", since])
    if tail is not None:
        args.extend(["--tail", str(tail)])
    args.extend(services)
    if not target.is_remote:
        _ensure_local_env_file(target, require_secrets=False)
        return _run_passthrough_command(args, cwd=target.deploy_path)
    return _run_target_compose_command(target, args)


def restart_deploy_target_services(target: DeployTarget, services: list[str]) -> int:
    args = _compose_base_args(target) + ["restart", *services]
    if not target.is_remote:
        _ensure_local_env_file(target, require_secrets=False)
        return _run_passthrough_command(args, cwd=target.deploy_path)
    return _run_target_compose_command(target, args)


__all__ = [
    "exec_spreads_command",
    "logs_deploy_target",
    "restart_deploy_target_services",
    "run_target_spreads_command",
    "start_deploy_target",
    "status_deploy_target",
    "stop_deploy_target",
]
