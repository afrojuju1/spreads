from __future__ import annotations

import os
import shlex
import subprocess
import tempfile
from pathlib import Path

from core.services.deployment_constants import REPO_ROOT
from core.services.deployment_env import build_host_env_values, render_deploy_env_file
from core.services.deployment_targets import DeployTarget, DeploymentConfigError


def _run_command(command: list[str], *, cwd: Path | None = None) -> None:
    try:
        subprocess.run(command, cwd=cwd, check=True)
    except subprocess.CalledProcessError as exc:
        rendered = " ".join(shlex.quote(part) for part in exc.cmd)
        raise RuntimeError(f"Command failed with exit {exc.returncode}: {rendered}") from exc


def _run_passthrough_command(
    command: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> int:
    completed = subprocess.run(command, cwd=cwd, env=env, check=False)
    return int(completed.returncode)


def _ssh_command(target: DeployTarget, command: str) -> list[str]:
    if not target.ssh_host:
        raise DeploymentConfigError(f"Deploy target {target.name!r} has no ssh host.")
    return ["ssh", target.ssh_host, command]


def _remote_shell_command(target: DeployTarget, command: str) -> list[str]:
    quoted_root = shlex.quote(target.deploy_root)
    return _ssh_command(target, f"cd {quoted_root} && {command}")


def _compose_base_args(target: DeployTarget) -> list[str]:
    args = [
        "docker",
        "compose",
        "--env-file",
        target.env_file,
        "-f",
        target.compose_file,
    ]
    if target.web_enabled:
        args.extend(["--profile", "web"])
    args.extend(["--profile", "container-valuation"])
    args.extend(["--profile", "container-research"])
    return args


def _compose_up_args(target: DeployTarget, *, build: bool) -> list[str]:
    args = _compose_base_args(target)
    args.extend(["up", "-d", "--remove-orphans"])
    if build:
        args.append("--build")
    args.extend(["--scale", f"worker-runtime={target.worker_runtime_replicas}"])
    args.extend(["--scale", f"worker-data={target.worker_data_replicas}"])
    args.extend(["--scale", f"worker-valuation={target.worker_valuation_replicas}"])
    args.extend(["--scale", f"worker-research={target.worker_research_replicas}"])
    return args


def _compose_down_args(target: DeployTarget) -> list[str]:
    return _compose_base_args(target) + ["down", "--remove-orphans"]


def _compose_ps_args(target: DeployTarget) -> list[str]:
    return _compose_base_args(target) + ["ps"]


def _render_shell_command(command: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in command)


def _ensure_local_env_file(
    target: DeployTarget,
    *,
    require_secrets: bool,
) -> Path:
    env_text = render_deploy_env_file(target, require_secrets=require_secrets)
    path = REPO_ROOT / target.env_file
    path.write_text(env_text)
    return path


def _local_target_command_env(
    target: DeployTarget,
    *,
    require_secrets: bool = False,
) -> dict[str, str]:
    env = os.environ.copy()
    env.update(build_host_env_values(target, require_secrets=require_secrets))
    return env


def _can_run_target_locally(target: DeployTarget) -> bool:
    try:
        return target.deploy_path.exists()
    except OSError:
        return False


def _run_target_compose_command(
    target: DeployTarget,
    args: list[str],
    *,
    ensure_env_file: bool = False,
) -> int:
    if target.is_remote and not _can_run_target_locally(target):
        return _run_passthrough_command(_remote_shell_command(target, _render_shell_command(args)))
    if ensure_env_file and not target.is_remote:
        _ensure_local_env_file(target, require_secrets=False)
    return _run_passthrough_command(args, cwd=target.deploy_path)


def _write_remote_text_file(target: DeployTarget, *, remote_path: str, text: str) -> None:
    with tempfile.NamedTemporaryFile("w", delete=False) as handle:
        handle.write(text)
        temp_path = Path(handle.name)
    try:
        remote_target = f"{target.ssh_host}:{remote_path}"
        _run_command(["scp", str(temp_path), remote_target])
    finally:
        temp_path.unlink(missing_ok=True)


__all__ = [
    "_can_run_target_locally",
    "_compose_base_args",
    "_compose_down_args",
    "_compose_ps_args",
    "_compose_up_args",
    "_ensure_local_env_file",
    "_local_target_command_env",
    "_remote_shell_command",
    "_render_shell_command",
    "_run_command",
    "_run_passthrough_command",
    "_run_target_compose_command",
    "_ssh_command",
    "_write_remote_text_file",
]
