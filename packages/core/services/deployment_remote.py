from __future__ import annotations

import shlex
import textwrap

from core.services.deployment_commands import (
    _can_run_target_locally,
    _compose_down_args,
    _compose_up_args,
    _compose_routine_reconcile_args,
    _compose_runtime_verify_args,
    _run_command,
    _ssh_command,
    _write_remote_text_file,
)
from core.services.deployment_constants import REPO_ROOT, REPO_SYNC_EXCLUDES
from core.services.deployment_env import render_deploy_env_file, render_host_env_file
from core.services.deployment_targets import DeployTarget, DeploymentConfigError


def sync_deploy_target(
    target: DeployTarget,
    *,
    require_secrets: bool = True,
) -> None:
    if not target.is_remote:
        raise DeploymentConfigError("sync only applies to ssh deployment targets.")
    if _can_run_target_locally(target):
        raise DeploymentConfigError(f"Deploy target {target.name!r} already resolves locally at {target.deploy_root}; sync is not needed.")
    _run_command(_ssh_command(target, f"mkdir -p {shlex.quote(target.deploy_root)}"))
    rsync_command = [
        "rsync",
        "-az",
        "--delete",
    ]
    for pattern in REPO_SYNC_EXCLUDES:
        rsync_command.append(f"--exclude={pattern}")
    rsync_command.extend([f"{REPO_ROOT}/", f"{target.ssh_host}:{target.deploy_root}/"])
    _run_command(rsync_command)
    env_text = render_deploy_env_file(target, require_secrets=require_secrets)
    _write_remote_text_file(
        target,
        remote_path=f"{target.deploy_root}/{target.env_file}",
        text=env_text,
    )
    host_env_text = render_host_env_file(target, require_secrets=require_secrets)
    _write_remote_text_file(
        target,
        remote_path=f"{target.deploy_root}/.env",
        text=host_env_text,
    )


def bootstrap_remote_target(target: DeployTarget) -> None:
    if not target.is_remote:
        raise DeploymentConfigError("bootstrap only applies to ssh deployment targets.")
    root_parent = str(target.remote_parent)
    script = textwrap.dedent(f"""
        set -euo pipefail
        export DEBIAN_FRONTEND=noninteractive
        sudo apt-get update
        sudo apt-get install -y ca-certificates curl gnupg rsync
        sudo install -m 0755 -d /etc/apt/keyrings
        if [ ! -f /etc/apt/keyrings/docker.asc ]; then
          curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo tee /etc/apt/keyrings/docker.asc >/dev/null
          sudo chmod a+r /etc/apt/keyrings/docker.asc
        fi
        if [ ! -f /etc/apt/sources.list.d/docker.list ]; then
          echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable" | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
        fi
        sudo apt-get update
        sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
        sudo install -d -m 0755 /etc/docker
        sudo tee /etc/docker/daemon.json >/dev/null <<'JSON'
        {{
          "log-driver": "local",
          "log-opts": {{
            "max-size": "10m",
            "max-file": "5"
          }}
        }}
        JSON
        sudo systemctl enable --now docker
        sudo systemctl restart docker
        sudo usermod -aG docker "$(id -un)"
        sudo mkdir -p {shlex.quote(root_parent)}
        sudo mkdir -p {shlex.quote(root_parent)}/backups/postgres
        sudo chown -R "$(id -un)":"$(id -gn)" {shlex.quote(root_parent)}
        mkdir -p {shlex.quote(target.deploy_root)}
        """).strip()
    _run_command(_ssh_command(target, script))


def _systemd_unit_text(target: DeployTarget) -> str:
    up_args = " ".join(shlex.quote(part) for part in _compose_up_args(target, build=False))
    reconcile_args = " ".join(shlex.quote(part) for part in _compose_routine_reconcile_args(target))
    verify_args = " ".join(shlex.quote(part) for part in _compose_runtime_verify_args(target))
    down_args = " ".join(shlex.quote(part) for part in _compose_down_args(target))
    return (
        textwrap.dedent(f"""
        [Unit]
        Description=Spreads Docker Compose stack ({target.name})
        Requires=docker.service
        After=docker.service network-online.target
        Wants=network-online.target

        [Service]
        Type=oneshot
        WorkingDirectory={target.deploy_root}
        ExecStart={up_args}
        ExecStartPost={reconcile_args}
        ExecStartPost={verify_args}
        ExecStop={down_args}
        RemainAfterExit=yes
        TimeoutStartSec=0

        [Install]
        WantedBy=multi-user.target
        """).strip()
        + "\n"
    )


def install_systemd_service(target: DeployTarget) -> None:
    if not target.is_remote:
        raise DeploymentConfigError("install-service only applies to ssh deployment targets.")
    unit_text = _systemd_unit_text(target)
    unit_path = f"/tmp/{target.service_name}"
    _write_remote_text_file(target, remote_path=unit_path, text=unit_text)
    command = textwrap.dedent(f"""
        set -euo pipefail
        sudo mv {shlex.quote(unit_path)} /etc/systemd/system/{shlex.quote(target.service_name)}
        sudo systemctl daemon-reload
        sudo systemctl enable --now {shlex.quote(target.service_name)}
        """).strip()
    _run_command(_ssh_command(target, command))


__all__ = [
    "bootstrap_remote_target",
    "install_systemd_service",
    "sync_deploy_target",
]
