from __future__ import annotations

import shlex
import textwrap
from pathlib import Path

from core.services.deployment_commands import (
    _can_run_target_locally,
    _compose_down_args,
    _compose_up_args,
    _run_command,
    _ssh_command,
    _write_remote_text_file,
)
from core.services.deployment_constants import DEPLOY_OPS_ROOT, REPO_ROOT, REPO_SYNC_EXCLUDES
from core.services.deployment_env import render_deploy_env_file, render_host_env_file
from core.services.deployment_paths import _backup_root, _ops_log_dir, _ops_root
from core.services.deployment_targets import DeployTarget, DeploymentConfigError


def _repo_ops_script_paths() -> tuple[Path, ...]:
    return (
        DEPLOY_OPS_ROOT / "compose_up.sh",
        DEPLOY_OPS_ROOT / "backup_postgres.sh",
        DEPLOY_OPS_ROOT / "health_check.sh",
        DEPLOY_OPS_ROOT / "trading_ops_monitor.sh",
        DEPLOY_OPS_ROOT / "rotate_ops_logs.sh",
    )


def _ensure_repo_ops_scripts_exist() -> None:
    missing = [path for path in _repo_ops_script_paths() if not path.exists()]
    if missing:
        rendered = ", ".join(str(path.relative_to(REPO_ROOT)) for path in missing)
        raise DeploymentConfigError(f"Missing repo ops scripts: {rendered}.")


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
        ExecStop={down_args}
        RemainAfterExit=yes
        TimeoutStartSec=0

        [Install]
        WantedBy=multi-user.target
        """).strip()
        + "\n"
    )


def _ops_script_command(target: DeployTarget, script_path: Path) -> str:
    env_values = {
        "SPREADS_DEPLOY_ENV": target.name,
        "SPREADS_CONTAINER_ENV_FILE": target.env_file,
        "SPREADS_COMPOSE_FILE": target.compose_file,
        "SPREADS_WEB_ENABLED": "true" if target.web_enabled else "false",
        "SPREADS_WORKER_RUNTIME_REPLICAS": str(target.worker_runtime_replicas),
        "SPREADS_WORKER_DATA_REPLICAS": str(target.worker_data_replicas),
        "SPREADS_WORKER_VALUATION_REPLICAS": str(target.worker_valuation_replicas),
        "SPREADS_WORKER_RESEARCH_REPLICAS": str(target.worker_research_replicas),
        "SPREADS_BACKUP_RETENTION_DAYS": str(target.backup_retention_days),
        "SPREADS_BACKUP_ROOT": str(_backup_root(target)),
        "SPREADS_OPS_LOG_DIR": str(_ops_log_dir(target)),
    }
    return " ".join(
        [
            "env",
            *(shlex.quote(f"{key}={value}") for key, value in env_values.items()),
            "/bin/bash",
            shlex.quote(str(script_path)),
        ]
    )


def _cron_block_text(target: DeployTarget) -> str:
    marker = f"spreads-ops:{target.name}"
    compose_up = _ops_root(target) / "compose_up.sh"
    backup = _ops_root(target) / "backup_postgres.sh"
    health = _ops_root(target) / "health_check.sh"
    trading_ops_monitor = _ops_root(target) / "trading_ops_monitor.sh"
    rotate_logs = _ops_root(target) / "rotate_ops_logs.sh"
    log_dir = _ops_log_dir(target)
    health_minutes = max(int(target.health_check_minutes), 1)
    return "\n".join(
        [
            f"# BEGIN {marker}",
            f"@reboot {_ops_script_command(target, compose_up)} >> {shlex.quote(str(log_dir / 'compose-up.log'))} 2>&1",
            f"*/{health_minutes} * * * * {_ops_script_command(target, health)} >> {shlex.quote(str(log_dir / 'health.log'))} 2>&1",
            f"*/15 * * * * {_ops_script_command(target, trading_ops_monitor)} >> {shlex.quote(str(log_dir / 'trading-ops-monitor.log'))} 2>&1",
            f"17 * * * * {_ops_script_command(target, rotate_logs)} >> {shlex.quote(str(log_dir / 'log-rotate.log'))} 2>&1",
            f"0 0 * * * {_ops_script_command(target, backup)} >> {shlex.quote(str(log_dir / 'backup.log'))} 2>&1",
            f"# END {marker}",
            "",
        ]
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


def install_target_ops_schedule(target: DeployTarget) -> None:
    _ensure_repo_ops_scripts_exist()
    ops_root = _ops_root(target)
    log_dir = _ops_log_dir(target)
    backup_root = _backup_root(target)
    marker = f"spreads-ops:{target.name}"
    local_install = not target.is_remote or _can_run_target_locally(target)

    def run_shell(command: str) -> None:
        if local_install:
            _run_command(["bash", "-lc", command])
            return
        _run_command(_ssh_command(target, command))

    run_shell(
        " && ".join(
            [
                "set -euo pipefail",
                f"mkdir -p {shlex.quote(str(ops_root))}",
                f"mkdir -p {shlex.quote(str(log_dir))}",
                f"mkdir -p {shlex.quote(str(backup_root))}",
                f"test -f {shlex.quote(str(ops_root / 'compose_up.sh'))}",
                f"test -f {shlex.quote(str(ops_root / 'backup_postgres.sh'))}",
                f"test -f {shlex.quote(str(ops_root / 'health_check.sh'))}",
                f"test -f {shlex.quote(str(ops_root / 'trading_ops_monitor.sh'))}",
                f"test -f {shlex.quote(str(ops_root / 'rotate_ops_logs.sh'))}",
            ]
        )
    )
    run_shell(
        " && ".join(
            [
                "set -euo pipefail",
                f"chmod +x {shlex.quote(str(ops_root / 'compose_up.sh'))}",
                f"chmod +x {shlex.quote(str(ops_root / 'backup_postgres.sh'))}",
                f"chmod +x {shlex.quote(str(ops_root / 'health_check.sh'))}",
                f"chmod +x {shlex.quote(str(ops_root / 'trading_ops_monitor.sh'))}",
                f"chmod +x {shlex.quote(str(ops_root / 'rotate_ops_logs.sh'))}",
                f"rm -f {shlex.quote(str(Path(target.deploy_root) / '.ops' / 'compose_up.sh'))}",
                f"rm -f {shlex.quote(str(Path(target.deploy_root) / '.ops' / 'backup_postgres.sh'))}",
                f"rm -f {shlex.quote(str(Path(target.deploy_root) / '.ops' / 'health_check.sh'))}",
                f"rm -f {shlex.quote(str(Path(target.deploy_root) / '.ops' / 'retention_prune.sh'))}",
                f"rm -f {shlex.quote(str(Path(target.deploy_root) / '.ops' / 'rotate_ops_logs.sh'))}",
                f"rm -f {shlex.quote(str(Path(target.deploy_root) / '.ops' / 'spreads.cron'))}",
                f"rmdir {shlex.quote(str(Path(target.deploy_root) / '.ops'))} 2>/dev/null || true",
            ]
        )
    )
    cron_block_remote = f"/tmp/spreads-{target.name}.cron"
    if local_install:
        Path(cron_block_remote).write_text(_cron_block_text(target))
    else:
        _write_remote_text_file(
            target,
            remote_path=cron_block_remote,
            text=_cron_block_text(target),
        )
    command = textwrap.dedent(f"""
        set -euo pipefail
        marker_begin="# BEGIN {marker}"
        marker_end="# END {marker}"
        tmp_file="$(mktemp)"
        (crontab -l 2>/dev/null || true) | sed "/$marker_begin/,/$marker_end/d" > "$tmp_file"
        cat {shlex.quote(cron_block_remote)} >> "$tmp_file"
        crontab "$tmp_file"
        rm -f "$tmp_file"
        """).strip()
    run_shell(command)


__all__ = [
    "bootstrap_remote_target",
    "install_systemd_service",
    "install_target_ops_schedule",
    "sync_deploy_target",
]
