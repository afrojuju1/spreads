from __future__ import annotations

from pathlib import Path

from core.services.deployment_targets import DeployTarget


def _ops_root(target: DeployTarget) -> Path:
    return Path(target.deploy_root) / "ops"


def _ops_state_root(target: DeployTarget) -> Path:
    deploy_root = Path(target.deploy_root)
    if deploy_root.name == "app":
        return deploy_root.parent
    if not target.is_remote:
        return target.deploy_path
    return deploy_root


def _ops_log_dir(target: DeployTarget) -> Path:
    return _ops_state_root(target) / "logs" / "ops"


def _backup_root(target: DeployTarget) -> Path:
    return _ops_state_root(target) / "backups" / "postgres"


__all__ = [
    "_backup_root",
    "_ops_log_dir",
    "_ops_root",
    "_ops_state_root",
]
