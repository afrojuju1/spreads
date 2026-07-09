from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, ValidationError, field_validator, model_validator

from core.model_contracts import DomainModel
from core.services.config_inheritance import load_yaml_mapping
from core.services.deployment_constants import DEPLOY_TARGETS_ROOT, REPO_ROOT
from core.services.payload_validation import (
    format_validation_error,
    normalize_optional_text,
    normalize_required_text,
    normalize_text_tuple,
)


class DeploymentConfigError(ValueError):
    """Raised when deployment target configuration is invalid."""


class DeployTargetYamlPayload(DomainModel):
    name: str | None = None
    mode: Literal["local", "ssh"]
    description: str | None = None
    deploy_root: str = "."
    compose_file: str = "docker-compose.prod.yml"
    env_file: str | None = None
    compose_project_name: str | None = None
    bind_host: str = "127.0.0.1"
    api_port: int = Field(default=58080, gt=0)
    postgres_port: int = Field(default=55432, gt=0)
    clickhouse_port: int = Field(default=58123, gt=0)
    redis_port: int = Field(default=56379, gt=0)
    web_port: int = Field(default=53000, gt=0)
    worker_runtime_replicas: int = Field(default=1, gt=0)
    worker_data_replicas: int = Field(default=2, gt=0)
    worker_valuation_replicas: int = Field(default=0, ge=0)
    worker_research_replicas: int = Field(default=0, ge=0)
    web_enabled: bool = True
    postgres_volume_name: str | None = None
    clickhouse_volume_name: str | None = None
    docker_log_driver: str = "local"
    docker_log_max_size: str = "10m"
    docker_log_max_file: int = Field(default=5, gt=0)
    backup_retention_days: int = Field(default=7, gt=0)
    health_check_minutes: int = Field(default=5, gt=0)
    excluded_job_types: tuple[str, ...] = Field(default_factory=tuple)
    market_recorder_owner_env: str | None = None
    ssh_host: str | None = None

    @field_validator(
        "name",
        "description",
        "env_file",
        "compose_project_name",
        "postgres_volume_name",
        "clickhouse_volume_name",
        "market_recorder_owner_env",
        "ssh_host",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        return normalize_optional_text(value)

    @field_validator(
        "deploy_root",
        "compose_file",
        "bind_host",
        "docker_log_driver",
        "docker_log_max_size",
        mode="before",
    )
    @classmethod
    def _normalize_required_text(cls, value: Any) -> str:
        return normalize_required_text(value)

    @field_validator("mode", mode="before")
    @classmethod
    def _normalize_mode(cls, value: Any) -> str:
        return normalize_required_text(value).lower()

    @field_validator("excluded_job_types", mode="before")
    @classmethod
    def _normalize_excluded_job_types(cls, value: Any) -> tuple[str, ...]:
        if isinstance(value, str):
            return tuple(part.strip() for part in value.split(",") if part.strip())
        return normalize_text_tuple(value)

    @model_validator(mode="after")
    def _validate_target(self) -> DeployTargetYamlPayload:
        if self.mode == "ssh" and self.ssh_host is None:
            raise ValueError("ssh_host is required for ssh targets")
        return self


@dataclass(frozen=True)
class DeployTarget:
    name: str
    mode: Literal["local", "ssh"]
    description: str
    deploy_root: str
    compose_file: str
    env_file: str
    compose_project_name: str
    bind_host: str
    api_port: int
    postgres_port: int
    clickhouse_port: int
    redis_port: int
    web_port: int
    worker_runtime_replicas: int
    worker_data_replicas: int
    worker_valuation_replicas: int
    worker_research_replicas: int
    web_enabled: bool
    postgres_volume_name: str
    clickhouse_volume_name: str
    docker_log_driver: str
    docker_log_max_size: str
    docker_log_max_file: int
    backup_retention_days: int
    health_check_minutes: int
    excluded_job_types: tuple[str, ...]
    market_recorder_owner_env: str | None = None
    ssh_host: str | None = None

    @property
    def is_remote(self) -> bool:
        return self.mode == "ssh"

    @property
    def deploy_path(self) -> Path:
        if self.is_remote:
            return Path(self.deploy_root)
        return (REPO_ROOT / self.deploy_root).resolve()

    @property
    def remote_parent(self) -> Path:
        return Path(self.deploy_root).parent

    @property
    def local_overlay_env_path(self) -> Path:
        return REPO_ROOT / self.env_file

    @property
    def service_name(self) -> str:
        return f"spreads-compose-{self.name}.service"


def _load_target(path: Path) -> DeployTarget:
    try:
        raw_payload = load_yaml_mapping(path)
    except ValueError as exc:
        raise DeploymentConfigError(str(exc)) from exc
    try:
        payload = DeployTargetYamlPayload.model_validate(raw_payload)
    except ValidationError as exc:
        raise DeploymentConfigError(f"Invalid deployment target config in {path}: {format_validation_error(exc)}") from exc
    name = payload.name or path.stem
    return DeployTarget(
        name=name,
        mode=payload.mode,
        description=payload.description or "",
        deploy_root=payload.deploy_root,
        compose_file=payload.compose_file,
        env_file=payload.env_file or f".env.deploy.{name}",
        compose_project_name=payload.compose_project_name or f"spreads-{name}",
        bind_host=payload.bind_host,
        api_port=payload.api_port,
        postgres_port=payload.postgres_port,
        clickhouse_port=payload.clickhouse_port,
        redis_port=payload.redis_port,
        web_port=payload.web_port,
        worker_runtime_replicas=payload.worker_runtime_replicas,
        worker_data_replicas=payload.worker_data_replicas,
        worker_valuation_replicas=payload.worker_valuation_replicas,
        worker_research_replicas=payload.worker_research_replicas,
        web_enabled=payload.web_enabled,
        postgres_volume_name=payload.postgres_volume_name or f"spreads_{name.replace('-', '_')}_postgres_data",
        clickhouse_volume_name=payload.clickhouse_volume_name or f"spreads_{name.replace('-', '_')}_clickhouse_data",
        docker_log_driver=payload.docker_log_driver,
        docker_log_max_size=payload.docker_log_max_size,
        docker_log_max_file=payload.docker_log_max_file,
        backup_retention_days=payload.backup_retention_days,
        health_check_minutes=payload.health_check_minutes,
        excluded_job_types=payload.excluded_job_types,
        market_recorder_owner_env=payload.market_recorder_owner_env,
        ssh_host=payload.ssh_host,
    )


def list_deploy_targets() -> list[DeployTarget]:
    targets = [_load_target(path) for path in sorted(DEPLOY_TARGETS_ROOT.glob("*.yaml"))]
    return sorted(targets, key=lambda item: item.name)


def get_deploy_target(name: str) -> DeployTarget:
    normalized = str(name or "").strip().lower()
    for target in list_deploy_targets():
        if target.name.lower() == normalized:
            return target
    available = ", ".join(target.name for target in list_deploy_targets()) or "<none>"
    raise DeploymentConfigError(f"Unknown deploy target {name!r}. Available targets: {available}.")


def deploy_target_payload(target: DeployTarget) -> dict[str, Any]:
    return {
        "name": target.name,
        "mode": target.mode,
        "description": target.description,
        "ssh_host": target.ssh_host,
        "deploy_root": target.deploy_root,
        "compose_file": target.compose_file,
        "env_file": target.env_file,
        "compose_project_name": target.compose_project_name,
        "bind_host": target.bind_host,
        "api_port": target.api_port,
        "postgres_port": target.postgres_port,
        "clickhouse_port": target.clickhouse_port,
        "redis_port": target.redis_port,
        "web_port": target.web_port,
        "worker_runtime_replicas": target.worker_runtime_replicas,
        "worker_data_replicas": target.worker_data_replicas,
        "worker_valuation_replicas": target.worker_valuation_replicas,
        "worker_research_replicas": target.worker_research_replicas,
        "web_enabled": target.web_enabled,
        "postgres_volume_name": target.postgres_volume_name,
        "clickhouse_volume_name": target.clickhouse_volume_name,
        "docker_log_driver": target.docker_log_driver,
        "docker_log_max_size": target.docker_log_max_size,
        "docker_log_max_file": target.docker_log_max_file,
        "backup_retention_days": target.backup_retention_days,
        "health_check_minutes": target.health_check_minutes,
        "excluded_job_types": list(target.excluded_job_types),
        "market_recorder_owner_env": target.market_recorder_owner_env,
        "overlay_env_file": str(target.local_overlay_env_path),
        "service_name": target.service_name,
    }


__all__ = [
    "DeploymentConfigError",
    "DeployTarget",
    "DeployTargetYamlPayload",
    "deploy_target_payload",
    "get_deploy_target",
    "list_deploy_targets",
]
