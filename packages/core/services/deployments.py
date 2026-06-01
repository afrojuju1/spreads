from __future__ import annotations

import os
import re
import shlex
import subprocess
import tempfile
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml
from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parents[3]
DEPLOY_TARGETS_ROOT = REPO_ROOT / "packages" / "config" / "deploy_targets"
PROD_COMPOSE_PATH = REPO_ROOT / "docker-compose.prod.yml"
DEPLOY_OPS_ROOT = REPO_ROOT / "ops"

REPO_SYNC_EXCLUDES = (
    ".git",
    ".venv",
    ".ruff_cache",
    "__pycache__",
    "outputs",
    "node_modules",
    ".next",
    ".env",
    ".env.deploy*",
    "*.pyc",
)

REQUIRED_SECRET_KEYS = (
    "APCA_API_KEY_ID",
    "APCA_API_SECRET_KEY",
    "POSTGRES_PASSWORD",
)
OPTIONAL_SECRET_KEYS = ("SPREADS_DISCORD_WEBHOOK_URL",)

ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


class DeploymentConfigError(ValueError):
    """Raised when deployment target configuration is invalid."""


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
    redis_port: int
    web_port: int
    worker_runtime_replicas: int
    worker_discovery_replicas: int
    worker_research_replicas: int
    web_enabled: bool
    postgres_volume_name: str
    docker_log_driver: str
    docker_log_max_size: str
    docker_log_max_file: int
    backup_retention_days: int
    health_check_minutes: int
    excluded_job_types: tuple[str, ...]
    market_recorder_owner_env: str | None = None
    nautilus_bridge_host_binary: str | None = None
    nautilus_bridge_container_binary: str = (
        "/usr/local/bin/alpaca-submit-order-list-bridge"
    )
    nautilus_bridge_command: str | None = None
    nautilus_bridge_timeout_seconds: int = 45
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


def _normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text()) or {}
    if not isinstance(payload, dict):
        raise DeploymentConfigError(f"Deployment target file is not a mapping: {path}")
    return payload


def _coerce_positive_int(value: Any, *, field_name: str) -> int:
    try:
        resolved = int(value)
    except (TypeError, ValueError) as exc:
        raise DeploymentConfigError(f"{field_name} must be an integer.") from exc
    if resolved <= 0:
        raise DeploymentConfigError(f"{field_name} must be greater than 0.")
    return resolved


def _coerce_non_negative_int(value: Any, *, field_name: str) -> int:
    try:
        resolved = int(value)
    except (TypeError, ValueError) as exc:
        raise DeploymentConfigError(f"{field_name} must be an integer.") from exc
    if resolved < 0:
        raise DeploymentConfigError(f"{field_name} must be greater than or equal to 0.")
    return resolved


def _coerce_text_list(value: Any, *, field_name: str) -> tuple[str, ...]:
    if value in (None, "", ()):
        return ()
    if isinstance(value, str):
        items = [part.strip() for part in value.split(",")]
    elif isinstance(value, (list, tuple)):
        items = [str(part).strip() for part in value]
    else:
        raise DeploymentConfigError(f"{field_name} must be a list of strings.")
    normalized = tuple(item for item in items if item)
    return normalized


def _load_target(path: Path) -> DeployTarget:
    payload = _load_yaml(path)
    mode = str(payload.get("mode") or "").strip().lower()
    if mode not in {"local", "ssh"}:
        raise DeploymentConfigError(f"{path.name}: unsupported mode {mode!r}.")
    name = _normalize_text(payload.get("name")) or path.stem
    ssh_host = _normalize_text(payload.get("ssh_host"))
    if mode == "ssh" and ssh_host is None:
        raise DeploymentConfigError(f"{path.name}: ssh_host is required for ssh targets.")
    return DeployTarget(
        name=name,
        mode=mode,
        description=_normalize_text(payload.get("description")) or "",
        deploy_root=_normalize_text(payload.get("deploy_root")) or ".",
        compose_file=_normalize_text(payload.get("compose_file"))
        or "docker-compose.prod.yml",
        env_file=_normalize_text(payload.get("env_file")) or f".env.deploy.{name}",
        compose_project_name=_normalize_text(payload.get("compose_project_name"))
        or f"spreads-{name}",
        bind_host=_normalize_text(payload.get("bind_host")) or "127.0.0.1",
        api_port=_coerce_positive_int(
            payload.get("api_port", 58080),
            field_name="api_port",
        ),
        postgres_port=_coerce_positive_int(
            payload.get("postgres_port", 55432),
            field_name="postgres_port",
        ),
        redis_port=_coerce_positive_int(
            payload.get("redis_port", 56379),
            field_name="redis_port",
        ),
        web_port=_coerce_positive_int(
            payload.get("web_port", 53000),
            field_name="web_port",
        ),
        worker_runtime_replicas=_coerce_positive_int(
            payload.get("worker_runtime_replicas", 1),
            field_name="worker_runtime_replicas",
        ),
        worker_discovery_replicas=_coerce_positive_int(
            payload.get("worker_discovery_replicas", 2),
            field_name="worker_discovery_replicas",
        ),
        worker_research_replicas=_coerce_non_negative_int(
            payload.get("worker_research_replicas", 0),
            field_name="worker_research_replicas",
        ),
        web_enabled=bool(payload.get("web_enabled", True)),
        postgres_volume_name=_normalize_text(payload.get("postgres_volume_name"))
        or f"spreads_{name.replace('-', '_')}_postgres_data",
        docker_log_driver=_normalize_text(payload.get("docker_log_driver")) or "local",
        docker_log_max_size=_normalize_text(payload.get("docker_log_max_size")) or "10m",
        docker_log_max_file=_coerce_positive_int(
            payload.get("docker_log_max_file", 5),
            field_name="docker_log_max_file",
        ),
        backup_retention_days=_coerce_positive_int(
            payload.get("backup_retention_days", 7),
            field_name="backup_retention_days",
        ),
        health_check_minutes=_coerce_positive_int(
            payload.get("health_check_minutes", 5),
            field_name="health_check_minutes",
        ),
        excluded_job_types=_coerce_text_list(
            payload.get("excluded_job_types"),
            field_name="excluded_job_types",
        ),
        market_recorder_owner_env=_normalize_text(
            payload.get("market_recorder_owner_env")
        ),
        nautilus_bridge_host_binary=_normalize_text(
            payload.get("nautilus_bridge_host_binary")
        ),
        nautilus_bridge_container_binary=_normalize_text(
            payload.get("nautilus_bridge_container_binary")
        )
        or "/usr/local/bin/alpaca-submit-order-list-bridge",
        nautilus_bridge_command=_normalize_text(payload.get("nautilus_bridge_command")),
        nautilus_bridge_timeout_seconds=_coerce_positive_int(
            payload.get("nautilus_bridge_timeout_seconds", 45),
            field_name="nautilus_bridge_timeout_seconds",
        ),
        ssh_host=ssh_host,
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
    raise DeploymentConfigError(
        f"Unknown deploy target {name!r}. Available targets: {available}."
    )


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    payload = dotenv_values(path)
    resolved: dict[str, str] = {}
    for key, value in payload.items():
        text = _normalize_text(value)
        if key and text is not None:
            resolved[str(key)] = text
    return resolved


def _merged_secret_sources(target: DeployTarget) -> dict[str, str]:
    merged: dict[str, str] = {}
    merged.update(_load_env_file(REPO_ROOT / ".env"))
    merged.update(_load_env_file(target.local_overlay_env_path))
    for key, value in os.environ.items():
        text = _normalize_text(value)
        if text is not None:
            merged[key] = text
    return merged


def _secret_value(
    merged: dict[str, str],
    *keys: str,
    default: str | None = None,
) -> str | None:
    for key in keys:
        text = _normalize_text(merged.get(key))
        if text is not None:
            return text
    return default


def build_deploy_env_values(
    target: DeployTarget,
    *,
    require_secrets: bool = False,
) -> dict[str, str]:
    merged = _merged_secret_sources(target)
    postgres_password = _secret_value(
        merged,
        "POSTGRES_PASSWORD",
        "SPREADS_POSTGRES_PASSWORD",
        default="replace-me",
    )
    api_key = _secret_value(
        merged,
        "APCA_API_KEY_ID",
        "ALPACA_API_KEY",
        default="replace-me",
    )
    api_secret = _secret_value(
        merged,
        "APCA_API_SECRET_KEY",
        "ALPACA_SECRET_KEY",
        default="replace-me",
    )
    webhook = _secret_value(
        merged,
        "SPREADS_DISCORD_WEBHOOK_URL",
        "DISCORD_WEBHOOK_URL",
        default="",
    )
    tradingagents_host_dir = _secret_value(
        merged,
        "SPREADS_TRADINGAGENTS_HOST_DIR",
        default="/home/ade/Projects/TradingAgents",
    )
    tradingagents_container_dir = _secret_value(
        merged,
        "SPREADS_TRADINGAGENTS_CONTAINER_DIR",
        default="/tradingagents",
    )
    tradingagents_uv_environment = _secret_value(
        merged,
        "SPREADS_TRADINGAGENTS_CONTAINER_UV_ENVIRONMENT",
        default="/app/outputs/tradingagents/.venv",
    )
    ollama_base_url = _secret_value(
        merged,
        "SPREADS_CONTAINER_OLLAMA_BASE_URL",
        default="http://host.docker.internal:11434/v1",
    )

    env_values = {
        "SPREADS_DEPLOY_ENV": target.name,
        "SPREADS_EXCLUDED_JOB_TYPES": ",".join(target.excluded_job_types),
        "SPREADS_COMPOSE_PROJECT_NAME": target.compose_project_name,
        "SPREADS_CONTAINER_ENV_FILE": target.env_file,
        "SPREADS_BIND_HOST": target.bind_host,
        "SPREADS_API_PORT": str(target.api_port),
        "SPREADS_POSTGRES_PORT": str(target.postgres_port),
        "SPREADS_REDIS_PORT": str(target.redis_port),
        "SPREADS_WEB_PORT": str(target.web_port),
        "SPREADS_POSTGRES_VOLUME_NAME": target.postgres_volume_name,
        "SPREADS_DOCKER_LOG_DRIVER": target.docker_log_driver,
        "SPREADS_DOCKER_LOG_MAX_SIZE": target.docker_log_max_size,
        "SPREADS_DOCKER_LOG_MAX_FILE": str(target.docker_log_max_file),
        "SPREADS_COMPOSE_FILE": target.compose_file,
        "SPREADS_WEB_ENABLED": "true" if target.web_enabled else "false",
        "SPREADS_WORKER_RUNTIME_REPLICAS": str(target.worker_runtime_replicas),
        "SPREADS_WORKER_DISCOVERY_REPLICAS": str(target.worker_discovery_replicas),
        "SPREADS_WORKER_RESEARCH_REPLICAS": str(target.worker_research_replicas),
        "SPREADS_BACKUP_RETENTION_DAYS": str(target.backup_retention_days),
        "SPREADS_HEALTH_CHECK_MINUTES": str(target.health_check_minutes),
        "SPREADS_WEB_INTERNAL_API_BASE_URL": "http://api:8000",
        "POSTGRES_DB": "spreads",
        "POSTGRES_USER": "spreads",
        "POSTGRES_PASSWORD": str(postgres_password),
        "SPREADS_DATABASE_URL": (
            f"postgresql://spreads:{postgres_password}@postgres:5432/spreads"
        ),
        "REDIS_URL": "redis://redis:6379/0",
        "SPREADS_INTERNAL_API_BASE_URL": "http://api:8000",
        "SPREADS_API_BASE_URL": "http://api:8000",
        "NEXT_PUBLIC_SPREADS_API_WS_PORT": str(target.api_port),
        "APCA_API_KEY_ID": str(api_key),
        "APCA_API_SECRET_KEY": str(api_secret),
        "SPREADS_DISCORD_WEBHOOK_URL": str(webhook),
        "SPREADS_TRADINGAGENTS_HOST_DIR": str(tradingagents_host_dir),
        "SPREADS_TRADINGAGENTS_DIR": str(tradingagents_container_dir),
        "SPREADS_TRADINGAGENTS_UV_ENVIRONMENT": str(tradingagents_uv_environment),
        "OLLAMA_BASE_URL": str(ollama_base_url),
        "SPREADS_MARKET_RECORDER_OWNER_ENV": str(
            _normalize_text(target.market_recorder_owner_env) or ""
        ),
        "SPREADS_NAUTILUS_BRIDGE_HOST_BINARY": str(
            _normalize_text(target.nautilus_bridge_host_binary) or "/dev/null"
        ),
        "SPREADS_NAUTILUS_BRIDGE_CONTAINER_BINARY": str(
            target.nautilus_bridge_container_binary
        ),
        "SPREADS_NAUTILUS_BRIDGE_COMMAND": str(
            _normalize_text(target.nautilus_bridge_command)
            or target.nautilus_bridge_container_binary
        ),
        "SPREADS_NAUTILUS_BRIDGE_TIMEOUT_SECONDS": str(
            target.nautilus_bridge_timeout_seconds
        ),
    }

    if require_secrets:
        missing = [
            key
            for key in REQUIRED_SECRET_KEYS
            if _normalize_text(env_values.get(key)) in {None, "replace-me"}
        ]
        if missing:
            raise DeploymentConfigError(
                "Missing required deployment secrets: " + ", ".join(sorted(missing))
            )
    return env_values


def render_host_env_file(
    target: DeployTarget,
    *,
    require_secrets: bool = False,
) -> str:
    lines = [
        "# Generated by spreads deploy for host-side operator CLI use.",
        *(
            f"{key}={value}"
            for key, value in build_host_env_values(
                target,
                require_secrets=require_secrets,
            ).items()
        ),
    ]
    return "\n".join(lines) + "\n"


def build_host_env_values(
    target: DeployTarget,
    *,
    require_secrets: bool = False,
) -> dict[str, str]:
    values = build_deploy_env_values(target, require_secrets=require_secrets)
    postgres_password = values["POSTGRES_PASSWORD"]
    if target.compose_file == "docker-compose.yml":
        postgres_password = "spreads"
    host_bind_host = values["SPREADS_BIND_HOST"]
    host_database_url = (
        "postgresql://"
        f"{values['POSTGRES_USER']}:{postgres_password}"
        f"@{host_bind_host}:{values['SPREADS_POSTGRES_PORT']}/{values['POSTGRES_DB']}"
    )
    host_redis_url = f"redis://{host_bind_host}:{values['SPREADS_REDIS_PORT']}/0"
    api_base_url = f"http://{host_bind_host}:{values['SPREADS_API_PORT']}"
    return {
        "SPREADS_DEPLOY_ENV": values["SPREADS_DEPLOY_ENV"],
        "SPREADS_EXCLUDED_JOB_TYPES": values["SPREADS_EXCLUDED_JOB_TYPES"],
        "SPREADS_CONTAINER_ENV_FILE": values["SPREADS_CONTAINER_ENV_FILE"],
        "SPREADS_COMPOSE_FILE": values["SPREADS_COMPOSE_FILE"],
        "SPREADS_WEB_ENABLED": values["SPREADS_WEB_ENABLED"],
        "SPREADS_WORKER_RUNTIME_REPLICAS": values["SPREADS_WORKER_RUNTIME_REPLICAS"],
        "SPREADS_WORKER_DISCOVERY_REPLICAS": values[
            "SPREADS_WORKER_DISCOVERY_REPLICAS"
        ],
        "SPREADS_WORKER_RESEARCH_REPLICAS": values[
            "SPREADS_WORKER_RESEARCH_REPLICAS"
        ],
        "SPREADS_BACKUP_RETENTION_DAYS": values["SPREADS_BACKUP_RETENTION_DAYS"],
        "SPREADS_HEALTH_CHECK_MINUTES": values["SPREADS_HEALTH_CHECK_MINUTES"],
        "SPREADS_DATABASE_URL": host_database_url,
        "DATABASE_URL": host_database_url,
        "REDIS_URL": host_redis_url,
        "SPREADS_API_BASE_URL": api_base_url,
        "SPREADS_INTERNAL_API_BASE_URL": api_base_url,
        "APCA_API_KEY_ID": values["APCA_API_KEY_ID"],
        "APCA_API_SECRET_KEY": values["APCA_API_SECRET_KEY"],
        "SPREADS_DISCORD_WEBHOOK_URL": values["SPREADS_DISCORD_WEBHOOK_URL"],
        "SPREADS_TRADINGAGENTS_HOST_DIR": values["SPREADS_TRADINGAGENTS_HOST_DIR"],
        "SPREADS_TRADINGAGENTS_DIR": values["SPREADS_TRADINGAGENTS_HOST_DIR"],
        "SPREADS_TRADINGAGENTS_UV_ENVIRONMENT": str(
            Path(values["SPREADS_TRADINGAGENTS_HOST_DIR"]) / ".venv"
        ),
        "OLLAMA_BASE_URL": "http://localhost:11434/v1",
        "SPREADS_MARKET_RECORDER_OWNER_ENV": values[
            "SPREADS_MARKET_RECORDER_OWNER_ENV"
        ],
        "SPREADS_NAUTILUS_BRIDGE_COMMAND": (
            target.nautilus_bridge_host_binary
            or values["SPREADS_NAUTILUS_BRIDGE_COMMAND"]
        ),
        "SPREADS_NAUTILUS_BRIDGE_TIMEOUT_SECONDS": values[
            "SPREADS_NAUTILUS_BRIDGE_TIMEOUT_SECONDS"
        ],
    }


def render_deploy_env_file(
    target: DeployTarget,
    *,
    require_secrets: bool = False,
) -> str:
    values = build_deploy_env_values(target, require_secrets=require_secrets)
    lines = [
        f"# Generated by `uv run spreads deploy render-env --env {target.name}`",
        "# Source precedence: process env > target overlay env file > .env",
        "",
        "# Compose target",
        f"SPREADS_DEPLOY_ENV={values['SPREADS_DEPLOY_ENV']}",
        f"SPREADS_EXCLUDED_JOB_TYPES={values['SPREADS_EXCLUDED_JOB_TYPES']}",
        f"SPREADS_COMPOSE_PROJECT_NAME={values['SPREADS_COMPOSE_PROJECT_NAME']}",
        f"SPREADS_CONTAINER_ENV_FILE={values['SPREADS_CONTAINER_ENV_FILE']}",
        f"SPREADS_COMPOSE_FILE={values['SPREADS_COMPOSE_FILE']}",
        f"SPREADS_BIND_HOST={values['SPREADS_BIND_HOST']}",
        f"SPREADS_API_PORT={values['SPREADS_API_PORT']}",
        f"SPREADS_POSTGRES_PORT={values['SPREADS_POSTGRES_PORT']}",
        f"SPREADS_REDIS_PORT={values['SPREADS_REDIS_PORT']}",
        f"SPREADS_WEB_PORT={values['SPREADS_WEB_PORT']}",
        f"SPREADS_WEB_ENABLED={values['SPREADS_WEB_ENABLED']}",
        f"SPREADS_WORKER_RUNTIME_REPLICAS={values['SPREADS_WORKER_RUNTIME_REPLICAS']}",
        f"SPREADS_WORKER_DISCOVERY_REPLICAS={values['SPREADS_WORKER_DISCOVERY_REPLICAS']}",
        f"SPREADS_WORKER_RESEARCH_REPLICAS={values['SPREADS_WORKER_RESEARCH_REPLICAS']}",
        f"SPREADS_BACKUP_RETENTION_DAYS={values['SPREADS_BACKUP_RETENTION_DAYS']}",
        f"SPREADS_HEALTH_CHECK_MINUTES={values['SPREADS_HEALTH_CHECK_MINUTES']}",
        f"SPREADS_POSTGRES_VOLUME_NAME={values['SPREADS_POSTGRES_VOLUME_NAME']}",
        f"SPREADS_DOCKER_LOG_DRIVER={values['SPREADS_DOCKER_LOG_DRIVER']}",
        f"SPREADS_DOCKER_LOG_MAX_SIZE={values['SPREADS_DOCKER_LOG_MAX_SIZE']}",
        f"SPREADS_DOCKER_LOG_MAX_FILE={values['SPREADS_DOCKER_LOG_MAX_FILE']}",
        f"SPREADS_MARKET_RECORDER_OWNER_ENV={values['SPREADS_MARKET_RECORDER_OWNER_ENV']}",
        f"SPREADS_NAUTILUS_BRIDGE_HOST_BINARY={values['SPREADS_NAUTILUS_BRIDGE_HOST_BINARY']}",
        f"SPREADS_NAUTILUS_BRIDGE_CONTAINER_BINARY={values['SPREADS_NAUTILUS_BRIDGE_CONTAINER_BINARY']}",
        "",
        "# Application secrets",
        f"APCA_API_KEY_ID={values['APCA_API_KEY_ID']}",
        f"APCA_API_SECRET_KEY={values['APCA_API_SECRET_KEY']}",
        f"SPREADS_DISCORD_WEBHOOK_URL={values['SPREADS_DISCORD_WEBHOOK_URL']}",
        "",
        "# Container runtime env",
        f"POSTGRES_DB={values['POSTGRES_DB']}",
        f"POSTGRES_USER={values['POSTGRES_USER']}",
        f"POSTGRES_PASSWORD={values['POSTGRES_PASSWORD']}",
        f"SPREADS_DATABASE_URL={values['SPREADS_DATABASE_URL']}",
        f"REDIS_URL={values['REDIS_URL']}",
        f"SPREADS_INTERNAL_API_BASE_URL={values['SPREADS_INTERNAL_API_BASE_URL']}",
        f"SPREADS_API_BASE_URL={values['SPREADS_API_BASE_URL']}",
        f"SPREADS_WEB_INTERNAL_API_BASE_URL={values['SPREADS_WEB_INTERNAL_API_BASE_URL']}",
        f"NEXT_PUBLIC_SPREADS_API_WS_PORT={values['NEXT_PUBLIC_SPREADS_API_WS_PORT']}",
        f"SPREADS_TRADINGAGENTS_HOST_DIR={values['SPREADS_TRADINGAGENTS_HOST_DIR']}",
        f"SPREADS_TRADINGAGENTS_DIR={values['SPREADS_TRADINGAGENTS_DIR']}",
        f"SPREADS_TRADINGAGENTS_UV_ENVIRONMENT={values['SPREADS_TRADINGAGENTS_UV_ENVIRONMENT']}",
        f"OLLAMA_BASE_URL={values['OLLAMA_BASE_URL']}",
        f"SPREADS_NAUTILUS_BRIDGE_COMMAND={values['SPREADS_NAUTILUS_BRIDGE_COMMAND']}",
        f"SPREADS_NAUTILUS_BRIDGE_TIMEOUT_SECONDS={values['SPREADS_NAUTILUS_BRIDGE_TIMEOUT_SECONDS']}",
        "",
    ]
    return "\n".join(lines)


def render_prod_compose(target: DeployTarget) -> str:
    values = build_deploy_env_values(target, require_secrets=False)
    template = PROD_COMPOSE_PATH.read_text()

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            raise DeploymentConfigError(
                f"Compose template references missing deployment key {key!r}."
            )
        return values[key]

    return ENV_PATTERN.sub(replace, template)


def _run_command(command: list[str], *, cwd: Path | None = None) -> None:
    try:
        subprocess.run(command, cwd=cwd, check=True)
    except subprocess.CalledProcessError as exc:
        rendered = " ".join(shlex.quote(part) for part in exc.cmd)
        raise RuntimeError(
            f"Command failed with exit {exc.returncode}: {rendered}"
        ) from exc


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
    return args


def _compose_up_args(target: DeployTarget, *, build: bool) -> list[str]:
    args = _compose_base_args(target)
    args.extend(["up", "-d", "--remove-orphans"])
    if build:
        args.append("--build")
    args.extend(["--scale", f"worker-runtime={target.worker_runtime_replicas}"])
    args.extend(["--scale", f"worker-discovery={target.worker_discovery_replicas}"])
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
        return _run_passthrough_command(
            _remote_shell_command(target, _render_shell_command(args))
        )
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


def _repo_ops_script_paths() -> tuple[Path, ...]:
    return (
        DEPLOY_OPS_ROOT / "compose_up.sh",
        DEPLOY_OPS_ROOT / "backup_postgres.sh",
        DEPLOY_OPS_ROOT / "health_check.sh",
        DEPLOY_OPS_ROOT / "market_open_monitor.sh",
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
        raise DeploymentConfigError(
            f"Deploy target {target.name!r} already resolves locally at "
            f"{target.deploy_root}; sync is not needed."
        )
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
    script = textwrap.dedent(
        f"""
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
        """
    ).strip()
    _run_command(_ssh_command(target, script))


def _systemd_unit_text(target: DeployTarget) -> str:
    up_args = " ".join(shlex.quote(part) for part in _compose_up_args(target, build=False))
    down_args = " ".join(shlex.quote(part) for part in _compose_down_args(target))
    return textwrap.dedent(
        f"""
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
        """
    ).strip() + "\n"


def _ops_root(target: DeployTarget) -> Path:
    return Path(target.deploy_root) / "ops"


def _ops_state_root(target: DeployTarget) -> Path:
    deploy_root = Path(target.deploy_root)
    if deploy_root.name == "app":
        return deploy_root.parent
    return deploy_root


def _ops_log_dir(target: DeployTarget) -> Path:
    return _ops_state_root(target) / "logs" / "ops"


def _backup_root(target: DeployTarget) -> Path:
    return _ops_state_root(target) / "backups" / "postgres"


def _ops_script_command(target: DeployTarget, script_path: Path) -> str:
    env_values = {
        "SPREADS_DEPLOY_ENV": target.name,
        "SPREADS_CONTAINER_ENV_FILE": target.env_file,
        "SPREADS_COMPOSE_FILE": target.compose_file,
        "SPREADS_WEB_ENABLED": "true" if target.web_enabled else "false",
        "SPREADS_WORKER_RUNTIME_REPLICAS": str(target.worker_runtime_replicas),
        "SPREADS_WORKER_DISCOVERY_REPLICAS": str(target.worker_discovery_replicas),
        "SPREADS_WORKER_RESEARCH_REPLICAS": str(target.worker_research_replicas),
        "SPREADS_BACKUP_RETENTION_DAYS": str(target.backup_retention_days),
        "SPREADS_BACKUP_ROOT": str(_backup_root(target)),
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
    market_open_monitor = _ops_root(target) / "market_open_monitor.sh"
    log_dir = _ops_log_dir(target)
    health_minutes = max(int(target.health_check_minutes), 1)
    return "\n".join(
        [
            f"# BEGIN {marker}",
            f"@reboot {_ops_script_command(target, compose_up)} >> {shlex.quote(str(log_dir / 'compose-up.log'))} 2>&1",
            f"*/{health_minutes} * * * * {_ops_script_command(target, health)} >> {shlex.quote(str(log_dir / 'health.log'))} 2>&1",
            f"*/15 * * * * {_ops_script_command(target, market_open_monitor)} >> {shlex.quote(str(log_dir / 'market-open-monitor.log'))} 2>&1",
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
    command = textwrap.dedent(
        f"""
        set -euo pipefail
        sudo mv {shlex.quote(unit_path)} /etc/systemd/system/{shlex.quote(target.service_name)}
        sudo systemctl daemon-reload
        sudo systemctl enable --now {shlex.quote(target.service_name)}
        """
    ).strip()
    _run_command(_ssh_command(target, command))


def install_target_ops_automation(target: DeployTarget) -> None:
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
                f"test -f {shlex.quote(str(ops_root / 'market_open_monitor.sh'))}",
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
                f"chmod +x {shlex.quote(str(ops_root / 'market_open_monitor.sh'))}",
                f"rm -f {shlex.quote(str(Path(target.deploy_root) / '.ops' / 'compose_up.sh'))}",
                f"rm -f {shlex.quote(str(Path(target.deploy_root) / '.ops' / 'backup_postgres.sh'))}",
                f"rm -f {shlex.quote(str(Path(target.deploy_root) / '.ops' / 'health_check.sh'))}",
                f"rm -f {shlex.quote(str(Path(target.deploy_root) / '.ops' / 'market_open_monitor.sh'))}",
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
    command = textwrap.dedent(
        f"""
        set -euo pipefail
        marker_begin="# BEGIN {marker}"
        marker_end="# END {marker}"
        tmp_file="$(mktemp)"
        (crontab -l 2>/dev/null || true) | sed "/$marker_begin/,/$marker_end/d" > "$tmp_file"
        cat {shlex.quote(cron_block_remote)} >> "$tmp_file"
        crontab "$tmp_file"
        rm -f "$tmp_file"
        """
    ).strip()
    run_shell(command)


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
        return
    if not target.is_remote:
        _ensure_local_env_file(target, require_secrets=require_secrets)
    _run_command(_compose_up_args(target, build=build), cwd=target.deploy_path)


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
        return _run_passthrough_command(
            _remote_shell_command(target, _render_shell_command(command))
        )
    cwd = target.deploy_path
    return _run_passthrough_command(
        command,
        cwd=cwd,
        env=_local_target_command_env(target, require_secrets=False),
    )


def exec_spreads_command(target: DeployTarget, cli_args: list[str]) -> int:
    command = ["uv", "run", "spreads", *cli_args]
    if target.is_remote and not _can_run_target_locally(target):
        return _run_passthrough_command(
            _remote_shell_command(target, _render_shell_command(command))
        )
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
        "redis_port": target.redis_port,
        "web_port": target.web_port,
        "worker_runtime_replicas": target.worker_runtime_replicas,
        "worker_discovery_replicas": target.worker_discovery_replicas,
        "worker_research_replicas": target.worker_research_replicas,
        "web_enabled": target.web_enabled,
        "postgres_volume_name": target.postgres_volume_name,
        "docker_log_driver": target.docker_log_driver,
        "docker_log_max_size": target.docker_log_max_size,
        "docker_log_max_file": target.docker_log_max_file,
        "backup_retention_days": target.backup_retention_days,
        "health_check_minutes": target.health_check_minutes,
        "excluded_job_types": list(target.excluded_job_types),
        "market_recorder_owner_env": target.market_recorder_owner_env,
        "nautilus_bridge_host_binary": target.nautilus_bridge_host_binary,
        "nautilus_bridge_container_binary": target.nautilus_bridge_container_binary,
        "nautilus_bridge_command": target.nautilus_bridge_command,
        "nautilus_bridge_timeout_seconds": target.nautilus_bridge_timeout_seconds,
        "overlay_env_file": str(target.local_overlay_env_path),
        "service_name": target.service_name,
    }


__all__ = [
    "DeploymentConfigError",
    "DeployTarget",
    "build_host_env_values",
    "bootstrap_remote_target",
    "deploy_target_payload",
    "exec_spreads_command",
    "get_deploy_target",
    "install_systemd_service",
    "install_target_ops_automation",
    "list_deploy_targets",
    "logs_deploy_target",
    "run_target_spreads_command",
    "render_deploy_env_file",
    "render_host_env_file",
    "render_prod_compose",
    "restart_deploy_target_services",
    "start_deploy_target",
    "status_deploy_target",
    "stop_deploy_target",
    "sync_deploy_target",
]
