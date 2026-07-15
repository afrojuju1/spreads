from __future__ import annotations

import os
import re
from pathlib import Path
from urllib.parse import unquote, urlparse

from dotenv import dotenv_values

from core.services.deployment_constants import (
    ENV_PATTERN,
    PROD_COMPOSE_PATH,
    REPO_ROOT,
    REQUIRED_SECRET_KEYS,
)
from core.services.deployment_paths import _ops_log_dir
from core.services.deployment_targets import DeployTarget, DeploymentConfigError
from core.value_coercion import as_text


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    payload = dotenv_values(path)
    resolved: dict[str, str] = {}
    for key, value in payload.items():
        text = as_text(value)
        if key and text is not None:
            resolved[str(key)] = text
    return resolved


def _merged_secret_sources(target: DeployTarget) -> dict[str, str]:
    merged: dict[str, str] = {}
    merged.update(_load_env_file(REPO_ROOT / ".env"))
    merged.update(_load_env_file(target.local_overlay_env_path))
    for key, value in os.environ.items():
        text = as_text(value)
        if text is not None:
            merged[key] = text
    return merged


def _secret_value(
    merged: dict[str, str],
    *keys: str,
    default: str | None = None,
) -> str | None:
    for key in keys:
        text = as_text(merged.get(key))
        if text is not None:
            return text
    return default


def _password_from_url(value: str | None) -> str | None:
    text = as_text(value)
    if text is None:
        return None
    try:
        password = urlparse(text).password
    except ValueError:
        return None
    return None if password is None else unquote(password)


def _web_allowed_dev_origins(target: DeployTarget) -> str:
    origins = ["localhost", "127.0.0.1"]
    for value in (target.bind_host, target.ssh_host, target.name):
        normalized = as_text(value)
        if normalized is None or normalized == "0.0.0.0":
            continue
        origins.append(normalized)
    return ",".join(dict.fromkeys(origins))


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
        default=_password_from_url(merged.get("SPREADS_DATABASE_URL")) or "replace-me",
    )
    clickhouse_password = _secret_value(
        merged,
        "CLICKHOUSE_PASSWORD",
        "SPREADS_CLICKHOUSE_PASSWORD",
        default=(
            _password_from_url(merged.get("SPREADS_CLICKHOUSE_URL"))
            or ("spreads" if target.compose_file == "docker-compose.yml" else "replace-me")
        ),
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
        default="/home/ade/Projects/spreads/external/TradingAgents",
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
    web_allowed_dev_origins = _web_allowed_dev_origins(target)

    env_values = {
        "SPREADS_DEPLOY_ENV": target.name,
        "SPREADS_COMPOSE_PROJECT_NAME": target.compose_project_name,
        "SPREADS_CONTAINER_ENV_FILE": target.env_file,
        "SPREADS_BIND_HOST": target.bind_host,
        "SPREADS_API_PORT": str(target.api_port),
        "SPREADS_POSTGRES_PORT": str(target.postgres_port),
        "SPREADS_CLICKHOUSE_PORT": str(target.clickhouse_port),
        "SPREADS_REDIS_PORT": str(target.redis_port),
        "SPREADS_WEB_PORT": str(target.web_port),
        "SPREADS_POSTGRES_VOLUME_NAME": target.postgres_volume_name,
        "SPREADS_CLICKHOUSE_VOLUME_NAME": target.clickhouse_volume_name,
        "SPREADS_DOCKER_LOG_DRIVER": target.docker_log_driver,
        "SPREADS_DOCKER_LOG_MAX_SIZE": target.docker_log_max_size,
        "SPREADS_DOCKER_LOG_MAX_FILE": str(target.docker_log_max_file),
        "SPREADS_COMPOSE_FILE": target.compose_file,
        "SPREADS_WEB_ENABLED": "true" if target.web_enabled else "false",
        "SPREADS_RUNTIME_LANE_REPLICAS": str(target.runtime_lane_replicas),
        "SPREADS_DATA_LANE_REPLICAS": str(target.data_lane_replicas),
        "SPREADS_MAINTENANCE_LANE_REPLICAS": str(target.maintenance_lane_replicas),
        "SPREADS_VALUATION_LANE_REPLICAS": str(target.valuation_lane_replicas),
        "SPREADS_RESEARCH_LANE_REPLICAS": str(target.research_lane_replicas),
        "SPREADS_DISABLED_WORKFLOW_LANES": ",".join(
            lane
            for lane, replicas in (
                ("valuation", target.valuation_lane_replicas),
                ("research", target.research_lane_replicas),
            )
            if replicas == 0
        ),
        "SPREADS_BACKUP_RETENTION_DAYS": str(target.backup_retention_days),
        "SPREADS_HEALTH_CHECK_MINUTES": str(target.health_check_minutes),
        "SPREADS_OPS_LOG_DIR": str(_ops_log_dir(target)),
        "SPREADS_WEB_INTERNAL_API_BASE_URL": "http://api:8000",
        "SPREADS_WEB_ALLOWED_DEV_ORIGINS": web_allowed_dev_origins,
        "POSTGRES_DB": "spreads",
        "POSTGRES_USER": "spreads",
        "POSTGRES_PASSWORD": str(postgres_password),
        "CLICKHOUSE_DB": "spreads",
        "CLICKHOUSE_USER": "spreads",
        "CLICKHOUSE_PASSWORD": str(clickhouse_password),
        "SPREADS_DATABASE_URL": (f"postgresql://spreads:{postgres_password}@postgres:5432/spreads"),
        "SPREADS_CLICKHOUSE_URL": f"http://spreads:{clickhouse_password}@clickhouse:8123/spreads",
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
        "SPREADS_CAPTURE_OWNER_TARGET": str(as_text(target.capture_owner_target) or ""),
    }

    if require_secrets:
        missing = [key for key in REQUIRED_SECRET_KEYS if as_text(env_values.get(key)) in {None, "replace-me"}]
        if missing:
            raise DeploymentConfigError("Missing required deployment secrets: " + ", ".join(sorted(missing)))
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
    clickhouse_password = values["CLICKHOUSE_PASSWORD"]
    if target.compose_file == "docker-compose.yml":
        postgres_password = "spreads"
        clickhouse_password = "spreads"
    host_bind_host = values["SPREADS_BIND_HOST"]
    host_database_url = (
        f"postgresql://{values['POSTGRES_USER']}:{postgres_password}@{host_bind_host}:{values['SPREADS_POSTGRES_PORT']}/{values['POSTGRES_DB']}"
    )
    host_redis_url = f"redis://{host_bind_host}:{values['SPREADS_REDIS_PORT']}/0"
    host_clickhouse_url = f"http://{values['CLICKHOUSE_USER']}:{clickhouse_password}@{host_bind_host}:{values['SPREADS_CLICKHOUSE_PORT']}/spreads"
    api_base_url = f"http://{host_bind_host}:{values['SPREADS_API_PORT']}"
    return {
        "SPREADS_DEPLOY_ENV": values["SPREADS_DEPLOY_ENV"],
        "SPREADS_DISABLED_WORKFLOW_LANES": values["SPREADS_DISABLED_WORKFLOW_LANES"],
        "SPREADS_CONTAINER_ENV_FILE": values["SPREADS_CONTAINER_ENV_FILE"],
        "SPREADS_COMPOSE_FILE": values["SPREADS_COMPOSE_FILE"],
        "SPREADS_WEB_ENABLED": values["SPREADS_WEB_ENABLED"],
        "SPREADS_RUNTIME_LANE_REPLICAS": values["SPREADS_RUNTIME_LANE_REPLICAS"],
        "SPREADS_DATA_LANE_REPLICAS": values["SPREADS_DATA_LANE_REPLICAS"],
        "SPREADS_MAINTENANCE_LANE_REPLICAS": values["SPREADS_MAINTENANCE_LANE_REPLICAS"],
        "SPREADS_VALUATION_LANE_REPLICAS": values["SPREADS_VALUATION_LANE_REPLICAS"],
        "SPREADS_RESEARCH_LANE_REPLICAS": values["SPREADS_RESEARCH_LANE_REPLICAS"],
        "SPREADS_BACKUP_RETENTION_DAYS": values["SPREADS_BACKUP_RETENTION_DAYS"],
        "SPREADS_HEALTH_CHECK_MINUTES": values["SPREADS_HEALTH_CHECK_MINUTES"],
        "SPREADS_DATABASE_URL": host_database_url,
        "DATABASE_URL": host_database_url,
        "SPREADS_CLICKHOUSE_URL": host_clickhouse_url,
        "CLICKHOUSE_URL": host_clickhouse_url,
        "REDIS_URL": host_redis_url,
        "SPREADS_API_BASE_URL": api_base_url,
        "SPREADS_INTERNAL_API_BASE_URL": api_base_url,
        "SPREADS_OPS_LOG_DIR": str(_ops_log_dir(target)),
        "APCA_API_KEY_ID": values["APCA_API_KEY_ID"],
        "APCA_API_SECRET_KEY": values["APCA_API_SECRET_KEY"],
        "SPREADS_DISCORD_WEBHOOK_URL": values["SPREADS_DISCORD_WEBHOOK_URL"],
        "SPREADS_TRADINGAGENTS_HOST_DIR": values["SPREADS_TRADINGAGENTS_HOST_DIR"],
        "SPREADS_TRADINGAGENTS_DIR": values["SPREADS_TRADINGAGENTS_HOST_DIR"],
        "SPREADS_TRADINGAGENTS_UV_ENVIRONMENT": str(Path(values["SPREADS_TRADINGAGENTS_HOST_DIR"]) / ".venv"),
        "OLLAMA_BASE_URL": "http://localhost:11434/v1",
        "SPREADS_CAPTURE_OWNER_TARGET": values["SPREADS_CAPTURE_OWNER_TARGET"],
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
        f"SPREADS_DISABLED_WORKFLOW_LANES={values['SPREADS_DISABLED_WORKFLOW_LANES']}",
        f"SPREADS_COMPOSE_PROJECT_NAME={values['SPREADS_COMPOSE_PROJECT_NAME']}",
        f"SPREADS_CONTAINER_ENV_FILE={values['SPREADS_CONTAINER_ENV_FILE']}",
        f"SPREADS_COMPOSE_FILE={values['SPREADS_COMPOSE_FILE']}",
        f"SPREADS_BIND_HOST={values['SPREADS_BIND_HOST']}",
        f"SPREADS_API_PORT={values['SPREADS_API_PORT']}",
        f"SPREADS_POSTGRES_PORT={values['SPREADS_POSTGRES_PORT']}",
        f"SPREADS_CLICKHOUSE_PORT={values['SPREADS_CLICKHOUSE_PORT']}",
        f"SPREADS_REDIS_PORT={values['SPREADS_REDIS_PORT']}",
        f"SPREADS_WEB_PORT={values['SPREADS_WEB_PORT']}",
        f"SPREADS_WEB_ENABLED={values['SPREADS_WEB_ENABLED']}",
        f"SPREADS_RUNTIME_LANE_REPLICAS={values['SPREADS_RUNTIME_LANE_REPLICAS']}",
        f"SPREADS_DATA_LANE_REPLICAS={values['SPREADS_DATA_LANE_REPLICAS']}",
        f"SPREADS_MAINTENANCE_LANE_REPLICAS={values['SPREADS_MAINTENANCE_LANE_REPLICAS']}",
        f"SPREADS_VALUATION_LANE_REPLICAS={values['SPREADS_VALUATION_LANE_REPLICAS']}",
        f"SPREADS_RESEARCH_LANE_REPLICAS={values['SPREADS_RESEARCH_LANE_REPLICAS']}",
        f"SPREADS_BACKUP_RETENTION_DAYS={values['SPREADS_BACKUP_RETENTION_DAYS']}",
        f"SPREADS_HEALTH_CHECK_MINUTES={values['SPREADS_HEALTH_CHECK_MINUTES']}",
        f"SPREADS_OPS_LOG_DIR={values['SPREADS_OPS_LOG_DIR']}",
        f"SPREADS_POSTGRES_VOLUME_NAME={values['SPREADS_POSTGRES_VOLUME_NAME']}",
        f"SPREADS_CLICKHOUSE_VOLUME_NAME={values['SPREADS_CLICKHOUSE_VOLUME_NAME']}",
        f"SPREADS_DOCKER_LOG_DRIVER={values['SPREADS_DOCKER_LOG_DRIVER']}",
        f"SPREADS_DOCKER_LOG_MAX_SIZE={values['SPREADS_DOCKER_LOG_MAX_SIZE']}",
        f"SPREADS_DOCKER_LOG_MAX_FILE={values['SPREADS_DOCKER_LOG_MAX_FILE']}",
        f"SPREADS_CAPTURE_OWNER_TARGET={values['SPREADS_CAPTURE_OWNER_TARGET']}",
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
        f"CLICKHOUSE_DB={values['CLICKHOUSE_DB']}",
        f"CLICKHOUSE_USER={values['CLICKHOUSE_USER']}",
        f"CLICKHOUSE_PASSWORD={values['CLICKHOUSE_PASSWORD']}",
        f"SPREADS_DATABASE_URL={values['SPREADS_DATABASE_URL']}",
        f"SPREADS_CLICKHOUSE_URL={values['SPREADS_CLICKHOUSE_URL']}",
        f"REDIS_URL={values['REDIS_URL']}",
        f"SPREADS_INTERNAL_API_BASE_URL={values['SPREADS_INTERNAL_API_BASE_URL']}",
        f"SPREADS_API_BASE_URL={values['SPREADS_API_BASE_URL']}",
        f"SPREADS_WEB_INTERNAL_API_BASE_URL={values['SPREADS_WEB_INTERNAL_API_BASE_URL']}",
        f"SPREADS_WEB_ALLOWED_DEV_ORIGINS={values['SPREADS_WEB_ALLOWED_DEV_ORIGINS']}",
        f"NEXT_PUBLIC_SPREADS_API_WS_PORT={values['NEXT_PUBLIC_SPREADS_API_WS_PORT']}",
        f"SPREADS_TRADINGAGENTS_HOST_DIR={values['SPREADS_TRADINGAGENTS_HOST_DIR']}",
        f"SPREADS_TRADINGAGENTS_DIR={values['SPREADS_TRADINGAGENTS_DIR']}",
        f"SPREADS_TRADINGAGENTS_UV_ENVIRONMENT={values['SPREADS_TRADINGAGENTS_UV_ENVIRONMENT']}",
        f"OLLAMA_BASE_URL={values['OLLAMA_BASE_URL']}",
        "",
    ]
    return "\n".join(lines)


def render_prod_compose(target: DeployTarget) -> str:
    values = build_deploy_env_values(target, require_secrets=False)
    template = PROD_COMPOSE_PATH.read_text()

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            raise DeploymentConfigError(f"Compose template references missing deployment key {key!r}.")
        return values[key]

    return ENV_PATTERN.sub(replace, template)


__all__ = [
    "build_deploy_env_values",
    "build_host_env_values",
    "render_deploy_env_file",
    "render_host_env_file",
    "render_prod_compose",
]
