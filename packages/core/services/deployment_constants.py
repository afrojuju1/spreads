from __future__ import annotations

import re
from pathlib import Path

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
    "CLICKHOUSE_PASSWORD",
    "POSTGRES_PASSWORD",
)
OPTIONAL_SECRET_KEYS = ("SPREADS_DISCORD_WEBHOOK_URL",)

ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")

__all__ = [
    "DEPLOY_OPS_ROOT",
    "DEPLOY_TARGETS_ROOT",
    "ENV_PATTERN",
    "OPTIONAL_SECRET_KEYS",
    "PROD_COMPOSE_PATH",
    "REPO_ROOT",
    "REPO_SYNC_EXCLUDES",
    "REQUIRED_SECRET_KEYS",
]
