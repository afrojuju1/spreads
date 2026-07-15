from __future__ import annotations

from datetime import UTC, datetime, timedelta
import gzip
import os
from pathlib import Path
import shutil
import subprocess
from typing import Any
from urllib.parse import unquote, urlparse

from core.services.ops.jobs.state_compact import build_jobs_compact_state
from core.services.ops.storage.state import build_storage_ops_state
from core.services.ops.trading.state import build_trading_ops_state


def run_postgres_backup(*, database_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    parsed = urlparse(database_url)
    backup_root = Path(str(payload.get("backup_root") or os.environ.get("SPREADS_BACKUP_ROOT") or "/app/backups/postgres"))
    backup_root.mkdir(parents=True, exist_ok=True)
    created_at = datetime.now(UTC)
    destination = backup_root / f"spreads-{created_at.strftime('%Y%m%dT%H%M%SZ')}.sql.gz"
    temporary_destination = destination.with_name(f"{destination.name}.tmp")
    command = [
        "pg_dump",
        "--host", parsed.hostname or "postgres",
        "--port", str(parsed.port or 5432),
        "--username", unquote(parsed.username or "spreads"),
        "--dbname", (parsed.path or "/spreads").lstrip("/"),
        "--no-owner",
        "--no-privileges",
    ]
    env = {**os.environ, "PGPASSWORD": unquote(parsed.password or "")}
    process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE)
    try:
        if process.stdout is None:
            raise RuntimeError("pg_dump did not expose a stdout stream")
        with process.stdout, gzip.open(temporary_destination, "wb") as output:
            shutil.copyfileobj(process.stdout, output, length=1024 * 1024)
        return_code = process.wait()
        if return_code:
            raise subprocess.CalledProcessError(return_code, command)
        temporary_destination.chmod(0o600)
        if os.geteuid() == 0:
            backup_owner = backup_root.stat()
            os.chown(temporary_destination, backup_owner.st_uid, backup_owner.st_gid)
        temporary_destination.replace(destination)
    except Exception:
        process.kill()
        process.wait()
        temporary_destination.unlink(missing_ok=True)
        raise

    retention_days = max(int(payload.get("retention_days") or os.environ.get("SPREADS_BACKUP_RETENTION_DAYS") or 14), 1)
    cutoff = created_at - timedelta(days=retention_days)
    removed: list[str] = []
    for path in backup_root.glob("spreads-*.sql.gz"):
        if datetime.fromtimestamp(path.stat().st_mtime, tz=UTC) < cutoff:
            path.unlink()
            removed.append(path.name)
    return {
        "status": "succeeded",
        "artifact": str(destination),
        "size_bytes": destination.stat().st_size,
        "retention_days": retention_days,
        "removed_artifacts": removed,
    }


def run_ops_health_snapshot(*, database_url: str) -> dict[str, Any]:
    states = {
        "trading": build_trading_ops_state(db_target=database_url),
        "storage": build_storage_ops_state(db_target=database_url),
        "jobs": build_jobs_compact_state(db_target=database_url),
    }
    statuses = {name: str(state.get("status") or "unknown") for name, state in states.items()}
    return {
        "status": "succeeded",
        "observed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "component_statuses": statuses,
        "operator_status": "blocked" if "blocked" in statuses.values() else ("degraded" if "degraded" in statuses.values() else "healthy"),
    }


def run_ops_log_retention(*, payload: dict[str, Any]) -> dict[str, Any]:
    log_root = Path(str(payload.get("log_root") or os.environ.get("SPREADS_OPS_LOG_DIR") or "/app/logs/ops"))
    retention_days = max(int(payload.get("retention_days") or 14), 1)
    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    removed: list[str] = []
    if log_root.exists():
        for path in log_root.rglob("*.log*"):
            if path.is_file() and datetime.fromtimestamp(path.stat().st_mtime, tz=UTC) < cutoff:
                path.unlink()
                removed.append(str(path))
    return {"status": "succeeded", "retention_days": retention_days, "removed_files": removed}
