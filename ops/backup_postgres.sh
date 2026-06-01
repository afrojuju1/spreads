#!/usr/bin/env bash
set -euo pipefail
umask 077

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

container_env_file="${SPREADS_CONTAINER_ENV_FILE:-.env}"
compose_file="${SPREADS_COMPOSE_FILE:-docker-compose.yml}"
backup_retention_days="${SPREADS_BACKUP_RETENTION_DAYS:-7}"
backup_root="${SPREADS_BACKUP_ROOT:-$HOME/spreads/backups/postgres}"
web_enabled="${SPREADS_WEB_ENABLED:-true}"

mkdir -p "$backup_root"

args=(
  docker compose
  --env-file "$container_env_file"
  -f "$compose_file"
)
if [[ "$web_enabled" == "true" ]]; then
  args+=(--profile web)
fi

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
tmp_path="$backup_root/spreads-${stamp}.sql.gz.tmp"
final_path="$backup_root/spreads-${stamp}.sql.gz"

"${args[@]}" exec -T postgres /bin/sh -lc 'PGPASSWORD="$POSTGRES_PASSWORD" pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB"' | gzip -c > "$tmp_path"
mv "$tmp_path" "$final_path"
find "$backup_root" -type f -name 'spreads-*.sql.gz' -mtime +"$backup_retention_days" -delete
