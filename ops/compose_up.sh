#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

container_env_file="${SPREADS_CONTAINER_ENV_FILE:-.env.deploy.${SPREADS_DEPLOY_ENV:-local}}"
compose_file="${SPREADS_COMPOSE_FILE:-docker-compose.prod.yml}"
runtime_replicas="${SPREADS_WORKER_RUNTIME_REPLICAS:-1}"
discovery_replicas="${SPREADS_WORKER_DISCOVERY_REPLICAS:-2}"
research_replicas="${SPREADS_WORKER_RESEARCH_REPLICAS:-0}"
web_enabled="${SPREADS_WEB_ENABLED:-true}"

args=(
  docker compose
  --env-file "$container_env_file"
  -f "$compose_file"
)
if [[ "$web_enabled" == "true" ]]; then
  args+=(--profile web)
fi
args+=(
  up -d --remove-orphans
  --scale "worker-runtime=${runtime_replicas}"
  --scale "worker-discovery=${discovery_replicas}"
  --scale "worker-research=${research_replicas}"
)

"${args[@]}"
