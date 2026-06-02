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

target_env="${SPREADS_DEPLOY_ENV:-}"
command=(uv run spreads retention prune --execute --json)
if [[ -n "$target_env" ]]; then
  command+=(--env "$target_env")
fi
"${command[@]}"
