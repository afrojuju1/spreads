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

tz="${SPREADS_MARKET_MONITOR_TZ:-America/New_York}"
dow="$(TZ="$tz" date +%u)"
hour="$(TZ="$tz" date +%H)"
minute="$(TZ="$tz" date +%M)"
minutes_since_midnight=$((10#$hour * 60 + 10#$minute))
start_minutes="${SPREADS_MARKET_MONITOR_START_MINUTES:-565}" # 09:25 ET
end_minutes="${SPREADS_MARKET_MONITOR_END_MINUTES:-965}"     # 16:05 ET

if (( dow < 1 || dow > 5 )); then
  exit 0
fi

if (( minutes_since_midnight < start_minutes || minutes_since_midnight > end_minutes )); then
  exit 0
fi

if [[ -n "${SPREADS_DEPLOY_ENV:-}" ]]; then
  uv run spreads live-doctor --env "$SPREADS_DEPLOY_ENV" --no-color
else
  uv run spreads live-doctor --no-color
fi
