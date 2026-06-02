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
health_command=(uv run spreads live-doctor --json)
if [[ -n "$target_env" ]]; then
  health_command+=(--env "$target_env")
fi
set +e
health_output="$("${health_command[@]}" 2>&1)"
health_exit_code=$?
set -e

SPREADS_HEALTH_TARGET_ENV="${target_env:-local}" \
SPREADS_HEALTH_EXIT_CODE="$health_exit_code" \
SPREADS_HEALTH_RAW="$health_output" \
uv run python -c '
from __future__ import annotations

import json
import os
from datetime import UTC, datetime

raw = os.environ.get("SPREADS_HEALTH_RAW", "")
target_env = os.environ.get("SPREADS_HEALTH_TARGET_ENV")
exit_code = int(os.environ.get("SPREADS_HEALTH_EXIT_CODE") or 0)
timestamp = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
try:
    payload = json.loads(raw)
except Exception:
    print(
        json.dumps(
            {
                "event": "ops_health",
                "timestamp": timestamp,
                "target_env": target_env,
                "status": "unknown",
                "exit_code": exit_code,
                "parse_error": True,
                "raw_excerpt": raw[-800:],
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    raise SystemExit(0)

summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
attention = payload.get("attention") if isinstance(payload.get("attention"), list) else []
record = {
    "event": "ops_health",
    "timestamp": timestamp,
    "target_env": target_env,
    "status": payload.get("status"),
    "exit_code": exit_code,
    "market_date": summary.get("market_date"),
    "market_session_status": summary.get("market_session_status"),
    "trading_allowed": summary.get("trading_allowed"),
    "scheduler_status": summary.get("scheduler_status"),
    "worker_lane_count": summary.get("worker_lane_count"),
    "blocked_worker_lane_count": summary.get("blocked_worker_lane_count"),
    "actionable_failed_job_count": summary.get("actionable_failed_job_count"),
    "broker_sync_status": summary.get("broker_sync_status"),
    "finviz_feed_status": summary.get("finviz_feed_status"),
    "finviz_feed_symbol_count": summary.get("finviz_feed_symbol_count"),
    "finviz_direct_status": summary.get("finviz_direct_status"),
    "finviz_direct_candidate_count": summary.get("finviz_direct_candidate_count"),
    "active_intent_count": summary.get("active_intent_count"),
    "open_position_count": summary.get("open_position_count"),
    "net_pnl": summary.get("net_pnl"),
    "attention_count": len(attention),
    "attention_codes": [
        item.get("code")
        for item in attention[:5]
        if isinstance(item, dict) and item.get("code")
    ],
}
print(json.dumps(record, separators=(",", ":"), sort_keys=True))
'
exit "$health_exit_code"
