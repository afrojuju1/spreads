#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

log_dir="${SPREADS_OPS_LOG_DIR:-$repo_root/logs/ops}"
max_size="${SPREADS_OPS_LOG_MAX_SIZE_BYTES:-10485760}"
keep="${SPREADS_OPS_LOG_MAX_FILE:-5}"

mkdir -p "$log_dir"

size_to_bytes() {
  local raw="${1,,}"
  case "$raw" in
    *k) echo "$((${raw%k} * 1024))" ;;
    *m) echo "$((${raw%m} * 1024 * 1024))" ;;
    *g) echo "$((${raw%g} * 1024 * 1024 * 1024))" ;;
    *) echo "$raw" ;;
  esac
}

max_size_bytes="$(size_to_bytes "$max_size")"
if (( max_size_bytes < 1 )); then
  max_size_bytes=10485760
fi
if (( keep < 1 )); then
  keep=5
fi

timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
shopt -s nullglob
for log_path in "$log_dir"/*.log; do
  base="$(basename "$log_path")"
  if [[ "$base" == "log-rotate.log" ]]; then
    continue
  fi
  size_bytes="$(wc -c < "$log_path")"
  if (( size_bytes < max_size_bytes )); then
    continue
  fi

  rm -f "$log_path.$keep.gz"
  for ((idx = keep - 1; idx >= 1; idx--)); do
    if [[ -f "$log_path.$idx.gz" ]]; then
      mv "$log_path.$idx.gz" "$log_path.$((idx + 1)).gz"
    fi
  done
  gzip -c "$log_path" > "$log_path.1.gz.tmp"
  mv "$log_path.1.gz.tmp" "$log_path.1.gz"
  : > "$log_path"
  printf '{"event":"ops_log_rotated","timestamp":"%s","path":"%s","size_bytes":%s,"keep":%s}\n' \
    "$timestamp" "$log_path" "$size_bytes" "$keep"
done
