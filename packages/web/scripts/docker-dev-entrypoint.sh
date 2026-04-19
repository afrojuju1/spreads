#!/bin/sh

set -eu

cd /app

LOCKFILE="package-lock.json"
STAMP_DIR="node_modules/.spreads-dev"
STAMP_FILE="${STAMP_DIR}/package-lock.sha256"

current_lock_hash() {
  sha256sum "${LOCKFILE}" | awk '{print $1}'
}

needs_install="0"

if [ ! -f "${LOCKFILE}" ]; then
  echo "[web-dev] Missing ${LOCKFILE}; cannot synchronize dependencies." >&2
  exit 1
fi

if [ ! -d node_modules ]; then
  needs_install="1"
elif [ ! -x node_modules/.bin/next ]; then
  needs_install="1"
elif [ ! -f "${STAMP_FILE}" ]; then
  needs_install="1"
else
  stored_lock_hash="$(cat "${STAMP_FILE}")"
  if [ "${stored_lock_hash}" != "$(current_lock_hash)" ]; then
    needs_install="1"
  fi
fi

if [ "${needs_install}" = "1" ]; then
  echo "[web-dev] Synchronizing node_modules with ${LOCKFILE}..."
  npm ci
  mkdir -p "${STAMP_DIR}"
  current_lock_hash > "${STAMP_FILE}"
else
  echo "[web-dev] node_modules already matches ${LOCKFILE}."
fi

exec npm run dev -- --hostname 0.0.0.0 --port 3000
