#!/bin/sh
# Adapted from Temporal's official samples-server PostgreSQL setup script.
set -eu

: "${POSTGRES_SEEDS:?POSTGRES_SEEDS is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"

port=${DB_PORT:-5432}
nc -z -w 10 "$POSTGRES_SEEDS" "$port"

for database in temporal temporal_visibility; do
  if [ "$database" = temporal ]; then
    schema=temporal
  else
    schema=visibility
  fi
  temporal-sql-tool --plugin postgres12 --ep "$POSTGRES_SEEDS" -u "$POSTGRES_USER" -p "$port" --db "$database" create
  temporal-sql-tool --plugin postgres12 --ep "$POSTGRES_SEEDS" -u "$POSTGRES_USER" -p "$port" --db "$database" setup-schema -v 0.0
  temporal-sql-tool --plugin postgres12 --ep "$POSTGRES_SEEDS" -u "$POSTGRES_USER" -p "$port" --db "$database" \
    update-schema -d "/etc/temporal/schema/postgresql/v12/$schema/versioned"
done
