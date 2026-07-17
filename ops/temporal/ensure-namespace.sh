#!/bin/sh
set -eu

address=${TEMPORAL_ADDRESS:-temporal:7233}
namespace=${TEMPORAL_NAMESPACE:-default}
retention=${TEMPORAL_NAMESPACE_RETENTION:-7d}
max_attempts=${TEMPORAL_HEALTH_CHECK_MAX_ATTEMPTS:-30}
attempt=1

until temporal operator cluster health --address "$address" >/dev/null 2>&1; do
  if [ "$attempt" -ge "$max_attempts" ]; then
    echo "Temporal did not become healthy after $max_attempts attempts" >&2
    exit 1
  fi
  attempt=$((attempt + 1))
  sleep 2
done

if temporal operator namespace describe --address "$address" --namespace "$namespace" >/dev/null 2>&1; then
  temporal operator namespace update --address "$address" --namespace "$namespace" --retention "$retention"
else
  temporal operator namespace create --address "$address" --namespace "$namespace" --retention "$retention"
fi
