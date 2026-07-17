# Temporal operations

Spreads runs a single-node self-hosted Temporal cluster for paper trading. PostgreSQL owns the `temporal` persistence store and the `temporal_visibility` visibility store. Single-node availability is accepted; database backups, explicit schema ownership, sequential upgrades, health checks, and metrics are not optional.

## Owned deployment

- `temporalio/admin-tools:1.31.2` owns schema setup and migrations through the one-shot `temporal-schema` service.
- `temporalio/server:1.31.2` runs the cluster. The application container never mutates its schema.
- `temporalio/ui:2.52.1` provides the local UI.
- The `default` namespace retains closed workflows for seven days.
- gRPC (`127.0.0.1:57233`) and the UI (`127.0.0.1:58088`) are loopback-only. Containers use `temporal:7233` on `spreads_default`.
- Server metrics are exposed only on the Docker network at `temporal:8000`. Required Python workers expose official SDK metrics at container port `9464`.
- The NUC Prometheus/Grafana deployment in `ade-tools/services/nucbox-observability` owns scraping, alerts, and the `Spreads Temporal` dashboard.

## Normal deployment

Validate the declarative stack, then let the one-shot schema and namespace services gate the server and workers:

```bash
docker compose config --quiet
docker compose up -d temporal-schema temporal temporal-namespace temporal-ui
docker compose up -d --force-recreate api workflow-runtime workflow-lifecycle workflow-data workflow-maintenance capture-worker
```

The schema service is idempotent. It creates missing databases for a fresh deployment, initializes version tables, and applies the schemas bundled with the pinned official admin-tools image. The namespace service creates `default` when absent and otherwise enforces seven-day retention.

## Backup and restore

The daily `postgres_backup` routine writes PostgreSQL custom-format dumps for `spreads`, `temporal`, and `temporal_visibility` under `backups/postgres`, mode `0600`, with the deployment's configured seven-day retention. A restore drill must use an isolated PostgreSQL container and Docker network; never restore over the live databases.

Example isolated database restore:

```bash
docker network create spreads-temporal-restore-verify
docker run -d --name spreads-temporal-restore-db \
  --network spreads-temporal-restore-verify --network-alias restore-db \
  -e POSTGRES_USER=verify -e POSTGRES_PASSWORD=verify postgres:17-alpine
docker exec spreads-temporal-restore-db createdb -U verify temporal
docker exec spreads-temporal-restore-db createdb -U verify temporal_visibility
docker exec -i spreads-temporal-restore-db pg_restore -U verify -d temporal --no-owner --no-privileges < backups/postgres/temporal-<timestamp>.dump
docker exec -i spreads-temporal-restore-db pg_restore -U verify -d temporal_visibility --no-owner --no-privileges < backups/postgres/temporal_visibility-<timestamp>.dump
```

Start the server version compatible with the restored schema on that isolated network and validate all of the following before declaring the backup usable:

- cluster ID and history shard count;
- namespace identity and retention;
- schedule count and pause state;
- open and closed workflow visibility;
- representative trade, close, routine, and capture histories.

## Upgrade policy

Follow Temporal's supported sequence: upgrade the current minor to its highest patch, then advance one minor at a time. For each hop, apply the core and visibility schema shipped for that target before starting the target server. Restore workers, prove real workflow activation, and allow approximately ten minutes for shard metadata to stabilize before advancing.

The 2026-07-16 upgrade used this verified matrix:

| Server | Schema tool image | Core schema | Visibility schema |
| --- | --- | ---: | ---: |
| 1.25.2 | `temporalio/auto-setup:1.25.2` | 1.14 | 1.6 |
| 1.26.3 | `temporalio/auto-setup:1.26.3` | 1.14 | 1.7 |
| 1.27.4 | `temporalio/auto-setup:1.27.4` | 1.16 | 1.9 |
| 1.28.4 | `temporalio/auto-setup:1.28.4` | 1.17 | 1.9 |
| 1.29.7 | `temporalio/auto-setup:1.29.7` | 1.18 | 1.9 |
| 1.30.6 | `temporalio/admin-tools:1.30.6` | 1.18 | 1.13 |
| 1.31.2 | `temporalio/admin-tools:1.31.2` | 1.19 | 1.14 |

`auto-setup` appears only in the historical upgrade matrix because matching admin-tools tags were not published for 1.25–1.29. It is used as a one-shot source of the official versioned SQL tool and schemas, never as the production server.

## Hop validation

For every version, capture:

```bash
temporal operator cluster describe --address temporal:7233 --output json
temporal operator namespace describe --address temporal:7233 --namespace default --output json
temporal schedule list --address temporal:7233 --namespace default --output json
temporal workflow list --address temporal:7233 --namespace default --query 'ExecutionStatus="Running"' --output json
```

Also verify required pollers, a newly completed scheduled workflow, the long-running capture workflow, a representative closed trade/close history, `TradingOpsState`, `JobsState`, and `StorageOpsState`.

## Rollback

Schema migrations are forward-only. Never try to reverse SQL in place.

1. Stop workers and the Temporal server.
2. If only the server binary is bad, start the immediately previous verified server against the already-upgraded schema. Temporal schemas are designed to preserve this rolling-upgrade compatibility boundary; validate histories and activation before restoring workers.
3. If persistence itself is suspect, discard the failed database instance and restore both Temporal dumps into a clean PostgreSQL instance. Start the exact server version recorded with that backup's schema boundary.
4. Do not restore `temporal` without its matching `temporal_visibility` dump.
5. Keep the failed instance intact until cluster ID, namespace, schedules, visibility, histories, and activation are proven on the rollback instance.

The 2026-07-16 drill restored the 1.25.2 backup into isolated PostgreSQL, upgraded it through every matrix row, and exercised a 1.31.2 → 1.30.6 → 1.31.2 server rollback against the final schemas without losing workflow visibility or history.

## Monitoring

The primary dashboard is `Spreads Temporal` in Grafana. Prometheus alerts cover server scrape loss, frontend TCP health, missing SDK targets, stale task-queue backlog, no-recent-poller tasks, persistence failures, workflow failures/retries, and schedule action errors.

Useful direct checks:

```bash
curl -fsS http://127.0.0.1:9090/api/v1/query?query=up%7Bjob%3D%22temporal-server%22%7D
curl -fsS http://127.0.0.1:9090/api/v1/query?query=probe_success%7Bjob%3D%22temporal-frontend%22%7D
curl -fsS http://127.0.0.1:9090/api/v1/query?query=count%28count%20by%20%28instance%29%20%28up%7Bjob%3D%22temporal-sdk%22%7D%3D%3D1%29%29
curl -fsS 'http://127.0.0.1:9090/api/v1/query?query=ALERTS%7Balertstate%3D%22firing%22%2Calertname%3D~%22Temporal.%2A%22%7D'
```

Primary upstream guidance:

- [Temporal server upgrades](https://docs.temporal.io/self-hosted-guide/upgrade-server)
- [Temporal self-hosted monitoring](https://docs.temporal.io/self-hosted-guide/monitoring)
- [Official samples-server Compose deployment](https://github.com/temporalio/samples-server/tree/main/compose)
