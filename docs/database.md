# Database Setup

## Local Services

Start Postgres and Redis:

```bash
docker compose up -d postgres redis
```

If you previously used the old Docker init-schema bootstrap, reset the local volume once:

```bash
docker compose down -v
docker compose up -d postgres redis
```

Default connection URL:

```text
postgresql://spreads:spreads@localhost:55432/spreads
```

Add this to your local `.env`:

```bash
SPREADS_DATABASE_URL=postgresql://spreads:spreads@localhost:55432/spreads
```

The backend now auto-loads the repo-root `.env` on startup for CLI commands, the API app, and Alembic.

## Migrations

Apply the schema with Alembic:

```bash
uv run alembic upgrade head
```

There is no job-definition seed step anymore. Scheduled job declarations now load directly from YAML under `packages/config/`.

Create a new migration:

```bash
uv run alembic revision -m "describe change"
```

Rollback one revision:

```bash
uv run alembic downgrade -1
```

## DB Selection

The runtime history backend is Postgres only.

Resolution order:

1. `SPREADS_DATABASE_URL`
2. `DATABASE_URL`
3. local Docker default: `postgresql://spreads:spreads@localhost:55432/spreads`

The existing commands use Postgres automatically:

```bash
uv run spreads config validate --json
uv run spreads status --json
uv run spreads storage --json
uv run spreads jobs --json
```

ARQ orchestration defaults:

```bash
uv run spreads scheduler
uv run arq core.jobs.worker.RuntimeWorkerSettings
uv run arq core.jobs.worker.DataWorkerSettings
uv run arq core.jobs.worker.ValuationWorkerSettings
uv run arq core.jobs.worker.ResearchWorkerSettings
```

Redis default connection URL:

```text
redis://localhost:56379/0
```

Ticker sources, strategy runs, execution facts, capture targets, option quote/trade ticks, and capture summaries are persisted directly to Postgres. Discovery-run and generic DB event-log surfaces are retired from the active runtime.

Discord alert delivery is optional. If configured, alerts are sent through:

```bash
SPREADS_DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

The runtime also accepts legacy `DISCORD_WEBHOOK_URL` if that is already present in your local `.env`.

If the webhook is missing, alert rows are still persisted in Postgres with status `skipped`.

Operator analytics are exposed through the ops CLI and API read models; ad hoc scanner commands should not become new durable ownership paths.

## API

The FastAPI app is DB-backed. Useful active endpoints include:

- `/health`
- `/internal/trading-ops/state`
- `/internal/storage-ops/state`
- `/account/overview`
- `/control/state`
- `/positions`
- `/executions/runtimes`
- `/company-valuation/screen`

## Notes

- Docker Compose can run `postgres`, `redis`, `api`, `worker`, and `scheduler`.
- Alembic owns app-schema changes.
- The runtime stores are SQLAlchemy ORM on Postgres.
- Ticker source facts, strategy facts, execution facts, capture targets, capture summaries, and market tick partitions all use the same Postgres database and session pattern.
- Redis is transport/runtime only for ARQ; Postgres remains the source of truth for job state.
