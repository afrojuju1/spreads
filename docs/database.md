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
uv run spreads-seed-jobs
```

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
uv run spreads-scan --symbol SPY
uv run spreads-collect --profile weekly --universe explore_10
uv run spreads-analyze --label explore_10_combined_weekly_auto
```

ARQ orchestration defaults:

```bash
uv run spreads-scheduler
uv run arq spreads.jobs.worker.WorkerSettings
```

Redis default connection URL:

```text
redis://localhost:56379/0
```

`spreads-collect` now persists live collector cycles, board/watchlist selections, events, and quote events directly to Postgres.

Discord alert delivery is optional. If configured, collector alerts are sent through:

```bash
SPREADS_DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

The runtime also accepts legacy `DISCORD_WEBHOOK_URL` if that is already present in your local `.env`.

If the webhook is missing, alert rows are still persisted in Postgres with status `skipped`.

`spreads-analyze` renders the post-close markdown report to stdout from Postgres-backed analytics; it does not write a report file.

## API

The FastAPI app is DB-backed. Useful endpoints include:

- `/live/{label}`
- `/live/{label}/cycles`
- `/live/{label}/events`
- `/history/runs`
- `/history/runs/{run_id}`
- `/history/runs/{run_id}/candidates`
- `/sessions/{session_date}/{label}/outcomes`
- `/sessions/{session_date}/{label}/summary`
- `/alerts`
- `/alerts/latest`
- `/alerts/{alert_id}`
- `/jobs`
- `/jobs/runs`
- `/jobs/runs/{job_run_id}`
- `/jobs/health`

## Notes

- Docker Compose can run `postgres`, `redis`, `api`, `worker`, and `scheduler`.
- Alembic owns app-schema changes.
- The runtime stores are SQLAlchemy ORM on Postgres.
- Run history, collector live state, and calendar events all use the same Postgres database and session pattern.
- Redis is transport/runtime only for ARQ; Postgres remains the source of truth for job state.
