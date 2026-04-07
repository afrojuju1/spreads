# Database Setup

## Local Postgres

Start Postgres:

```bash
docker compose up -d postgres
```

If you previously used the old Docker init-schema bootstrap, reset the local volume once:

```bash
docker compose down -v
docker compose up -d postgres
```

Default connection URL:

```text
postgresql://spreads:spreads@localhost:55432/spreads
```

Add this to your local `.env`:

```bash
SPREADS_DATABASE_URL=postgresql://spreads:spreads@localhost:55432/spreads
```

## Migrations

Apply the schema with Alembic:

```bash
uv run alembic upgrade head
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

`spreads-collect` now persists live collector cycles, board/watchlist selections, events, and quote events directly to Postgres.

`spreads-analyze` renders the post-close markdown report to stdout from Postgres-backed analytics; it does not write a report file.

## Notes

- Docker only starts Postgres now; it does not create app tables.
- Alembic owns app-schema changes.
- The runtime stores are SQLAlchemy ORM on Postgres.
- Run history, collector live state, and calendar events all use the same Postgres database and session pattern.
