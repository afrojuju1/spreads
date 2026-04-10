# spreads

Alpaca-based options spread scanner, live collector, and operator dashboard.

It scans candidate spreads, runs live collection sessions, persists runtime history to Postgres, delivers Discord alerts through a durable outbox, and renders post-market analysis plus a web UI for operators.

## What Is Here

- `apps/api`
  - FastAPI backend for sessions, alerts, jobs, account state, and post-market analysis
- `apps/web`
  - Next.js operator dashboard
- `src/spreads/services`
  - scanner, session, account, alert, and post-market logic
- `src/spreads/jobs`
  - scheduler and ARQ worker entrypoints
- `src/spreads/storage`
  - Postgres-backed repositories and models
- `docs`
  - setup notes, research references, and planning docs

## Quick Start

### 1. Prerequisites

- Python `3.11+`
- `uv`
- Docker and Docker Compose
- Alpaca API credentials

### 2. Configure Environment

```bash
cp .env.example .env
```

Set the values you need in `.env`.

Minimum useful local config:

```bash
APCA_API_KEY_ID=...
APCA_API_SECRET_KEY=...
SPREADS_DATABASE_URL=postgresql://spreads:spreads@localhost:55432/spreads
REDIS_URL=redis://localhost:56379/0
```

Optional:

```bash
SPREADS_DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

If the Discord webhook is missing, alert rows are still persisted and delivery is skipped.

### 3. Start The Local Stack

Bring up the full dev stack:

```bash
docker compose up -d
```

Apply migrations and seed job definitions:

```bash
uv run alembic upgrade head
uv run spreads-seed-jobs
```

Main local surfaces:

- API: [http://localhost:58080](http://localhost:58080)
- Web: [http://localhost:53000](http://localhost:53000)
- Postgres: `localhost:55432`
- Redis: `localhost:56379`

## Common Commands

Scan a symbol:

```bash
uv run spreads-scan --symbol SPY
```

Run a collector profile:

```bash
uv run spreads-collect --profile weekly --universe explore_10
```

Analyze a label:

```bash
uv run spreads-analyze --label explore_10_combined_weekly_auto
```

Run post-market analysis:

```bash
uv run spreads-post-market-analyze --label explore_10_combined_weekly_auto --date 2026-04-10
```

Run the scheduler directly:

```bash
uv run spreads-scheduler
```

## Local Development Notes

- In Docker, `api` hot-reloads source changes.
- `worker-main`, `worker-collector`, and `scheduler` do not hot-reload. Restart those containers after backend changes they import.
- Prefer using the existing Docker services for runtime checks instead of starting duplicate local processes.
- Postgres is the source of truth for runtime history, sessions, alerts, jobs, and post-market analysis.
- Post-market outcomes are modeled analysis, not realized account PnL.

## Useful Docs

- [Database Setup](docs/database.md)
- [Web README](apps/web/README.md)
- [Alpaca Capabilities Statement](docs/research/alpaca_capabilities_statement.md)
- [Ops CLI Visibility Plan](docs/planning/ops_cli_visibility_plan.md)

## Current Direction

The repo is actively evolving toward:

- durable alert delivery
- stronger session and post-market visibility
- one canonical `spreads` CLI for operator workflows

The planning docs in [`docs/planning`](docs/planning) are the best place to see in-flight architecture and execution plans.
