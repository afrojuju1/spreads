# Backend Repo Plan

## Goal

Move from a flat script repo to a backend-first structure that:

- keeps current collectors working
- supports an API next
- leaves room for a frontend later

## Target Structure

```text
apps/
  api/
  web/

src/
  spreads/
    domain/
    integrations/
      alpaca/
      calendar_events/
      greeks/
    services/
      scanner.py
      ranking.py
      setup.py
      replay.py
      analysis.py
    storage/
      db.py
      models.py
      calendar_models.py
      run_history_repository.py
      records.py
      serializers.py
    jobs/
      live_collector.py
      post_close.py
    cli/
      scan.py
      collect.py
      analyze.py

data/
outputs/
docs/
```

## Current Mapping

- scanner CLI now lives at `packages/core/cli/scan.py`
- collector CLI now lives at `packages/core/cli/collect.py`
- analysis CLI now lives at `packages/core/cli/analyze.py`
- `calendar_events/` and `greeks/`
  - live under `packages/core/integrations/`
- persistence models and repositories
  - live under `packages/core/storage/`

## Database

Use **PostgreSQL** as the backend database.

Why:

- the system is relational, not just time-series
- future backend/frontend work needs stable queries across runs, candidates, watchlists, events, and outcomes
- Postgres gives us native partitioning for heavy quote-event tables

Plan:

- use Postgres as the system of record
- partition quote-event tables by `captured_at`
- archive older raw quote history later if needed

Do not:

- keep SQLite as the long-term backend DB
- make Timescale a v1 dependency

## Migration Order

1. Create `packages/core`.
2. Move reusable code into packages without changing behavior.
3. Keep root scripts as wrappers so current commands do not break.
4. Add Postgres-backed storage behind the same interfaces.
5. Add the API.
6. Add the frontend later.

## Current Status

- phase 1 complete
- phase 2 complete with `packages/core/services`, `packages/core/jobs`, `packages/core/cli`, and `packages/core/integrations/alpaca`
- Postgres-only runtime storage and minimal FastAPI app added
- local Postgres development uses `docker-compose`
- Alembic owns schema migrations
- calendar events and run history share the same Postgres/session layer

## Next Step

- split `packages/core/services/scanner.py` into smaller service modules behind the same CLI
- start the frontend against the FastAPI surface under `packages/api`
