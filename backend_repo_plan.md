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
      history.py
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

- `credit_spread_scanner.py`
  - split into Alpaca integration, scanner service, ranking, setup, replay, and a thin CLI
- `intraday_idea_collector.py`
  - move to `jobs/live_collector.py`
- `post_close_analysis.py`
  - move to `services/analysis.py`
- `scanner_history.py`
  - move to `storage/history.py`
- `calendar_events/` and `greeks/`
  - move under `src/spreads/integrations/`

## Database

Use **PostgreSQL** as the backend database.

Why:

- the system is relational, not just time-series
- future backend/frontend work needs stable queries across runs, candidates, watchlists, events, and outcomes
- Postgres gives us native partitioning for heavy quote-event tables

Plan:

- keep SQLite for local collection/dev for now
- use Postgres as the future system of record
- partition quote-event tables by `captured_at`
- archive older raw quote history later if needed

Do not:

- keep SQLite as the long-term backend DB
- make Timescale a v1 dependency

## Migration Order

1. Create `src/spreads`.
2. Move reusable code into packages without changing behavior.
3. Keep root scripts as wrappers so current commands do not break.
4. Add Postgres-backed storage behind the same interfaces.
5. Add the API.
6. Add the frontend later.

## Next Step

Start phase 1:

- create `src/spreads`
- move `scanner_history.py` to `src/spreads/storage/history.py`
- move `calendar_events/` and `greeks/` under `src/spreads/integrations/`
- keep current root files as wrappers
