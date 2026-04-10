# Repo Instructions

## Core

- Keep changes minimal and focused unless broader refactors are explicitly requested.
- Do not commit or push unless explicitly asked.
- Prefer `uv run` for Python commands in this repo.
- For Alpaca-related research, scanner design, or alerting work, read the canonical capability statement in [docs/research/alpaca_capabilities_statement.md](/Users/adeb/Projects/spreads/docs/research/alpaca_capabilities_statement.md) first. Re-check Alpaca's official docs/OpenAPI only when the task depends on current product changes, limits, or newly added endpoints.

## Dev Workflow

- This repo is in active development by default.
- For runtime verification of the API, workers, scheduler, or web app, prefer the existing `docker compose` services when they are already running instead of starting duplicate local processes.
- Use `docker compose ps`, `docker compose logs`, and `docker compose restart` for stack-level checks before falling back to ad hoc local `uvicorn`, worker, or scheduler runs.
- In Docker, the `api` service hot-reloads source changes, but the `worker-main`, `worker-collector`, and `scheduler` processes do not. After changing job, worker, or shared backend runtime code that those services import, restart the affected containers before trusting runtime behavior.
- Do not run production build commands such as `npm run build` or `next build` unless the user explicitly asks for a production check or release validation.
- Do not run repo-wide Python compile checks such as `python -m compileall` unless the user explicitly asks for them.
- Prefer dev-safe verification during normal work, such as linting, targeted type checks, and narrow runtime checks.

## Backend Services

- For storage-backed backend work, use the repo’s configured Postgres target via existing helpers; do not assume SQLite or ad hoc local storage.
- Prefer extending existing services and repositories with thin adapters before introducing new abstractions or frameworks.
- For new API work, start with the narrowest interface that satisfies the current use case and expand only when there is a real caller.
- Prefer targeted service, API, and data-backed smoke checks during normal development; avoid broad verification unless the user asks.

## Planning Docs

- If a planning document is being used as an active checkpoint for implementation work, keep its completion status current when a milestone meaningfully changes.

## Web App

- Prefer established utility helpers over bespoke one-off implementations for common collection, object, and string transforms.
- In `apps/web`, prefer `lodash-es` when it covers the job cleanly instead of writing custom utility code from scratch.
- Read and follow the more specific instructions in [apps/web/AGENTS.md](/Users/adeb/Projects/spreads/apps/web/AGENTS.md) when working under `apps/web`.
