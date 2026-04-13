# Repo Instructions

## Core

- Keep changes minimal and focused unless broader refactors are explicitly requested.
- Do not commit or push unless explicitly asked.
- Prefer `uv run` for Python commands in this repo.
- For Alpaca-related research, scanner design, or alerting work, read the canonical capability statement in [docs/research/alpaca_capabilities_statement.md](docs/research/alpaca_capabilities_statement.md) first. Re-check Alpaca's official docs/OpenAPI only when the task depends on current product changes, limits, or newly added endpoints.

## Code Quality And Architecture

- Prefer clean, reusable, modular code over narrow patch work.
- Before implementing, check whether the change duplicates logic, creates a parallel path, or deepens a weak abstraction. If it does, prefer a small structural cleanup or shared helper/service extraction.
- Extend one canonical path per behavior instead of maintaining near-duplicate flows.
- If the current architecture is weak, call it out explicitly and propose the better approach before proceeding. Weigh:
  - current callers
  - migration cost
  - runtime risk
  - verification cost
  - whether the user asked for the smallest change or the most durable fix
- Prefer a targeted refactor over a fragile minimal patch when the refactor materially improves structure and can be validated safely.
- If debt is being accepted, state it explicitly rather than hiding it behind vague follow-up language.

## Dev Workflow

- This repo is in active development by default.
- For operator visibility or runtime triage, prefer the shipped ops CLI first when it fits the question:
  - `uv run spreads status`
  - `uv run spreads trading`
  - `uv run spreads sessions`
  - `uv run spreads jobs`
  - `uv run spreads uoa`
  - `uv run spreads audit <session-id>`
- For offline selection research or policy tuning, prefer the replay CLI before ad hoc scripts or raw SQL:
  - `uv run spreads replay`
  - `uv run spreads replay --label <label> --date <YYYY-MM-DD>`
  - `uv run spreads replay recent --limit <N>`
- Treat `uv run spreads replay` as the canonical decision-evaluation path.
- Treat `uv run spreads analyze` as a legacy post-close report surface, not the canonical replay or policy-tuning workflow.
- Do not assume `uv run spreads doctor` exists; it is intentionally deferred.
- For runtime verification of the API, workers, scheduler, or web app, prefer the existing `docker compose` services when they are already running instead of starting duplicate local processes.
- Use `docker compose ps`, `docker compose logs`, and `docker compose restart` for stack-level checks before falling back to ad hoc local `uvicorn`, worker, or scheduler runs.
- In Docker, the `api` service hot-reloads source changes, but the `worker-main`, `worker-collector`, and `scheduler` processes do not. After changing job, worker, or shared backend runtime code that those services import, restart the affected containers before trusting runtime behavior.
- Do not run production build commands such as `npm run build` or `next build` unless the user explicitly asks for a production check or release validation.
- Do not run repo-wide Python compile checks such as `python -m compileall` unless the user explicitly asks for them.
- Prefer dev-safe verification during normal work, such as linting, targeted type checks, and narrow runtime checks.

## Backend Work

- For storage-backed backend work, use the repo’s configured Postgres target via existing helpers; do not assume SQLite or ad hoc local storage.
- For new API work, start with the narrowest interface that satisfies the current use case and expand only when there is a real caller.
- Prefer targeted service, API, and data-backed smoke checks during normal development; avoid broad verification unless the user asks.
- Read and follow the more specific backend instructions in [src/spreads/AGENTS.md](src/spreads/AGENTS.md) when working under `src/spreads`.
- Read and follow the API-specific instructions in [apps/api/AGENTS.md](apps/api/AGENTS.md) when working under `apps/api`.

## Planning Docs

- If a planning document is being used as an active checkpoint for implementation work, keep its completion status current when a milestone meaningfully changes.
- For selection-architecture work, start with `docs/planning/2026-04-11_fresh_spread_system_design.md` and `docs/planning/2026-04-11_spread_selection_refactor_plan.md`.
- Treat older planning docs as historical context unless they are explicitly called out as the active source of truth.

## Web App

- Read and follow the more specific instructions in [apps/web/AGENTS.md](apps/web/AGENTS.md) when working under `apps/web`.
