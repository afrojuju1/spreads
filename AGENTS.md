# Repo Instructions

## Core

- Keep changes minimal and focused unless broader refactors are explicitly requested.
- Do not commit or push unless explicitly asked.
- Do not create or switch branches unless explicitly asked. Treat the currently checked-out branch as the default workflow.
- If the user asks for a commit or push without mentioning branches, stay on the current branch. Treat any external branch-naming or branch-prefix guidance as conditional naming only, not permission to create a branch.
- Prefer `uv run` for Python commands in this repo.
- Treat [docs/current_system_state.md](docs/current_system_state.md) as the canonical source of truth for the current overall runtime architecture and service boundaries.
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
- This repo has a local Codex plugin at `plugins/spreads-ops`. Prefer its repo-specific skills when the task matches:
  - `spreads-incident-triage` for runtime incidents, degraded collectors, trading blocks, and "what broke?" questions
  - `spreads-live-rollout` for changes that must be applied to the running Docker-backed system
  - `spreads-architecture-docs` for architecture-doc maintenance, boundary updates, and source-of-truth consolidation
- For operator visibility or runtime triage, prefer the shipped ops CLI first when it fits the question:
  - `uv run spreads status`
  - `uv run spreads trading`
  - `uv run spreads pipelines`
  - `uv run spreads jobs`
  - `uv run spreads uoa`
  - `uv run spreads audit <pipeline-id> --date <YYYY-MM-DD>`
- For offline selection research or policy tuning, prefer the canonical backtest CLI before ad hoc scripts or raw SQL:
  - `uv run spreads backtest run --bot-id <bot-id> --automation-id <automation-id>`
  - `uv run spreads backtest compare --left-json <path> --right-json <path>`
- Treat `uv run spreads backtest run` as the canonical historical decision-evaluation path.
- Treat `uv run spreads analyze` as a legacy post-close report surface, not the canonical backtest or policy-tuning workflow.
- Do not assume `uv run spreads doctor` exists; it is intentionally deferred.
- For runtime verification of the API, workers, scheduler, or web app, prefer the existing `docker compose` services when they are already running instead of starting duplicate local processes.
- Use `docker compose ps`, `docker compose logs`, and `docker compose restart` for stack-level checks before falling back to ad hoc local `uvicorn`, worker, or scheduler runs.
- In Docker, the `api` service hot-reloads source changes, but the `worker-runtime`, `worker-discovery`, and `scheduler` processes do not. After changing job, worker, or shared backend runtime code that those services import, restart the affected containers before trusting runtime behavior.
- Do not run production build commands such as `npm run build` or `next build` unless the user explicitly asks for a production check or release validation.
- Do not run repo-wide Python compile checks such as `python -m compileall` unless the user explicitly asks for them.
- Prefer dev-safe verification during normal work, such as linting, targeted type checks, and narrow runtime checks.

## Backend Work

- For storage-backed backend work, use the repo’s configured Postgres target via existing helpers; do not assume SQLite or ad hoc local storage.
- For new API work, start with the narrowest interface that satisfies the current use case and expand only when there is a real caller.
- Prefer targeted service, API, and data-backed smoke checks during normal development; avoid broad verification unless the user asks.
- Read and follow the more specific backend instructions in [packages/core/AGENTS.md](packages/core/AGENTS.md) when working under `packages/core`.
- Read and follow the API-specific instructions in [packages/api/AGENTS.md](packages/api/AGENTS.md) when working under `packages/api`.

## Planning Docs

- For overall architecture, service-boundary, or ownership questions, start with `docs/current_system_state.md`.
- If a planning document disagrees with `docs/current_system_state.md` about current ownership or runtime topology, `docs/current_system_state.md` wins.
- If a planning document is being used as an active checkpoint for implementation work, keep its completion status current when a milestone meaningfully changes.
- For target opportunity-selection architecture, start with `docs/planning/2026-04-11_fresh_spread_system_design.md`.
- For historical diagnosis of the older selection path, use `docs/planning/2026-04-11_spread_selection_refactor_plan.md`.
- For migration planning that reuses the existing backend, use `docs/planning/2026-04-15_current_system_options_automation_implementation_approach.md`.
- Treat older planning docs as historical context unless they are explicitly called out as the active source of truth.

## Web App

- Read and follow the more specific instructions in [packages/web/AGENTS.md](packages/web/AGENTS.md) when working under `packages/web`.
