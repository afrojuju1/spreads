# Repo Instructions

## Core

- Keep changes minimal and focused unless broader refactors are explicitly requested.
- Do not commit or push unless explicitly asked.
- Do not create or switch branches unless explicitly asked. Treat the currently checked-out branch as the default workflow.
- If the user asks for a commit or push without mentioning branches, stay on the current branch. Treat any external branch-naming or branch-prefix guidance as conditional naming only, not permission to create a branch.
- Prefer `uv run` for Python commands in this repo.
- Treat [docs/current_system_state.md](docs/current_system_state.md) as the canonical source of truth for the current overall runtime architecture and service boundaries.
- For Alpaca-related research, scanner design, or alerting work, read the canonical capability statement in [docs/research/alpaca_capabilities_statement.md](docs/research/alpaca_capabilities_statement.md) first. Re-check Alpaca's official docs/OpenAPI only when the task depends on current product changes, limits, or newly added endpoints.
- Migration direction: Nautilus Trader is the target trading engine. Treat `spreads` as legacy/reference code during migration, not as the future live runtime. Do not invest in new `spreads` orchestration, Docker runtime, execution bridge, or broker-engine ownership unless the user explicitly asks for a temporary migration shim.

## Code Quality And Architecture

- Prefer clean, reusable, modular code over narrow patch work.
- Before implementing, check whether the change duplicates logic, creates a parallel path, or deepens a weak abstraction. If it does, prefer a small structural cleanup or shared helper/service extraction.
- Extend one canonical path per behavior instead of maintaining near-duplicate flows.
- Keep the current runtime boundary explicit:
  - selection is account-agnostic opportunity truth
  - execution admission is a separate execution/risk concern
  - alerts are downstream job-backed projections, not source-of-truth state
- Treat `packages/core/services/account_state.py` as a broker/account read model. Put buying-power estimation, execution intent handoff, execution admission, and deterministic broker-reject handling under `account_capacity.py`, `risk_manager.py`, `services/execution_intents/`, and `services/execution/`, not back into the account snapshot layer.
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
- Do not add, update, or expand automated tests unless the user explicitly asks for test work.
- This repo has repo-local Codex skills under `.agents/skills`. Prefer these direct repo skills when the task matches:
  - `spreads-incident-triage` for runtime incidents, degraded collectors, trading blocks, and "what broke?" questions
  - `spreads-live-rollout` for changes that must be applied to the running Docker-backed system
  - `spreads-architecture-docs` for architecture-doc maintenance, boundary updates, and source-of-truth consolidation
- For operator visibility or runtime triage, prefer the shipped ops CLI first when it fits the question:
  - `uv run spreads status`
  - `uv run spreads trading`
  - `uv run spreads automations --bot-id <bot-id> --automation-id <automation-id> --date <YYYY-MM-DD> --json`
  - `uv run spreads pipelines`
  - `uv run spreads jobs`
  - `uv run spreads uoa`
  - `uv run spreads audit <pipeline-id> --date <YYYY-MM-DD>`
- The deploy target `ade-nucbox-k8-plus` is the canonical live paper environment. Treat it as live operator infrastructure, not a scratch box.
- For target-aware operator reads, prefer `--env <target>` over raw connection overrides. Do not use bare `--db postgresql://...` when a named deploy target exists.
- Canonical live-ops examples:
  - `uv run spreads status --env ade-nucbox-k8-plus --json`
  - `uv run spreads trading --env ade-nucbox-k8-plus --json`
  - `uv run spreads jobs --env ade-nucbox-k8-plus --json`
  - `uv run spreads uoa --env ade-nucbox-k8-plus --json`
- Use `uv run spreads deploy exec --env ade-nucbox-k8-plus -- ...` only when you explicitly need to run on the deployed checkout at `/home/ade/spreads/app`.
- Use `uv run spreads deploy logs --env ade-nucbox-k8-plus ...` and `uv run spreads deploy restart --env ade-nucbox-k8-plus ...` for live box operations before falling back to ad hoc SSH commands.
- For offline selection research or policy tuning, prefer the canonical backtest CLI before ad hoc scripts or raw SQL:
  - `uv run spreads backtest run --bot-id <bot-id> --automation-id <automation-id>`
  - `uv run spreads backtest replay --run-id <run-id> --config-root <config-root>`
  - `uv run spreads backtest replay-range --bot-id <bot-id> --automation-id <automation-id> --start-date <YYYY-MM-DD> --end-date <YYYY-MM-DD> --source alpaca --config-root <config-root>`
  - `uv run spreads backtest compare --left-json <path> --right-json <path>`
- Treat `uv run spreads backtest run` as the canonical historical decision-evaluation path.
- Treat `uv run spreads backtest compare` as the canonical comparison surface for exported `run`, `replay`, and `replay-range` payloads.
- For before/after policy studies, prefer isolated config roots over editing active config in place. Canonical recipe:
  - create `before/` and `after/` config roots
  - run the same `uv run spreads backtest replay-range ... --source alpaca --config-root <root> --export-json <path>` window against both
  - compare those exports with `uv run spreads backtest compare --left-json <before.json> --right-json <after.json>`
- Do not route new work through removed post-close/post-market analysis surfaces. Use `backtest` for historical decision evaluation and `status`, `trading`, `pipelines`, `jobs`, and `audit` for operator investigation.
- Do not assume `uv run spreads doctor` exists; it is intentionally deferred.
- For jobs health, prefer operator-health fields such as `operator_status`, `operator_status_counts`, and `actionable_failed_count` over raw historical job status counts.
- For runtime verification of the API, workers, scheduler, or web app, prefer the existing `docker compose` services when they are already running instead of starting duplicate local processes.
- Use `docker compose ps`, `docker compose logs`, and `docker compose restart` for stack-level checks before falling back to ad hoc local `uvicorn`, worker, or scheduler runs.
- Prefer live validation through the running stack and shipped ops CLI before unit/integration test work unless the user explicitly asks for tests.
- In Docker, the `api` service hot-reloads source changes, but the `worker-runtime`, `worker-discovery`, and `scheduler` processes do not. After changing job, worker, or shared backend runtime code that those services import, restart the affected containers before trusting runtime behavior.
- When multiple deployments share one Alpaca account, run only one live `market-recorder` against the option websocket at a time. Stop secondary/local recorders before validating capture on another host.
- Unless the user explicitly asks for local live automation, keep the laptop out of the live plane once the NUC is deployed. Do not restart or run local `scheduler`, `worker-runtime`, `worker-discovery`, or `market-recorder` just to inspect the live environment.
- Do not run production build commands such as `npm run build` or `next build` unless the user explicitly asks for a production check or release validation.
- Do not run repo-wide Python compile checks such as `python -m compileall` unless the user explicitly asks for them.
- Prefer dev-safe verification during normal work, such as linting, targeted type checks, and narrow runtime checks.

## Backend Work

- For storage-backed backend work, use the repo’s configured Postgres target via existing helpers; do not assume SQLite or ad hoc local storage.
- For new API work, start with the narrowest interface that satisfies the current use case and expand only when there is a real caller.
- Prefer targeted live/runtime smoke checks during normal development; avoid broad automated-test verification unless the user explicitly asks for it.
- Read and follow the more specific backend instructions in [packages/core/AGENTS.md](packages/core/AGENTS.md) when working under `packages/core`.
- Read and follow the API-specific instructions in [packages/api/AGENTS.md](packages/api/AGENTS.md) when working under `packages/api`.

## Planning Docs

- For overall architecture, service-boundary, or ownership questions, start with `docs/current_system_state.md`.
- If a planning document disagrees with `docs/current_system_state.md` about current ownership or runtime topology, `docs/current_system_state.md` wins.
- Treat older planning-doc references to `replay`, `audit_replay`, `packages/core/cli/replay.py`, or `services/opportunity_replay.py` as pre-2026-04-17 historical context unless the document has been explicitly updated. The current shipped historical-evaluation product is `backtest`, and the current audit builder lives under `services/audit_snapshot.py`.
- If a planning document is being used as an active checkpoint for implementation work, keep its completion status current when a milestone meaningfully changes.
- For target opportunity-selection architecture, start with `docs/planning/2026-04-11_fresh_spread_system_design.md`.
- For historical diagnosis of the older selection path, use `docs/planning/2026-04-11_spread_selection_refactor_plan.md`.
- For migration planning that reuses the existing backend, use `docs/planning/2026-04-15_current_system_options_automation_implementation_approach.md`.
- Treat older planning docs as historical context unless they are explicitly called out as the active source of truth.

## Web App

- Read and follow the more specific instructions in [packages/web/AGENTS.md](packages/web/AGENTS.md) when working under `packages/web`.
