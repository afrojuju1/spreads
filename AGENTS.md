# Repo Instructions

## Core

- Spreads is live trading infrastructure. Treat runtime safety, operator clarity, and clean ownership as first-class constraints.
- Keep changes minimal and focused unless broader refactors are explicitly requested.
- Do not commit or push unless explicitly asked.
- Do not create or switch branches unless explicitly asked. Treat the currently checked-out branch as the default workflow.
- If the user asks for a commit or push without mentioning branches, stay on the current branch. Treat any external branch-naming or branch-prefix guidance as conditional naming only, not permission to create a branch.
- Prefer `uv run` for Python commands in this repo.
- Treat [docs/current_system_state.md](docs/current_system_state.md) as the canonical source of truth for the current overall runtime architecture and service boundaries.
- Do not duplicate the domain ownership map in `AGENTS.md`. If ownership, object vocabulary, or runtime topology matters, read or update [docs/current_system_state.md](docs/current_system_state.md).
- For Alpaca-related research, candidate-building, or alerting work, read the canonical capability statement in [docs/research/alpaca_capabilities_statement.md](docs/research/alpaca_capabilities_statement.md) first. Re-check Alpaca's official docs/OpenAPI only when the task depends on current product changes, limits, or newly added endpoints.
- Current execution direction: `spreads` owns the live paper runtime. The active execution adapter is `alpaca_direct`; Nautilus Trader is retained only as historical context and a source of architectural ideas. Do not route new live Spreads work through Nautilus, Rust bridge paths, or host-managed Nautilus services unless the user explicitly asks to re-enable a separate experiment.
- Canonical operator state is split between `TradingOpsState` and `StorageOpsState`. Do not add parallel operator products, compatibility routes, or duplicate status pages outside those read models.
- Multi-strategy execution must be activated deliberately through `activation.state` and `execution.mode` in the strategy catalog. Do not auto-enable disabled strategy breadth or add automatic all-strategy observation scheduling; non-long-call families must pass family quality, portfolio admission, execution admission, and queued submit readiness before paper or live scheduler enablement.

## Code Quality And Architecture

- Prefer clean, reusable, modular code over narrow patch work.
- For Python quality checks, use the required and advisory Ruff commands in [docs/development/python_quality.md](docs/development/python_quality.md). Keep complexity scans report-only unless a bead explicitly includes that cleanup.
- Before implementing, check whether the change duplicates logic, creates a parallel path, or deepens a weak abstraction. If it does, prefer a small structural cleanup or shared helper/service extraction.
- Extend one canonical path per behavior instead of maintaining near-duplicate flows.
- For owned Pydantic contracts, use `core.model_contracts.DomainModel` as the default base. It owns the shared repo defaults: frozen models, extra-field rejection, alias population, arbitrary type support, and `to_payload()`.
- Do not create thin Pydantic base classes, wrapper methods, or compatibility layers that only rename `DomainModel` behavior. Add a domain-specific base only when it carries real domain rules, such as company valuation blank-string normalization.
- Keep public serialization surfaces consistent. If a model has `to_payload()`, callers should use that method; the implementation can delegate to Pydantic internals such as `model_dump(mode="json")`.
- Leave intentionally open Pydantic shapes on `BaseModel` when their behavior differs from `DomainModel`, such as `extra="allow"` vendor/dynamic payloads or `extra="ignore"` override payloads. Do not force those through `DomainModel`.
- Prefer library-backed primitives and repo-level helpers over hand-rolled parsing, money math, time parsing, clamping, or serialization. If a helper only wraps one obvious library call, remove it unless it encodes domain language or policy.
- Keep local model transforms declarative with Pydantic validators, aliases, and `model_dump`/`model_validate` before writing manual object-to-dict plumbing.
- For trading-engine architecture, strategy-quality, scanner/source, execution, risk, or portfolio refactor work, start from [docs/planning/2026-06-08_trading_engine_inspiration_repos.md](docs/planning/2026-06-08_trading_engine_inspiration_repos.md) and borrow patterns from the listed inspiration repos before inventing a local shape.
- If research surfaces another repo or framework with a clearly better pattern for the active design problem, propose adding it to the inspiration list with the specific pattern to borrow and what not to copy. Do not silently expand the inspiration set or chase broad framework rewrites.
- Keep the current runtime boundary explicit:
  - candidate and signal selection are account-agnostic strategy truth
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
- For substantial work, use Beads as the durable work ledger. Create or update beads for meaningful discoveries, claim the active bead before implementation, and close beads with live validation notes when the work is done.
- When Ade asks to commit per bead, finish and validate the bead before committing; do not bundle unrelated beads into one commit.
- Default to live validation through the running stack, shipped ops CLI, and targeted runtime smoke checks.
- Do not add, update, or expand automated tests unless the user explicitly asks for test work.
- Only write or modify e2e tests when the user explicitly asks for e2e coverage. Do not add e2e tests as default regression coverage for implementation work.
- This repo has repo-local Codex skills under `.agents/skills`. Prefer these direct repo skills when the task matches:
  - `spreads-ops` for live and post-market system health, market-open readiness, blocked or degraded trading, capture or alert triage, worker or scheduler status, and "how is the system doing?" checks
  - `spreads-strategy-lab` for strategy evidence review, strategy catalog/profile changes, quality-profile tuning, new strategy families, and "why did this strategy select or skip?" questions
  - `spreads-data-platform` for ClickHouse/Postgres/Redis storage health, DB sizing, capture pressure, market-recorder behavior, retention, rollups, and market-data quality
  - `spreads-live-rollout` for changes that must be applied to the running Docker-backed system
  - `spreads-architecture-docs` for architecture-doc maintenance, boundary updates, and source-of-truth consolidation
- Spreads owns the active repo-local operator guidance. Do not add new active guidance to the retired `trading_operator` hub repo.
- The external research AI layer is linked at [external/TradingAgents](external/TradingAgents), which resolves to `/home/ade/Projects/TradingAgents`. Spreads owns orchestration, job config, alerts, outputs, and operator visibility around that layer; the external repo owns its own agent internals.
- For operator visibility or runtime triage, prefer the shipped ops CLI first when it fits the question:
  - `uv run spreads ops state`
  - `uv run spreads ops storage`
  - `uv run spreads jobs`
  - `uv run spreads jobs lanes`
  - `docker compose logs --tail=200 <service>`
  - `uv run spreads execution positions --date <YYYY-MM-DD> --json`
  - `uv run spreads execution list --date <YYYY-MM-DD>`
  - `uv run spreads execution runtimes --json`
- Do not add frontend or API callers to retired fragmented ops surfaces.
- The deploy target `ade-nucbox-k8-plus` is the canonical live paper environment. Treat it as live operator infrastructure, not a scratch box.
- When you are already running inside `/home/ade/Projects/spreads` on `ade-nucbox-k8-plus`, use local CLI and Docker commands directly. Do not use `--env ade-nucbox-k8-plus` passthrough from the same box.
- Command-level `--env` passthrough on non-deploy commands was intentionally removed. Use deploy-owned commands for target operations, and do not use bare `--db postgresql://...` when a named deploy target exists.
- Canonical live-ops examples on the live box:
  - `uv run spreads ops state --json`
  - `uv run spreads ops storage --json`
  - `uv run spreads jobs --json`
  - `uv run spreads execution list --date <YYYY-MM-DD>`
  - `uv run spreads execution positions --date <YYYY-MM-DD> --json`
  - `docker compose logs --tail=200 scheduler worker-runtime worker-data market-recorder`
- Canonical remote live-ops examples from another host:
  - `uv run spreads deploy exec --env ade-nucbox-k8-plus -- ops state --json`
  - `uv run spreads deploy exec --env ade-nucbox-k8-plus -- ops storage --json`
  - `uv run spreads deploy exec --env ade-nucbox-k8-plus -- jobs --json`
  - `uv run spreads deploy exec --env ade-nucbox-k8-plus -- execution list --date <YYYY-MM-DD>`
- Use `uv run spreads deploy exec --env ade-nucbox-k8-plus -- ...`, `uv run spreads deploy logs --env ade-nucbox-k8-plus ...`, and `uv run spreads deploy restart --env ade-nucbox-k8-plus ...` only from another host or when intentionally exercising deploy-target plumbing.
- Runtime resource policy lives in [docs/current_system_state.md](docs/current_system_state.md). Market-closed `market_recorder_idle` logs are expected and healthy; do not treat recorder idling outside market hours as a capture outage.
- Do not tell operators to run removed or currently unshipped `spreads scan`, `spreads audit`, `spreads automations`, `spreads backtest`, `spreads research`, `spreads replay`, `spreads analyze`, or `spreads post-market analyze` commands. Use shipped operator surfaces first and create a bead before reintroducing a historical evaluation CLI.
- For offline selection research or policy tuning, start by validating current strategy config and stored engine facts. If a historical evaluator is needed, design it explicitly against the current ticker-source/candidate/signal/decision model instead of reviving old audit/backtest wrappers.
- Do not assume `uv run spreads doctor` exists; it is intentionally deferred.
- For jobs health, prefer operator-health fields such as `operator_status`, `operator_status_counts`, and `actionable_failed_count` over raw historical job status counts.
- For runtime verification of the API, workers, scheduler, or web app, prefer the existing `docker compose` services when they are already running instead of starting duplicate local processes.
- Use `docker compose ps`, `docker compose logs`, and `docker compose restart` for stack-level checks before falling back to ad hoc local `uvicorn`, worker, or scheduler runs.
- Prefer live validation through the running stack and shipped ops CLI before unit/integration test work unless the user explicitly asks for tests.
- In Docker, the `api` service hot-reloads source changes, but the `worker-runtime`, `worker-data`, and `scheduler` processes do not. After changing job, worker, or shared backend runtime code that those services import, restart the affected containers before trusting runtime behavior.
- When multiple deployments share one Alpaca account, run only one live `market-recorder` against the option websocket at a time. Stop secondary/local recorders before validating capture on another host.
- Unless the user explicitly asks for local live automation, keep the laptop out of the live plane once the NUC is deployed. Do not restart or run local `scheduler`, `worker-runtime`, `worker-data`, or `market-recorder` just to inspect the live environment.
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
- Treat older planning-doc references to `replay`, `audit_replay`, `backtest`, `packages/core/cli/replay.py`, `packages/core/cli/backtest.py`, or `services/audit_snapshot.py` as historical context unless the document has been explicitly updated. There is no currently shipped historical-evaluation CLI.
- If a planning document is being used as an active checkpoint for implementation work, keep its completion status current when a milestone meaningfully changes.
- For current candidate, signal, decision, and admission ownership, start with `docs/current_system_state.md`.
- For historical diagnosis of the older selection path, use `docs/planning/2026-04-11_spread_selection_refactor_plan.md`.
- For migration planning that reuses the existing backend, use `docs/planning/2026-04-15_current_system_options_automation_implementation_approach.md`.
- Treat older planning docs as historical context unless they are explicitly called out as the active source of truth.

## Web App

- Read and follow the more specific instructions in [packages/web/AGENTS.md](packages/web/AGENTS.md) when working under `packages/web`.
