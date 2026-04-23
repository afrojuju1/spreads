# Backend Instructions

## Backend Architecture Rules

- Prefer extending existing service entrypoints instead of adding parallel aggregators.
- Keep module boundaries clear: `services/` owns business logic, `storage/` owns persistence and query shapes, `jobs/` owns scheduling and worker entrypoints, and `packages/api` stays a thin adapter over services.
- Treat [../../docs/current_system_state.md](../../docs/current_system_state.md) as the canonical source of truth for current backend ownership and runtime boundaries.
- `services/market_recorder.py` is the sole owner of the Alpaca option websocket connection in the normal runtime. Do not add API-owned or discovery-run-owned reactive option stream capture paths; discovery runs and APIs should consume recorder-backed persisted rows or shared services over that state unless an explicit architecture change is being made.
- Favor one canonical backend path per responsibility. If logic is already repeated, extract the shared behavior before adding more.
- Keep the recent package splits canonical. Do not reintroduce monolithic ownership around old `scanner.py`, `discovery_run.py`, `execution.py`, or `ops_visibility.py` mental models.
- For multi-leg options work, keep `legs[]` canonical end to end. Do not add new 3+ leg special cases around `short_symbol` / `long_symbol`, and route quote/mark math through the shared structure snapshot path.
- For long-vol families such as `long_straddle` and `long_strangle`, do not force them through vertical-only live validation or exposure math. If they remain shadow-only in live trading, document that explicitly in the plan/runbook and in seeded job policy instead of relying on implicit execution failure.
- Prefer small composable helpers when they remove duplication, but do not add abstraction layers with only one caller and no clear reuse value.
- If a requested change pushes against a bad boundary, call it out and propose the boundary fix first. Unless the user explicitly wants the smallest patch only, prefer the boundary fix.
- When changing architecture, explain the tradeoff in terms of:
  - duplicate logic removed
  - callers affected
  - migration or rollout risk
  - validation needed after the change

## Canonical Ownership

- discovery and collection flow: `services/scanners/`, `services/discovery_runs/`, `services/live_selection.py`, `services/opportunity_scoring.py`, and `services/candidate_policy.py`
- canonical signal and opportunity state: `services/signal_state.py`, `services/opportunity_generation.py`, and `services/opportunities.py`
- runtime and operator read models: `services/live_runtime.py`, `services/discovery_run_health/`, `services/pipelines.py`, and `services/ops/`
- pipeline/session runtime list/detail: `services/pipelines.py`
- actual account and trading health: `services/account_state.py`
- execution, portfolio, and reconciliation: `services/execution/`, `services/execution_portfolio.py`, `services/session_positions.py`, `services/broker_sync.py`, `services/risk_manager.py`, and `services/exit_manager.py`
- historical evaluation and policy research: `backtest/`
- alert delivery state: `storage/alert_repository.py`
- job execution and scheduler behavior: `jobs/worker.py`, `jobs/registry.py`, and `storage/job_repository.py`
- broker-sync distinction: `services/broker_sync.py` owns actual broker reconciliation, while `services/ops/broker_sync.py` owns operator-read normalization only.

## Operator Visibility

- For operator visibility work, reuse these modules with thin adapters instead of introducing parallel API-only logic.
- For session health and current runtime state, prefer `services/live_runtime.py`, `services/discovery_run_health/`, `services/pipelines.py`, and `services/ops/` over creating new read-model owners.
- For jobs health, read operator-facing status fields first. Raw historical failed runs can remain visible while `operator_status` and `actionable_failed_count` show whether they still require action.
- For first-pass ops/runtime checks and historical backtest workflows, follow the repo-level CLI guidance in [../../AGENTS.md](../../AGENTS.md). Keep the canonical command list there instead of repeating it in backend-specific instructions.

## End-Of-Day And Ops Queries

- For questions about "how did we do today", market-close summaries, discovery run health, or live ops status, prefer the running Docker-backed system state before code inspection.
- Use the existing stack and narrow live reads first:
  - account and trading health: `services/account_state.py` or `http://localhost:58080/account/overview?history_range=1D`
  - pipeline/session runtime health: `services/pipelines.py` or `uv run spreads pipelines`
- historical evaluation and tuning: `uv run spreads backtest ...`
- After market close, use exact dates in summaries.

## Rollout Checklist

- After schema changes, run `uv run alembic upgrade head`.
- If declared job YAML or discovery run config changed, restart the scheduler and affected workers so they reload config.
- After changing code imported by `worker-runtime`, `worker-discovery`, or `scheduler`, restart those containers before trusting runtime behavior.
- Use `docker compose ps` and recent `docker compose logs` to verify startup and job execution after restart.
- Restart `api` only when the changed runtime surface requires it or when explicitly requested.
