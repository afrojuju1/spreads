# Backend Instructions

## Backend Architecture Rules

- Prefer extending existing service entrypoints instead of adding parallel aggregators.
- Keep module boundaries clear: `services/` owns business logic, `storage/` owns persistence and query shapes, `jobs/` owns scheduling and worker entrypoints, and `packages/api` stays a thin adapter over services.
- Treat [../../docs/current_system_state.md](../../docs/current_system_state.md) as the canonical source of truth for current backend ownership and runtime boundaries.
- `services/market_recorder.py` is the sole owner of the Alpaca option websocket connection in the normal runtime. Do not add API-owned or discovery-run-owned reactive option stream capture paths; discovery runs and APIs should consume recorder-backed persisted rows or shared services over that state unless an explicit architecture change is being made.
- When multiple hosts share one Alpaca account, only one live `market-recorder` should own the option websocket at a time. Stop secondary/local recorders before validating another host's live capture.
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

## Canonical Ownership Source

- Do not duplicate the domain ownership map in package `AGENTS.md` files. Use [../../docs/current_system_state.md](../../docs/current_system_state.md) for current domain vocabulary, service ownership, source-of-truth tables, and current-versus-target operator-state boundaries.
- Keep this file focused on backend operating rules and rollout constraints. If ownership changes, update `docs/current_system_state.md` first, then adjust package instructions only where the workflow changes.

## Operator Visibility

- For operator visibility work, reuse these modules with thin adapters instead of introducing parallel API-only logic.
- For session health and current runtime state, prefer `services/live_runtime.py`, `services/discovery_run_health/`, `services/pipelines.py`, and `services/ops/` over creating new read-model owners.
- Active cleanup `spr-zuy` is replacing fragmented operator health surfaces with `TradingOpsState` and `StorageOpsState`. During that work, remove old active `live-doctor`, `status`, `trading`, and `finviz-ledger` product surfaces rather than extending them.
- For jobs health, read operator-facing status fields first. Raw historical failed runs can remain visible while `operator_status` and `actionable_failed_count` show whether they still require action.
- For first-pass ops/runtime checks and historical backtest workflows, follow the repo-level CLI guidance in [../../AGENTS.md](../../AGENTS.md). Keep the canonical command list there instead of repeating it in backend-specific instructions.
- Treat `ade-nucbox-k8-plus` as the canonical live paper backend target. Prefer `uv run spreads ... --env ade-nucbox-k8-plus` for operator reads instead of raw `--db` overrides.
- Use `uv run spreads deploy exec --env ade-nucbox-k8-plus -- ...` only when you intentionally need the deployed checkout on the box at `/home/ade/Projects/spreads`.
- Default verification should be live/runtime validation against the running stack or shipped CLI. Do not add or update backend tests unless the user explicitly asks for test work.

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
- When the live target is already deployed on `ade-nucbox-k8-plus`, avoid bringing local scheduler/workers/recorder back up unless the user explicitly wants dual-host validation. The NUC is the live owner.
