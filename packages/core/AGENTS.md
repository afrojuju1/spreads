# Backend Instructions

## Backend Architecture Rules

- Prefer extending existing service entrypoints instead of adding parallel aggregators.
- Keep module boundaries clear: `services/` owns business logic, `storage/` owns persistence and query shapes, `jobs/` owns scheduling and worker entrypoints, and `packages/api` stays a thin adapter over services.
- `services/market_recorder.py` is the sole owner of the Alpaca option websocket connection in the normal runtime. Do not add API-owned or collector-owned reactive option stream capture paths; collectors and APIs should consume recorder-backed persisted rows or shared services over that state unless an explicit architecture change is being made.
- Favor one canonical backend path per responsibility. If logic is already repeated, extract the shared behavior before adding more.
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

- pipeline/session runtime list/detail: `services/pipelines.py`
- actual account and trading health: `services/account_state.py`
- closed-session verdicts and recommendations: `services/post_market_analysis.py` and `storage/post_market_repository.py`
- alert delivery state: `storage/alert_repository.py`
- job execution and scheduler behavior: `jobs/worker.py`, `jobs/registry.py`, and `storage/job_repository.py`

## Operator Visibility

- For operator visibility work, reuse these modules with thin adapters instead of introducing parallel API-only logic.
- For first-pass ops/runtime checks and replay workflows, follow the repo-level CLI guidance in [../../AGENTS.md](../../AGENTS.md). Keep the canonical command list there instead of repeating it in backend-specific instructions.
- Treat `services.analysis.py` as legacy post-close reporting, not the canonical decision-replay path.
- For closed-session investigations, check post-market analysis before tuning strategy thresholds from raw session counts alone.

## End-Of-Day And Ops Queries

- For questions about "how did we do today", market-close summaries, collector health, or live ops status, prefer the running Docker-backed system state before code inspection.
- Use the existing stack and narrow live reads first:
  - account and trading health: `services/account_state.py` or `http://localhost:58080/account/overview?history_range=1D`
  - pipeline/session runtime health: `services/pipelines.py` or `uv run spreads pipelines`
  - closed-session analysis: `storage/post_market_repository.py` / `services/post_market_analysis.py` or `http://localhost:58080/post-market/{session_date}/{label}`
- Always distinguish actual account PnL from modeled post-market outcomes. Do not present modeled idea outcomes as realized account performance.
- Replay output now includes modeled close/final PnL and actual traded-position PnL. Treat modeled and actual metrics as separate evaluation planes.
- After market close, use exact dates in summaries.

## Rollout Checklist

- After schema changes, run `uv run alembic upgrade head`.
- If job definitions or scheduled/manual job keys changed, run `uv run spreads jobs seed`.
- After changing code imported by `worker-main`, `worker-collector`, or `scheduler`, restart those containers before trusting runtime behavior.
- Use `docker compose ps` and recent `docker compose logs` to verify startup and job execution after restart.
- Restart `api` only when the changed runtime surface requires it or when explicitly requested.
