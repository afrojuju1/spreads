---
name: spreads-incident-triage
description: Triage degraded collectors, trading gates, recovery gaps, market-recorder issues, alert delivery problems, and runtime-vs-strategy questions in the spreads codebase.
---

# Spreads Incident Triage

Use this skill when the task is to figure out what broke in `spreads`, especially for prompts like:

- "what broke?"
- "why is 0DTE degraded?"
- "why is trading blocked?"
- "why is this session blocked?"
- "is this a runtime issue or a strategy issue?"
- "how did we do today?"
- "why were alerts missing?"

Apply it only inside this repo.

Use [docs/current_system_state.md](../../../docs/current_system_state.md) as the canonical source of truth for current runtime ownership and boundary questions.

Current product terminology note:

- `backtest` is the canonical historical-evaluation product
- `audit` is the canonical operator investigation surface
- `backtest compare` is the canonical comparison surface for exported `run`, `replay`, and `replay-range` payloads
- `analyze` and `post-market analyze` have been removed from the operator workflow
- do not tell operators to use the removed `spreads replay` command

## First Principle

Start with the running system, not code inspection.

For ops and end-of-day questions, prefer the live Docker-backed state before reading implementation files.

## Canonical Owners

- runtime and pipeline detail: `packages/core/services/live_runtime.py` and `packages/core/services/pipelines.py`
- operator health views: `packages/core/services/ops/`
- discovery and collection flow: `packages/core/services/discovery_runs/`, `packages/core/services/scanners/`, `packages/core/services/live_selection.py`, `packages/core/services/opportunity_scoring.py`, and `packages/core/services/candidate_policy.py`
- canonical opportunity state: `packages/core/services/signal_state.py`, `packages/core/services/opportunity_generation.py`, and `packages/core/services/opportunities.py`
- account and trading health: `packages/core/services/account_state.py` and `packages/core/services/ops/trading.py`
- historical decision evaluation and policy research: `packages/core/backtest/`
- alert delivery state: `packages/core/storage/alert_repository.py`
- worker and scheduler behavior: `packages/core/jobs/worker.py`, `packages/core/jobs/registry.py`, and `packages/core/storage/job_repository.py`

## Canonical Surfaces

Start with the shipped ops CLI, then fall back to logs or code:

```bash
docker compose ps
uv run spreads status
uv run spreads trading
uv run spreads pipelines
uv run spreads jobs
uv run spreads uoa
```

Use direct API reads or code inspection only when the CLI is insufficient.

## Key Signals

Read these fields first:

- `Trading Allowed`
- collector `status`
- collector `capture_status`
- `stream_quote_events_saved`
- `baseline_quote_events_saved`
- `recovery_state`
- `missed_slot_count`
- `unrecoverable_slot_count`
- `risk_status`
- `risk_note`
- `trading_allowed`
- `broker_sync.status`
- `actionable_failed_count`
- `operator_status`

Interpret them this way:

- `capture_status=healthy` means capture is good even if the session is still blocked for another reason.
- `capture_status=empty`, `baseline_only`, or `recovery_only` means capture is degraded.
- `recovery_state=clear` means recovery is not currently the blocker.
- `missed_slot_count>0` is the main active recovery blocker signal.
- `unrecoverable_slot_count>0` is audit truth, not automatically a current blocker once recovery is clear.
- `risk_status=blocked` with healthy capture usually means policy gating, not runtime breakage.
- `trading_allowed=false` before market open is expected; after open it should become true only when market session, broker sync, account, control, and execution gates are all healthy.
- Raw historical job failures are diagnostics. Prefer `operator_status`, `operator_status_counts`, and `actionable_failed_count` when deciding whether jobs are currently blocking the system.
- A historical failed `broker_sync:alpaca` run is not a live blocker if canonical broker-sync state recovered later and jobs health reports `actionable_failed_count=0`.

## Triage Order

### 1. Check Runtime Health

Run:

```bash
docker compose ps
uv run spreads status
uv run spreads trading
docker compose logs --tail=100 scheduler worker-runtime worker-discovery market-recorder api
```

Remember:

- `api` hot-reloads source changes in Docker
- `worker-runtime`, `worker-discovery`, and `scheduler` do not
- `market-recorder` is a dedicated service and owns the live stream continuity path

If backend code changed recently, stale workers are a first-class suspect.

### 2. Check Session And Capture Health

Use:

```bash
uv run spreads pipelines
uv run spreads pipelines <pipeline-id> --date YYYY-MM-DD
uv run spreads audit <pipeline-id> --date YYYY-MM-DD
```

Focus on:

- `status`
- `latest_capture_status`
- `stream_quote_events_saved`
- `baseline_quote_events_saved`
- `recovery_state`
- `missed_slot_count`
- `unrecoverable_slot_count`
- `risk_status`
- `risk_note`
- `alert_count`

Treat these as hard signals:

- `latest_capture_status=empty` means unusable capture
- `stream_quote_events_saved=0` for a live label means the stream or recorder path produced no usable live quote rows
- `risk_status=blocked` with a note like `max_open_positions_per_session reached` means policy saturation, not collector failure

If the pipeline id is not obvious, list pipelines first and use the exact `pipeline:<label>` id shown by `uv run spreads pipelines`.

### 3. Check Actual Trading Outcome

Use:

```bash
uv run spreads trading
```

Always separate:

- actual account PnL
- modeled backtest, audit, or selection diagnostics

Do not present modeled session results as realized account performance.

### 4. Check Historical Or Session Evaluation

Use:

```bash
uv run spreads audit <pipeline-id> --date YYYY-MM-DD
uv run spreads backtest run --bot-id <bot-id> --automation-id <automation-id>
uv run spreads backtest replay-range --bot-id <bot-id> --automation-id <automation-id> --start-date YYYY-MM-DD --end-date YYYY-MM-DD --source alpaca --config-root <config-root> --export-json <path>
uv run spreads backtest compare --left-json <path> --right-json <path>
```

Use `audit` for one pipeline/date operator investigation. Use `backtest` for automation-config historical decision evaluation, strategy tuning, and policy comparisons.

For before/after policy studies:

1. create isolated `before/` and `after/` config roots instead of editing active config in place
2. replay the same date window through both roots with `backtest replay-range`
3. compare the exported JSON payloads with `backtest compare`

Look for:

- runtime capture and recovery context
- opportunity counts and promotable versus monitor split
- selected versus rejected/blocked ideas
- top and bottom ideas

This is the main way to distinguish:

- runtime failure
- data capture failure
- weak strategy output
- mixed cases

Repeated weak verdicts or repeated recommendations are tuning signals, not just one-day noise.

### 5. Check Alerts And Jobs Only After The Above

If the issue involves delivery or orchestration, inspect:

- alert rows and statuses
- seeded job definitions
- recent job runs
- scheduler and worker logs
- market-recorder logs when live stream continuity is in question

Typical split:

- session healthy, alerts failed: delivery issue
- session blocked with healthy capture and blocked risk note: policy issue
- session degraded, alerts thin: upstream capture or selection issue
- session healthy, backtest/audit weak: strategy issue

### Alert Delivery Triage

Treat Discord delivery as a job-backed outbox, not an inline webhook send.

For “why were alerts missing?” questions, check in this order:

1. confirm the planner created a `delivery` row in `alert_events`
2. inspect the delivery status
3. confirm `alert_delivery` worker jobs are running or retrying
4. confirm the scheduled `alert_reconcile` job is seeded and healthy

Interpret delivery statuses this way:

- `delivered`: Discord send succeeded
- `pending`: planned and waiting for delivery job pickup
- `dispatching`: currently claimed by a worker; if stale, `alert_reconcile` should reset and requeue it
- `retry_wait`: delivery failed and is waiting for backoff/requeue
- `dead_letter`: delivery exhausted retries and needs operator attention
- `suppressed`: delivery was intentionally not queued, usually because webhook configuration was unavailable at plan time

When drilling deeper, prefer:

```bash
uv run spreads jobs
docker compose logs --tail=100 scheduler worker-runtime
```

Then inspect:

- `packages/core/storage/alert_repository.py`
- `packages/core/services/alert_delivery.py`

Classify missing alerts this way:

- row missing: planner or upstream selection issue
- row `pending` or stale `dispatching`: orchestration or worker issue
- row `retry_wait` or `dead_letter`: delivery failure, including rate limits or webhook errors
- row `suppressed`: configuration issue, not a worker failure

## Recorder And Recovery Notes

Use these assumptions unless current evidence disproves them:

- The collector should prefer recorder-backed market data instead of opening its own live stream.
- `406 connection limit exceeded` is usually a sign that the recorder path was bypassed or another stream owner is misconfigured.
- stale scheduled slots should be marked `missed`, not replayed.
- recovery should clear once missed gaps are resolved by a fresh healthy slot.

## Classification Vocabulary

Use one of these labels in the final diagnosis:

- `runtime failure`
- `capture failure`
- `recovery gating`
- `risk-policy gating`
- `delivery failure`
- `strategy weakness`
- `mixed issue`

## Rollout Checklist After Backend Fixes

If the task turns into a code change, finish with:

```bash
uv run ruff check <touched-python-files>
uv run python -m py_compile <touched-python-files>
docker compose ps
uv run spreads status
uv run spreads trading
uv run spreads pipelines
uv run spreads jobs
```

If the change needs live rollout, switch to `spreads-live-rollout` and restart only the affected Docker services.

## Response Shape

When answering the user, prefer this structure:

1. exact date and whether the answer is live or post-close
2. actual account result
3. runtime, capture, recovery, and risk status
4. affected labels
5. evidence for the diagnosis
6. next actions
