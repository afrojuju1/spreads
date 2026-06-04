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

Current shipped operator surfaces:

- `ops state` is the canonical live trading operator surface
- `ops storage` is the canonical storage and retention surface
- `jobs` and `jobs lanes` are the canonical scheduler/worker surfaces
- `positions` is the shipped position drilldown
- do not tell operators to use removed or currently unshipped `spreads audit`, `spreads automations`, `spreads backtest`, `spreads research`, `spreads replay`, `spreads analyze`, or `spreads post-market analyze` commands

## First Principle

Start with the running system, not code inspection.

For ops and end-of-day questions, prefer the live Docker-backed state before reading implementation files.

## Canonical Ownership Source

Do not rely on this skill as an architecture map. For domain ownership, object vocabulary, and current-versus-target operator-state boundaries, read:

- [docs/current_system_state.md](../../../docs/current_system_state.md)

That document owns the map for signals, decisions, admissions, intents, attempts, orders, fills, positions, closes, reconciliation, broker sync, trading ops state, and storage ops state.

`TradingOpsState` and `StorageOpsState` are the canonical operator health surfaces.

Keep these boundaries straight while triaging:

- selection says whether the idea is good
- execution admission says whether this account can carry it now
- alert delivery is a downstream projection of that state, not a separate decision layer

## Canonical Surfaces

Start with the shipped ops CLI, then fall back to logs or code:

```bash
docker compose ps
uv run spreads ops state
uv run spreads ops storage
uv run spreads jobs
uv run spreads jobs lanes
uv run spreads positions --date <YYYY-MM-DD> --json
```

Do not add new investigation workflow around retired fragmented ops, pipeline, discovery, or UOA product names.

Use direct API reads or code inspection only when the CLI is insufficient.

## Key Signals

Read these fields first:

- `Trading Allowed`
- `details.primary_trading_flow.source_state.status`
- `details.primary_trading_flow.source_state.age_seconds`
- `details.primary_trading_flow.candidate_state.status`
- `details.primary_trading_flow.intent_state.active_intent_count`
- `details.primary_trading_flow.position_state.open_position_count`
- `details.engine.summary.capture_status`
- `details.engine.summary.capture_active_target_count`
- `trading_allowed`
- `broker_sync.status`
- `summary.engine_selected_count`
- `summary.open_execution_count`
- `details.execution_health.stale_open_execution_count`
- `details.execution_health.submit_unknown_execution_count`
- `execution_admission.status`
- `execution_admission.reason`
- `actionable_failed_count`
- `operator_status`

Interpret them this way:

- `capture_status=healthy` means capture is good even if the session is still blocked for another reason.
- `capture_status=empty`, `baseline_only`, or `recovery_only` means capture is degraded.
- stale source or candidate state during market hours means the data/strategy lane is the first suspect.
- healthy source and candidate state with no selected decisions is usually strategy selection, not scheduler failure.
- `trading_allowed=false` before market open is expected; after open it should become true only when market session, broker sync, account, control, and execution gates are all healthy.
- selected decisions without active or filled intents point at admission, dispatch, or broker submission.
- open executions with stale age or unknown submit status point at execution lifecycle reconciliation.
- Raw historical job failures are diagnostics. Prefer `operator_status`, `operator_status_counts`, and `actionable_failed_count` when deciding whether jobs are currently blocking the system.
- A historical failed `broker_sync:alpaca` run is not a live blocker if canonical broker-sync state recovered later and jobs health reports `actionable_failed_count=0`.

## Triage Order

### 1. Check Runtime Health

Run:

```bash
docker compose ps
uv run spreads ops state
uv run spreads ops storage
docker compose logs --tail=100 scheduler worker-runtime worker-data market-recorder api
```

Remember:

- `api` hot-reloads source changes in Docker
- `worker-runtime`, `worker-data`, and `scheduler` do not
- `market-recorder` is a dedicated service and owns the live stream continuity path

If backend code changed recently, stale workers are a first-class suspect.

### 2. Check Session And Capture Health

Use:

```bash
uv run spreads ops state
uv run spreads jobs --limit 25 --json
uv run spreads positions --date YYYY-MM-DD --json
```

Focus on:

- `details.engine.summary.capture_status`
- `details.engine.summary.capture_active_target_count`
- `details.primary_trading_flow.source_state.status`
- `details.primary_trading_flow.candidate_state.status`
- `details.primary_trading_flow.intent_state.active_intent_count`
- `details.primary_trading_flow.position_state.open_position_count`
- `alert_count`

Treat these as hard signals:

- `capture_status=empty` means unusable capture
- source or candidate staleness during market hours means the ticker source, data worker, or strategy entry job needs attention
- active intents without broker progress means execution dispatch or broker sync needs attention

### 3. Check Actual Trading Outcome

Use:

```bash
uv run spreads ops state
uv run spreads positions --date YYYY-MM-DD --json
uv run spreads jobs --job-type execution_intent_dispatch --limit 10 --json
```

Always separate:

- actual account PnL
- source, candidate, signal, and decision diagnostics
- current execution-admission truth

Do not present modeled session results as realized account performance.

### 4. Check Historical Or Session Evaluation

There is no shipped `audit`, `automations`, or `backtest` CLI in the current app. If live ops state is not enough, inspect current persisted engine facts, positions, job runs, and logs through shipped surfaces first:

```bash
uv run spreads ops state --json
uv run spreads jobs --json
uv run spreads positions --date YYYY-MM-DD --json
docker compose logs --since 30m scheduler worker-runtime worker-data market-recorder
```

If a historical evaluator or policy comparison tool is needed, create or update a bead and design it against the current ticker-source/candidate/signal/decision model instead of reviving old pipeline/audit/backtest wrappers.

Look for:

- runtime capture context
- source, candidate, signal, and decision counts
- selected versus rejected/blocked ideas
- active intents, attempts, fills, and positions

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
- session healthy, selected opportunities present, and execution admission blocked: account-capacity or execution-policy issue
- session degraded, alerts thin: upstream capture or selection issue
- session healthy but current source/candidate/decision quality is weak: strategy issue

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
uv run spreads ops state
uv run spreads ops storage
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
