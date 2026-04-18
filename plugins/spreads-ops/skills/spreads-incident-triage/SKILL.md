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

Apply it only inside `/Users/adeb/Projects/spreads`.

Use [docs/current_system_state.md](../../../../docs/current_system_state.md) as the canonical source of truth for current runtime ownership and boundary questions.

Current product terminology note:

- `backtest` is the canonical historical-evaluation product
- `audit` is the canonical operator investigation surface
- `analyze` and `post-market analyze` remain legacy closed-session report surfaces
- do not tell operators to use the removed `spreads replay` command

## First Principle

Start with the running system, not code inspection.

For ops and end-of-day questions, prefer the live Docker-backed state before reading implementation files.

## Canonical Owners

- runtime and pipeline detail: `/Users/adeb/Projects/spreads/packages/core/services/live_runtime.py` and `/Users/adeb/Projects/spreads/packages/core/services/pipelines.py`
- operator health views: `/Users/adeb/Projects/spreads/packages/core/services/ops/`
- discovery and collection flow: `/Users/adeb/Projects/spreads/packages/core/services/collections/`, `/Users/adeb/Projects/spreads/packages/core/services/scanners/`, `/Users/adeb/Projects/spreads/packages/core/services/live_selection.py`, and `/Users/adeb/Projects/spreads/packages/core/services/opportunity_scoring.py`
- canonical opportunity state: `/Users/adeb/Projects/spreads/packages/core/services/signal_state.py`, `/Users/adeb/Projects/spreads/packages/core/services/opportunity_generation.py`, and `/Users/adeb/Projects/spreads/packages/core/services/opportunities.py`
- account and trading health: `/Users/adeb/Projects/spreads/packages/core/services/account_state.py` and `/Users/adeb/Projects/spreads/packages/core/services/ops/trading.py`
- closed-session analysis: `/Users/adeb/Projects/spreads/packages/core/services/post_market_analysis.py`
- post-market storage: `/Users/adeb/Projects/spreads/packages/core/storage/post_market_repository.py`
- alert delivery state: `/Users/adeb/Projects/spreads/packages/core/storage/alert_repository.py`
- worker and scheduler behavior: `/Users/adeb/Projects/spreads/packages/core/jobs/worker.py`, `/Users/adeb/Projects/spreads/packages/core/jobs/registry.py`, `/Users/adeb/Projects/spreads/packages/core/storage/job_repository.py`

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

Interpret them this way:

- `capture_status=healthy` means capture is good even if the session is still blocked for another reason.
- `capture_status=empty`, `baseline_only`, or `recovery_only` means capture is degraded.
- `recovery_state=clear` means recovery is not currently the blocker.
- `missed_slot_count>0` is the main active recovery blocker signal.
- `unrecoverable_slot_count>0` is audit truth, not automatically a current blocker once recovery is clear.
- `risk_status=blocked` with healthy capture usually means policy gating, not runtime breakage.

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
- modeled post-market idea outcomes

Do not present modeled session results as realized account performance.

### 4. Check Closed-Session Analysis

Use:

```bash
uv run spreads analyze --date YYYY-MM-DD --label <label>
uv run spreads post-market analyze --date YYYY-MM-DD --label <label>
curl -s 'http://localhost:58080/post-market/YYYY-MM-DD/<label>'
```

If the question is automation-config historical evaluation rather than one session label's closed-session report, switch to the canonical `uv run spreads backtest ...` surface instead of ad hoc SQL.

Look for:

- overall verdict
- recommendations
- promotable versus monitor modeled PnL
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
- session healthy, analysis weak: strategy issue

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
uv run alembic upgrade head
uv run spreads jobs seed
docker compose restart scheduler worker-runtime worker-discovery market-recorder
docker compose ps
docker compose logs --tail=100 scheduler worker-runtime worker-discovery market-recorder api
uv run spreads status
uv run spreads trading
uv run spreads pipelines
```

Restart `api` or `web` only when needed or when explicitly requested.

## Response Shape

When answering the user, prefer this structure:

1. exact date and whether the answer is live or post-close
2. actual account result
3. runtime, capture, recovery, and risk status
4. affected labels
5. evidence for the diagnosis
6. next actions
