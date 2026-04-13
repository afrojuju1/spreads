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

## First Principle

Start with the running system, not code inspection.

For ops and end-of-day questions, prefer the live Docker-backed state before reading implementation files.

## Canonical Owners

- session list and detail: `/Users/adeb/Projects/spreads/src/spreads/services/sessions.py`
- account and trading health: `/Users/adeb/Projects/spreads/src/spreads/services/account_state.py`
- closed-session analysis: `/Users/adeb/Projects/spreads/src/spreads/services/post_market_analysis.py`
- post-market storage: `/Users/adeb/Projects/spreads/src/spreads/storage/post_market_repository.py`
- alert delivery state: `/Users/adeb/Projects/spreads/src/spreads/storage/alert_repository.py`
- worker and scheduler behavior: `/Users/adeb/Projects/spreads/src/spreads/jobs/worker.py`, `/Users/adeb/Projects/spreads/src/spreads/jobs/registry.py`, `/Users/adeb/Projects/spreads/src/spreads/storage/job_repository.py`

## Canonical Surfaces

Start with the shipped ops CLI, then fall back to logs or code:

```bash
docker compose ps
uv run spreads status
uv run spreads trading
uv run spreads sessions --limit 5
uv run spreads sessions <session-id>
uv run spreads jobs
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
docker compose logs --tail=100 scheduler worker-main worker-collector market-recorder api
```

Remember:

- `api` hot-reloads source changes in Docker
- `worker-main`, `worker-collector`, and `scheduler` do not
- `market-recorder` is a dedicated service and owns the live stream continuity path

If backend code changed recently, stale workers are a first-class suspect.

### 2. Check Session And Capture Health

Use:

```bash
uv run spreads sessions --limit 20
uv run spreads sessions <session-id>
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
curl -s 'http://localhost:58080/post-market/YYYY-MM-DD/<label>'
```

Look for:

- overall verdict
- recommendations
- board versus watchlist modeled PnL
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
docker compose restart scheduler worker-main worker-collector market-recorder
docker compose ps
docker compose logs --tail=100 scheduler worker-main worker-collector market-recorder api
uv run spreads status
uv run spreads trading
uv run spreads sessions --limit 5
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
