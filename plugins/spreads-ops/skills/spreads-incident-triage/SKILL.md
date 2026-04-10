---
name: spreads-incident-triage
description: Triage degraded collectors, empty captures, worker or scheduler issues, alert delivery problems, and runtime-vs-strategy questions in the spreads codebase.
---

# Spreads Incident Triage

Use this skill when the task is to figure out what broke in `spreads`, especially for prompts like:

- "what broke?"
- "why is 0DTE degraded?"
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

## Triage Order

### 1. Check Runtime Health

Run:

```bash
docker compose ps
docker compose logs --tail=100 scheduler worker-main worker-collector api
```

Remember:

- `api` hot-reloads source changes in Docker
- `worker-main`, `worker-collector`, and `scheduler` do not

If backend code changed recently, stale workers are a first-class suspect.

### 2. Check Session And Capture Health

Use:

```bash
curl -s 'http://localhost:58080/sessions?limit=120'
```

Focus on:

- `status`
- `latest_capture_status`
- `websocket_quote_events_saved`
- `baseline_quote_events_saved`
- `board_count`
- `watchlist_count`
- `alert_count`

Treat these as hard signals:

- `latest_capture_status=empty` means unusable capture
- `websocket_quote_events_saved=0` for a live label means the stream may have technically run but did not produce usable session data

### 3. Check Actual Trading Outcome

Use:

```bash
curl -s 'http://localhost:58080/account/overview?history_range=1D'
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

Typical split:

- session healthy, alerts failed: delivery issue
- session degraded, alerts thin: upstream capture or selection issue
- session healthy, analysis weak: strategy issue

## Classification Vocabulary

Use one of these labels in the final diagnosis:

- `runtime failure`
- `capture failure`
- `delivery failure`
- `strategy weakness`
- `mixed issue`

## Rollout Checklist After Backend Fixes

If the task turns into a code change, finish with:

```bash
uv run alembic upgrade head
uv run spreads-seed-jobs
docker compose restart scheduler worker-main worker-collector
docker compose ps
docker compose logs --tail=100 scheduler worker-main worker-collector api
```

Restart `api` or `web` only when needed or when explicitly requested.

## Response Shape

When answering the user, prefer this structure:

1. exact date and whether the answer is live or post-close
2. actual account result
3. runtime and capture status
4. affected labels
5. evidence for the diagnosis
6. next actions
