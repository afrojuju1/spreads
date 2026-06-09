---
name: spreads-live-rollout
description: Roll out spreads backend or live-ops changes that touch schema, job definitions, workers, scheduler, market-recorder, or trading policy, then verify the live system through the project CLI and Docker services.
---

# Spreads Live Rollout

Use this skill inside the repo root when the user wants a change to be made live, applied to the running stack, or verified end to end.

Use [docs/current_system_state.md](../../../docs/current_system_state.md) as the canonical source of truth for current runtime ownership and restart boundaries.

Typical prompts:

- "finish this live fix"
- "roll this out"
- "is it live now?"
- "apply the migration and restart what is needed"
- "update the live policy"
- "make sure the new job definition is actually in effect"

## First Principle

Prefer the repo's canonical rollout path over ad hoc commands:

- `uv run` for Python
- `docker compose` for service status, logs, and restarts
- `uv run spreads ...` for operator verification

Do not start duplicate local API, worker, scheduler, or recorder processes if the Docker stack is already running.

Keep this boundary explicit while rolling changes out:

- candidate and signal selection are account-agnostic strategy state
- execution admission belongs to execution/risk/account-capacity
- alerts are downstream job-backed projections of stored state

## Change Classification

Before rollout, classify what changed:

- schema or Alembic files changed
- job definitions, schedules, or policy payloads changed
- code imported by `worker-runtime`
- code imported by `worker-data`
- scheduler enqueue logic changed
- `market-recorder` code changed
- CLI-only code changed
- API-only or web-only code changed

Use imports, touched paths, and the canonical architecture doc to decide the minimum safe rollout. Do not use this skill as an ownership map; read [docs/current_system_state.md](../../../docs/current_system_state.md), which owns the current domain vocabulary and service boundaries.

`TradingOpsState` and `StorageOpsState` are the canonical operator health surfaces.

## Validation Before Rollout

Prefer narrow validation:

```bash
uv run ruff check <touched-python-files>
uv run python -m py_compile <touched-python-files>
```

Do not run broad builds or repo-wide test suites unless the user asks.

## Rollout Matrix

Apply only the steps that match the change:

- schema changed:
  - `uv run alembic upgrade head`
- job definitions, seeded payloads, schedules, or policies changed:
  - `uv run spreads config validate --json`
  - restart the scheduler and affected workers so they load the current config/code
- code imported by `worker-runtime` changed:
  - `docker compose restart worker-runtime`
- code imported by `worker-data` changed:
  - `docker compose restart worker-data`
- scheduler code changed:
  - `docker compose restart scheduler`
- recorder code changed:
  - `docker compose restart market-recorder`
- live data-worker replica count changed:
  - `docker compose up -d --scale worker-data=<count> --no-deps worker-data`
- CLI-only code:
  - no Docker restart; validate through the CLI or targeted tests
- API runtime only:
  - usually no explicit restart; Docker API hot-reloads
- ops read-model only:
  - usually verify through `uv run spreads ...` and API reads; do not restart workers unless changed code is imported by them
- web-only code:
  - avoid production builds unless explicitly requested

If multiple backend runtime surfaces changed, restart only the affected services, not the whole stack by reflex.

In practice:

- most changes under ticker sources, candidate building, capture, market data, strategy entry, or shared backend code imported by data jobs require at least `worker-data`
- most changes under `services/execution/`, `services/session_positions.py`, `services/broker_sync.py`, `services/risk_manager.py`, or runtime job logic require at least `worker-runtime`
- if ownership crosses both lanes, restart both workers and the scheduler only when scheduling logic or job dispatch changed
- changes limited to `packages/core/cli/` do not require worker or scheduler restarts unless the touched module is imported by runtime services
- the live deploy target defaults to one `worker-data` container; if this changes, use compose scaling instead of a restart-only rollout
- `market-recorder` is deployed continuously but should idle outside regular market hours; closed-market `market_recorder_idle` logs are healthy unless other ops state says capture is degraded

## Verification After Rollout

Use the ops CLI first:

```bash
docker compose ps
uv run spreads ops state
uv run spreads ops storage
uv run spreads jobs
uv run spreads jobs lanes
uv run spreads positions --date <YYYY-MM-DD> --json
```

Do not add rollout checks around removed runtime product names or fragmented ops surfaces.

Then drill into impacted labels:

```bash
uv run spreads jobs --job-type <job-type> --limit 10 --json
docker compose logs --since 3m scheduler worker-runtime worker-data market-recorder
```

For policy or seeded job-definition changes, verify both layers:

1. the stored job definition has the new payload
2. the next enqueued or running job run actually carries the new payload

Do not assume reseeding alone changes already-enqueued runs.

## Interpretation Rules

- A stale-slot skip right after a restart can be benign.
- `capture_status=healthy` plus `risk_status=blocked` is a policy gate, not a collector outage.
- `recovery_state=clear` means recovery is no longer the blocker.
- recorder-backed quote rows are the canonical live stream path; a direct collector stream should be treated as fallback or a bug depending on current code.
- selected signals with blocked execution-admission counters are not alert failures and not selection bugs by themselves; verify intent `execution_admission` state before changing candidate builders or alerting.

## Rollout Close-Out

When reporting back, include:

1. exact timestamp of verification
2. what was changed
3. what commands were used to roll it out
4. what is healthy now
5. anything still degraded and whether it is runtime, policy, or data-related
6. what was not verified
