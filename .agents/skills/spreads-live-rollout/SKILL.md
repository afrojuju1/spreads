---
name: spreads-live-rollout
description: Roll out spreads backend or live-ops changes that touch schema, routine definitions, workflow lanes, routine schedules, capture, or trading policy, then verify the live system through the project CLI and Docker services.
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

Do not start duplicate local API, workflow lane, routine reconciler, or capture processes if the Docker stack is already running.

Keep this boundary explicit while rolling changes out:

- candidate and signal selection are account-agnostic strategy state
- execution admission belongs to execution/risk/account-capacity
- alerts are downstream job-backed projections of stored state

## Change Classification

Before rollout, classify what changed:

- schema or Alembic files changed
- job definitions, schedules, or policy payloads changed
- strategy catalog/profile config changed
- code imported by `workflow-runtime`
- code imported by `workflow-data`
- routine schedule reconciliation changed
- `capture-worker` code changed
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

If `py_compile` is blocked by stale Docker-owned `__pycache__` permissions, use a no-write compile check over the touched files instead of changing permissions as part of unrelated work.

Do not run broad builds or repo-wide test suites unless the user asks.

## Rollout Matrix

Apply only the steps that match the change:

- schema changed:
  - `uv run alembic upgrade head`
- job definitions, seeded payloads, schedules, strategy catalog/profiles, or policies changed:
  - `uv run spreads config validate --json`
  - reconcile routine schedules and restart affected workflow lanes so they load the current config/code
- code imported by `workflow-runtime` changed:
  - `docker compose restart workflow-runtime`
- code imported by `workflow-data` changed:
  - `docker compose restart workflow-data`
- routine schedule rendering or reconciliation changed:
  - `docker compose --profile deploy run --rm routine-schedules`
- capture code changed:
  - `docker compose restart capture-worker`
- live data-worker replica count changed:
  - `docker compose up -d --scale workflow-data=<count> --no-deps workflow-data`
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

- most changes under ticker sources, candidate building, market data, or shared backend code imported by data routines require at least `workflow-data`
- most changes under strategy entry, `services/execution/`, `services/session_positions.py`, `services/broker_sync.py`, `services/risk_manager.py`, or runtime job logic require at least `workflow-runtime`
- if ownership crosses lanes, restart only those lanes and reconcile only when routine definitions or schedule rendering changed
- changes limited to `packages/core/cli/` do not require workflow-lane restarts unless the touched module is imported by runtime services
- the live deploy target defaults to one `workflow-data` container; if this changes, use compose scaling instead of a restart-only rollout
- `capture-worker` is deployed continuously but its capture session should idle outside regular market hours; closed-market `capture_session_idle` logs are healthy unless other ops state says capture is degraded

## Verification After Rollout

Use the ops CLI first:

```bash
docker compose ps
uv run spreads ops state
uv run spreads ops storage
uv run spreads jobs
uv run spreads jobs lanes
uv run spreads execution positions --date <YYYY-MM-DD> --json
```

Do not add rollout checks around removed runtime product names or fragmented ops surfaces.

Then drill into impacted labels:

```bash
uv run spreads jobs --job-type <job-type> --limit 10 --json
docker compose logs --since 3m workflow-runtime workflow-data workflow-maintenance capture-worker
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
