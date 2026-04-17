---
name: spreads-live-rollout
description: Roll out spreads backend or live-ops changes that touch schema, job definitions, workers, scheduler, market-recorder, or trading policy, then verify the live system through the project CLI and Docker services.
---

# Spreads Live Rollout

Use this skill inside `/Users/adeb/Projects/spreads` when the user wants a change to be made live, applied to the running stack, or verified end to end.

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

## Change Classification

Before rollout, classify what changed:

- schema or Alembic files changed
- job definitions, schedules, or policy payloads changed
- code imported by `worker-runtime`
- code imported by `worker-discovery`
- scheduler enqueue logic changed
- `market-recorder` code changed
- API-only or web-only code changed

Use that classification to decide the minimum safe rollout.

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
  - `uv run spreads jobs seed`
- code imported by `worker-runtime` changed:
  - `docker compose restart worker-runtime`
- code imported by `worker-discovery` changed:
  - `docker compose restart worker-discovery`
- scheduler code changed:
  - `docker compose restart scheduler`
- recorder code changed:
  - `docker compose restart market-recorder`
- API runtime only:
  - usually no explicit restart; Docker API hot-reloads
- web-only code:
  - avoid production builds unless explicitly requested

If multiple backend runtime surfaces changed, restart only the affected services, not the whole stack by reflex.

## Verification After Rollout

Use the ops CLI first:

```bash
docker compose ps
uv run spreads status
uv run spreads trading
uv run spreads sessions --limit 5
```

Then drill into impacted labels:

```bash
uv run spreads sessions <session-id>
uv run spreads jobs
docker compose logs --since 3m scheduler worker-runtime worker-discovery market-recorder
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

## Rollout Close-Out

When reporting back, include:

1. exact timestamp of verification
2. what was changed
3. what commands were used to roll it out
4. what is healthy now
5. anything still degraded and whether it is runtime, policy, or data-related
6. what was not verified
