# Alert Delivery Refactor Plan

Status: implemented

Last updated: April 10, 2026

## Goal

Replace inline Discord sending with a durable, job-backed alert pipeline that:

- does not drop alerts when planners, workers, or Redis enqueue steps fail
- keeps alert delivery state in one table
- removes `alert_state` as a separate source of truth
- supports both automated collector alerts and manual operator alerts

## Shipped Design

### One Table, Reuse `alert_events`

The durable outbox now lives in `alert_events`.

The separate `alert_state` table was removed.

`alert_events` now carries both:

- `record_kind='delivery'` rows for deliverable alerts
- `record_kind='score_anchor'` rows for spread-score anchor state

### Shared Planner Path

Collector alerts and manual alerts now use the same planner flow:

1. build the alert payload
2. persist a `delivery` row
3. enqueue an `alert_delivery` job when delivery is possible
4. publish `alert.event.created`

The planner no longer sends Discord inline.

### Shared Delivery Path

Discord delivery now happens only in the worker path.

The worker:

1. claims a `pending` or due `retry_wait` delivery row
2. moves it to `dispatching`
3. sends the webhook
4. marks the row `delivered`, `retry_wait`, or `dead_letter`
5. publishes `alert.event.updated`

### Reconcile Path

A scheduled `alert_reconcile` job now runs every `1 minute`.

It:

- re-enqueues `pending` rows when no active delivery job is attached
- re-enqueues due `retry_wait` rows
- resets stale `dispatching` rows back to `pending` and requeues them

The original draft said `30-60s`.

The shipped implementation uses `1 minute` because the current scheduler only supports minute-granularity interval jobs.

## Table Model

Physical table:

- `alert_events`

Current fields used by the refactor:

- `alert_id`
- `record_kind`
- `created_at`
- `updated_at`
- `session_date`
- `label`
- `session_id`
- `cycle_id`
- `symbol`
- `alert_type`
- `delivery_target`
- `dedupe_key`
- `status`
- `attempt_count`
- `claimed_at`
- `last_attempt_at`
- `next_attempt_at`
- `delivered_at`
- `planner_job_run_id`
- `delivery_job_run_id`
- `worker_name`
- `payload_json`
- `state_json`
- `response_json`
- `error_text`

Record-kind expectations:

- `delivery` rows use the delivery lifecycle fields
- `score_anchor` rows use `state_json` and `status='anchor'`

Delivery statuses:

- `pending`
- `dispatching`
- `retry_wait`
- `delivered`
- `suppressed`
- `dead_letter`

Anchor status:

- `anchor`

## Dedupe And Anchors

### Dedupe

The source of truth for dedupe is now the latest matching `delivery` row in `alert_events`.

Current shipped behavior is intentionally conservative:

- the planner acquires a transaction advisory lock on `delivery_target + dedupe_key`
- if a matching delivery row already exists, the planner reuses it instead of creating a duplicate row
- duplicate attempts are not recorded as extra `suppressed` rows

This keeps the cutover coherent with the current alert families and avoids noisy duplicate audit rows.

If richer cooldown or “materially stronger” escalation logic is added later, it should extend this same planner path instead of reintroducing a side table.

### Score Anchors

Score anchors now live in `alert_events` as `record_kind='score_anchor'`.

The planner now upserts score anchors for the full board-candidate set after each cycle, not only for candidates that happened to emit an alert that cycle.

That closes the main gap from the earlier design: spread breakout state is now durable even when no delivery row is created.

## Event Semantics

Alert events are now split by lifecycle phase:

- `alert.event.created` when the planner persists a delivery row
- `alert.event.updated` when delivery status changes

API and web consumers were updated to:

- treat the alerts feed as `record_kind='delivery'` only
- ignore `score_anchor` rows in session counts and alert listings
- invalidate alert/session queries on both `created` and `updated`

## Code Surface

Primary files updated by this refactor:

- [alert_models.py](/Users/adeb/Projects/spreads/packages/core/storage/alert_models.py)
- [alert_repository.py](/Users/adeb/Projects/spreads/packages/core/storage/alert_repository.py)
- [dispatcher.py](/Users/adeb/Projects/spreads/packages/core/alerts/dispatcher.py)
- [operator_actions.py](/Users/adeb/Projects/spreads/packages/core/services/operator_actions.py)
- [alert_delivery.py](/Users/adeb/Projects/spreads/packages/core/services/alert_delivery.py)
- [live_collector.py](/Users/adeb/Projects/spreads/packages/core/jobs/live_collector.py)
- [registry.py](/Users/adeb/Projects/spreads/packages/core/jobs/registry.py)
- [worker.py](/Users/adeb/Projects/spreads/packages/core/jobs/worker.py)
- [seed.py](/Users/adeb/Projects/spreads/packages/core/jobs/seed.py)
- [main.py](/Users/adeb/Projects/spreads/packages/api/main.py)
- [providers.tsx](/Users/adeb/Projects/spreads/packages/web/components/providers.tsx)
- [alerts-feed.tsx](/Users/adeb/Projects/spreads/packages/web/components/alerts/alerts-feed.tsx)

## Migration Notes

The Alembic migration:

- extends `alert_events` with the outbox and anchor columns
- backfills existing rows as `record_kind='delivery'`
- maps legacy statuses:
  - `skipped -> suppressed`
  - `failed -> dead_letter`
- drops `alert_state`

## Verification

Targeted checks completed during implementation:

- Python changed files compile with `uv run python -m py_compile`
- web changes lint clean with `npm run lint -- components/providers.tsx components/alerts/alerts-feed.tsx lib/api.ts`

## Follow-Up Guidance

If this area evolves again, keep these constraints:

- do not reintroduce inline Discord sending
- do not add a second alert durability table without a clear migration reason
- keep `delivery` and `score_anchor` filtering explicit in read paths
- extend the shared planner path instead of adding special-case manual or collector delivery code
