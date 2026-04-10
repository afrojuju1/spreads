# Alert Delivery Refactor Plan

Status: deferred

## Goal

Replace inline alert sending with a durable, job-backed alert delivery pipeline that does not drop alerts when collectors or workers fail mid-send.

## Scope

- no backwards-compatibility patching of the current inline sender
- model alert planning and delivery as first-class runtime concerns
- keep the data model to one primary alerts table
- use ARQ jobs for delivery and recovery

## Target Model

Use a single `alerts` table as the durable outbox and delivery state store.

Each row represents one deliverable alert for one target.

Suggested fields:

- `alert_id`
- `created_at`
- `updated_at`
- `session_date`
- `label`
- `session_id`
- `cycle_id`
- `symbol`
- `alert_type`
- `direction`
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
- `response_json`
- `error_text`

Suggested statuses:

- `pending`
- `dispatching`
- `retry_wait`
- `delivered`
- `suppressed`
- `dead_letter`

## Runtime Flow

1. Alert planner computes alert candidates from collector or manual flows.
2. Planner persists `pending` alert rows before any outbound send.
3. Planner enqueues `alert_delivery` jobs.
4. Delivery worker claims a row and moves it to `dispatching`.
5. Successful send marks the row `delivered`.
6. Failed send marks the row `retry_wait` with backoff.
7. A scheduled `alert_reconcile` job re-enqueues `pending`, due `retry_wait`, and stale `dispatching` rows.

## Dedupe And Escalation

Use the latest alert row for the same `dedupe_key + delivery_target` as the source of truth.

Planner behavior:

- acquire a DB-level lock per `dedupe_key + delivery_target`
- inspect the latest active row
- suppress duplicate alerts during cooldown
- update or escalate only when the new signal is materially stronger

This design allows removal of the separate `alert_state` table.

## Code Refactor

- replace the current alert persistence model with a single-table `alerts` repository
- make `alerts/dispatcher.py` planner-only
- remove inline webhook sends from collector and operator flows
- add `alert_delivery` and `alert_reconcile` job types
- add worker handlers for delivery and reconcile jobs
- move all Discord delivery into the delivery worker path

## Proposed Order

1. Add the new alerts schema and repository methods.
2. Refactor alert planning to write durable `pending` rows only.
3. Add `alert_delivery` worker jobs.
4. Add `alert_reconcile` recovery jobs.
5. Cut collector and manual alert paths over.
6. Remove the old inline sender and legacy alert state logic.
