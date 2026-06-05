# Market Data Storage And Logging Design

Status: final refined design for `spr-zuy.6`; implementation tracked by `spr-4j0`
Date: 2026-06-05

## Bottom Line

The first proposal had the right instinct, but it was still too attached to the old table names.

The clean target is:

- Raw option quotes and trades are market data, not ops events.
- There is no database event journal.
- High-volume market data gets native PostgreSQL daily partitions.
- Operational visibility goes through standard structured application logging and the central logging stack.
- Durable runtime facts stay in their domain tables: jobs, execution, broker sync, alerts, controls, engine facts, positions, capture state.
- We rename market-data tables and env knobs instead of keeping old names alive.

This is a breaking storage cleanup. No compatibility aliases, wrappers, `event_log`, `ops_events`, or old `event_log_market_events` policy lane.

## Research And Local Evidence

Current deployment uses `postgres:17-alpine`.

Relevant primary docs:

- PostgreSQL 17 declarative partitioning says dropping or detaching partitions is far faster than bulk deletion and avoids the `VACUUM` overhead from bulk `DELETE`. It also notes partitioned-table unique/primary constraints must include all partition keys. See [PostgreSQL table partitioning](https://www.postgresql.org/docs/17/ddl-partitioning.html).
- PostgreSQL 17 BRIN indexes are designed for very large tables where indexed columns correlate with physical storage order, which fits append-only timestamped tick data. See [PostgreSQL BRIN indexes](https://www.postgresql.org/docs/17/brin.html).
- Timescale hypertables are good time-series storage, with automatic time chunks and `drop_chunks`, but they add an extension/runtime dependency and have the same unique-index partition-key constraint. See [Timescale hypertables](https://docs.timescale.com/use-timescale/latest/hypertables/), [unique indexes](https://docs.timescale.com/use-timescale/latest/hypertables/hypertables-and-unique-indexes/), and [drop_chunks](https://docs.timescale.com/api/latest/hypertable/drop_chunks/).

Live `ade-nucbox-k8-plus` evidence:

| Current table or topic | Size/count | Meaning |
| --- | ---: | --- |
| `option_quote_events` | 22.56 GB / 77.1M estimated rows | Real high-volume raw option quote storage. |
| `option_trade_events` | 194.6 MB / 385k estimated rows | Lower-volume raw trade storage, still time-retained market data. |
| `event_log` | 9.21 GB / 2.3M estimated rows | Bloated because it mixed raw market telemetry and old product events with ops events. |
| `market.quote.captured` | 2.06M rows | Historical raw quote envelope duplication; not an active product surface. |
| `job.run.updated` | 185k rows | Durable job state already belongs in job tables; persisting every update as generic event noise is not the clean model. |

## Problems With The Old Shape

`option_quote_events` and `option_trade_events` are misnamed. They are raw market ticks captured from the option stream. Calling them events makes future agents confuse market data with the ops event bus.

`event_log` is too generic. It became a dumping ground for market telemetry, old `signal_event` and `opportunity_event` rows, UOA/discovery leftovers, broker sync, scheduler updates, and alerts.

The split retention policy `event_log_market_events` versus `event_log_control_events` is a smell. It exists only because unrelated data classes share one table.

Persisting every broadcast event is not logging architecture. App logs belong in the central logging stack. Durable lifecycle facts belong in engine/execution/portfolio tables. A generic DB event journal should not exist.

## Target Tables

| Target table | Owns | Partitioning | Retention | Notes |
| --- | --- | --- | --- | --- |
| `option_quote_ticks` | Raw option quote ticks captured by market recorder. | Daily range partitions on `captured_at`. | Short, default 7 days. | Replaces `option_quote_events`. |
| `option_trade_ticks` | Raw option trade ticks captured by market recorder. | Daily range partitions on `captured_at`. | Short/medium, default 30 days. | Replaces `option_trade_events`. |
| `capture_targets` | Desired capture set. | None. | Lifecycle/state table. | Already proper. |
| `capture_summaries` | Recorder iteration health and row counts. | None. | Ops retention, not raw tick retention. | Already proper. |

Do not create `market_event_log`. That was the compatibility-minded version. Market telemetry should be raw ticks plus capture summaries. If a future strategy needs market-data-derived features, create a properly named aggregate or feature table, not another generic market event envelope.

## What Gets Dropped

Drop or stop persisting these to Postgres:

- `event_log`.
- Any replacement table such as `ops_events`.
- Raw market quote/trade capture envelopes.
- Old `signal_event`, `opportunity_event`, `risk_event`, `uoa.*`, `live.discovery_run.*`, and similar pre-engine product events.
- Generic `job.run.updated` rows that duplicate durable job tables.
- Any event whose only purpose is application logging. Logs belong in Loki/Grafana, not Postgres event facts.

Keep durable facts in their owning tables instead:

- Control-mode and operator actions stay in control tables.
- Alert lifecycle facts stay in alert tables/outbox.
- Job state stays in job tables.
- Broker and execution facts stay in broker/execution tables.
- Engine decisions, admissions, signals, positions, and capture state stay in engine/storage tables.
- Human/operator visibility uses structured logs in the logging stack.

## Partition Shape

All schema changes are Alembic-owned. Do not run ad-hoc DDL by hand during cutover.

The implementation should add a normal versioned Alembic migration that:

- Drops or tombstones the old active tables.
- Creates the partitioned parent tables.
- Creates the initial day partitions needed for cutover and the live validation window.
- Creates parent indexes and partition-local indexes through the migration.
- Removes `event_log` and its related indexes/tables from the active schema.

The examples below describe the schema contract. They are not operator runbooks or raw SQL to paste into `psql`.

### `option_quote_ticks`

Alembic creates a partitioned parent with:

| Column | Type | Notes |
| --- | --- | --- |
| `captured_at` | timezone-aware datetime | Partition key, not nullable. |
| `quote_tick_id` | bigint identity | Local database identifier. |
| `cycle_id` | text | Recorder cycle. |
| `label` | text | Capture label. |
| `underlying_symbol` | text nullable | Underlying ticker. |
| `strategy` | text nullable | Strategy id when known. |
| `profile` | text nullable | Capture/scanner profile. |
| `option_symbol` | text | Contract symbol. |
| `leg_role` | text | Target leg role. |
| `bid`, `ask`, `midpoint` | float | Quote values. |
| `bid_size`, `ask_size` | integer | Quote sizes. |
| `source_timestamp` | timezone-aware datetime nullable | Broker/source timestamp. |
| `source` | text | Default `alpaca_websocket`. |

Primary key: `(captured_at, quote_tick_id)`.

Partitioning: range by `captured_at`.

Daily child name:

`option_quote_ticks_2026_06_05`

Indexes on the parent:

- `btree (option_symbol, captured_at DESC)`
- `btree (label, captured_at DESC)`
- `btree (cycle_id)`
- `brin (captured_at)`

Do not keep a standalone unique `quote_id`. PostgreSQL requires partitioned unique/primary constraints to include the partition key. The composite primary key is the database identity; application code should not use raw ticks as cross-table foreign-key parents.

### `option_trade_ticks`

Alembic creates the same partitioned parent shape:

| Column | Type | Notes |
| --- | --- | --- |
| `captured_at` | timezone-aware datetime | Partition key, not nullable. |
| `trade_tick_id` | bigint identity | Local database identifier. |
| `cycle_id` | text | Recorder cycle. |
| `label` | text | Capture label. |
| `underlying_symbol` | text nullable | Underlying ticker. |
| `strategy` | text nullable | Strategy id when known. |
| `profile` | text nullable | Capture/scanner profile. |
| `option_symbol` | text | Contract symbol. |
| `leg_role` | text | Target leg role. |
| `price` | float | Trade price. |
| `size` | integer | Trade size. |
| `premium` | float | Trade premium. |
| `exchange_code` | text nullable | Source exchange. |
| `conditions_json` | jsonb | Trade conditions. |
| `source_timestamp` | timezone-aware datetime nullable | Broker/source timestamp. |
| `included_in_score` | boolean | Whether the trade contributes to scoring. |
| `exclusion_reason` | text nullable | Reason it was excluded from scoring. |
| `raw_payload_json` | jsonb | Broker payload. |
| `source` | text | Default `alpaca_websocket`. |

Primary key: `(captured_at, trade_tick_id)`.

Partitioning: range by `captured_at`.

Daily child name:

`option_trade_ticks_2026_06_05`

Indexes on the parent:

- `btree (option_symbol, captured_at DESC)`
- `btree (underlying_symbol, captured_at DESC)`
- `btree (label, captured_at DESC)`
- `btree (cycle_id)`
- `brin (captured_at)`

## Partition Maintenance

Create a Spreads-owned partition maintenance service, not pg_partman, and keep it aligned with the Alembic-created parent schema.

It should:

- Create daily partitions for today plus at least 14 future calendar days.
- Run before market open and after deploy.
- Drop expired tick partitions off-hours.
- Refuse market-open trading/capture readiness if today's tick partitions are missing.
- Report partition coverage through `StorageOpsState`.

The maintenance service is for routine child partition lifecycle after the Alembic cutover, not for untracked schema changes. It should use one audited partition helper shared by migrations and runtime maintenance, so partition naming, bounds, indexes, and safety checks are not duplicated.

Do not use default partitions. A default partition hides a failed partition-maintenance job and makes later attachment more expensive. Missing tick partitions should be a clear preflight/deploy defect, not silent quarantine storage.

Dropping uses partition bounds:

- Drop a tick partition only when its upper bound is older than the retention cutoff.
- Expect up to one extra partial UTC day retained.
- Use `DETACH PARTITION CONCURRENTLY` when archiving before drop is needed; otherwise drop old partitions off-hours.

## Retention Names

Rename environment knobs to match the new model:

- `SPREADS_OPTION_QUOTE_TICK_RETENTION_DAYS`
- `SPREADS_OPTION_TRADE_TICK_RETENTION_DAYS`

Delete the old split knobs:

- `SPREADS_EVENT_LOG_MARKET_RETENTION_DAYS`
- `SPREADS_EVENT_LOG_CONTROL_RETENTION_DAYS`

There is no market-event retention lane and no DB event-retention lane after this cleanup.

## Logging Target

Use regular application logging, not DB persistence, for operational events.

Implementation direction:

- Use the standard Python `logging` library behind a small project helper for consistent fields.
- Emit to stdout/stderr so Docker and the central logging collector own transport.
- Keep log fields stable: `service`, `component`, `event`, `status`, `strategy_id`, `job_id`, `intent_id`, `attempt_id`, `position_id`, `symbol`, `environment`, and `correlation_id` when present.
- Use log levels consistently: `info` for lifecycle transitions, `warning` for degraded/skipped operator attention, `error` for failed work, `exception` when stack traces matter.
- Do not log raw market tick payloads at normal levels.
- Do not add a second durable log table.

Grafana/Loki is the operator surface for logs. Postgres is the operator surface for durable facts.

## StorageOpsState

`StorageOpsState` should report storage families, not old logical policy rows:

- `option_quote_ticks`
- `option_trade_ticks`
- `capture_summaries`

For partitioned tick tables, report:

- `partition_count`
- `oldest_partition_start`
- `newest_partition_end`
- `current_partition_ready`
- `future_partition_days`
- `latest_partition_drop_at`
- `total_size_bytes`
- `estimated_live_rows`
- `estimated_dead_rows`

The dashboard must continue to read only `StorageOpsState`; no dashboard path should scan raw tick tables.

## Migration Plan

This is a full cutover, not a backfill project.

All schema mutation happens through Alembic revisions. The operator runs `uv run alembic upgrade head`; they do not run hand-written DDL.

1. Stop scheduler, workers, API write paths, and market recorder.
2. Take a database/volume snapshot or `pg_dump` backup for rollback.
3. Add and run the Alembic cutover revision that renames old tables to tombstones:
   - `option_quote_events_old_<date>`
   - `option_trade_events_old_<date>`
   - `event_log_old_<date>`
4. In the same Alembic revision, create `option_quote_ticks` and `option_trade_ticks`.
5. In the same Alembic revision, create tick partitions from the retention window through 14 future days.
6. Do not backfill raw quote ticks by default. They are ephemeral market telemetry; start clean.
7. Do not backfill old market/signal/opportunity/UOA/discovery events into any replacement event table.
8. Remove `EventLogModel`, `EventRepository`, `event_log` route/service references, and DB-persisting event-bus behavior from active code.
9. Replace useful event-bus persistence with structured logging or domain-table writes under proper ownership.
10. Update ORM models, repositories, retention service, storage ops state, CLI labels, dashboard labels, docs, and skills to the new names.
11. Restart services and validate live writes into the new tick partitions plus logs in the central logging stack.
12. Drop tombstone tables after one good live validation window and backup confirmation.

Rollback while tombstones remain:

1. Stop writers.
2. Drop new tables.
3. Rename tombstones back.
4. Restart services.

If post-cutover tick rows must be preserved during rollback, export only rows newer than the cutover timestamp first.

## Validation

No automated tests unless explicitly requested.

Implementation validation should be live/CLI/runtime focused:

- `uv run spreads config validate --json`
- `uv run alembic upgrade head`
- Partition catalog check through `StorageOpsState` or a shipped `spreads storage partitions --json` style command, not manual SQL.
- `uv run spreads ops storage --env ade-nucbox-k8-plus --json`
- Market recorder writes quote ticks into today's `option_quote_ticks` partition.
- Market recorder writes trade ticks into today's `option_trade_ticks` partition when trades occur.
- No runtime path writes to `event_log` or any replacement event-log table.
- Useful operational events appear as structured logs in the central logging stack.
- `event_log`, `event_log_market_events`, `option_quote_events`, and `option_trade_events` are gone from active CLI/API/dashboard/docs/skills.
- Dashboard storage panels remain responsive and do not run raw counts.
- Off-hours retention drops expired tick partitions.

## Rejected Options

### Keep `event_log` and partition it

Rejected. The table is bloated because it mixed unrelated data classes. Partitioning it would preserve the bad model and force future agents to keep reasoning about `event_log_market_events`.

### Add `ops_events` or `market_event_log`

Rejected. This was a compatibility idea. Raw market data already has proper tables; capture health has summaries; durable operational facts already have owning tables; app logs belong in the logging stack. A new generic event table would recreate the confusion under a new name.

### TimescaleDB now

Rejected for this stage. Timescale is credible for large time-series systems, but the current deployment is plain Postgres 17 and our required behavior is small: time partitions, partition drops, and catalog health. Adding an extension just to avoid a small partition-maintenance service is unnecessary operational weight.

Revisit Timescale only if we later need compression/columnstore, continuous aggregates, or years of retained raw tick analytics.

### Backfill everything

Rejected. The refactor is allowed to break and clean up. Raw market telemetry is short-lived operational data, not permanent research storage. Keep a backup for rollback; do not spend hours copying polluted storage into clean tables.
