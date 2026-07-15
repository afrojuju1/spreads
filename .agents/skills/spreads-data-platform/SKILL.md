---
name: spreads-data-platform
description: Market-data and storage operations workflow for Spreads. Use for ClickHouse, Postgres, Redis, capture pressure, capture-session behavior, quote/trade tick quality, retention, rollups, DB sizing, and storage health.
---

# Spreads Data Platform

Use this skill from `/home/ade/Projects/spreads` when the task is about:

- database size or growth
- OPRA-style quote/trade tick storage
- ClickHouse schemas, partitions, TTLs, projections, or rollups
- Postgres domain facts, trading feature snapshots, capture summaries, and operator state persistence
- Redis runtime coordination
- capture-session behavior and capture targets
- storage health and retention
- data quality problems that affect strategy output

If the user asks about live trading health, start with `spreads-ops`. If the user asks to tune strategy thresholds, use `spreads-strategy-lab`.

## Current Storage Model

Read [docs/current_system_state.md](../../../docs/current_system_state.md) for authoritative boundaries.

Working assumptions:

- ClickHouse owns high-volume option quote/trade events and market-data analytics.
- Postgres owns domain facts, job state, trading state, strategy state, trading feature snapshots, market context snapshots, capture summaries, calendar provider facts, provider fetch audit, earnings event consensus, and operator read models.
- Redis is runtime coordination/cache infrastructure, not the durable market-data store. For earnings/event providers, Redis owns only short-lived hot provider responses, backoff, and ad-hoc refresh locks; Postgres owns durable audit and consensus truth.
- `StorageOpsState` is the canonical storage health surface.
- Do not reintroduce Postgres tick partitions, Postgres tick-retention pruning, or dual-write tick stores.

## First Read

Start with shipped health surfaces:

```bash
uv run spreads ops storage --json
uv run spreads ops state --json
uv run spreads jobs --json
docker compose ps
```

For strategy-facing data quality, also inspect the persisted strategy evidence:

```bash
uv run spreads ops strategy-ledger --date YYYY-MM-DD --json
```

Then inspect logs for the affected lane:

```bash
docker compose logs --tail=200 capture-worker workflow-data clickhouse
```

Use direct DB queries only when the CLI cannot answer size, row-rate, partition, or quality questions.

## DB Size And Growth

When asked why storage is large, answer with concrete contributors:

- table sizes
- row counts
- active partitions or parts
- daily ingest rate
- retention policy
- compression ratio when available
- whether size is raw firehose, rollup, domain state, logs, or indexes

Use ClickHouse `system.tables`, `system.parts`, and table-specific summaries for market data. Use Postgres size functions for domain-store growth.

## Capture Triage

The workflow-supervised capture session is the sole normal websocket owner. Multiple live option websocket owners can hit provider connection limits and poison capture.

Check:

- capture-worker and capture-session status
- capture target count
- last event timestamps
- row rate by symbol or option contract
- ClickHouse insert errors
- provider connection-limit errors
- off-hours idle state

Market-closed capture-session idle is healthy unless storage state says capture is degraded.

## Architecture Rules

- Raw tick firehose goes to ClickHouse.
- Postgres stores facts needed by trading, operations, jobs, alerts, and summaries.
- Trading feature facts live in `trading_feature_snapshots`; the company-valuation `feature_snapshots` table is not the trading feature store.
- Shared broad-market context facts live in `market_context_snapshots`; backtests, ops, allocation, and entry quality should replay or link those facts rather than recomputing regime in strategy-local data paths.
- Entry-time market-data SLA evidence lives in strategy-ledger fields and `trading_feature_snapshots.market_data_quality_json`, while ClickHouse remains the raw event source for historical coverage checks.
- Calendar provider rows live in `calendar_events`; derived earnings truth lives in `earnings_event_consensus`; bounded provider fetch summaries live in `provider_fetch_audit`.
- Rollups should be named by the analytical question they answer, not by the ingestion accident that produced them.
- Retention should be explicit in code/config/docs, not hidden in ad hoc cleanup scripts.
- Prefer one owner per data class. Do not keep shadow copies for comfort.
- Schema changes that affect running services need `spreads-live-rollout`.

## Response Shape

Report:

1. database sizes and dominant tables
2. row counts, row rates, partitions, and retention
3. storage/capture health from `StorageOpsState`
4. feature/SLA label counts when strategy data quality is involved
5. whether the issue is expected firehose growth, missing retention, data quality, or runtime failure
6. proposed optimization or architecture change
7. rollout and verification steps
