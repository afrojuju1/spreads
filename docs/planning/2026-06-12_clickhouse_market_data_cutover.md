# ClickHouse Market Data Cutover

Status: implemented target architecture for `spr-djq`
Date: 2026-06-12

## Decision

Raw option quote/trade ticks belong in ClickHouse, not Postgres. Postgres remains the source of truth for trading domain state, ops state, jobs, execution, broker sync, capture targets, and capture summaries. Redis remains queue/lease/pub-sub transport.

This is a clean replacement of the 2026-06-05 Postgres partition plan. There is no compatibility layer, retention CLI, partition helper, or Postgres ORM model for raw market ticks after the cutover.

## Runtime Shape

```text
market_recorder
  |
  +--> Postgres capture_targets, leases
  |
  +--> Alpaca option websocket
  |
  +--> ClickHouse option_quote_ticks / option_trade_ticks
  |
  +--> ClickHouse option_quote_snapshots_1s / option_quote_snapshots_1m
  |
  +--> Postgres capture_summaries
```

## Storage Contract

ClickHouse owns high-volume market data:

- `option_quote_ticks`: raw quote firehose, short TTL.
- `option_trade_ticks`: raw option trades, medium TTL.
- `option_quote_snapshots_1s`: compact latest quote per contract per second.
- `option_quote_snapshots_1m`: compact latest quote per contract per minute for long-horizon operator/analytics use.

Postgres owns normalized operational facts:

- `capture_targets`
- `capture_summaries`
- trading strategy, candidate, signal, decision, admission, intent, attempt, order, fill, position, close, job, alert, control, broker, and account state.

## Operator Surfaces

`spreads ops storage` reports ClickHouse market-data table readiness, parts, rows, bytes, TTL ownership, plus latest Postgres capture-summary health.

Removed active surfaces:

- `spreads maintenance retention prune`
- `ops/retention_prune.sh`
- Postgres tick partition health and future partition coverage

## Deployment Contract

Compose includes a first-class `clickhouse` service with its own named volume and healthcheck. App containers receive:

```text
SPREADS_CLICKHOUSE_URL=http://spreads:<password>@clickhouse:8123/spreads
```

Host-side operator CLI uses:

```text
SPREADS_CLICKHOUSE_URL=http://spreads:<password>@<bind-host>:58123/spreads
```

## Rationale

OPRA-style quote data is append-heavy, time-oriented, and frequently queried by symbol/time windows or aggregate rollups. ClickHouse gives columnar compression, MergeTree ordering, table TTL, and cheap aggregate scans without making the operational Postgres database carry raw market firehose storage.
