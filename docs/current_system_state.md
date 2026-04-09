# Current System State

This document describes the current runtime shape of the application as it exists in code today.

It is meant to be a working architecture map, not a target-state proposal.

## Runtime Stack

```text
Browser
  |
  v
Next.js web app
  |
  +--> HTTP proxy to FastAPI
  |
  +--> direct WebSocket connection to FastAPI for live events

FastAPI
  |
  +--> Postgres
  +--> Redis pub/sub
  +--> Alpaca trading/account APIs
  +--> internal quote-capture broker

Scheduler
  |
  +--> Redis queue

ARQ workers
  |
  +--> Postgres
  +--> Redis
  +--> Alpaca trading/account APIs
  +--> FastAPI internal quote-capture endpoint
  +--> Discord webhook

Postgres
Redis
```

## High-Level System Diagram

```text
                                 +----------------------+
                                 |     Operator UI      |
                                 |  Browser + Next.js   |
                                 +----------+-----------+
                                            |
                               HTTP via /api/backend/*
                                            |
                                            v
+----------------------+        +-----------+-----------+        +----------------------+
|      Scheduler       |------->|        FastAPI        |<------>|  Browser WS client   |
|  due jobs -> Redis   |        |     apps/api/main     |        |   /ws/events etc.    |
+----------+-----------+        +-----------+-----------+        +----------------------+
           |                                |    \
           |                                |     \ internal quote-capture endpoint
           v                                |      \
+----------+-----------+                    |       v
|       Redis          |<-------------------+   +----------------------+
|   ARQ queue + pubsub |                        | Quote capture broker  |
+----------+-----------+                        |  holds Alpaca WS      |
           |                                    +----------+-----------+
           v                                               |
+----------+-----------+                                   |
|       Workers        |-----------------------------------+
| live_collector       |
| broker_sync          |
| session_exit_manager |
| generator            |
| post_close           |
| post_market          |
+----------+-----------+                      |
           |                                  |
           v                                  v
+----------+----------------------------------+-----------+
|                    Postgres                              |
| collector tables | execution tables | broker tables     |
| jobs | alerts | generator_jobs | post_market tables     |
+----------------------------------------------------------+

External edges:
- API/Workers <-> Alpaca trading + account REST
- Quote capture broker <-> Alpaca option quote websocket
- Workers -> Discord webhook
```

## Trading Core Diagram

```text
                    +-------------------+
                    |   live_collector  |
                    | scan + board rank |
                    +---------+---------+
                              |
                              | best candidate
                              v
                    +---------+---------+
                    |     risk_exit     |
                    |  risk_manager     |
                    |  exit_manager     |
                    +----+---------+----+
                         |         |
            open submit  |         | close submit
     manual or automated |         | automated or manual
                         v         v
                 +-------+---------+-------+
                 |       execution         |
                 | immutable broker ledger |
                 | attempts / orders / fills
                 +------------+------------+
                              |
                              | derive/update state
                              v
                 +------------+------------+
                 |    session_positions    |
                 | day-local ownership     |
                 | marks / pnl / status    |
                 | reconciliation flags    |
                 +------------+------------+
                              ^
                              |
                              | refresh / fills / reconcile
                 +------------+------------+
                 |      broker_sync        |
                 | poll Alpaca first       |
                 | account_snapshots       |
                 | broker_sync_state       |
                 +------------+------------+
                              |
                              v
                           Alpaca
```

## Core Constraint

```text
Manual open  ------\
Auto open ---------> submit_live_session_execution(...) ----> execution
Manual close ------\
Auto close --------> submit_session_position_close(...) ----> execution

No second workflow.
session_positions remains the owner of day/session attribution.
Alpaca broker positions are used for reconciliation, not session truth.
```

## Main Sections

### 1. Web And API

The web app is a Next.js operator dashboard. It does not talk directly to Postgres or Redis.

It uses:

- a Next route proxy at `/api/backend/*` for normal HTTP calls into FastAPI
- direct browser WebSocket connections to FastAPI for global realtime events and generator job updates

FastAPI is the main application surface. It serves:

- account overview
- live collector snapshots, cycles, and events
- sessions and session detail
- execution open, close, and refresh actions
- alerts
- jobs and job health
- generator job creation and retrieval
- post-close and post-market analysis reads

FastAPI is also the mutation boundary for manual trading actions.

### 2. Jobs, Scheduler, And Workers

The background runtime is split into a scheduler and ARQ workers.

The scheduler:

- reads `job_definitions` from Postgres
- determines which jobs are due
- creates `job_runs`
- enqueues work into Redis
- uses leases to avoid duplicate singleton scheduling

Workers:

- consume ARQ jobs from Redis
- update `job_runs` and leases in Postgres
- publish runtime events to Redis pub/sub
- execute the actual business jobs

Current main job types are:

- `live_collector`
- `broker_sync`
- `session_exit_manager`
- `post_close_analysis`
- `post_market_analysis`
- `generator`

Redis is transport and event fanout. Postgres remains the source of truth for job state.

### 3. Live Collector

The live collector is the intraday scanning loop.

At a high level it:

1. scans the configured universe
2. builds board and watchlist candidates
3. compares the new board against the previous cycle
4. persists the new cycle, candidates, and events
5. optionally auto-submits an open execution through the normal execution service
6. dispatches alerts
7. captures option quote events for selected legs

Its persistent outputs live mainly in:

- `collector_cycles`
- `collector_cycle_candidates`
- `collector_cycle_events`
- `option_quote_events`

This is the source of live session opportunity state.

### 4. Execution Domain

`execution` is the immutable broker-facing ledger.

Its main tables are:

- `execution_attempts`
- `execution_orders`
- `execution_fills`

This domain records:

- what the app tried to submit
- what Alpaca accepted or rejected
- which broker order ids exist
- which fills occurred

It is the broker-order history, not the mutable session position state.

All opens and closes, manual or automated, flow through this domain first.

### 5. Session Positions Domain

`session_positions` is the mutable day-local state derived from executions.

Its main tables are:

- `session_positions`
- `session_position_closes`

This domain owns:

- session/day attribution
- current open or partial-close state
- realized and unrealized PnL
- latest close mark and mark source
- snapshotted exit policy and risk policy
- reconciliation flags and notes

It is intentionally separate from the immutable execution ledger.

This is the system of record for "which session owns this trade".

### 6. Broker Sync Domain

`broker_sync` is poll-first and broker-global.

Its main tables are:

- `account_snapshots`
- `broker_sync_state`

At a high level it:

1. snapshots account balances and broker positions
2. ingests recent Alpaca fill activities
3. refreshes non-terminal execution attempts
4. reconciles local open `session_positions` against broker inventory
5. publishes sync health events

Important behavior:

- broker data is authoritative for order, fill, and account state
- local `session_positions` is authoritative for session ownership
- reconciliation updates status, fills, close marks, and mismatch flags only
- it never reassigns session ownership from broker positions

### 7. Risk Exit Domain

The decision layer is split into `risk_manager` and `exit_manager`.

`risk_manager` handles gating before submission:

- environment gate
- kill switch
- max open positions
- contract limits
- notional and max loss limits
- duplicate underlying and strategy limits
- stale quote protection

It is used by both manual and automated submit paths.

`exit_manager` handles automated close decisions for already-open `session_positions`.

It:

- evaluates position-level exit policy snapshots
- checks the latest marks
- avoids duplicate close attempts
- submits closes through `submit_session_position_close(...)`

Forced end-of-day exits are treated as just another exit reason.

### 8. Account And Realtime Read Model

The user-facing read model is assembled from multiple domains.

Examples:

- account overview can use live Alpaca data and attach broker sync health
- session detail joins collector state, execution ledger, positions, alerts, job runs, and analysis
- execution portfolio computes current marks and PnL for open positions

Realtime updates are pushed through Redis pub/sub and exposed by FastAPI WebSockets.

The UI uses this for:

- generator updates
- job run updates
- live collector degradation notices
- execution status changes
- alert feed notices
- broker sync health events

### 9. Generator, Alerts, And Analysis

These are adjacent subsystems, not part of the core trade ownership model.

Generator:

- runs one-off symbol idea generation jobs
- stores job state in `generator_jobs`
- exposes both HTTP and dedicated realtime updates

Alerts:

- create persisted `alert_events`
- maintain `alert_state` for dedupe
- optionally deliver to Discord

Post-close and post-market analysis:

- read stored session and quote history
- compute summaries, diagnostics, and recommendations
- persist post-market analysis runs in `post_market_analysis_runs`

### 10. Persistence Layout

At a high level Postgres currently holds these logical groups:

```text
collector:
  collector_cycles
  collector_cycle_candidates
  collector_cycle_events
  option_quote_events

execution:
  execution_attempts
  execution_orders
  execution_fills

session_positions:
  session_positions
  session_position_closes

broker_sync:
  account_snapshots
  broker_sync_state

jobs:
  job_definitions
  job_runs
  job_leases
  generator_jobs

alerts:
  alert_events
  alert_state

analysis:
  post_market_analysis_runs
  plus read-only derived summaries built from collector and execution history
```

## Current System Summary

The current application is best understood as one operator dashboard sitting on top of one backend runtime with several cooperating subsystems:

- a live collector that discovers intraday opportunities
- an execution ledger that records broker interactions immutably
- a session position model that owns day-local trade state
- a broker sync process that reconciles broker reality without taking ownership
- a shared risk and exit layer for both manual and automated actions
- supporting generator, alerts, and analysis subsystems around that core

If you want to drill further, the next useful cuts are:

- `execution` vs `session_positions`
- `broker_sync`
- `risk_exit`
- `live_collector`
- `web/API`
- `scheduler/worker`
- Postgres table groups and read models
