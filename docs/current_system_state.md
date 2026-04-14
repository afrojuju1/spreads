# Current System State

This document describes the current runtime shape of the application as it exists in code today.

It is meant to be a working architecture map, not a target-state proposal.

Last updated: 2026-04-13

## Runtime Stack

```text
Operator
  |
  +--> Browser
  |     |
  |     +--> Next.js web app
  |             |
  |             +--> HTTP to FastAPI
  |             +--> WebSocket to FastAPI (/ws/events)
  |
  +--> `uv run spreads ...`
        |
        +--> direct CLI entrypoints for ops, replay, scan, collect, analyze,
             research, scheduler, and job seeding

FastAPI
  |
  +--> Postgres reads and writes
  +--> Redis pub/sub subscription and event publishing
  +--> Alpaca account / trading / market-data calls
  +--> internal option stream + combined market-data capture broker

Scheduler
  |
  +--> reads job definitions from Postgres
  +--> enqueues ARQ jobs into Redis

ARQ workers
  |
  +--> read and write Postgres
  +--> consume Redis queues
  +--> publish global events to Redis
  +--> call Alpaca REST and option websocket flows
  +--> deliver alerts to Discord when configured

Postgres = source of truth
Redis = transport, queueing, leases, and pub/sub fanout
```

## High-Level System Diagram

```text
                              +------------------------------+
                              |           Operator           |
                              |  Browser + `uv run spreads`  |
                              +---------------+--------------+
                                              |
                     +------------------------+------------------------+
                     |                                                 |
                     v                                                 v
          +----------+-----------+                         +-----------+----------+
          |   Web UI (Next.js)   |                         |   CLI entrypoints    |
          |     apps/web         |                         | scan / collect / ops |
          +----------+-----------+                         | replay / research    |
                     |                                     +-----------+----------+
                     | HTTP + WS                                      |
                     v                                                 |
          +----------+-------------------------------------------------+----------+
          |                         API (FastAPI)                                  |
          |                        apps/api/app                                    |
          | account | control | sessions | internal capture | UOA | ws/events     |
          +----------+-------------------------------+----------------+------------+
                     |                               |                |
                     | SQL reads / writes            | publish / sub  | Alpaca REST
                     v                               v                v
          +----------+-----------+        +----------+-----------+   +------------+
          |       Postgres       |        |         Redis        |   |   Alpaca   |
          | source of truth      |        | queues + leases      |   | trading +  |
          | jobs/events/state    |        | + spreads:events     |   | market data|
          +----------+-----------+        +----------+-----------+   +------------+
                     ^                               ^                ^
                     |                               |                |
          +----------+-----------+                   |                |
          |      Scheduler       |-------------------+                |
          |   `spreads scheduler`|   enqueue due jobs                 |
          +----------+-----------+                                    |
                     |                                                |
                     v                                                |
   +-----------------+------------------+               +-------------+--------------+
   |      Main workers (2 replicas)     |               | Collector workers (3 repl.)|
   | broker_sync                         |               | live_collector             |
   | execution_submit                    |               | scanner + selection        |
   | alert_delivery / alert_reconcile    |               | quote/trade capture        |
   | session_exit_manager                |               | UOA + signal persistence   |
   | post_close_analysis                 |               | live_action_gate           |
   | post_market_analysis                |               +-------------+--------------+
   +-----------------+------------------+                             |
                     |                                                |
                     +-------------------+----------------------------+
                                         |
                                         v
                              +----------+-----------+
                              |   External sinks     |
                              | Discord webhook      |
                              | published events     |
                              +----------------------+
```

## Service And Queue Diagram

```text
             +---------------------------+
             | Postgres job_definitions  |
             | Postgres job_runs         |
             +-------------+-------------+
                           |
                           v
             +-------------+-------------+
             | scheduler                  |
             | `uv run spreads scheduler` |
             +-------------+-------------+
                           |
                           | enqueue by job_type
                           v
        +------------------+------------------+
        |               Redis                 |
        | arq:queue:fast | arq:queue:collector|
        +--------+-------------------+--------+
                 |                   |
                 v                   v
   +-------------+----------+   +----+----------------------+
   | MainWorkerSettings     |   | CollectorWorkerSettings   |
   | queue: arq:queue:fast  |   | queue: arq:queue:collector|
   +-------------+----------+   +----+----------------------+
                 |                   |
                 | runs              | runs
                 |                   |
                 | broker_sync       | live_collector
                 | execution_submit  |
                 | alert_delivery    |
                 | alert_reconcile   |
                 | session_exit_mgr  |
                 | post_close        |
                 | post_market       |
                 v                   v
        +--------+-------------------+--------+
        |            Postgres                 |
        | state tables + event log + outputs  |
        +-------------------------------------+
```

## Domain Slice Diagrams

### Collector -> Signals -> Opportunities

```text
        market calendar + profile
                  |
                  v
        +---------+----------+
        |   live_collector   |
        | scanner + ranking  |
        +---------+----------+
                  |
                  | cycle result
                  v
   +--------------+------------------+
   | collector_cycles                |
   | collector_cycle_candidates      |
   | collector_cycle_events          |
   +--------------+------------------+
                  |
                  | quote/trade context + UOA
                  v
   +--------------+------------------+
   | option_quote_events             |
   | option_trade_events             |
   | uoa summaries in job results    |
   +--------------+------------------+
                  |
                  | normalize state
                  v
   +--------------+------------------+
   | signal_states                   |
   | signal_state_transitions        |
   | opportunities                   |
   +--------------+------------------+
                  |
                  | operator reads + auto-open input
                  v
   +--------------+------------------+
   | sessions / ops / audit / replay |
   +---------------------------------+
```

### Execution -> Session Positions -> Broker Sync

```text
 manual open / auto open / manual close / exit_manager close
                           |
                           v
              +------------+-------------+
              | execution service        |
              | submit_*_execution(...)  |
              +------------+-------------+
                           |
                           | immutable broker ledger
                           v
              +------------+-------------+
              | execution_attempts       |
              | execution_orders         |
              | execution_fills          |
              +------------+-------------+
                           |
                           | derive session ownership
                           v
              +------------+-------------+
              | session_positions        |
              | session_position_closes  |
              +------------+-------------+
                           ^
                           |
                           | refresh / reconcile / marks
                           |
              +------------+-------------+
              | broker_sync              |
              | account_snapshots        |
              | broker_sync_state        |
              +------------+-------------+
                           |
                           v
                        Alpaca

Rule:
- execution = immutable broker interaction log
- session_positions = mutable session/day ownership model
- broker_sync updates state and mismatches, but does not take ownership away
```

### Scheduler -> Queues -> Workers -> Event Fanout

```text
      job_definitions
           |
           v
   +-------+--------+
   |   scheduler    |
   +-------+--------+
           |
           | create job_runs + enqueue
           v
   +-------+------------------------------+
   | Redis                               |
   | arq:queue:fast                      |
   | arq:queue:collector                 |
   | spreads:events                      |
   +-------+------------------------------+
           |                      ^
           |                      |
           v                      | publish global events
   +-------+--------+    +--------+---------+
   | main workers   |    | collector workers|
   +-------+--------+    +--------+---------+
           |                      |
           +----------+-----------+
                      |
                      v
               Postgres writes
                      |
                      v
                 API WebSocket
                      |
                      v
                    Web UI
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

The web app is a narrow Next.js runtime console. It does not talk directly to Postgres or Redis.

It uses:

- a Next route proxy at `/api/backend/*` for normal HTTP calls into FastAPI
- direct browser WebSocket connections to FastAPI for global realtime events

FastAPI is the main application surface. It serves:

- account overview
- control state and mode changes
- sessions and session detail
- execution open, close, and refresh actions
- internal option market-data capture and stream health
- internal UOA state reads
- global realtime events over `/ws/events`

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

Current worker topology is:

- `MainWorkerSettings`
- `CollectorWorkerSettings`

Current main job types are:

- `live_collector`
- `broker_sync`
- `execution_submit`
- `alert_delivery`
- `alert_reconcile`
- `session_exit_manager`
- `post_close_analysis`
- `post_market_analysis`

Redis is transport and event fanout. Postgres remains the source of truth for job state.

### 3. Live Collector

The live collector is the intraday scanning loop.

At a high level it:

1. scans the configured universe
2. ranks live candidates into canonical `promotable` and `monitor` states
3. compares the new cycle against prior selection memory
4. captures quote and trade data for the chosen option legs
5. computes and persists UOA, signal-state, and opportunity data
6. applies `live_action_gate` behavior before alerts or auto-execution
7. optionally auto-submits an open execution through the normal execution service
8. dispatches alerts when the gate allows it

Its persistent outputs live mainly in:

- `collector_cycles`
- `collector_cycle_candidates`
- `collector_cycle_events`
- `option_quote_events`
- `option_trade_events`
- `signal_states`
- `signal_state_transitions`
- `opportunities`

This is the source of live session opportunity state.

For `0dte`, degraded quote capture can now persist the cycle and diagnostics while still blocking alerts and auto-execution. That block is surfaced as `live_action_gate`.

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

- session and execution updates
- live collector degradation notices
- execution status changes
- session-linked alert notices
- session-linked job notices
- broker sync health events

### 9. Alerts And Analysis

These are adjacent subsystems, not part of the core trade ownership model.

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
  option_trade_events

signals:
  signal_states
  signal_state_transitions
  opportunities

risk:
  risk_decisions

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

alerts:
  alert_events
  alert_state

analysis:
  post_market_analysis_runs
  plus read-only derived summaries built from collector and execution history

control:
  control_state
  operator_actions
  policy_rollouts

events:
  event_log
```

## Current System Summary

The current application is best understood as one narrow runtime console sitting on top of one backend runtime with several cooperating subsystems:

- a live collector that discovers intraday opportunities and persists promotable and monitor state
- an execution ledger that records broker interactions immutably
- a session position model that owns day-local trade state
- a broker sync process that reconciles broker reality without taking ownership
- a shared risk and exit layer for both manual and automated actions
- an API and WebSocket layer that assembles read models and fans realtime events to the UI
- a scheduler plus two worker lanes over Redis ARQ
- supporting alerts and analysis subsystems around that core

If you want to drill further, the next useful cuts are:

- `execution` vs `session_positions`
- `broker_sync`
- `risk_exit`
- `live_collector`
- `web/API`
- `scheduler/worker`
- Postgres table groups and read models
