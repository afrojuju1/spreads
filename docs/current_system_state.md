# System Architecture

This document is the canonical source of truth for the spreads system's overall architecture and current service boundaries.

It describes the runtime shape of the application as it exists in code today.

If another planning or design document disagrees about current ownership, topology, or boundary placement, this document wins.

Use planning documents for target-state design, subsystem specifications, migration plans, and historical context.

Last updated: 2026-04-17

Related:

- [Fresh Spread Opportunity System Design](./planning/2026-04-11_fresh_spread_system_design.md) for the target opportunity-selection architecture inside the broader system
- [Current-System Options Automation Implementation Approach](./planning/2026-04-15_current_system_options_automation_implementation_approach.md) for the migration path that reuses the current backend
- [Planning Docs](./planning/README.md) for supporting design notes, implementation plans, and historical references

## Top-Level Boundaries

| Boundary | Current owner | Notes |
|---|---|---|
| Operator interfaces | `packages/web`, `packages/api`, `packages/core/cli` | Web and CLI are interface layers. They should not own business logic. |
| Scheduling and control | `packages/core/jobs`, `services/control_plane.py`, `services/runtime_policy.py` | Owns schedules, worker routing, control state, and runtime-policy gates. |
| Market-data capture and recovery | `services/market_recorder.py`, `services/live_recovery/`, `services/collections/capture/` | `market_recorder.py` remains the sole Alpaca option websocket owner in normal runtime. |
| Discovery and collection | `services/scanners/`, `services/collections/`, `services/live_selection.py`, `services/opportunity_scoring.py`, `services/candidate_policy.py` | Owns symbol scanning, cycle orchestration, live ranking, and promotable/monitor state assignment. |
| Canonical opportunity state | `services/signal_state.py`, `services/opportunity_generation.py`, `services/opportunities.py`, `storage/signal_repository.py` | Owns signal state, canonical opportunity rows, and runtime-owned projections derived from collector cycles. |
| Runtime, pipeline, and ops read models | `services/live_runtime.py`, `services/live_collector_health/`, `services/pipelines.py`, `services/ops/` | Owns current session views, health summaries, pipeline projections, and operator CLI payloads. |
| Execution and portfolio state | `services/execution/`, `services/execution_portfolio.py`, `services/session_positions.py`, `services/broker_sync.py`, `services/risk_manager.py`, `services/exit_manager.py` | Owns broker submission, immutable execution ledger, day-local position ownership, reconciliation, and exit behavior. |
| Historical backtest and evaluation | `backtest/`, `services/post_close/`, `services/post_market_analysis.py` | `backtest/` owns the canonical historical evaluation engine and artifacts; post-close services own legacy report rendering and closed-session analysis. |
| Persistence and event transport | Postgres, Redis | Postgres is source of truth. Redis handles queues, leases, and pub/sub fanout. |

## Non-Negotiable Boundary Rules

- `services/market_recorder.py` is the sole Alpaca option websocket owner in normal runtime.
- API routes, web surfaces, and ops views are read models over service-owned state. They are not business-logic owners.
- The discovery path may persist collector-cycle artifacts, but canonical live selection state lives in `signal_states`, `signal_state_transitions`, and `opportunities`.
- Runtime-owned automation opportunities are projections over canonical cycle opportunities, not a separate selection system.
- `execution` is the immutable broker-facing ledger. `session_positions` is the mutable owner of day-local position attribution.
- `broker_sync` reconciles broker reality and health, but it does not take ownership of session attribution away from `session_positions`.

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
        +--> direct CLI entrypoints for ops, backtest, scan, collect, analyze,
             research, scheduler, and job seeding

FastAPI
  |
  +--> Postgres reads and writes
  +--> Redis pub/sub subscription and event publishing
  +--> Alpaca account / trading / market-data REST calls
  +--> serves runtime/session/UOA reads over persisted state

Market Recorder
  |
  +--> owns the Alpaca option websocket connection
  +--> records option quote/trade rows into Postgres

Scheduler
  |
  +--> reads job definitions from Postgres
  +--> enqueues ARQ jobs into Redis

ARQ workers
  |
  +--> read and write Postgres
  +--> consume Redis queues
  +--> publish global events to Redis
  +--> call Alpaca REST and recorder-backed market-data reads
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
           |     packages/web         |                         | scan / collect / ops |
           +----------+-----------+                         | backtest / research  |
                      |                                     +-----------+----------+
                      | HTTP + WS                                      |
                      v                                                 |
           +----------+-------------------------------------------------+----------+
           |                         API (FastAPI)                                  |
           |                        packages/api/app                                    |
           | account | control | sessions | UOA | ws/events                        |
           +----------+-------------------------------+----------------+------------+
                      |                               |                |
                      | SQL reads / writes            | publish / sub  | Alpaca REST
                      v                               v                v
           +----------+-----------+        +----------+-----------+   +------------+
           |       Postgres       |        |         Redis        |   |   Alpaca   |
           | source of truth      |        | queues + leases      |   | trading +  |
           | jobs/events/state    |        | + spreads:events     |   | market data|
           +----+-------------+---+        +----------+-----------+   +------+-----+
                ^             ^                        ^                     ^
                |             |                        |                     |
   writes quote/trade rows    |             +----------+-----------+         |
                |             |             |      Scheduler       |---------+
                |             |             |   `spreads scheduler`| enqueue due jobs
                |             |             +----------+-----------+
                |             |                        |
                |             |                        v
        +-------+--------+    |     +------------------+------------------+
        | market-recorder|    |     |               Redis                 |
        | sole Alpaca    |    |     | arq:queue:runtime | arq:queue:discovery|
        | option WS owner|    |     +--------+-------------------+--------+
        +-------+--------+    |              |                   |
                |             |              v                   v
                +-------------+   +----------+----------+   +----+----------------------+
                                  | RuntimeWorkerSettings |  | DiscoveryWorkerSettings   |
                                  | queue: arq:queue:runtime| | queue: arq:queue:discovery|
                                  +----------+----------+   +----+----------------------+
                                             |                   |
                                             | runs              | runs
                                             |                   |
                                             | broker_sync       | live_collector
                                             | execution_submit  | collections + scanners
                                             | alert_delivery    | live_selection + signal sync
                                             | alert_reconcile   | recorder-backed quote/trade reads
                                             | session_exit_mgr  | UOA + live_action_gate
                                             | post_close        |
                                             | post_market       |
                                             v                   v
                                  +----------+-------------------+----------+
                                  |   External sinks / persisted state      |
                                  | Discord webhook | Postgres | Redis      |
                                  +-----------------------------------------+
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
        | arq:queue:runtime | arq:queue:discovery|
        +--------+-------------------+--------+
                 |                   |
                 v                   v
   +-------------+----------+   +----+----------------------+
   | RuntimeWorkerSettings  |   | DiscoveryWorkerSettings   |
   | queue: arq:queue:runtime|  | queue: arq:queue:discovery|
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

### Discovery -> Signals -> Opportunities

```text
        market calendar + profile
                  |
                  v
        +---------+-----------------------------+
        | live_collector job                    |
        | collections/ + scanners/ + selection |
        +---------+-----------------------------+
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
                  | normalize + project state
                  v
   +--------------+------------------+
   | signal_states                   |
   | signal_state_transitions        |
   | opportunities                   |
   | runtime-owned opportunity views |
   +--------------+------------------+
                  |
                  | runtime reads + ops + backtest
                  v
   +--------------+------------------+
   | live_runtime / pipelines / ops  |
   | audit / backtest                |
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
   | arq:queue:runtime                   |
   | arq:queue:discovery                 |
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

- `RuntimeWorkerSettings`
- `DiscoveryWorkerSettings`

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

### 3. Discovery, Collection, And Opportunity State

The `live_collector` job remains the discovery worker entrypoint, but it is no longer the right architectural owner for all of the logic it triggers.

Today that path is split across:

- `services/collections/` for collector entrypoints, cycle orchestration, capture helpers, and collection-time shared logic
- `services/scanners/` for strategy scanning, builder logic, market-slice assembly, output formatting, and historical evaluation adapters
- `services/live_selection.py` plus `services/opportunity_scoring.py` for live state assignment and scoring
- `services/signal_state.py`, `services/opportunity_generation.py`, and `services/opportunities.py` for canonical signal and opportunity persistence

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

This is the source of canonical live session opportunity state. Entry-automation runtime projections are derived from the same cycle source rather than through a separate parallel selector.

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

### 8. Runtime, Account, And Realtime Read Models

The user-facing read model is assembled from multiple domains.

Current service owners here are:

- `services/live_runtime.py` for session detail and current collector-backed runtime state
- `services/live_collector_health/` for capture, selection, enrichment, and tradeability summaries
- `services/pipelines.py` for pipeline-facing runtime projections
- `services/ops/` for operator CLI read models such as `status`, `trading`, `jobs`, `audit`, and `uoa`

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

- a discovery and collection stack built from `services/collections/`, `services/scanners/`, `services/live_selection.py`, and canonical signal/opportunity persistence
- an execution ledger that records broker interactions immutably
- a session position model that owns day-local trade state
- a broker sync process that reconciles broker reality without taking ownership
- a shared risk and exit layer for both manual and automated actions
- runtime, pipeline, and ops read models assembled by `live_runtime`, `live_collector_health`, `pipelines`, and `ops`
- an API and WebSocket layer that exposes those read models and fans realtime events to the UI
- a scheduler plus two worker lanes over Redis ARQ
- supporting alerts and analysis subsystems around that core

If you want to drill further, the next useful cuts are:

- `execution` vs `session_positions`
- `broker_sync`
- `risk_exit`
- `collections` / `scanners` / `live_selection`
- `web/API`
- `scheduler/worker`
- Postgres table groups and read models
