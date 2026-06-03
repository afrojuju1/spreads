# System Architecture

This document is the canonical source of truth for the spreads system's overall architecture and current service boundaries.

It describes the runtime shape of the application as it exists in code today.

If another planning or design document disagrees about current ownership, topology, or boundary placement, this document wins.

Use planning documents for target-state design, subsystem specifications, migration plans, and historical context.

Last updated: 2026-06-03

Related:

- [Fresh Spread Opportunity System Design](./planning/2026-04-11_fresh_spread_system_design.md) for the target opportunity-selection architecture inside the broader system
- [Current-System Options Automation Implementation Approach](./planning/2026-04-15_current_system_options_automation_implementation_approach.md) for the migration path that reuses the current backend
- [Planning Docs](./planning/README.md) for supporting design notes, implementation plans, and historical references

## Top-Level Boundaries

| Boundary | Current owner | Notes |
|---|---|---|
| Operator interfaces | `packages/web`, `packages/api`, `packages/core/cli` | Web and CLI are interface layers. They should not own business logic. |
| Scheduling and control | `packages/core/jobs`, `services/control_plane.py`, `services/runtime_policy.py` | Owns schedules, worker routing, control state, and runtime-policy gates. |
| Dynamic symbol feeds | `services/symbol_feeds.py`, `packages/core/jobs` | Owns materialized shared underlying lists for feed-driven consumers such as UOA. |
| Market-data capture and recovery | `services/market_recorder.py`, `services/discovery_recovery/`, `services/discovery_runs/capture/` | `market_recorder.py` remains the sole Alpaca option websocket owner in normal runtime. |
| Discovery and collection | `services/scanners/`, `services/discovery_runs/`, `services/live_selection.py`, `services/opportunity_scoring.py`, `services/candidate_policy.py` | Owns symbol scanning, cycle orchestration, live ranking, and promotable/monitor state assignment. |
| Canonical opportunity state | `services/signal_state.py`, `services/opportunity_generation.py`, `services/opportunities.py`, `storage/signal_repository.py` | Owns signal state, canonical opportunity rows, and runtime-owned projections derived from discovery run cycles. |
| Runtime, automation, discovery-session, pipeline-compat, and ops read models | `services/automation_runtimes.py`, `services/discovery_sessions.py`, `services/live_runtime.py`, `services/discovery_run_health/`, `services/pipelines.py`, `services/ops/` | Owns owner-plane automation runtime views, discovery-run-owned discovery-session views, compatibility pipeline projections, and operator CLI payloads. |
| Execution and portfolio state | `services/execution/`, `services/execution_intents/`, `services/execution_portfolio.py`, `services/session_positions.py`, `services/broker_sync.py`, `services/risk_manager.py`, `services/exit_manager.py` | Owns execution intents, broker/runtime handoff, immutable execution ledger, day-local position ownership, reconciliation, and exit behavior. `alpaca_direct` is the only supported execution runtime for equity, option, and Alpaca order-payload submission. |
| Historical backtest and evaluation | `backtest/` | `backtest/` owns the canonical historical evaluation engine and artifacts. |
| Persistence and event transport | Postgres, Redis | Postgres is source of truth. Redis handles queues, leases, and pub/sub fanout. |

## Non-Negotiable Boundary Rules

- `services/market_recorder.py` is the sole Alpaca option websocket owner in normal runtime.
- API routes, web surfaces, and ops views are read models over service-owned state. They are not business-logic owners.
- The discovery path may persist discovery run-cycle artifacts, but canonical live selection state lives in `signal_states`, `signal_state_transitions`, and `opportunities`.
- Runtime-owned automation opportunities are projections over canonical cycle opportunities, not a separate selection system.
- Bot plus automation is the primary operator/product ownership plane. Discovery sessions remain discovery-run-owned diagnostic surfaces, and `pipeline_id` is discovery lineage plus compatibility identity rather than the primary runtime owner.
- Config-backed symbol ownership is `bot -> automation -> universe -> symbols[]`. Bots own limits, runtime flags, and automation references; they do not directly own persisted symbol lists.
- `EntryRuntime.symbols` and `ManagementRuntime.symbols` are derived from the resolved automation universe. Discovery-run scope may union symbols across active entry automations when building a scanner scope.
- Scanner and collection CLI flags such as `--symbols` and `--symbols-file` are ad hoc operator and research overrides, not the persisted bot or automation ownership model.
- Shared dynamic symbol lists are owned separately by declared `symbol_feed` jobs and `services/symbol_feeds.py`. They materialize bounded underlying lists for consumers such as UOA, but they do not own bots, opportunities, or execution. The live `uoa_weekly` feed currently applies a minimum daily-volume floor and excludes leveraged or inverse ETFs before handing symbols to UOA.
- `execution_intents` is the strategy/control-plane handoff boundary. It chooses the execution runtime before broker submission. Runtime `alpaca_direct` is the current Python-native Alpaca adapter path for equity, single-leg option, and Alpaca order-payload submission. `services/execution/runtimes.py` owns the runtime capability declaration, and `spreads execution-runtimes`, `GET /executions/runtimes`, `spreads trading`, and the runtime catalog expose the active adapter surface.
- `execution` is the immutable broker-facing ledger. `session_positions` is the mutable owner of day-local position attribution.
- `broker_sync` reconciles broker reality and health, but it does not take ownership of session attribution away from `session_positions`.
- Active operator-state refactor `spr-zuy` replaces the fragmented `live-doctor`, `status`, `trading`, and `finviz-ledger` product surfaces with `TradingOpsState` and `StorageOpsState`. During that refactor, prefer full removal of the old active surfaces over compatibility wrappers.

## Domain Ownership Map

This map defines domain vocabulary and source-of-truth ownership. Agents should update this document when ownership changes instead of copying the map into `AGENTS.md` or repo-local skills.

| Domain object | Meaning | Current source of truth / owner | Must not own |
|---|---|---|---|
| Signal | A normalized market/setup observation that may become an opportunity. | `services/signal_state.py`, `services/opportunity_generation.py`, `storage/signal_repository.py`; persisted in `signal_states` and `signal_state_transitions`. | Web/API route handlers, alerts, or execution code. |
| Decision | A strategy or lifecycle choice made from a signal/opportunity, including selected, skipped, blocked, or no-entry reasons. | Strategy and lifecycle services that create canonical opportunities or execution-intent payloads; currently split across opportunity generation, Finviz direct logic, and lifecycle projection code. Active cleanup should move decision vocabulary toward explicit lifecycle/state objects. | Alert delivery, dashboard-only read models, or broker-sync reconciliation. |
| Admission | Account/risk/policy answer to "can this approved idea be carried now?" | `services/account_capacity.py`, `services/risk_manager.py`, `services/execution/`, and execution-intent admission payloads. | `services/account_state.py`; account snapshots are broker/account reads, not admission policy owners. |
| Intent | The control-plane handoff requesting an open or close action. | `services/execution_intents/` and `execution_intents`. | Broker ledger tables, alerts, or frontend state. |
| Attempt | One broker-facing submission/refresh/cancel lifecycle for an intent. | `services/execution/` and `execution_attempts`. | `session_positions`; positions are projections/ownership, not submission history. |
| Order | Broker order facts attached to an attempt. | `execution_orders`, written/refreshed by `services/execution/` and broker sync. | Opportunity, alert, or dashboard services. |
| Fill | Broker fill facts attached to an order/attempt. | `execution_fills`, written/refreshed by `services/execution/` and broker sync. | Position ownership logic except as an input to projection. |
| Position | Day/session-local ownership and PnL projection for a trade. | `services/session_positions.py`, `services/execution_portfolio.py`, and `session_positions`. | Broker positions alone; broker inventory is reconciliation input, not session ownership truth. |
| Close | A decision, intent, attempt, and fill path that reduces or exits a position. | `services/exit_manager.py`, `services/session_positions.py`, `services/execution_intents/`, and `services/execution/`; close facts persist in `session_position_closes` plus execution tables. | Separate close-only workflows that bypass execution intents and attempts. |
| Reconciliation | Matching local attempts/positions against broker reality and marking mismatches. | `services/broker_sync.py`, `account_snapshots`, and `broker_sync_state`; position mismatch flags live on position projections. | Discovery, alerts, or account snapshots as independent truth. |
| Broker sync | Poll-first broker/account health and fact ingestion. | `services/broker_sync.py`; operator normalization in `services/ops/broker_sync.py`. | Session attribution, risk policy, or trading decision ownership. |
| Trading ops state | Operator-facing live trading control-room state: market, control, scheduler/workers, runtime, broker sync, flows, decisions, intents, attempts, positions, exits, risk, and attention. | Target owner under `spr-zuy`: `services/ops/trading_ops_state.py` and `/internal/trading-ops/state`. Current state is fragmented across `services/ops/live_doctor.py`, `services/ops/trading.py`, `services/ops/system.py`, and `services/ops/finviz.py`. | Frontend stitching, API-only aggregators, live Alpaca account calls during normal dashboard render, or vendor-ledger product surfaces. |
| Storage ops state | Operator-facing storage/retention health: table sizes, stats, latest retention result, schedule, and maintenance warnings. | Target owner under `spr-zuy`: `services/ops/storage_ops_state.py` and `/internal/storage-ops/state`. Current retention state lives in `services/retention.py` and `/internal/ops/retention`. | Live trading state, raw quote/event scans on default dashboard load, or dashboard-only calculations. |

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
        +--> direct CLI entrypoints for ops, backtest, scan, collect,
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
  +--> reads declared job specs from YAML/config
  +--> enqueues ARQ jobs into Redis

ARQ workers
  |
  +--> read and write Postgres
  +--> consume Redis queues
  +--> publish global events to Redis
  +--> call Alpaca REST and recorder-backed market-data reads
  +--> manage alert outbox delivery and Discord webhook sends when configured

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
                                             | broker_sync       | discovery_run
                                             | discovery_recovery| collections + scanners
                                             | execution_submit  | live_selection + signal sync
                                             | alert_delivery    | recorder-backed quote/trade reads
                                             | alert_reconcile   | UOA + live_action_gate
                                             | options_automation_entry
                                             | options_automation_execute
                                             | position_exit_manager
                                             v                   v
                                  +----------+-------------------+----------+
                                  |   External sinks / persisted state      |
                                  | Discord webhook | Postgres | Redis      |
                                  +-----------------------------------------+
```

## Service And Queue Diagram

```text
             +---------------------------+
             | YAML declared jobs        |
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
                 | broker_sync       | discovery_run
                 | discovery_recovery|
                 | execution_submit  |
                 | alert_delivery    |
                 | alert_reconcile   |
                 | options_automation_entry
                 | options_automation_execute
                 | position_exit_manager
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
        | discovery_run job                    |
        | collections/ + scanners/ + selection |
        +---------+-----------------------------+
                  |
                  | cycle result
                  v
   +--------------+------------------+
   | discovery_runs                |
   | discovery_run_candidates      |
   | discovery_run_events          |
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
              | execution_intents        |
              | runtime handoff boundary |
              +------------+-------------+
                           |
                           v
              +------------+-------------+
              | execution service        |
              | submit_*_execution(...)  |
              | alpaca_direct adapter    |
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
- execution_intents = strategy/control-plane runtime handoff
- execution = immutable broker interaction log
- session_positions = mutable session/day ownership model
- broker_sync updates state and mismatches, but does not take ownership away
```

### Scheduler -> Queues -> Workers -> Event Fanout

```text
      declared job YAML
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
   | arq:queue:valuation                 |
   | spreads:events                      |
   +-------+------------------------------+
           |                      ^
           |                      |
           v                      | publish global events
   +-------+--------+    +--------+---------+    +--------+---------+
   | main workers   |    | discovery workers|    | valuation workers|
   +-------+--------+    +--------+---------+    +--------+---------+
           |                      |                       |
           +----------+-----------+-----------+-----------+
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
Auto open ---------> execution_intents / submit_live_session_execution(...)
Manual close ------\
Auto close --------> execution_intents / submit_session_position_close(...)
                                      |
                                      v
                         execution runtime selection
                         | alpaca_direct -> current Alpaca submit path
                                      |
                                      v
                                  execution ledger

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

- reads declared job specs from YAML/config
- determines which jobs are due
- creates `job_runs`
- enqueues work into Redis
- uses leases to avoid duplicate singleton scheduling
- supports queue-domain-scoped mode for future service-specific deployments, but valuation currently runs without its own scheduler by default

Workers:

- consume ARQ jobs from Redis
- update `job_runs` and leases in Postgres
- publish runtime events to Redis pub/sub
- execute the actual business jobs

Current worker topology is:

- `RuntimeWorkerSettings`
- `DiscoveryWorkerSettings`
- `ValuationWorkerSettings`
- `ResearchWorkerSettings` on the NUC host for TradingAgents jobs

Current main job types are:

- `discovery_run`
- `broker_sync`
- `discovery_recovery`
- `execution_submit`
- `alert_delivery`
- `alert_reconcile`
- `options_automation_entry`
- `options_automation_execute`
- `position_exit_manager`
- `company_valuation_bootstrap`
- `company_valuation_screen_materialize`
- `company_valuation_resolve_unresolved`
- `tradingagents_scan`

Management automations are still config-owned runtime concepts, but they are evaluated inside `position_exit_manager` rather than through a separate `options_automation_management` job type.
Recurring maintenance jobs now run only through declared job definitions; `discovery_recovery` no longer inlines `broker_sync` or `position_exit_manager`.
The TradingAgents research lane is intentionally host-run on the NUC so it can use `/home/ade/Projects/TradingAgents/.venv`; the compose deployment scales the container `worker-research` service to `0` by default.

Redis is transport and event fanout. Postgres remains the source of truth for job state.

### 3. Discovery, Collection, And Opportunity State

The `discovery_run` job remains the discovery worker entrypoint, but it is no longer the right architectural owner for all of the logic it triggers.

Today that path is split across:

- `services/discovery_runs/` for discovery run entrypoints, cycle orchestration, capture helpers, and collection-time shared logic
- `services/scanners/` for strategy scanning, builder logic, market-slice assembly, output formatting, and historical evaluation adapters
- `services/live_selection.py` plus `services/opportunity_scoring.py` for live state assignment and scoring
- `services/signal_state.py`, `services/opportunity_generation.py`, and `services/opportunities.py` for canonical signal and opportunity persistence

For config-backed options automation, discovery symbol scope is resolved through:

1. bot config for limits, runtime flags, and referenced automations
2. automation config for `strategy_config` and `universe`
3. universe config for concrete `symbols[]`
4. runtime scope assembly that unions symbols across active entry automations when a shared discovery-run scope is built

This means persisted bots do not currently carry direct symbol lists. Direct symbol lists exist today only as scanner and collection CLI overrides such as `--symbols` and `--symbols-file`.

Shared dynamic symbol feeds are a separate, account-agnostic input plane. They materialize reusable underlying lists from feed recipes and are consumed by feed-driven lanes such as the dedicated UOA discovery run, but they do not replace automation universes as the primary static ownership model.

At a high level it:

1. scans the configured universe
2. ranks live candidates into canonical `promotable` and `monitor` states
3. compares the new cycle against prior selection memory
4. captures quote and trade data for the chosen option legs
5. computes and persists UOA, signal-state, and opportunity data
6. applies `live_action_gate` behavior before alerts or auto-execution
7. optionally auto-submits an open execution through the normal execution service
8. plans persisted alerts and asynchronous delivery when the gate allows it

Its persistent outputs live mainly in:

- `discovery_runs`
- `discovery_run_candidates`
- `discovery_run_events`
- `option_quote_events`
- `option_trade_events`
- `signal_states`
- `signal_state_transitions`
- `opportunities`

This is the source of canonical live session opportunity state. Entry-automation runtime projections are derived from the same cycle source rather than through a separate parallel selector.

For `0dte`, degraded quote capture can now persist the cycle and diagnostics while still blocking alerts and auto-execution. That block is surfaced as `live_action_gate`.

### 4. Execution Domain

`execution_intents` is the strategy/control-plane execution handoff. It records the selected runtime, claim/dispatch lifecycle, and runtime-specific payloads before broker submission.

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

All opens and closes, manual or automated, flow through the intent/ledger path first. The default and supported runtime is `alpaca_direct`; declarations for any other execution runtime fail validation. The Python-native adapter centralizes Alpaca submit, nested order refresh, submit-unknown reconciliation support, and cancel requests while lifecycle state remains in the Spreads execution ledger.

On the `ade-nucbox-k8-plus` deployment, the active Spreads runtime does not mount any external bridge binary into API or worker containers. Generated compose and env output should expose only the active `alpaca_direct` adapter.

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

- `services/automation_runtimes.py` for bot-and-automation-oriented runtime summaries and detail views
- `services/discovery_sessions.py` for discovery-run-owned discovery-session detail and compatibility reads
- `services/live_runtime.py` for session detail and current discovery run-backed runtime state
- `services/discovery_run_health/` for capture, selection, enrichment, and tradeability summaries
- `services/pipelines.py` for pipeline-facing runtime projections
- `services/ops/` for operator CLI read models such as `status`, `trading`, `jobs`, `audit`, and `uoa`

Active cleanup `spr-zuy` is replacing the fragmented operator health/read-model surfaces with `TradingOpsState` for live trading operations and `StorageOpsState` for storage and retention health. While that work is in progress, treat the existing `status`, `trading`, `live-doctor`, and `finviz-ledger` style surfaces as current implementation details to be removed from active product surfaces, not as names to extend.

Examples:

- account overview can use live Alpaca data and attach broker sync health
- session detail joins discovery run state, execution ledger, positions, alerts, job runs, and analysis
- execution portfolio computes current marks and PnL for open positions

Realtime updates are pushed through Redis pub/sub and exposed by FastAPI WebSockets.

The UI uses this for:

- session and execution updates
- discovery run degradation notices
- execution status changes
- session-linked alert notices
- session-linked job notices
- broker sync health events

### 9. Alerts And Analysis

These are adjacent subsystems, not part of the core trade ownership model.

Alerts:

- persist score-anchor and delivery rows in `alert_events`
- keep dedupe state, delivery status, attempts, and responses on those rows; there is no separate `alert_state` table
- queue asynchronous `alert_delivery` jobs for pending deliveries
- use `alert_reconcile` to reclaim stale `dispatching` rows and requeue due `retry_wait` deliveries
- optionally deliver to Discord webhook sinks when configured

### 10. Persistence Layout

At a high level Postgres currently holds these logical groups:

```text
discovery run:
  discovery_runs
  discovery_run_candidates
  discovery_run_events
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
  job_runs
  job_leases

alerts:
  alert_events

control:
  control_state
  operator_actions
  policy_rollouts

events:
  event_log
```

## Current System Summary

The current application is best understood as one narrow runtime console sitting on top of one backend runtime with several cooperating subsystems:

- a discovery and collection stack built from `services/discovery_runs/`, `services/scanners/`, `services/live_selection.py`, and canonical signal/opportunity persistence
- an execution ledger that records broker interactions immutably
- a session position model that owns day-local trade state
- a broker sync process that reconciles broker reality without taking ownership
- a shared risk and exit layer for both manual and automated actions
- runtime, pipeline, and ops read models assembled by `live_runtime`, `discovery_run_health`, `pipelines`, and `ops`
- an API and WebSocket layer that exposes those read models and fans realtime events to the UI
- a scheduler plus three worker lanes over Redis ARQ
- supporting alerts and analysis subsystems around that core

If you want to drill further, the next useful cuts are:

- `execution` vs `session_positions`
- `broker_sync`
- `risk_exit`
- `collections` / `scanners` / `live_selection`
- `web/API`
- `scheduler/worker`
- Postgres table groups and read models
