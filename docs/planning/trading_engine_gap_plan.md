# Trading Engine Gap Analysis And Implementation Plan

Status: proposed

Related:

- [Current System State](/Users/adeb/Projects/spreads/docs/current_system_state.md)
- [Trading Engine Architecture](/Users/adeb/Projects/spreads/docs/planning/trading_engine_architecture.md)
- [Signal State Platform](/Users/adeb/Projects/spreads/docs/planning/signal_state_platform.md)
- [0DTE System Architecture](/Users/adeb/Projects/spreads/docs/planning/0dte_system_architecture.md)

## Goal

Compare the current implemented system to the target trading-engine architecture and turn that into a concrete refactor / implementation plan.

This document answers four questions:

1. what is already implemented and should be preserved
2. what is partially implemented and should be formalized
3. what is still missing
4. what order to build the remaining pieces in

## Executive Summary

The current system is not a greenfield prototype. It already has a meaningful trading core.

Strongly implemented today:

- async job orchestration
- live collector runtime
- immutable execution ledger
- session-owned position model
- broker sync and reconciliation
- risk gating and automated exits
- operator dashboard and live events

Partially implemented today:

- control plane
- state platform
- opportunity model
- policy snapshots
- degradation handling
- analysis and replay-adjacent workflows

Largely missing today:

- canonical event model
- first-class signal state platform
- explicit opportunity domain
- formal policy/versioning model
- deterministic replay
- explicit operator-control domain

So the right path is not a rewrite.

The right path is:

- preserve the current trading core
- formalize the state/control/research layers around it
- refactor the collector into the first strategy adapter on top of a shared state platform

## Current-To-Target Map

### 1. Control Plane

Target responsibility:

- mode control
- policy/config versioning
- operator actions
- kill switches
- degraded-mode handling

Current status: `partial`

Already implemented:

- operator UI exists
- job health and diagnostics exist
- kill-switch behavior exists in risk gating
- some degraded-state surfacing exists

Evidence:

- [current_system_state.md](/Users/adeb/Projects/spreads/docs/current_system_state.md)
- [risk_manager.py](/Users/adeb/Projects/spreads/src/spreads/services/risk_manager.py)
- [providers.tsx](/Users/adeb/Projects/spreads/apps/web/components/providers.tsx)

What is missing:

- explicit control-event model
- centralized mode state
- durable operator action log
- explicit approval model
- formal policy rollout / rollback semantics

### 2. State Platform

Target responsibility:

- normalized events
- point-in-time features
- signal state transitions
- opportunity surfacing
- freshness / staleness semantics

Current status: `partial`

Already implemented:

- live collector captures intraday opportunity snapshots
- collector cycles, candidates, and events are persisted
- option quote events are persisted
- scanner computes meaningful setup/context features
- realtime event fanout exists

Evidence:

- [current_system_state.md](/Users/adeb/Projects/spreads/docs/current_system_state.md)
- [scanner.py](/Users/adeb/Projects/spreads/src/spreads/services/scanner.py)
- [live_collector.py](/Users/adeb/Projects/spreads/src/spreads/jobs/live_collector.py)
- [collector_models.py](/Users/adeb/Projects/spreads/src/spreads/storage/collector_models.py)

What is missing:

- one canonical raw event model
- first-class feature store
- first-class signal state store
- first-class transition log for signals
- generic opportunity object
- unified freshness semantics

Current approximation:

- `collector_cycles` and `collector_cycle_candidates` act like a strategy-specific opportunity snapshot store
- setup scoring exists, but not a reusable signal-state domain

### 3. Trading Core

Target responsibility:

- risk approval
- OMS
- execution
- positions
- reconciliation

Current status: `strong`

Already implemented:

- immutable execution ledger
- async open/close submission
- broker order and fill ingestion
- session-owned position model
- broker-global sync and reconciliation
- risk gating
- exit management

Evidence:

- [current_system_state.md](/Users/adeb/Projects/spreads/docs/current_system_state.md)
- [execution.py](/Users/adeb/Projects/spreads/src/spreads/services/execution.py)
- [broker_sync.py](/Users/adeb/Projects/spreads/src/spreads/services/broker_sync.py)
- [risk_manager.py](/Users/adeb/Projects/spreads/src/spreads/services/risk_manager.py)
- [exit_manager.py](/Users/adeb/Projects/spreads/src/spreads/services/exit_manager.py)

What is still missing or weak:

- explicit opportunity-to-risk decision object
- explicit OMS vocabulary distinct from execution attempts
- formal portfolio allocator beyond current policy checks
- canonical broker-event normalization inside the event model

Important conclusion:

The Trading Core should be refactored and clarified, not replaced.

### 4. Research Platform

Target responsibility:

- deterministic replay
- simulation
- attribution
- diagnostics
- tuning

Current status: `partial`

Already implemented:

- post-close analysis
- post-market analysis
- signal/outcome summaries in analysis
- generator and diagnostics around stored history

Evidence:

- [analysis.py](/Users/adeb/Projects/spreads/src/spreads/services/analysis.py)
- [post_market_analysis.py](/Users/adeb/Projects/spreads/src/spreads/services/post_market_analysis.py)
- [generator.py](/Users/adeb/Projects/spreads/src/spreads/services/generator.py)

What is missing:

- deterministic replay engine
- explicit audit replay mode
- explicit decision replay mode
- execution simulation mode
- point-in-time parity between live and replay pipelines

## Detailed Gap Assessment

## A. What Is Already Good And Should Be Preserved

These are the strongest current assets.

### Async Job Orchestration

The scheduler + worker model is already real and useful.

Preserve:

- `job_definitions`
- `job_runs`
- `job_leases`
- lane-based workers where justified

This should remain the orchestration substrate, even if the engine gains better state models.

### Execution Ledger

The immutable execution ledger is the right core shape.

Preserve:

- `execution_attempts`
- `execution_orders`
- `execution_fills`

Do not collapse this into mutable position state.

### Session Position Ownership

The session-owned position model is also correct.

Preserve:

- `session_positions`
- `session_position_closes`
- day/session attribution ownership

This is already aligned with the target truth-boundary design.

### Broker Sync And Reconciliation

The broker sync domain is real value, not incidental glue.

Preserve:

- `account_snapshots`
- `broker_sync_state`
- reconciliation logic and mismatch visibility

### Risk And Exit Controls

The current `risk_manager` and `exit_manager` are meaningful engine components already.

Preserve them, but later wrap them in more explicit policy and state structures.

## B. What Exists But Needs Refactoring Into Platform Concepts

These are the places where current code is already doing useful work but under the wrong abstraction.

### Live Collector -> First Strategy Adapter

Current role:

- scan
- rank
- persist board/watchlist
- optionally auto-submit

Target role:

- strategy adapter built on the shared State Platform

Refactor direction:

- keep collector persistence as historical opportunity history
- stop treating it as the future generic signal-state model
- use it as the first strategy-specific consumer/producer on top of the platform

### Collector Candidates -> Opportunity Model

Current role:

- board/watchlist candidates are the closest thing to opportunities

Target role:

- explicit `opportunity` object between signal state and Trading Core

Refactor direction:

- board/watchlist candidate rows should map into a generic opportunity vocabulary
- avoid letting candidate rows become the universal engine object

### Execution Attempts -> Proto-OMS

Current role:

- execution attempts already hold much of the order-intent lifecycle

Target role:

- explicit OMS boundary

Refactor direction:

- likely evolve `execution_attempts` into the OMS/order-intent surface
- avoid inventing a duplicate second order-intent model unless current shape truly cannot support it

### Policy Snapshots -> Versioned Policies

Current role:

- positions snapshot risk and exit policy
- job definitions embed execution policy

Target role:

- explicit strategy/risk/execution/operator policy model with versions

Refactor direction:

- preserve snapshots
- add explicit policy version references and lifecycle semantics

## C. What Is Missing

These are the major missing platform pieces.

### Canonical Event Model

Missing:

- one normalized event envelope
- one durable raw event log
- consistent correlation/causation model

Current consequence:

- each subsystem has its own storage shape
- replay and cross-domain traceability are weak

### Signal State Platform

Missing:

- current signal-state store
- signal transition log
- generic trigger/state lifecycle
- signal freshness semantics

Current consequence:

- setup is recomputed and persisted indirectly through collector snapshots
- the engine does not yet have a reusable notion of signal truth

### Opportunity Domain

Missing:

- generic opportunity object
- opportunity lifecycle
- strategy-to-trading handoff contract

Current consequence:

- opportunity logic leaks into collector and execution paths

### Control Plane Domain

Missing:

- explicit operator events
- formal mode state
- approval workflow
- policy rollout / rollback model

Current consequence:

- operational control exists, but mostly as scattered runtime behavior

### Replay Platform

Missing:

- audit replay
- decision replay
- execution simulation

Current consequence:

- analysis is informative, but not a full replayable engine workflow

## D. Cross-Cutting Status

### Entity Model

Current status: `implicit`

Current system already has:

- underlying-like identities
- contracts
- candidate rows
- attempts/orders/fills
- positions

But these are not yet expressed as one generic entity model.

### Freshness And Degradation

Current status: `partial`

Already present:

- stale quote protection
- broker sync health
- collector degraded events
- account/session health summaries

Missing:

- one shared freshness model across sources, features, signals, and opportunities

### Operator Visibility

Current status: `good but fragmented`

Already present:

- jobs health
- collector degradation
- execution updates
- broker sync health

Missing:

- one unified operator control/state surface

## Recommended Refactor Principle

Refactor by adding missing domains around the existing core, not by replacing the core.

In practice:

- keep `execution`
- keep `session_positions`
- keep `broker_sync`
- keep scheduler/workers
- keep live collector as the first adapter
- introduce `events`, `signal_state`, and `opportunity` around them

That is a layering refactor, not a rewrite.

## Implementation Plan

## Phase 0: Freeze The Good Boundaries

Before introducing new abstractions, explicitly preserve:

- immutable execution ledger
- session-owned position truth
- broker sync as broker-global reconciliation
- shared risk/exit submission path

This prevents platform work from breaking the most valuable current invariants.

## Phase 1: Canonical Event Layer

Status: completed

Implemented:

- normalized event envelope
- durable `event_log`
- market-event adapter for captured option quotes
- broker-event adapters for broker sync and execution updates
- control/analytics adapters for job, operator, alert, and analysis events

Build:

- normalized event envelope
- raw event log
- event adapters for:
  - market events
  - broker events
  - control events

Deliverable:

- one durable event stream that can be replayed and joined across domains

## Phase 2: Signal State And Opportunity Layer

Status: completed

Implemented:

- durable current `signal_states` store
- append-only `signal_state_transitions` log
- generic `opportunities` table with lifecycle state
- live collector adapter mapping board/watchlist candidates into signal state and opportunities
- opportunity lookup and consume handoff on the existing execution path
- read paths for `/signal-state`, `/signal-state/transitions`, and `/opportunities`

Build:

- current signal-state rows
- signal transition log
- generic opportunity object
- first strategy adapter contract

Initial producer:

- live collector / intraday strategy path

Deliverable:

- the engine can say what is active, why, and for how long without depending on collector-cycle rows alone

## Phase 3: Formalize OMS/Risk Decisions

Status: completed

Implemented:

- durable `risk_decisions` store with policy references, evidence, and decision metrics
- explicit `opportunity_id` and `risk_decision_id` handoff fields on `execution_attempts`
- structured risk evaluation output on the open-execution path
- API read paths for `/risk-decisions` and session-level risk decision views

Build:

- explicit risk decision object
- explicit opportunity-to-order handoff
- cleaner OMS vocabulary on top of current execution primitives
- policy version references on decisions

Deliverable:

- a clearer Trading Core without replacing the current execution backbone

## Phase 4: Control Plane Formalization

Status: completed

Implemented:

- durable global `control_state` record with `normal`, `degraded`, and `halted` modes
- append-only `operator_actions` audit log for mode changes
- internal `policy_rollouts` history with active rollout refs in control resolution
- minimal operator API surface at `/control/state` and `/control/mode`
- control-event emission for mode changes and control-based open-execution skips and blocks

Build:

- control events
- mode state
- operator actions log
- policy rollout / rollback model
- explicit degraded-state handling surface

Deliverable:

- operator behavior becomes replayable and auditable instead of implicit

## Phase 5: Replay Platform

Build:

- audit replay
- decision replay
- execution simulation later

Deliverable:

- strategy and engine behavior can be reproduced and tuned under versioned policies

## First Real Vertical Slice

The first platform slice should use the current strongest path:

- one intraday strategy
- one small symbol universe
- current execution and position core
- new event/signal/opportunity overlay

Success criteria:

- the slice can explain signal truth
- risk decision
- order intent
- broker result
- position truth
- replay result

all from one consistent model.

## Sequence Summary

1. preserve and document current Trading Core invariants
2. add canonical event layer
3. add signal-state and opportunity domains
4. formalize OMS/risk decision surface
5. formalize control plane
6. build replay modes

## Bottom Line

The current system is already a functioning partial trading engine.

What it lacks is not a new execution core.
What it lacks is the shared platform structure around that core:

- canonical events
- signal state
- opportunities
- policy/versioning
- control events
- replay

So the correct implementation plan is:

- preserve the current Trading Core
- reframe the live collector as the first strategy adapter
- add the missing state/control/research layers around it

That is the shortest path from current system to target engine.
