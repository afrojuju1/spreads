# Trading Engine Architecture

Status: proposed

Related:

- [Current System State](../current_system_state.md)
- [Signal State Platform](./signal_state_platform.md)
- [0DTE System Architecture](./0dte_system_architecture.md)
- [Alpaca Capabilities Statement](../research/alpaca_capabilities_statement.md)

## Goal

Define the target architecture for the full trading engine in a way that is actually buildable.

This document is intentionally narrower than a generic system-design essay. It focuses on the five decisions that matter most:

1. the canonical event model
2. truth ownership boundaries
3. the first vertical slice to build
4. replay semantics
5. the operator model

If those five are vague, the rest of the engine stays vague.

This document also includes the supporting models that make those decisions implementable:

- entity model
- opportunity model
- policy and versioning
- freshness and degradation semantics

## Design Principles

1. Build around state, not polling loops.
2. Keep signal truth, order truth, and position truth separate.
3. Use one canonical event model across live, paper, replay, and analytics.
4. Treat risk and operator control as first-class system concerns.
5. Prefer a modular monolith with explicit boundaries before splitting runtime services.

## Recommended Framing

The cleanest high-level mental model is four macro-systems:

1. `Control Plane`
2. `State Platform`
3. `Trading Core`
4. `Research Platform`

These are logical domains, not a requirement to run four separate services.

```text
                 +---------------------------+
                 | Alpaca                    |
                 | market data + broker APIs |
                 +------+--------------------+
                        | 
        market events   | broker events / order acks / fills
                        |
                        v
    +-------------------+--------------------+
    | State Platform                         |
    | events, features, signal state, opps   |
    +-------------------+--------------------+
                        |            ^
                        |            |
                        v            | outcomes / fills / recon
    +-------------------+------------+----+
    | Trading Core                         |
    | risk, OMS, execution, positions      |
    +-------------------+------------------+
                        ^
                        |
                 orders / cancels / replaces
                        |
                 +------+--------------------+
                 | Control Plane             |
                 | mode, policy, ops, kills  |
                 +------+--------------------+
                        |
                        v
              +---------+------------------+
              | Research Platform          |
              | replay, sim, attribution   |
              +----------------------------+
```

## Macro-Systems

### 1. Control Plane

Owns:

- session/calendar state
- mode selection: live, paper, replay
- policy/config versioning
- approvals and kill switches
- degraded-mode behavior

### 2. State Platform

Owns:

- normalized event ingestion
- point-in-time feature computation
- signal state transitions
- opportunity surfacing
- freshness and staleness semantics

This is where strategy families can differ in cadence and trigger style while still sharing the same engine core.

Examples:

- `0dte` can behave as an event-driven intraday adapter
- `core` can behave as a slower intraday or session-based adapter
- `weekly` can behave as a wider-horizon adapter with less reactive triggers

### 3. Trading Core

Owns:

- risk approval
- capital and exposure decisions
- order intent lifecycle
- broker submission
- fills, positions, accounting, reconciliation

### 4. Research Platform

Owns:

- deterministic replay
- simulation
- attribution
- diagnostics
- tuning

## Non-Negotiable Truth Boundaries

There are three truths that must stay separate.

### 1. Signal Truth

Answers:

- what the market state is
- what changed
- what opportunities are active
- what is blocked or stale

This belongs to the `State Platform`.

### 2. Order Truth

Answers:

- what the system intended to send
- what was sent
- what the broker acknowledged, rejected, canceled, or filled

This belongs to the `Trading Core`, specifically the OMS and execution domains.

### 3. Position Truth

Answers:

- what exposure actually exists
- what PnL exists
- what is still open, closing, or closed
- whether internal and broker state reconcile

This also belongs to the `Trading Core`, but it is not the same as order truth.

Do not collapse these three into one shared status model.

## 1. Canonical Event Model

The engine should be built around one normalized event model.

Everything else derives from that.

## Event Classes

At the top level, events should fall into five classes:

1. `market_event`
2. `broker_event`
3. `control_event`
4. `signal_event`
5. `analytics_event`

Not every downstream component needs every class, but they should all share the same event envelope.

## Canonical Event Envelope

Every event should carry a small common envelope.

Suggested fields:

- `event_id`
- `event_class`
- `event_type`
- `occurred_at`
- `ingested_at`
- `source`
- `entity_type`
- `entity_key`
- `session_date`
- `market_session`
- `payload`
- `schema_version`
- `producer_version`
- `correlation_id`
- `causation_id`

## Why This Matters

This gives the engine:

- replay input
- auditability
- ordering and causality hooks
- version-aware debugging
- common joins across market, signal, order, and operator history

## Event Examples

### Market Event

```json
{
  "event_id": "evt_mkt_01",
  "event_class": "market_event",
  "event_type": "bar_updated",
  "occurred_at": "2026-04-09T14:35:00Z",
  "ingested_at": "2026-04-09T14:35:00.420Z",
  "source": "alpaca.stock.updatedBars",
  "entity_type": "underlying",
  "entity_key": "SPY",
  "session_date": "2026-04-09",
  "market_session": "regular",
  "schema_version": 1,
  "producer_version": "market_adapter_v1",
  "correlation_id": null,
  "causation_id": null,
  "payload": {
    "open": 510.12,
    "high": 510.45,
    "low": 509.98,
    "close": 510.38,
    "volume": 182340
  }
}
```

### Broker Event

```json
{
  "event_id": "evt_brk_01",
  "event_class": "broker_event",
  "event_type": "order_partially_filled",
  "occurred_at": "2026-04-09T14:36:14.003Z",
  "ingested_at": "2026-04-09T14:36:14.120Z",
  "source": "alpaca.trade_updates",
  "entity_type": "broker_order",
  "entity_key": "alpaca:6607f3c8-3fcc-4afd-8e07-25249dcc3a93",
  "session_date": "2026-04-09",
  "market_session": "regular",
  "schema_version": 1,
  "producer_version": "broker_adapter_v1",
  "correlation_id": "order_intent:abc123",
  "causation_id": "submit_request:xyz789",
  "payload": {
    "filled_qty": 1,
    "avg_fill_price": -0.32,
    "status": "partially_filled"
  }
}
```

### Control Event

```json
{
  "event_id": "evt_ctl_01",
  "event_class": "control_event",
  "event_type": "kill_switch_enabled",
  "occurred_at": "2026-04-09T14:40:00Z",
  "ingested_at": "2026-04-09T14:40:00Z",
  "source": "operator_ui",
  "entity_type": "engine",
  "entity_key": "global",
  "session_date": "2026-04-09",
  "market_session": "regular",
  "schema_version": 1,
  "producer_version": "ops_ui_v1",
  "correlation_id": null,
  "causation_id": null,
  "payload": {
    "scope": "all_new_entries",
    "reason": "operator_manual"
  }
}
```

## Event Ordering Rules

The engine should prefer:

- `occurred_at` for business logic
- `ingested_at` for operational diagnostics

The platform should never silently treat ingestion time as event time.

## 2. Truth Ownership

The architecture only works if ownership is explicit.

## Ownership Table

### Event Domain

Owns:

- normalized raw event history
- source metadata
- event timestamps
- replay input

Does not own:

- current signal state
- order state
- positions

### Feature Domain

Owns:

- derived point-in-time features
- feature version
- freshness metadata

Does not own:

- signal transitions
- strategy opportunities

### Signal State Domain

Owns:

- current signal state
- signal transition history
- active, blocked, stale, invalidated states
- opportunity-ready state

Does not own:

- order submission
- fills
- positions

### OMS Domain

Owns:

- order intent
- internal order lifecycle
- idempotency
- duplicates, retries, dependencies
- internal state transitions like created, submitted, cancel_requested

Does not own:

- broker market data
- strategy state
- final position truth

### Execution/Broker Domain

Owns:

- broker requests
- broker acknowledgements
- broker order ids
- broker update history
- fills as reported by broker

Does not own:

- whether the strategy should have submitted the order
- final portfolio attribution

### Position Domain

Owns:

- derived position state
- realized and unrealized PnL
- open/closed status
- reconciliation state

Does not own:

- signal generation
- order intent history beyond what is needed for derivation

### Control Plane Domain

Owns:

- policy versions
- kill switches
- manual overrides
- mode state
- approval actions

Does not own:

- market-state inference
- position derivation

## Simplified Rule

If a question starts with:

- "what is happening in the market?" -> `State Platform`
- "what did we try to send?" -> `OMS / Execution`
- "what exposure actually exists?" -> `Position Domain`
- "was the engine allowed to act?" -> `Control Plane`

## 3. Entity Model

The engine needs a generic entity model so events, signals, opportunities, orders, and positions all reference the same kinds of things consistently.

### Core Entity Families

The platform should reason about four families:

1. `market entities`
   - underlying
   - contract
   - basket

2. `signal entities`
   - the units the signal platform evaluates
   - usually underlyings, baskets, contracts, or synthetic subjects

3. `opportunity entities`
   - time-bounded strategy-shaped action candidates

4. `trading entities`
   - order intent
   - broker order
   - fill
   - position

### Recommended Core Entity Types

The first engine version should standardize:

1. `underlying`
2. `contract`
3. `basket`
4. `signal_subject`
5. `opportunity`
6. `order_intent`
7. `broker_order`
8. `position`

### Entity Key Pattern

Keys should be stable and namespaced.

Examples:

- `underlying:SPY`
- `contract:SPY250409P00650000`
- `basket:etf_core`
- `signal_subject:underlying:SPY`
- `opportunity:strategy_x:SPY:2026-04-09T14:35:00Z`
- `order_intent:uuid`
- `broker_order:alpaca:uuid`
- `position:session:uuid`

### Relationship Model

The engine should preserve explicit relationships:

```text
underlying / contract / basket
            |
            v
       signal_subject
            |
            v
        opportunity
            |
            v
        order_intent
            |
            v
       broker_order
            |
            v
         position
```

The important principle is that the platform core knows entity identity and relationships, but not one strategy's special semantics.

## 4. Opportunity Model

The opportunity object is the bridge between the `State Platform` and the `Trading Core`.

Without it, signal logic leaks into risk, OMS, and execution decisions.

### What An Opportunity Is

An opportunity is:

- strategy-shaped
- time-bounded
- derived from current signal state
- not yet an order

It should answer:

- what action is worth considering
- why it is worth considering
- how long it remains valid
- what blockers still apply

### Core Opportunity Fields

Suggested fields:

- `opportunity_id`
- `strategy_family`
- `entity_type`
- `entity_key`
- `side`
- `classification`
- `confidence`
- `signal_state_ref`
- `created_at`
- `expires_at`
- `reason_codes`
- `blockers`
- `execution_shape`
- `risk_hints`

### Opportunity Lifecycle

```text
candidate
  -> ready
  -> blocked
  -> expired
  -> consumed
```

`consumed` means the opportunity has already been used to create an order intent.

### Practical Rule

Signal state should answer:

- is the setup active

Opportunity should answer:

- what action is currently worth considering

Trading Core should answer:

- are we allowed to do it now

This means `0dte`, `core`, and `weekly` can all produce different opportunity shapes and timing behavior, while still flowing into the same downstream Trading Core for:

- risk approval
- order intent creation
- execution
- position ownership
- reconciliation

## 5. Policy And Versioning

Policy needs to be explicit and versioned across the engine.

### Core Policy Families

The engine should distinguish:

1. `strategy policy`
   - feature thresholds
   - trigger rules
   - opportunity shaping

2. `risk policy`
   - max exposure
   - sizing limits
   - duplicate-exposure rules
   - session and symbol constraints

3. `execution policy`
   - pricing mode
   - concession limits
   - retry / cancel / replace behavior
   - broker-specific submission semantics

4. `operator policy`
   - approvals required
   - mode gates
   - kill-switch scopes
   - degraded-mode fallback rules

### Versioning Requirements

Every meaningful decision should be attributable to:

- policy family
- policy version
- effective time

This means signal transitions, opportunities, risk approvals, order intents, and replay runs should all carry policy references.

### Recommended Policy Lifecycle

```text
draft -> active -> superseded -> retired
```

The engine should avoid mutating historical policy versions in place.

### Practical Rule

Store both:

- policy references
- resolved values used in the decision

Resolved values are operationally useful.
Policy references are required for audit and replay.

## 6. Freshness And Degradation

Freshness is part of engine state, not just observability.

The system should explicitly know:

- whether a source is fresh
- whether a feature is fresh
- whether a signal is still valid
- whether execution is still allowed

### Freshness Layers

1. `source freshness`
   - stock stream fresh/stale
   - option stream fresh/stale
   - broker update stream fresh/stale

2. `feature freshness`
   - VWAP current or stale
   - spread-quality metrics current or stale
   - expected-move estimate current or stale

3. `signal freshness`
   - active state still supported by fresh data
   - armed state expired due to missing confirmation

4. `opportunity freshness`
   - opportunity still actionable
   - execution window expired

### Degradation States

The engine should use a small explicit model:

- `healthy`
- `degraded`
- `blocked`

### Blocking Rules

The system should automatically block at least:

- new entries when core market data is stale
- new entries when broker state is stale beyond tolerance
- opportunity promotion when required features are stale

It may still allow:

- passive monitoring
- replay
- operator visibility

### Operator Interaction

Degradation should surface as explicit engine state and `control_event`s where relevant.

Operators should be able to see:

- what is stale
- how stale it is
- what is blocked because of it
- what can be overridden, if anything

## 7. First Vertical Slice

The first real build should prove the architecture end to end without trying to solve every strategy.

## Recommended Slice

Use one narrow but real slice:

- one underlying-driven strategy family
- one opportunity type
- one opportunity-to-order path
- one order-to-position path
- full replayable state transitions

The best candidate is still a narrow 0DTE or intraday spread workflow, but the slice should be framed as a platform slice, not as a one-off strategy implementation.

## Slice Contents

### In Scope

- canonical market events for one small universe
- current signal state + transition log
- one strategy adapter
- opportunity object
- pre-trade risk decision
- OMS order intent
- broker submit and updates
- derived position truth
- reconciliation
- replay of the exact same slice

### Out Of Scope

- multi-strategy orchestration
- advanced capital allocation
- smart routing
- complex execution tactics
- full analytics suite

## Why This Slice

It proves:

- the event model
- the truth boundaries
- the control points
- the replay path

without forcing the whole engine to be complete first.

## 8. Replay Semantics

Replay is one of the hardest parts of the whole engine, so the rules must be explicit early.

## Replay Goal

Given:

- the same normalized event stream
- the same policy versions
- the same feature logic versions

the replay should produce materially the same:

- feature states
- signal transitions
- opportunities
- risk decisions
- order intents

It does not need to reproduce external fills exactly unless the broker/execution simulator is deterministic enough to do so.

## Replay Levels

The engine should explicitly support three replay modes.

### 1. Audit Replay

Purpose:

- reproduce what happened historically as faithfully as possible

Inputs:

- historical market events
- historical broker events
- historical control events

Outputs:

- reconstructed signal states
- reconstructed opportunities
- reconstructed order and position evolution

### 2. Decision Replay

Purpose:

- re-run decision logic under versioned policies

Inputs:

- historical market events
- optional control events

Outputs:

- recomputed features
- recomputed signal transitions
- recomputed opportunities
- recomputed risk decisions
- recomputed order intents

### 3. Execution Simulation

Purpose:

- test how decisions might have performed under a simulated execution model

Inputs:

- decision replay outputs
- execution model assumptions

Outputs:

- simulated fills
- simulated positions
- simulated performance

## Replay Rules

The engine should decide explicitly:

- how out-of-order events are handled
- what lateness tolerance is allowed
- whether derived events are recomputed or stored
- whether broker events are replayed as facts or simulated
- whether operator actions are part of replay

## Recommended Default

- raw market, broker, and control events are replay inputs
- features and signal transitions are recomputed
- risk decisions are recomputed under versioned policy
- broker fills are either:
  - replayed as historical facts for audit mode, or
  - simulated for strategy-analysis mode

These are distinct modes and should not be confused.

## 9. Operator Model

A trading engine is also an operations system.

The operator model should be designed explicitly.

## What Operators Need To Control

- global kill switch
- new-entry disable
- strategy-specific pause
- symbol-specific block
- paper vs live mode
- policy version roll-forward / rollback
- manual approvals where required

## What Operators Need To See

- current signal states
- active opportunities
- blocked opportunities and reasons
- current open orders
- current positions
- current reconciliation mismatches
- stale feeds and degraded components
- current policy versions
- recent operator actions

## What Should Be Manual vs Automatic

### Automatic

- stale-data blocking
- duplicate-order prevention
- risk-limit enforcement
- forced degraded modes for broken dependencies

### Operator-Controlled

- enabling or disabling live trading
- releasing kill switch
- approving risky policy changes
- emergency flatten decisions

## Operator Events

All important operator actions should become `control_event`s.

That means:

- approvals
- pauses
- kill-switch changes
- mode changes
- policy rollouts

This is required for auditability and replay context.

## Three Core State Machines

The engine should still maintain three separate state machines.

### Signal State

Examples:

- idle
- arming
- active
- blocked
- cooldown
- invalidated

### Order State

Examples:

- created
- submitted
- acknowledged
- partially_filled
- filled
- pending_cancel
- canceled
- rejected
- expired

### Position State

Examples:

- flat
- opening
- open
- partially_closed
- closing
- closed
- reconciled
- mismatched

These should never be merged into one catch-all status.

## Canonical Flows

### Opportunity To Order

```text
events
  -> features
  -> signal state
  -> strategy opportunity
  -> risk approval
  -> order intent
  -> broker submission
```

### Broker Truth To Position Truth

```text
broker updates / fills
  -> OMS / execution ledger
  -> position derivation
  -> accounting update
  -> reconciliation
  -> operator visibility
```

### Replay

```text
historical events
  -> feature recomputation
  -> signal transitions
  -> opportunity generation
  -> risk decisions
  -> order intents
  -> optional execution simulation
```

## Practical Build Order

### Phase 1

Build the platform foundation.

- canonical event model
- raw event log
- feature layer
- current signal state + transition log

### Phase 2

Build the trading truth boundaries.

- risk decision objects
- OMS boundary
- execution boundary
- position/reconciliation boundary

### Phase 3

Build the first vertical slice.

- one strategy adapter
- one opportunity-to-order flow
- one position lifecycle
- one replay path

### Phase 4

Build the operator layer.

- kill switches
- mode control
- approvals
- degraded-state visibility

### Phase 5

Expand strategy coverage and research depth.

- more adapters
- simulation
- attribution
- tuning

## 10. Edge Cases And Failure Modes

The architecture is still incomplete unless it explicitly accounts for failure and exception paths.

These are the main edge areas the engine should plan for.

### A. Broker Exception Lifecycle

Not all broker-relevant state changes arrive as normal order/fill updates.

The engine should explicitly model:

- option assignment
- option exercise
- option expiry
- contract adjustments after corporate actions
- out-of-band broker position changes

This matters especially because broker websockets may not deliver every non-trade lifecycle event.

Practical rule:

- treat these as first-class broker exception events
- poll and reconcile them explicitly when streaming coverage is incomplete
- never assume order lifecycle events alone explain all position changes

### B. Unknown Submit Outcome

One of the most important trading-engine failure modes is:

- the engine sends an order
- the request times out or the worker crashes
- the broker may or may not have accepted it

The architecture should explicitly support an `unknown_submit_state`.

The engine needs a recovery path for:

- request timeout after submission attempt
- submit acknowledged by broker but not persisted locally
- duplicate resubmission risk
- cancel/replace race during recovery

Practical rule:

- uncertain submits must enter a recoverable pending state
- recovery must query broker truth before allowing retry
- idempotency must be enforced above raw broker calls

### C. Duplicate, Late, And Out-Of-Order Events

The event model should assume:

- duplicate events can happen
- events can arrive late
- events can arrive out of order

The platform should therefore define:

- dedupe keys
- ordering rules by event type
- lateness tolerance
- reconciliation rules when event order is violated

This should apply to market events, broker events, and control events.

### D. Adapter Failure And Recovery

External adapters will fail in uneven ways.

The architecture should explicitly define behavior for:

- websocket disconnects
- REST fallback windows
- rate limiting
- partial symbol coverage
- quote stream stalls
- broker update stalls
- stale account snapshots

The engine should degrade explicitly:

- `healthy`
- `degraded`
- `blocked`

and expose which downstream behaviors are disabled in each state.

### E. Restart And State Rebuild

The engine must define what happens after a process restart or worker crash.

Questions the architecture should answer:

- what current state is rebuilt from durable storage
- what is replayed from raw events
- what is recomputed from broker truth
- what in-flight actions need recovery handling

Recommended principle:

- current state should be reconstructible
- recovery should not depend on in-memory-only facts

### F. Session, Clock, And Calendar Edge Cases

Time handling needs more rigor than just market-open vs market-closed.

The engine should plan for:

- half days
- DST transitions
- opening and closing auction windows
- session cutoffs for new entries
- late-day behavior changes
- halts and resumes
- overnight session boundaries

Practical rule:

- calendar/session state should be explicit engine input, not scattered strategy logic

### G. Cross-Strategy Allocation Conflicts

As strategies expand, multiple opportunities may compete for the same:

- capital
- symbol exposure
- sector exposure
- volatility budget

The architecture should eventually support:

- opportunity reservation
- priority / tie-break policy
- capital allocation across strategy families

This does not need to be fully built in the first slice, but it should be recognized as a future Trading Core concern.

### H. Schema Evolution And Replay Compatibility

The engine will evolve:

- event schemas
- feature definitions
- policy structures
- opportunity shapes

Replay compatibility requires planning for:

- schema versions
- producer versions
- policy references
- migration or compatibility rules for historical data

Without this, replay becomes less trustworthy as the system matures.

### I. Operator And Compliance Edges

The architecture should also account for operational control concerns that are not pure trading logic.

Examples:

- role-based permissions for sensitive actions
- audit retention for operator actions
- runbooks for degraded states
- emergency flatten procedures
- secret / credential rotation
- alert thresholds and escalation rules

These are not implementation details. They materially affect whether the engine is operable.

## Bottom Line

The trading engine should be designed around five build-critical decisions:

1. one canonical event model
2. explicit truth ownership
3. one narrow but real first vertical slice
4. clear replay semantics
5. an explicit operator model

All of that fits cleanly inside four macro-systems:

- `Control Plane`
- `State Platform`
- `Trading Core`
- `Research Platform`

And within those four, keep the non-negotiable truth boundaries:

- signal truth
- order truth
- position truth

That is the cleanest streamlined architecture.

And to make it real, the engine must also explicitly handle failure and exception paths:

- broker exceptions
- unknown submit outcomes
- late / duplicate / out-of-order events
- adapter degradation
- restart recovery
- calendar edge cases
- allocation conflicts
- schema evolution
- operator/compliance controls

## References

- [SEC Rule 15c3-5 Market Access Rule](https://www.nasdaqtrader.com/content/productsservices/trading/ften/SECRule_15c3_5.pdf)
- [FIX Order State Changes](https://www.fixtrading.org/wp-content/uploads/download-manager-files/Order-State-Changes.pdf)
- [Alpaca Trade Updates](https://docs.alpaca.markets/docs/websocket-streaming)
- [Apache Flink Stateful Stream Processing](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/concepts/stateful-stream-processing/)
- [Apache Flink Fault Tolerance](https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/fault_tolerance/)
- [Apache Flink Event Time](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/concepts/time/)
- [Tecton Streaming Features and Point-in-Time Correctness](https://docs.tecton.ai/docs/beta/tutorials/building-streaming-features)
