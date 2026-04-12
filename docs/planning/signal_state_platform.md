# Signal State Platform

Status: proposed

Related:

- [Current System State](../current_system_state.md)
- [0DTE System Architecture](./0dte_system_architecture.md)
- [Alpaca Capabilities Statement](../research/alpaca_capabilities_statement.md)

## Goal

Define a generic signal-state platform that can support:

- 0DTE spreads
- longer-dated spreads
- directional setups
- watchlist and alerting systems
- future model-driven or non-options strategies

This platform should be strategy-agnostic at the core.

Its job is to maintain a point-in-time-correct, replayable, explainable view of which symbols or instruments are in meaningful states right now.

## Why A Platform

Most trading systems start with scanners, indicators, or strategy scripts.

That works until you need:

- multiple strategies
- multiple time horizons
- multiple asset types
- live and replay consistency
- explainability
- robust operator visibility

At that point, the real problem is no longer "how do we compute a score?" It becomes:

- what state is the market in
- what changed
- which opportunities are active
- which are stale
- which are blocked
- why the system did or did not act

That is a platform problem, not just a strategy problem.

## Platform Responsibilities

The signal-state platform should do five things well.

1. Ingest timestamped market and derived events.
2. Maintain current signal state for entities of interest.
3. Emit append-only state-transition events.
4. Support deterministic replay from historical event streams.
5. Hand off clean, strategy-ready opportunities to downstream components.

## What The Platform Is

The platform is a stateful layer between raw market data and strategy execution.

```text
market data / derived events
            |
            v
    +-------+--------+
    | feature layer  |
    +-------+--------+
            |
            v
    +-------+--------+
    | signal state   |
    | platform core  |
    +---+--------+---+
        |        |
        |        +--------------------------+
        |                                   |
        v                                   v
state transition events              current signal states
        |                                   |
        +------------------+----------------+
                           |
                           v
                 strategy adapters / policies
                           |
                           v
               scanners / alerts / execution gates
```

## Core Design Principles

### 1. Event-Time Correctness

The platform should reason about when events happened, not only when they arrived.

That matters for:

- out-of-order events
- reconnections
- vendor delays
- replay
- multi-source merging

### 2. Deterministic Replay

Given the same ordered input event stream and the same strategy policy version, replay should produce the same state transitions.

Without this, debugging and research are weak.

### 3. Separation Of Concerns

The platform should separate:

- feature computation
- market-state interpretation
- trigger detection
- strategy policy
- execution policy

These should not be mixed together in one scan loop.

### 4. Explainability

Every state should answer:

- what changed
- why it changed
- what evidence supports it
- what would invalidate it

### 5. Online / Offline Consistency

The same feature definitions and state transitions should work in:

- live mode
- backtest / replay mode
- post-trade analysis

### 6. Strategy-Agnostic Core

The core platform should not know what a spread is.

It should know about:

- entities
- features
- regimes
- triggers
- states
- transitions
- expiries
- confidence

Strategy-specific logic should sit on top.

## Platform Model

The clean conceptual model is:

```text
events -> features -> regimes -> triggers -> states -> opportunities
```

### Events

Raw or normalized inputs.

Examples:

- bar update
- quote update
- trade print
- session-status change
- news event
- volatility event
- corporate-action event
- derived event from another service

### Features

Continuous measurements computed from events.

Examples:

- spot vs VWAP
- realized volatility over rolling windows
- volume percentile
- spread width quality
- expected move distance
- overnight gap
- breadth or relative-strength measures

### Regimes

Explainable interpretations of feature state.

Examples:

- above VWAP
- volatility expanding
- trend supportive
- liquidity degraded
- opening-range breakout
- event-risk regime

### Triggers

Tradeable or actionable transitions.

Examples:

- reclaim confirmed
- breakout held
- failed breakout reversal
- volatility contraction break
- unusual liquidity improvement
- catalyst follow-through

### States

The platform's current view of an entity.

Examples:

- idle
- arming
- armed
- candidate_ready
- blocked
- cooldown
- invalidated

### Opportunities

A strategy-facing handoff object derived from current state.

Examples:

- "SPY is armed for bullish premium-selling"
- "NVDA is in a breakout continuation state"
- "IWM is blocked due to degraded liquidity"

## Entity Model

The platform should be generic about what it tracks.

An entity might be:

- an underlying symbol
- an option contract
- a spread candidate
- a sector or basket
- a model-defined grouping

The same state machinery should work across entity types.

## Recommended Runtime Shape

```text
                     +----------------------+
                     | stream adapters      |
                     | bars / quotes / news |
                     +----------+-----------+
                                |
                                v
                     +----------+-----------+
                     | feature engine       |
                     | rolling windows      |
                     | derived metrics      |
                     +----------+-----------+
                                |
                                v
                     +----------+-----------+
                     | signal state core    |
                     | regime + trigger     |
                     | transition logic     |
                     +-----+-----------+----+
                           |           |
                           |           |
                           v           v
                 +---------+--+   +----+----------------+
                 | event log  |   | current state store |
                 | immutable  |   | mutable             |
                 +---------+--+   +----+----------------+
                           |           |
                           +-----+-----+
                                 |
                                 v
                    +------------+-------------+
                    | strategy adapters        |
                    | spreads / swing / alerts |
                    +------------+-------------+
                                 |
                                 v
                    +------------+-------------+
                    | execution / alerting /   |
                    | dashboards / analytics   |
                    +--------------------------+
```

## Minimum Platform Domains

The high-level platform should expose four first-class domains.

### 1. Event Domain

Append-only normalized event stream.

This is the raw truth used for replay and audit.

### 2. Feature Domain

Point-in-time-correct feature values and rolling summaries.

This is the quantitative context layer.

### 3. Signal State Domain

Current state plus state-transition events.

This is the decision layer.

### 4. Opportunity Domain

Strategy-facing surfaced opportunities derived from signal state.

This is the handoff layer.

## State Management

The platform should maintain both:

- current mutable state
- immutable transition history

You need both.

Current state answers:

- what is active now
- what is armed now
- what is blocked now

Transition history answers:

- what changed
- when it changed
- why it changed
- how long it stayed active

## What Makes A Good State Model

A good state model has:

- explicit states
- explicit transitions
- explicit expiries
- explicit invalidation conditions
- explicit cooldowns
- explicit confidence and reason codes

A weak state model just stores a score and hopes downstream logic infers the rest.

## Generic State Pattern

The core platform should support a common state pattern:

```text
IDLE
  -> ARMING
  -> ACTIVE
  -> BLOCKED
  -> COOLDOWN
  -> INVALIDATED
  -> IDLE
```

Different strategies can rename or refine these, but the generic lifecycle should be stable.

## Platform Interfaces

The platform should expose three interface types.

### Read Interfaces

- current state by entity
- recent transitions
- active opportunities
- blocked states
- freshness / staleness

### Write Interfaces

- ingest normalized events
- publish derived events
- publish transition events
- publish opportunity updates

### Replay Interfaces

- replay by session or date range
- replay by symbol or entity
- inspect state diffs between live and replay

## Good Platform Qualities

A good signal-state platform should be:

- deterministic
- point-in-time correct
- observable
- versioned
- explainable
- replayable
- strategy-agnostic at the core
- latency-aware
- failure-aware

## Things To Get Right Early

### Feature Versioning

Feature logic changes over time.

If feature definitions are not versioned, research and replay become hard to trust.

### Policy Versioning

State transitions and strategy thresholds also need versions.

Otherwise it is difficult to know which rule set created a past signal.

### Freshness Semantics

The platform should know when state is stale.

Freshness should be first-class, not an afterthought.

### Confidence And Evidence

A state should carry not just a label, but also:

- score or confidence
- reason codes
- supporting features
- contradictory features

### Decay And TTL

Signals should expire naturally.

No platform should assume that once active means active forever.

### Blockers

The platform should support blocked states directly.

Examples:

- liquidity degraded
- stale market data
- risk gate closed
- event-risk window
- duplicate exposure already present

## What Makes It Robust In Practice

Robustness comes from handling the messy parts directly:

- out-of-order data
- data-source degradation
- partial data availability
- strategy conflicts
- duplicate opportunities
- stale features
- non-stationary regimes

The platform should degrade explicitly, not silently.

## Strategy Adapter Layer

The platform core should end at generic state and opportunity objects.

Strategy adapters should convert those into domain-specific decisions.

Examples:

- spread adapter
- directional equity adapter
- alert-only adapter
- generator / research adapter

This keeps the core reusable.

## Observability

Operators should be able to answer:

- what is active right now
- why it is active
- what changed recently
- what data source is stale
- what strategies are blocked
- what transitions are noisy
- what states lead to successful outcomes

If the platform cannot answer those questions, it is incomplete.

## What To Focus On Building

If the goal is a durable platform, the highest-ROI focus areas are:

1. A canonical normalized event model.
2. A current-state store plus append-only transition log.
3. Deterministic replay.
4. Point-in-time-correct feature computation.
5. Generic transition semantics with expiry, invalidation, and cooldown.
6. Strategy adapters on top of the core state platform.
7. Strong observability and versioning.

## What Not To Build First

Do not start by building:

- a giant indicator library
- many strategy-specific scanners
- a complicated ML layer
- asset-specific state models in the platform core

Those are downstream concerns.

The platform should first establish:

- state
- time
- replay
- lineage
- transition semantics

## Suggested Build Order

### Phase 1

Create the core event and state domains.

- normalized event schema
- current state store
- transition log
- replay harness

### Phase 2

Add feature computation and freshness semantics.

- rolling features
- feature versioning
- stale-state handling

### Phase 3

Add regime and trigger policies.

- generic transition framework
- strategy adapter hooks

### Phase 4

Hook downstream consumers into the platform.

- scanners
- alerts
- execution systems
- analytics

## Bottom Line

The signal-state platform should be the shared decision substrate for the trading system.

It should not be built around one strategy or one asset class.

At a high level, the platform should provide:

- canonical events
- reusable features
- explicit states
- explicit transitions
- deterministic replay
- explainable opportunities

Once those exist, spreads, directional systems, alerts, and future strategies can all plug into the same core instead of reinventing their own partial signal logic.
