# 0DTE System Architecture

Status: proposed

Based on:

- [Alpaca Capabilities Statement](../research/alpaca_capabilities_statement.md)
- [Current System State](../current_system_state.md)
- [Alpaca-Only Unusual Activity Scanner Design](./unusual_activity_scanner_design.md)
- [Signal State Platform](./signal_state_platform.md)

## Goal

Build a 0DTE credit-spread system that is:

- event-driven on the underlying
- selective on option-chain work
- reactive only when a real candidate exists
- explicit about state transitions, risk gates, and execution quality

This should not be designed as a "scan everything every minute" system. The right design is to react to intraday market structure changes, then selectively enrich options only when the underlying context justifies it.

This 0DTE architecture should be treated as one strategy-specific application of the broader [Signal State Platform](./signal_state_platform.md), not as its own independent state framework.

## Design Principles

1. Use stock market structure as the primary signal surface.
2. Treat option data as enrichment and execution confirmation, not the main trigger.
3. Keep full-chain work off the hot path as much as possible.
4. Persist signal state, not just periodic scan results.
5. Separate signal generation from execution and exit management.

## High-Level System

```text
                    +----------------------+
                    | Market Session Gate  |
                    | open / halt / close  |
                    +----------+-----------+
                               |
                               v
    +------------------+   +---+------------------+   +----------------------+
    | Stock Stream     |-->| Signal Engine        |-->| Armed Symbol Registry|
    | 1m bars          |   | VWAP / ORB / regime  |   | side, score, TTL,    |
    | updatedBars      |   | transitions          |   | decay, hysteresis    |
    | trades optional  |   +---+------------------+   +----------+-----------+
    +------------------+       |                                 |
                               |                                 |
                               v                                 v
                      +--------+---------------------------------+--------+
                      | Candidate Narrower / Option Enrichment            |
                      | same-day expiry only                              |
                      | strike window                                     |
                      | delta / width / expected move / liquidity filters |
                      +-------------------------+--------------------------+
                                                |
                                                v
                                    +-----------+------------+
                                    | Targeted Quote Watch   |
                                    | exact shortlisted legs |
                                    | live executable credit |
                                    +-----------+------------+
                                                |
                                                v
                                    +-----------+------------+
                                    | Execution Gate         |
                                    | setup still valid      |
                                    | risk ok                |
                                    | price policy ok        |
                                    | cooldown ok            |
                                    +-----------+------------+
                                                |
                                                v
                                    +-----------+------------+
                                    | Broker / OMS           |
                                    | multi-leg submission   |
                                    +-----------+------------+
                                                |
                                                v
                                    +-----------+------------+
                                    | Position / Exit Engine |
                                    | stop / target / time   |
                                    | reconciliation         |
                                    +------------------------+
```

## Runtime Components

The clean runtime shape is four services.

### 1. Signal Service

Consumes stock market data and maintains intraday state for the supported 0DTE symbols.

Responsibilities:

- maintain 1-minute and live session context
- compute VWAP, opening range, session high/low, intraday return, and volatility regime
- detect meaningful transitions, not just current score
- arm and disarm symbols
- emit signal-state events

### 2. Option Enrichment Service

Runs only for armed symbols.

Responsibilities:

- load and cache same-day contract metadata
- narrow to relevant side and strike neighborhood
- score spread candidates using delta, width, expected move, liquidity, and credit quality
- hand a small shortlist to the quote watcher

### 3. Execution Service

Turns a vetted candidate into a broker order.

Responsibilities:

- subscribe to targeted quotes for shortlisted legs
- confirm executable spread credit and quote freshness
- apply entry-pricing policy
- submit orders
- capture broker updates and fills

### 4. Position / Exit Service

Runs independently from entry.

Responsibilities:

- maintain authoritative live position state
- refresh marks
- evaluate stop / profit / force-close conditions
- reconcile local state against broker state

## Signal Engine

The signal engine is the core missing piece in the current design.

Today the system has setup analysis. That is not the same thing as a signal engine.

Setup analysis answers:

- what the current market structure looks like
- whether it is supportive right now

A signal engine answers:

- what changed
- whether the change is meaningful enough to trade
- whether the symbol should stay armed
- when the opportunity has decayed

### Signal Engine Layers

The signal engine should have three layers.

#### 1. Feature State

Continuous market measurements.

Examples:

- spot vs VWAP
- spot vs opening-range high / low
- intraday return from open
- distance to session high / low
- realized intraday volatility
- bar volume and trade-count context

This layer should be updated continuously and stored as raw state.

#### 2. Regime State

Interpretation of the feature state.

Examples:

- above VWAP / below VWAP
- inside opening range / supportive breakout / adverse break
- positive trend / negative trend / flat
- near session extreme / away from session extreme
- compressed volatility / expanding volatility

This layer converts raw numbers into explainable structure.

#### 3. Trigger State

This is the tradeable layer.

Examples:

- bullish reclaim confirmed
- bearish breakdown confirmed
- failed breakout reversal
- trend continuation after pullback
- opening drive exhaustion

This layer decides whether a symbol should arm, stay armed, or disarm.

The current setup model already covers parts of feature state and regime state. The missing part is trigger state.

### Signal Engine Inputs

- stock 1-minute bars
- stock updated bars
- optional stock trades for finer threshold triggers
- market session state
- prior signal state

### Signal Engine Outputs

- `idle`
- `arming`
- `armed`
- `candidate_ready`
- `cooldown`
- `disarmed`

Each state change should carry:

- symbol
- side bias
- event type
- current score
- score delta
- trigger reason
- expiry / TTL
- invalidation conditions

### Signal Engine State Model

```text
IDLE
  |
  | setup transition crosses arm threshold
  v
ARMING
  |
  | persistence / confirmation satisfied
  v
ARMED
  |
  | option narrowing finds tradeable structure
  v
CANDIDATE_READY
  |
  | quotes validate executable entry
  v
ENTRY_ELIGIBLE
  |
  | submit
  v
SUBMITTED
  |
  | fill
  v
OPEN
  |
  | exit or invalidate
  v
COOLDOWN
  |
  | timer or reset condition
  v
IDLE
```

### What Should Trigger Arming

The signal engine should arm on transitions, not on fixed time ticks.

Examples:

- score crossing from neutral to favorable
- VWAP reclaim or VWAP loss with follow-through
- opening-range break and hold
- opening-range failed break and reversal
- bounce from session extreme for put-credit setups
- rejection from session extreme for call-credit setups

Good triggers should include hysteresis so the system does not thrash around a single threshold.

### Arming Should Not Equal Scoring

A high score alone should not immediately mean "trade now".

Good 0DTE behavior usually comes from a sequence:

1. market structure becomes supportive
2. that support persists or confirms
3. an entry pattern appears
4. options still offer acceptable economics

That means:

- score is context
- trigger is change
- arming is a state decision
- execution is a separate decision

This separation is important because a symbol can be:

- structurally favorable but not actionable yet
- actionable briefly even if the absolute score is only modest
- no longer actionable even though the score is still technically favorable

### Hysteresis and Decay

The system should not arm and disarm on every small move. It needs explicit persistence rules.

Use:

- separate arm and disarm thresholds
- minimum persistence window before `ARMED`
- TTL on armed state
- cooldown after order submission or invalidation
- side-specific invalidation rules

Example:

- arm at score `>= 72`
- disarm at score `< 66`
- require 2 consecutive confirming state updates
- expire armed state after 120 seconds if no candidate becomes eligible

### Signal Payload

Each signal-state update should be persisted as a small event payload.

Suggested fields:

- `symbol`
- `timestamp`
- `side_bias`
- `feature_state`
- `regime_state`
- `trigger_state`
- `score`
- `score_delta`
- `armed`
- `armed_reason`
- `disarmed_reason`
- `ttl_seconds`
- `cooldown_until`

This creates a replayable event stream for:

- debugging
- trade attribution
- threshold tuning
- operator visibility

## Option Enrichment

Option enrichment should be selective.

It should not start from the full chain unless necessary. The system should already know:

- symbol
- side bias
- same-day expiry
- likely short-delta band
- likely strike neighborhood

The enrichment step should then:

- load same-day contract metadata from cache
- narrow to the correct side
- narrow to a strike window around target delta and expected move
- rank widths and short strikes
- produce only a few candidate spreads

## Targeted Quote Confirmation

The quote watcher should only monitor exact shortlisted legs.

It should answer:

- is the spread executable now
- is credit retained above the policy floor
- are quotes fresh enough
- is relative spread still acceptable

If yes, the candidate moves to `ENTRY_ELIGIBLE`. If no, the symbol stays armed until TTL expires or the setup degrades.

## Execution Policy

The execution gate should remain strict even after a symbol is armed.

Required gates:

- setup still favorable
- armed state still valid
- live quote freshness
- minimum retained edge
- session and position risk headroom
- no duplicate exposure
- cooldown not active

This means the signal engine creates opportunity, but the execution gate still decides whether to spend risk.

## Why This Is Better Than Pure Polling

Polling every symbol every minute is simple, but wasteful.

A good 0DTE system should spend compute in proportion to market opportunity:

- most of the time, most symbols stay `IDLE`
- only active symbols enter the option-enrichment path
- only shortlisted legs enter the quote-watch path

That improves:

- timeliness
- signal quality
- API efficiency
- operational clarity

## Why This Is Better Than Pure Option-Quote Reactivity

Option quotes alone are the wrong primary trigger surface for this strategy.

The strategy thesis comes from underlying intraday structure:

- VWAP
- opening range
- session trend
- session extremes

Option quotes are best used to:

- validate tradability
- validate retained credit
- tune the order price

They should confirm the entry, not invent the trade thesis.

## What The Current System Has

Current setup logic already computes the right kinds of intraday context:

- VWAP relationship
- opening-range behavior
- intraday trend
- distance to session extremes

That is the right raw material.

## What The Current System Lacks

The current system still lacks a true signal layer with:

- persistent intraday state
- transition detection
- arm / disarm semantics
- hysteresis
- TTL and cooldown
- selective option enrichment driven by signal state

That gap is the main reason the 0DTE path still behaves like a periodic scanner instead of a real intraday trading engine.

More specifically:

- current setup scoring is mostly stateless and recomputed from a fresh bar window
- favorable vs neutral vs unfavorable is too coarse for event-driven decisions
- the system records setup level, but not setup transition
- there is no distinction between "supportive backdrop" and "entry trigger"
- there is no first-class representation of armed state, persistence, or decay

## North-Star Operating Model

Use clock time for housekeeping.

Use event time for decisions.

In practice:

- housekeeping can stay on a minute cadence
- signal transitions should be event-driven
- option enrichment should be triggered by armed state
- quote watching should be short-lived and targeted

## Recommended Build Order

1. Build the signal-state model and armed-symbol registry.
2. Add stock-stream driven state updates and transition detection.
3. Move 0DTE option enrichment behind armed-symbol gating.
4. Keep targeted quote watching only for shortlisted legs.
5. Tune thresholds, TTLs, and hysteresis from observed outcomes.

## References

- [Alpaca Capabilities Statement](../research/alpaca_capabilities_statement.md)
- [Alpaca Market Data Overview](https://docs.alpaca.markets/v1.3/docs/about-market-data-api)
- [Alpaca Real-time Stock Data](https://docs.alpaca.markets/docs/real-time-stock-pricing-data)
- [Alpaca Real-time Option Data](https://docs.alpaca.markets/docs/real-time-option-data)
- [Alpaca Option Chain Snapshot](https://docs.alpaca.markets/reference/optionchain)
