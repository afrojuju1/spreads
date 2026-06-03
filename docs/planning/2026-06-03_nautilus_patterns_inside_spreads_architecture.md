# Nautilus Patterns Inside Spreads

Date: 2026-06-03

Status: target architecture planning note. No implementation has been started from this document.

Related reviews:

- [Spreads Architecture Review](./2026-06-03_spreads_architecture_review.md)
- [Nautilus Architecture Review](./2026-06-03_nautilus_architecture_review.md)
- [Trading Lifecycle State Machines](./2026-06-03_trading_lifecycle_state_machines.md)
- [Trading Lifecycle Bead Plan](./2026-06-03_trading_lifecycle_bead_plan.md)

## Decision Frame

The goal is to house Ade's trading system in one place: `spreads`.

That does not mean discarding everything learned from Nautilus. It means copying the right architecture patterns into Spreads and moving the live engine path away from Rust-hosted sidecars and standalone host services.

The target should be:

```text
one product repo
one operational source of truth
one execution and position model
one logging and dashboard surface
one Python runtime stack
```

The target should not be:

```text
Nautilus rewritten line-for-line in Python
another actor framework
another durable database
another scheduler
another operator CLI
another set of trading nouns that bypasses the objects already in Spreads
```

## Goals

- Make Spreads the canonical operator/state model for trading decisions, execution attempts, broker orders, fills, positions, closes, and logs.
- Keep the existing operator shell: web, API, CLI, Docker, jobs, logs, dashboards.
- Preserve current good boundaries: execution intents, immutable broker ledger, session positions, broker sync.
- Normalize direct feed trading into the canonical opportunity and intent flow.
- Make trading-engine behavior easier to reason about through explicit lifecycle objects, transitions, and projections.
- Replace Rust bridge dependence only after Python-native execution parity exists.
- Keep live validation as the primary acceptance path unless automated tests are explicitly requested.

## Non-Goals

- Do not rewrite unrelated product shell pieces just to rewrite them. Full replacement of trading lifecycle internals is allowed where it produces the cleaner design.
- Do not move the product into Nautilus.
- Do not introduce Kafka, Temporal, or a new workflow engine.
- Do not introduce a second Postgres database for live trading.
- Do not force every existing table into event sourcing immediately.
- Do not require a full upstream Nautilus `TradingNode` migration.
- Do not block useful Finviz long-call improvements on spread-engine parity.
- Do not require a one-broker-owner rule as a foundational prerequisite. Runtime coexistence can remain valid when provenance, state ownership, and execution contracts are explicit.
- Do not require backwards-compatible internals. Planned Spreads downtime is acceptable while trading lifecycle internals are replaced.
- Do not add dual-write paths, shadow-only lifecycle paths, or fallback runtimes solely to keep old behavior alive during the refactor.

## The Useful Nautilus Ideas

Nautilus is useful here because it shows the shape of a trading engine:

```text
Data -> Cache -> Strategy -> Risk -> Execution -> Portfolio -> Events
```

In Spreads terms, that becomes:

```text
Market data/feed source
  |
  v
Opportunity and quote read models
  |
  v
Decision engine
  |
  v
Risk and account-capacity engine
  |
  v
Intent and attempt lifecycle
  |
  v
Execution adapter
  |
  v
Broker ledger, fills, session positions
  |
  v
Exit manager, broker sync, operator dashboards
```

The point is not to copy Nautilus components by name. The point is to make ownership explicit.

## Foundational Lifecycle Context

Before adding engine vocabulary, understand the object model that already exists. These names are current context, not a backward compatibility contract.

```text
SignalState
  |
  v
Opportunity
  |
  v
OpportunityDecision
  |
  v
ExecutionIntent
  |
  v
Admission / RiskDecision
  |
  v
ExecutionAttempt
  |
  +-- ExecutionOrder
  +-- ExecutionFill
  |
  v
PortfolioPosition
  |
  v
PositionClose
```

These are not just tables. They are the main domain objects.

| Object | Current meaning |
| --- | --- |
| `SignalState` | Current state of a signal subject for a label, symbol, strategy family, profile, and session. |
| `Opportunity` | A normalized executable or monitorable candidate with canonical legs, economics, evidence, lifecycle, eligibility, and source identity. |
| `OpportunityDecision` | The bot/automation decision over an opportunity for one run and scope. |
| `ExecutionIntent` | The durable dispatch request. It has TTL, slot, claim, action type, source references, supersession, payload, and an intent event stream. |
| `Admission` / `RiskDecision` | The pre-attempt gate that records whether an intent is approved, blocked, or unknown before broker submission is allowed. |
| `ExecutionAttempt` | The immutable broker-submission attempt. It carries canonical legs, order payload, economics, source job, policies, risk decision, and broker ids. |
| `ExecutionOrder` | Broker order snapshot projection linked to an attempt. |
| `ExecutionFill` | Broker fill projection linked to an attempt and order. |
| `PortfolioPosition` | Session-owned position projection created from filled open attempts and updated by closes/reconciliation. |
| `PositionClose` | Close record linked to a close attempt, quantity, exit value, realized PnL, and broker order. |

The target architecture should preserve the important lifecycle semantics without being forced to preserve current table names, function names, or service layout.

## Existing Invariants To Preserve

The architecture should explicitly preserve these existing rules:

- `ExecutionIntent` active states are `pending`, `claimed`, `submitted`, and `partially_filled`.
- Terminal intent states include `failed`, `canceled`, `revoked`, and `expired`.
- Pending intents can expire before dispatch.
- Intents can be revoked when the source opportunity or position is no longer active.
- Intent slots prevent duplicate active entries for the same bot/strategy/symbol shape.
- Repricing creates a replacement intent and supersedes the previous intent instead of mutating history in place.
- A close intent targets a `PortfolioPosition`; an open intent targets an `Opportunity` or direct asset payload.
- Active close attempts block another close attempt for the same position.
- `ExecutionAttempt` owns broker submission facts; orders and fills are projections under it.
- Broker-sync reconciles reality into attempts, orders, fills, and positions; it does not become the strategy decision layer.
- `session_positions` owns day/session attribution and should not be bypassed by adapter work.

The Nautilus pattern to borrow is typed lifecycle discipline. The Spreads implementation should express that through the existing objects above.

## Target Architecture

Recommended target shape:

```text
                              +----------------+
                              |  Operator UI   |
                              | API / CLI / UI |
                              +-------+--------+
                                      |
                                      v
+----------------+          +-------------------+          +----------------+
| Market Sources |--------->| Spreads Engine    |--------->| Alpaca Broker  |
| feeds, quotes  |          | Python in-repo    |          | data/execution |
+----------------+          +---------+---------+          +----------------+
                                      |
                  +-------------------+-------------------+
                  |                   |                   |
                  v                   v                   v
          +---------------+   +---------------+   +----------------+
          | Postgres      |   | Redis/ARQ     |   | Logs/Metrics   |
          | facts/models  |   | jobs/pubsub   |   | Grafana/Loki   |
          +---------------+   +---------------+   +----------------+
```

Inside the Spreads Engine:

```text
MarketDataAdapter
  -> Signal/Opportunity lifecycle
  -> Decision lifecycle
  -> Intent lifecycle
  -> Admission/risk lifecycle
  -> Attempt lifecycle
  -> Execution adapter
  -> Order/fill projections
  -> Position/close lifecycle
  -> Broker reconciliation
```

These are architecture roles first. They do not need to become a giant new framework or a pile of abstract base classes.

## Component Responsibilities

### MarketDataAdapter

Use existing pieces:

- `market_recorder.py`
- `symbol_feeds.py`
- Alpaca quote snapshots
- persisted quote tables and retained market data

Responsibilities:

- provide latest known quotes
- expose quote freshness and source
- normalize feed candidates into engine-readable records
- keep market-data retention explicit

Do not create a second data capture service unless there is a concrete source that cannot fit the existing recorder/feed model.

### SignalEngine

Use existing pieces:

- `opportunity_generation.py`
- `opportunities.py`
- `signal_state.py`
- discovery services
- Finviz feed outputs

Responsibilities:

- normalize every tradeable idea into an opportunity-like record
- make direct feed lanes visible in the same opportunity lifecycle as scanner lanes
- keep source-specific fields in payloads, not in separate control paths

Main change implied later:

```text
Finviz feed candidate -> Opportunity -> OpportunityDecision -> ExecutionIntent
```

instead of:

```text
Finviz feed candidate -> finviz_direct_trading special path -> ExecutionIntent
```

### DecisionEngine

Use existing pieces:

- `decision_engine.py`
- `finviz_direct_trading.py` rule logic
- opportunity decisions
- automation config

Responsibilities:

- decide skip/no-entry/selected-blocked/selected
- preserve clear skip reasons
- write decisions for operator audit
- avoid hidden lane-specific semantics

Borrow from Nautilus:

```text
Skip
NoEntry
RiskBlocked
SelectedBlocked
Selected
```

This gives better operator language than a generic "no trade".

### RiskEngine

Use existing pieces:

- `risk_manager.py`
- `account_capacity.py`
- bot and automation caps
- broker permission/capability checks

Responsibilities:

- centralize account, strategy, symbol, sector, daily, and open-position caps
- distinguish broker capability blocks from strategy rejections
- run before any execution adapter submission
- publish durable reasoned blocks

Important direction: account capabilities should be Spreads-owned, not hidden in a Rust fleet registry.

### Admission Lifecycle

Use existing pieces:

- `risk_manager.py`
- `risk_decisions`
- execution admission errors
- close validation and exit-manager guards

Responsibilities:

- decide whether an intent can become an execution attempt
- keep `approved`, `blocked`, and `unknown` distinct
- persist policy snapshot, metrics, blockers, evidence, and reason codes
- attach approved admission decisions to attempts
- fail or hold intents before attempt creation when admission is blocked or unknown

Direct paths such as Finviz long calls should move through this lifecycle too. They should not rely only on lane-local caps and validation once they are part of the unified trading core.

### Intent And Attempt Lifecycle

The existing intent and attempt path is the first engine boundary. Do not replace it with a generic OMS abstraction before the current invariants are documented and preserved.

Use existing pieces:

- `execution_intents`
- `execution/attempts.py`
- execution models
- close/reprice logic from `exit_manager.py`
- `execution_lifecycle.py`

Responsibilities:

- accept execution intents as the durable dispatch request
- preserve claim, TTL, slot, revocation, and supersession semantics
- create immutable execution attempts
- build broker order payloads from canonical legs and policy snapshots
- classify pending, unknown, working, terminal, stale, and filled attempt states
- handle retry, replacement, cancel, and close attempts without mutating history in place
- ensure no duplicate submission on worker restart

This should be a clear lifecycle model. It may wrap, simplify, or replace today's implementation where the rewrite is cleaner.

### ExecutionAdapter

Use existing pieces:

- `alpaca_direct`
- Nautilus bridge payload builder as reference
- Alpaca REST clients already used by Spreads

Responsibilities:

- submit single-leg and multi-leg orders
- cancel and replace orders
- fetch broker order snapshots
- fetch fills and activities
- return normalized execution events

Long-term target:

```text
Spreads SubmitOrderCommand -> Python Alpaca adapter -> ExecutionEvents
Spreads SubmitOrderListCommand -> Python Alpaca adapter -> ExecutionEvents
```

instead of:

```text
Spreads handoff JSON -> Rust bridge subprocess -> Alpaca -> bridge result JSON
```

### PortfolioEngine

Use existing pieces:

- `broker_sync.py`
- `session_positions.py`
- `execution_portfolio.py`
- `account_state.py`

Responsibilities:

- reconcile broker truth
- project open positions and PnL
- preserve session ownership
- connect fills to intents and attempts
- expose operator-ready state

Do not let this become an order submission path. It should remain reconciliation and projection.

### ManagementEngine

Use existing pieces:

- `exit_manager.py`
- position exit jobs
- Finviz long-call exit policy
- Nautilus close-management patterns as reference

Responsibilities:

- evaluate profit target, stop loss, max hold, stale order, and force-close rules
- create close intents through the same lifecycle path as entries
- maintain close attempt counts and reasons
- avoid one-off close behavior per lane

This is one of the highest-value consolidation areas because entry logic is only half the trading system. Exit behavior must be equally visible and durable.

## Command And Event Model

Spreads already has event infrastructure and an `execution_intent_events` stream. The target is to use those deliberately around the real objects, not to add a second event system.

Object-scoped command examples:

```text
EvaluateOpportunity
CreateExecutionIntent
ClaimExecutionIntent
EvaluateAdmission
CreateExecutionAttempt
SubmitExecutionAttempt
RefreshExecutionAttempt
CancelAttemptForReprice
CreateReplacementIntent
CreateCloseIntent
SyncBrokerState
```

Object-scoped event examples:

```text
OpportunityEvaluated
OpportunityDecisionRecorded
ExecutionIntentCreated
ExecutionIntentClaimed
ExecutionIntentRevoked
ExecutionIntentExpired
ExecutionIntentReplaced
AdmissionApproved
AdmissionBlocked
AdmissionUnknown
RiskCheckPassed
RiskCheckBlocked
ExecutionAttemptQueued
OrderSubmitted
OrderAccepted
OrderRejected
OrderPartiallyFilled
OrderFilled
OrderCanceled
PositionOpened
PositionClosed
BrokerSyncCompleted
ExitRuleTriggered
```

Recommended rule:

```text
Commands request something.
Events say something happened.
Tables project what the operator needs to see.
```

Do not make every service publish every tiny internal detail as a domain event. Start with trading lifecycle facts that affect money, position state, or operator decisions.

## State Ownership

Use the current durable owners as reference points, then choose the clean target owner for each lifecycle fact:

| State | Owner |
| --- | --- |
| Trading command/event facts | `event_log`, promoted engine event subset |
| Dispatch request | `execution_intents` |
| Admission decisions | `risk_decisions` today, or a replacement admission fact table if the rewrite makes that cleaner |
| Submission attempts | `execution_attempts` |
| Broker orders | `execution_orders` |
| Broker fills | `execution_fills` |
| Day/session position ownership | `session_positions` |
| Current portfolio projection | `portfolio_positions` and account read models |
| Opportunity lifecycle | `signal_states`, `opportunities`, `opportunity_decisions` |
| Runtime work | `job_runs`, ARQ queues, job leases |

These names are not storage compatibility requirements. If a replacement table or projection is cleaner, prefer the replacement and document how existing data is archived, backfilled, discarded, or reset.

## Runtime Loop

Keep ARQ and scheduled jobs, but make the engine tick explicit:

```text
scheduler
  |
  +-- feed refresh jobs
  +-- opportunity normalization jobs
  +-- engine evaluation jobs
  +-- execution dispatch jobs
  +-- broker sync jobs
  +-- position management jobs
```

The immediate improvement is not a new process. It is clearer job roles and shared command/event semantics.

Later, if one flow needs stricter deterministic ordering, add a narrow lifecycle worker around the existing objects:

```text
lifecycle_tick(scope)
  |
  +-- expire/revoke stale intents
  +-- claim pending intents
  +-- refresh active attempts
  +-- create replacement intents when needed
  +-- submit eligible attempts
  +-- sync order/fill/position projections
```

That is enough. It does not need a full actor framework.

## Runtime Coexistence And Submission Provenance

Do not make single broker ownership a prerequisite.

The better foundation is explicit provenance and shared state semantics. A runtime can coexist with Spreads when its role is clear and when any order path that participates in Spreads-owned flows writes or reconciles through Spreads' canonical objects.

Required provenance fields:

- execution runtime
- source job type/key/run id
- bot and automation ids when applicable
- strategy config id and config hash when applicable
- execution intent id when applicable
- execution attempt id when applicable
- broker order id and client order id
- position id for closes or reconciled positions

Bridge and standalone Nautilus paths can remain valid only as clearly scoped external runtimes. They do not need compatibility hooks in the new Spreads lifecycle. The architectural line is not "only one process may talk to Alpaca." The line is "Spreads' operator truth cannot be bypassed for flows Spreads is expected to observe, manage, or close."

Bridge retirement path:

```text
current: Spreads -> Rust bridge -> Alpaca
next:    Spreads -> Python adapter validation path -> Alpaca
final:   Rust bridge disabled for live Spreads runtime
```

## Config Ownership

Keep Spreads YAML and env-based deployment conventions.

Borrow from Nautilus:

- account capability profiles
- account risk budgets
- strategy family permissions
- broker permission checks
- explicit dry-run versus submit mode
- per-account kill switches

Avoid copying:

- separate TOML strategy runtime config
- separate fleet registry as the primary live owner
- duplicate state paths under Nautilus

Recommended target:

```text
packages/config/accounts/
packages/config/automations/
packages/config/jobs/
runtime env for secrets and endpoints
```

The exact paths can be decided during implementation, but account capabilities should be first-class config in Spreads.

## Logging And Observability

All engine components should log through the shared logging convention and include stable fields:

```text
log_origin_project=spreads
log_origin_service=<api|scheduler|worker-runtime|worker-discovery|market-recorder|engine>
component=<engine component>
account_id=<broker account or logical paper account>
strategy_family=<family when known>
symbol=<underlying when known>
intent_id=<id when known>
attempt_id=<id when known>
order_id=<broker order id when known>
correlation_id=<event correlation id>
```

Nautilus bridge logs can remain tagged as `log_origin_project=nautilus` while the bridge exists. Long term, live order submission should not require hunting through separate host service logs.

Quote/log retention should be an explicit part of the market-data adapter boundary. Latest quote read models and historical quote archives should not grow forever without pruning or partitioning.

## Migration Plan

### Phase 0: Domain Model Alignment

Outcome:

- Document the existing lifecycle objects and invariants as the foundation.
- Define the clean target lifecycle objects without treating the current schema as fixed.
- Decide which facts are authoritative for signal, decision, intent, attempt, order, fill, position, and close.

No code required except docs/config decisions.

### Phase 1: Lifecycle Contracts In Spreads

Outcome:

- Define object-scoped command and event names.
- Map each command/event to the target facts and projections, using existing tables and service owners only as input.
- Draw compact state diagrams for `ExecutionIntent`, `ExecutionAttempt`, `PortfolioPosition`, and close attempts.

This should be a narrow design pass, not a framework build.

### Phase 2: Normalize Finviz Into Canonical Opportunity Flow

Outcome:

- Finviz feed candidates produce canonical opportunities or signal states.
- Existing direct trading rules become a decision engine policy.
- Operator views show Finviz decisions in the same audit model as other automations.

This removes the biggest active lane split.

### Phase 3: Admission Lifecycle Boundary

Outcome:

- Admission is first-class before attempt creation.
- Direct Finviz and canonical automation paths share the same open-admission language.
- Close validation either becomes a close-admission lifecycle or is explicitly kept separate with documented reasons.

### Phase 4: Intent And Attempt Lifecycle Boundary

Outcome:

- `execution_intents` are consumed by one lifecycle path.
- Entry, close, and reprice attempts share idempotency and lifecycle handling.
- Skip/block/submit/close reasons are represented consistently.

This can replace existing execution services where wrapping would preserve the wrong shape.

### Phase 5: Python-Native Multi-Leg Execution Adapter

Outcome:

- Build parity for the order behaviors currently delegated to the Rust bridge.
- Use the bridge as a reference implementation, not a required fallback.
- Compare attempts, broker snapshots, fills, and session positions.

This is where moving away from Rust becomes real.

### Phase 6: Close Management Consolidation

Outcome:

- Profit target, stop loss, force close, stale order, reprice, and max-hold behavior all create close intents through the same lifecycle path.
- Close lifecycle becomes visible in the same ledger as entries.

### Phase 7: Retire Rust Live Runtime Dependencies

Outcome:

- Spreads no longer needs a rebuilt Nautilus bridge for live paper operation.
- Nautilus remains a reference repo, research sandbox, or source of backtest/engine ideas only.

## First Implementation Candidates

When this moves from architecture to implementation, the first beads should be small:

1. Document lifecycle state diagrams for `ExecutionIntent`, `ExecutionAttempt`, `PortfolioPosition`, and close attempts.
2. Define the clean target lifecycle object model, using current Spreads and Nautilus only as input.
3. Add a thin object-scoped command/event vocabulary around the chosen lifecycle model.
4. Normalize Finviz decisions into the canonical opportunity-decision audit surface.
5. Make open admission/risk decisions first-class across Finviz and canonical automation paths.
6. Build a Python-native multi-leg order-list adapter behind a feature flag.

These are intentionally incremental. The system should get cleaner after each bead.

## Main Risks

- Accidentally creating a second engine inside Spreads instead of clarifying the existing one.
- Retiring the bridge before Python-native order-list behavior has paper parity.
- Treating runtime coexistence as safe without provenance, reconciliation, and clear state ownership.
- Losing clear skip/block reasons while normalizing Finviz into opportunities.
- Overfitting the design to long calls and under-designing close/order-list lifecycle.
- Growing `event_log` and quote/log tables without retention boundaries.

## Architecture Recommendation

Proceed with a Spreads-owned lifecycle architecture.

Use Nautilus as a pattern source, especially for:

- typed lifecycle objects
- object-scoped commands
- explicit risk before execution
- cache/read model before strategy decision
- adapter isolation
- account-scoped policy
- deterministic lifecycle events
- clear strategy decision outcomes
- reconciliation as a first-class concern

Do not continue making the Rust fork the center of gravity. The bridge was useful. The standalone host engines taught us a lot. But the durable operator product is Spreads, and the architecture should now define the right lifecycle model clearly enough that order submission and position management can move there cleanly, including full rewrites where that is the better design.
