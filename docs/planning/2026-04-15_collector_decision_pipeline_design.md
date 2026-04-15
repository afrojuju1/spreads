# Collector Decision Pipeline Design

Status: partial reference (partially incorporated on 2026-04-15)

Incorporation note:

- the collector-versus-decision split in this document remains valid
- `OpportunityDecision` from this document is adopted by [Current-System Options Automation Implementation Approach](./2026-04-15_current_system_options_automation_implementation_approach.md)
- the newer top-level runtime nouns are now `Bot`, `StrategyConfig`, `Automation`, and `StrategyPosition`
- `opportunity_execution` in this document should be read as an entry-specific precursor to a broader `ExecutionIntent` concept, not the final universal action noun

As of: Wednesday, April 15, 2026

Related:

- [Current-System Options Automation Implementation Approach](./2026-04-15_current_system_options_automation_implementation_approach.md)
- [Fresh Spread Opportunity System Design](./2026-04-11_fresh_spread_system_design.md)
- [Spread Selection Review And Refactor Plan](./2026-04-11_spread_selection_refactor_plan.md)
- [Collector Decision Pipeline Data Model](./2026-04-15_collector_decision_pipeline_data_model.md)
- [Signal State Platform](./signal_state_platform.md)
- [Trading Engine Architecture](./trading_engine_architecture.md)
- [Execution Templates](./2026-04-12_execution_templates.md)
- [Evaluation And Rollout Plan](./2026-04-12_evaluation_and_rollout_plan.md)

## Goal

Define the improved runtime architecture for the spreads system in a way that is:

- simpler than the earlier broad clean-sheet design
- compatible with the current codebase
- durable enough to implement without creating another temporary path

This document is focused on one architectural correction:

- the collector-driven runtime must be built as a decision pipeline, not as a collector loop that sometimes executes

## What We Are Building

The system is not just:

- a scanner
- a watchlist
- a set of label-specific collectors

The system is a portfolio-aware options decision engine.

Its job is to:

1. discover opportunities
2. maintain current opportunity truth
3. decide what is actionable at the portfolio level
4. convert that selection into an `opportunity_execution` record
5. submit and manage `opportunity_execution` safely
6. support replay, audit, and operator visibility from the same artifacts

That means the stable core nouns should be:

- `strategy`
- `opportunity`
- `opportunity_decision`
- `opportunity_execution`

Presentation nouns such as `board`, `watchlist`, `pipeline`, and `uoa` remain useful, but they are views over the core decision pipeline, not the core architecture itself.

## Why The Current Shape Is Not Enough

The current system has already improved materially:

- `market_recorder.py` is now the sole Alpaca option websocket owner
- `live_runtime.py` is the canonical loader for current collector-backed runtime state
- current-session opportunities are read from one canonical signal/opportunity store

But one major coupling problem remains:

- `live_collector.py` still owns discovery, signal sync, action gating, alerts, and automatic execution inside one collection tick

That keeps the system collector-centric instead of decision-centric.

The practical consequences are:

- each collector still plans for itself
- allocation is recomputed ad hoc from a single `cycle_id`
- execution does not consume a durable `opportunity_execution` artifact
- portfolio-wide selection across labels and styles is weak
- alerting and actionability remain too tightly coupled to scan rank

## Design Principles

1. Keep one canonical opportunity store.
2. Separate discovery from decisioning.
3. Separate signal truth from `opportunity_decision` truth from `opportunity_execution` truth.
4. Make portfolio-aware action selection a first-class runtime stage.
5. Keep APIs, ops views, and UOA as read models, not business-logic owners.
6. Persist the minimum `opportunity_decision` and `opportunity_execution` artifacts needed for audit, replay, and deterministic execution.
7. Do not persist broad model hierarchies in the runtime unless they materially improve runtime decisions.
8. Prefer a modular monolith with explicit service boundaries over new distributed runtime services.

## Target Runtime Flow

```text
                         TARGET COLLECTOR DECISION FLOW

    +---------------------------+
    | market_recorder.py        |
    |---------------------------|
    | sole Alpaca stream owner  |
    | quote/trade ingestion     |
    | recorder-backed rows      |
    +-------------+-------------+
                  |
                  v
    +---------------------------+
    | jobs/live_collector.py    |
    |---------------------------|
    | discovery worker          |
    | scan + enrich             |
    | score candidates          |
    | capture session summary   |
    | no inline auto execution  |
    +-------------+-------------+
                  |
                  v
    +---------------------------+
    | services/signal_state.py  |
    | services/opportunities.py |
    |---------------------------|
    | canonical opportunity     |
    | store + signal truth      |
    +-------------+-------------+
                  |
                  v
    +===========================+
    | services/decision_engine  |
    |---------------------------|
    | read fresh opps           |
    | read risk/control state   |
    | rank + allocate           |
    | persist opportunity_      |
    | decisions                 |
    | persist selected          |
    | opportunity_executions    |
    +-------------+-------------+
                  |
                  v
    +---------------------------+
    | services/execution.py     |
    |---------------------------|
    | consume opportunity_      |
    | executions                |
    | validate current quotes   |
    | submit orders             |
    | persist lifecycle         |
    +-------------+-------------+
                  |
                  v
    +---------------------------+
    | broker / positions / risk |
    | audit / reconciliation    |
    +---------------------------+


                             READ / OBSERVE PATHS

    live_runtime.py     -> collector-backed current runtime state
    pipelines.py        -> pipeline-facing runtime projection
    uoa_state.py        -> UOA projection
    ops_visibility.py   -> operator summaries
    opportunity_replay.py -> offline replay and evaluation
```

## Service Boundaries

### 1. Market Data Capture

Owner:

- `services/market_recorder.py`

Responsibilities:

- own the Alpaca option websocket connection in normal runtime
- persist quote and trade rows
- maintain capture target coverage

Non-goals:

- ranking opportunities
- choosing trades
- executing trades

### 2. Discovery

Owner:

- `jobs/live_collector.py`

Responsibilities:

- run the scanner and targeted enrichment
- read recorder-backed quote and trade rows
- build current cycle candidate sets
- record cycle events and capture summaries
- hand off persisted opportunities to the canonical store

Non-goals:

- portfolio allocation
- final action selection
- inline automatic execution

### 3. Signal And Opportunity State

Owners:

- `services/signal_state.py`
- `services/opportunities.py`

Responsibilities:

- persist current opportunities
- maintain current signal state per symbol
- expire absent opportunities cleanly
- expose canonical current opportunity reads

This is the current state platform for runtime options decisioning.

It owns signal truth:

- what opportunities are active
- which symbols are armed or blocked
- which current candidates exist

It does not own final action selection.

### Discovery Tiers Versus Final Decisions

The current collector path already assigns coarse discovery states such as:

- `promotable`
- `monitor`
- `analysis_only`

That is acceptable as a discovery-stage filter.

Those states should mean:

- whether an opportunity is worthy enough to stay in the current store
- whether it should stay visible for monitoring
- whether it is analysis-only and not eligible for runtime action

They should not mean:

- final portfolio allocation
- final action selection
- final execution ordering

In other words:

- discovery may decide whether an opportunity remains in the candidate set
- decisioning decides whether the system should actually act on it

This keeps `selection_state` as a coarse eligibility tier instead of a hidden second planner.

### 4. Decisioning

New owner:

- `services/decision_engine.py`

Responsibilities:

- load active opportunities across the current planning scope
- apply ranking, portfolio allocation, and actionability logic
- incorporate portfolio, exposure, risk, and control state
- write durable `opportunity_decision` rows and selected `opportunity_execution` rows
- revoke or expire stale opportunity_executions when opportunity truth changes materially

This is the missing runtime stage today.

It owns decision truth:

- what the system would act on
- why it selected or rejected each current opportunity
- which current opportunities produced selected opportunity_executions

It should absorb and evolve the planning logic that currently lives in:

- `services/opportunity_execution_plan.py`
- the auto-selection path in `services/execution.py`

### 5. Execution

Owner:

- `services/execution.py`

Responsibilities:

- consume a persisted `opportunity_execution` row
- validate entry conditions at submit time
- resolve passive or reactive pricing
- submit, refresh, and reconcile broker orders
- persist execution lifecycle state

It owns execution truth:

- what the system intended to submit
- what the broker accepted, rejected, canceled, or filled

It should stop recomputing the execution plan from `cycle_id` inside the submission path.

### 6. Risk, Portfolio, And Control

Owners:

- `services/risk_manager.py`
- `services/execution_portfolio.py`
- `services/control_plane.py`

Responsibilities:

- account and exposure limits
- policy rollout and mode gating
- kill switches and trading controls
- current portfolio and open-activity state

These services should be dependencies of decisioning and execution, not hidden side logic embedded in collector ticks.

### 7. Read Models And Operator Views

Owners:

- `services/live_runtime.py`
- `services/pipelines.py`
- `services/uoa_state.py`
- `services/ops_visibility.py`

Responsibilities:

- expose current runtime state to APIs, CLI, and ops surfaces
- project collector state, `opportunity_decision` state, and `opportunity_execution` state into views

Non-goals:

- make trade decisions
- recompute ranking or allocation

## Strategy Model

One important thing must stay explicit in this architecture:

- a strategy is not the same thing as an opportunity
- a strategy decision is not the same thing as an opportunity_execution

The runtime needs three separate layers:

1. strategy
2. strategy decision
3. opportunity instance

If those stay blurred together, the system falls back into ad hoc candidate selection with scattered special cases.

### 1. Strategy

This is the static strategy catalog.

It is not per cycle and not per symbol.

Its purpose is to define the stable contract for a strategy family:

- what kind of thesis it expresses
- what signal patterns should activate it
- what evidence is required before it is considered valid
- what kind of product structure it requires
- what products and styles are admissible
- what portfolio and risk posture it assumes
- what execution template family it maps to
- what risk and complexity traits it carries

In other words, strategy must include four rule families:

1. signal rules
2. construction rules
3. decision rules
4. execution rules

#### Signal Rules

Signal rules define what wakes a strategy up in the first place.

They should answer:

- which signal sources matter
- which trigger patterns matter
- what persistence or confirmation is required
- what invalidates the setup
- what evidence must be attached for downstream decisioning

Representative strategy fields for signal rules:

- `signal_sources`
- `trigger_rules`
- `confirmation_rules`
- `invalidation_rules`
- `evidence_requirements`

#### Construction Rules

Construction rules define what concrete candidate shapes the strategy is allowed to build.

They should answer:

- which structures are admissible
- what delta, width, expiration, and liquidity constraints apply
- when the correct result is `pass` rather than forcing a candidate

Representative strategy fields for construction rules:

- `builder_constraints`
- `product_constraints`
- `style_profile_rules`
- `horizon_rules`

#### Decision Rules

Decision rules define how the strategy should behave once a candidate exists.

They should answer:

- when the family is preferred, allowed, discouraged, or blocked
- what portfolio posture it assumes
- what concentration or correlation rules apply
- when current positions or open orders should suppress it

Representative strategy fields for decision rules:

- `portfolio_rules`
- `risk_overlays`
- `concentration_rules`
- `blocking_rules`

#### Execution Rules

Execution rules define how the chosen strategy should be entered and managed.

They should answer:

- which entry template applies
- what quote-quality and readiness conditions are required
- what timeout and replace behavior applies
- which default exit template family it maps to

Suggested core fields:

- `strategy_family`
- `display_name`
- `thesis_kind`
- `directional_type`
- `premium_profile`
- `leg_structure`
- `signal_sources`
- `trigger_rules`
- `confirmation_rules`
- `invalidation_rules`
- `evidence_requirements`
- `builder_constraints`
- `product_constraints`
- `style_profile_rules`
- `horizon_rules`
- `portfolio_rules`
- `risk_overlays`
- `concentration_rules`
- `blocking_rules`
- `default_execution_template`
- `default_exit_template`
- `allowed_product_classes`
- `allowed_style_profiles`
- `assignment_risk_class`
- `complexity`
- `supports_reactive`
- `supports_tactical`
- `supports_carry`
- `default_enabled`

Representative examples:

- `long_call`
- `long_put`
- `call_debit_spread`
- `put_debit_spread`
- `call_credit_spread`
- `put_credit_spread`
- `iron_condor`

This can begin as a canonical registry in code rather than a new database table.

The key point is that `strategy_family` should stop behaving like an unstructured string with scattered behavior across:

- signal activation logic
- scanner logic
- selection logic
- replay logic
- execution logic

The key architectural correction is:

- strategy must say not only what a strategy is
- it must also say what signals activate it, what confirms it, and what invalidates it

### 2. Strategy Decision

This is the runtime policy outcome for a strategy family under current conditions.

It is per symbol and per planning scope, not global and not static.

Its purpose is to answer:

- did the strategy actually activate for this symbol under its signal rules
- is this family preferred, allowed, discouraged, blocked, or pass right now
- under which style posture
- under which horizon posture
- with which blockers or evidence

This is the runtime shape that the current design was missing.

The earlier planning docs called this `StrategyIntent` and `HorizonIntent`.

That remains a useful concept, but the runtime does not need the full clean-sheet hierarchy on day one.

The minimum strategy decision payload should include:

- `symbol`
- `strategy_family`
- `activation_state`
- `activation_reason`
- `policy_state`
- `desirability_score`
- `style_profile`
- `horizon_intent`
- `product_class`
- `blockers`
- `policy_ref`
- `evidence`

This can start as:

- a structured payload embedded in opportunity evidence
- or a lightweight persisted `opportunity_decision` artifact written by `decision_engine.py`

The important part is that the system has one canonical place where current strategy admissibility is decided.

### 3. Opportunity Instance

An opportunity is one concrete candidate produced under a strategy decision.

It is not the strategy itself.

It should represent:

- one symbol
- one concrete structure
- one expiration choice
- one set of legs
- one current execution shape
- one current economics snapshot

That means an opportunity should inherit strategy truth rather than invent it ad hoc.

An opportunity should carry:

- `strategy_family`
- `style_profile`
- `horizon_intent`
- `product_class`
- `execution_shape`
- `strategy_metrics`
- `evidence`

But those fields should be understood as:

- copied or derived from strategy and strategy decision
- plus concrete candidate-specific structure and pricing

### Recommended Runtime Rule

The runtime pipeline should follow this order:

1. strategy defines what a family is
2. strategy decision decides whether that family fits now
3. opportunity captures one concrete candidate built under that decision
4. decision engine decides whether to allocate and act on that opportunity
5. execution consumes a persisted `opportunity_execution` row for that opportunity

This is the clean separation that is currently only partially present in the codebase.

### What This Fixes

This gives the system a better answer to:

- where strategy truth lives
- what signal rules actually activate a strategy
- how `strategy_family` differs from raw legacy `strategy`
- how `style_profile` and `horizon_intent` should be interpreted
- why replay and runtime should use the same strategy vocabulary
- why opportunity rows should point back to strategy reasoning instead of burying it in custom evidence blobs

### What Not To Overbuild

The first implementation still should not:

- create a full live table graph for every planning-doc entity by default
- force all strategy policy logic into new persistence before runtime uses it
- build a huge registry framework if a simple canonical module is enough

The practical first step is smaller:

- define one canonical strategy module
- define one canonical signal-spec shape inside that module
- define one canonical runtime strategy decision payload
- make opportunity and `opportunity_decision` artifacts use that vocabulary consistently

That is enough to make the collector -> opportunity -> opportunity_decision -> opportunity_execution pipeline structurally coherent.

## Canonical Runtime Artifacts

The runtime does not need every domain object persisted immediately.

The minimum durable artifacts should be:

### `opportunity`

Current owner:

- `signal_state.py`

Purpose:

- canonical current candidate
- current signal/opportunity truth

### `opportunity_decision`

Purpose:

- record the per-opportunity outcome of a planning pass

Suggested fields:

- `id`
- `opportunity_id`
- `run_key`
- `session_date`
- `market_session`
- `scope`
- `source`
- `policy_ref`
- `state`
- `score`
- `rank`
- `reason_codes`
- `superseded_by_id`
- `decided_at`
- `payload`

### `opportunity_execution`

Purpose:

- selected opportunity_execution plus submit lifecycle

Suggested fields:

- `id`
- `opportunity_decision_id`
- `opportunity_id`
- `run_key`
- `state`
- `slot_key`
- `rank`
- `expires_at`
- `payload`

## Decisioning Semantics

The decision engine should be portfolio-aware and scope-aware.

It should not plan only from one collector label or one cycle unless explicitly requested.

The default planning scope should be:

- all active opportunities for the current market date

The decision engine should answer:

1. Which opportunities are currently promotable?
2. Which of those are actually allocatable given portfolio and risk state?
3. Which one or few are action-worthy now?
4. Which execution template applies?
5. Which opportunity_executions should be active, revoked, expired, or skipped?

## Decision Lifecycle

The runtime needs an explicit decision lifecycle.

Without it, the new stage will still degrade into ad hoc planning plus implicit side effects.

### `opportunity_decision`

`opportunity_decision` represents one opportunity outcome inside a planning pass.

Recommended states:

- `selected`
- `rejected`
- `blocked`
- `superseded`

Rules:

- every in-scope opportunity should receive one decision outcome per completed planning pass
- rejection reasons should be explicit, not inferred from missing rows
- rows sharing the same `run_key` belong to the same planning pass

### `opportunity_execution`

`opportunity_execution` is the selected action row plus submit lifecycle.

Recommended states:

- `pending`
- `claimed`
- `submitted`
- `partially_filled`
- `filled`
- `canceled`
- `expired`
- `revoked`
- `failed`

Rules:

- an opportunity_execution becomes `pending` when decisioning selects an opportunity for execution
- it becomes `claimed` when execution begins consuming it
- it becomes `submitted` after broker submission succeeds
- it becomes `filled` or `partially_filled` as broker state progresses
- it becomes `expired` when its opportunity, pricing window, or timing assumptions are no longer valid
- it becomes `revoked` when a newer planning pass invalidates it before completion

### Duplicate-Submit And Supersession Rules

The design must explicitly prevent duplicate action on superseded ideas.

Required rules:

- only one active open execution per opportunity at a time
- only one active opportunity_execution per mutually exclusive decision slot
- execution must atomically claim an opportunity_execution row before submission
- a newer planning pass must revoke older still-active opportunity_executions before those opportunity_executions can be consumed

This is a core runtime safety property, not an implementation detail.

## Decision Clock

The system needs one explicit decision clock.

The recommended first implementation is:

- run a decision pass after each successful collector cycle that changed current opportunities
- allow a low-frequency fallback planning tick to catch missed transitions or stale executions
- use idempotency by planning scope and source-cycle set so duplicate runs do not create duplicate active executions

This keeps the first implementation simple and compatible with the current job model.

It avoids introducing a second independent event bus before the core decision stage exists.

## Decision Orchestration Model

The new decision stage needs a concrete job model.

The recommended first implementation is:

- collector completion is the main trigger
- a dedicated decision job owns planning and opportunity_execution-row writing
- a low-frequency fallback decision tick handles recovery and stale opportunity_executions

### Trigger Sources

Primary trigger:

- successful collector cycle that changed current opportunity truth

Secondary triggers:

- stale active opportunity_execution without execution progress
- recovery after restart
- operator-requested replan
- control-plane or policy rollout change that invalidates current decisions

### Job Shape

Recommended job shape:

- one `decision_engine` job type
- one planning scope per run
- explicit idempotency key derived from:
  - `session_date`
  - `scope`
  - normalized `source`
  - `policy_ref`

### Concurrency Rules

- decision jobs for the same planning scope should serialize
- collector jobs do not need to serialize globally, but they should not directly submit orders
- when multiple collector cycles complete close together, only the latest relevant decision set should remain current

### Replan Versus Skip

The decision job should skip writing a new current decision set when:

- no current opportunity rows changed materially
- no relevant portfolio, control, or policy inputs changed
- no active opportunity_execution became stale, expired, or revoked

Otherwise it should replan and supersede the prior decision set for that scope.

## Portfolio Constraint Model

The current allocation logic is still provisional and too implicit.

The design should make clear which portfolio constraints are first-class inputs to decisioning.

Minimum first-class constraints:

- per-style budget
- per-symbol active slot limit
- strategy-family concentration limit
- current open-position interaction
- open-order interaction
- maximum concurrent entry count
- gross downside budget

Second-wave constraints that can be deferred but should be named:

- correlated-underlying buckets
- macro/event concentration
- premium profile concentration
- expiry ladder concentration

### Initial Recommendation

The first implementation should keep the allocator simple but explicit:

- one canonical allocator inside `decision_engine.py`
- portfolio constraints represented as structured inputs, not scattered helper logic
- decision outcomes recorded as explicit `opportunity_decision` rows with rejection reasons

## Policy And Versioning Model

The decision stage only becomes replayable and auditable if policy versioning is explicit.

At minimum, each planning pass should be tied to one `policy_ref` that resolves:

- strategy module version
- decision policy version
- execution template version
- risk policy version
- control-plane rollout version where applicable

This does not require one giant combined policy document.

It does require one canonical reference payload that lets replay answer:

- which policy stack produced this opportunity_decision set
- whether a new planning pass is genuinely different or just reprocessed data

## Recovery And Reconciliation

The design needs a restart and recovery story.

After crash, restart, or worker interruption, the system should be able to answer:

- which planning pass was current
- which opportunity_executions were still active
- which opportunity_executions had already been claimed by execution
- which opportunity_executions had submitted or partially submitted broker orders

### Recovery Rules

- incomplete `running` planning passes should not become current by default after restart
- active opportunity_executions should be revalidated against current opportunity, policy, and timing assumptions
- claimed or selected opportunity_executions should reconcile against broker/execution state before new opportunity_executions are activated
- collector and decision recovery should not generate duplicate entry submissions

### Reconciliation Boundary

Decision reconciliation and execution reconciliation are related but different:

- decision reconciliation answers whether the current opportunity_execution set is still valid
- execution reconciliation answers what actually happened at the broker

Those should remain separate truth surfaces.

## Alert Ownership

Alerting should be split by architectural stage.

### Collector Alerts

Owned by discovery and capture paths.

Examples:

- recorder coverage degraded
- collector cycle failed
- opportunity publication stalled
- quote or trade capture unhealthy

### Decision Alerts

Owned by the decision stage.

Examples:

- no opportunities cleared allocation
- active opportunity_execution revoked by policy or risk
- planning pass failed
- planning scope degraded or stale

### Execution Alerts

Owned by execution and lifecycle paths.

Examples:

- submission rejected
- stale broker acknowledgement
- reconciliation mismatch
- unexpected terminal broker state

This split prevents scan-quality alerts from being confused with actual actionability or execution incidents.

## Entry And Exit Scope

This design is primarily about entry decisioning.

That is intentional.

The current missing runtime stage is on the entry side:

- discover opportunity
- decide whether to act
- persist selected opportunity_execution
- submit and manage opportunity_execution lifecycle

Exit management remains critical, but it should not block the entry pipeline refactor.

For the first implementation:

- entry decisioning becomes first-class
- exit policy stays attached to `opportunity_execution`
- exit orchestration can remain in the current execution and lifecycle path until a dedicated exit engine is warranted

This is an explicit deferral, not an omission.

The first implementation should say clearly:

- entry pipeline refactor is in scope
- dedicated exit engine is out of scope
- exit behavior still needs versioned template ownership and auditability

## What Not To Build Yet

To avoid overengineering, the first implementation should not do these things:

1. Do not persist the full clean-sheet hierarchy of:
   - `RegimeSnapshot`
   - `StrategyIntent`
   - `HorizonIntent`
   as first-class runtime tables unless the decision path actually consumes them.
2. Do not introduce a second opportunity store.
3. Do not make one planner per collector label.
4. Do not split the modular monolith into separate deployable services.
5. Do not make ops or API surfaces the owner of decision logic.

The current runtime can stay simpler:

- opportunities are the handoff object
- opportunity_decisions and opportunity_executions are the new durable runtime outputs
- richer regime and strategy objects can remain replay-first until runtime decisioning proves they are necessary

## Migration Shape

The intended implementation order is:

### 1. Remove inline auto-execution from `live_collector.py`

Collectors should stop after:

- cycle completion
- signal sync
- persisted opportunity updates

### 2. Add opportunity_decision and opportunity_execution artifacts

Introduce storage and service support for:

- `opportunity_decision`
- `opportunity_execution`

### 3. Build `services/decision_engine.py`

Start by moving and reusing:

- ranking and allocation logic from `opportunity_execution_plan.py`
- gating inputs currently consulted in `execution.py`

### 4. Make `execution.py` consume selected opportunity_executions

`execution.py` should become an opportunity_execution-row consumer, not a planner.

### 5. Extend read models

Expose opportunity_decision and opportunity_execution state through:

- `live_runtime.py`
- `pipelines.py`
- `ops_visibility.py`

### 6. Tighten alert ownership

Separate:

- discovery alerts from collectors
- actionability alerts from decisioning

## Self-Audit

This design is intentionally narrower than the earlier clean-sheet architecture.

### What It Gets Right

- It fixes the actual missing stage in the runtime.
- It uses the canonical opportunity store that already exists.
- It reuses current services instead of inventing a parallel platform.
- It makes execution consume a durable `opportunity_execution` artifact.
- It aligns runtime, ops, and replay around shared opportunity_decision outputs.

### Where It Is Still Not Ideal

- The decision clock is still job-driven, not fully event-driven.
- Exit orchestration remains less explicit than entry orchestration.
- The current rich domain model is still ahead of the runtime in places.
- Strategy/regime/horizon intent objects are still better defined in planning docs than in current code.

### Why It Is Still The Right Next Design

Because the main runtime weakness today is not missing market data, missing replay, or missing operator visibility.

It is missing durable portfolio-aware action selection between opportunity generation and persisted opportunity_execution.

Fixing that gives the system:

- a real scan-to-opportunity_decision-to-opportunity_execution pipeline
- cleaner truth boundaries
- better replayability
- better operator explanation
- a safer path to future regime or strategy-specific sophistication

## Final Framing

The system should be understood as:

- discovery stage
- state stage
- decision stage
- execution stage

not as:

- collector stage with optional side effects

That is the architectural correction this design is meant to drive.
