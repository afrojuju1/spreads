# Current-System Options Automation Implementation Approach

Status: supporting implementation plan

As of: Wednesday, April 15, 2026

Related:

- [System Architecture](../current_system_state.md)
- [Alpaca Options Automation System Architecture](./2026-04-15_alpaca_options_automation_system_architecture.md)
- [Alpaca Options Automation Schema](./2026-04-15_alpaca_options_automation_schema.md)
- [Collector Decision Pipeline Design](./2026-04-15_collector_decision_pipeline_design.md)
- [Spread Selection Review And Refactor Plan](./2026-04-11_spread_selection_refactor_plan.md)
- [Options Alpha Product Research](../research/options_alpha_product_research.md)

## Goal

Define how to build the new CLI-first, single-operator, `1-14` DTE options automation system using the backend we already have.

This document intentionally ignores `packages/web`.

This is not a clean-sheet design. It starts from the runtime that already exists in code and assumes we are willing to do aggressive refactors and deletions where the current shape is wrong.

This document is not the canonical source of truth for the overall current architecture. That role belongs to [System Architecture](../current_system_state.md). This document is the migration plan that explains how to evolve that runtime toward the options-automation model.

Current shipped-surface note:

- `backtest` is now the canonical historical-evaluation product.
- `spreads replay`, `packages/core/cli/replay.py`, and `packages/core/services/opportunity_replay.py` were removed after this plan was written.
- Operator visibility now lives under `packages/core/services/ops/`, and the audit builder lives in `packages/core/services/audit_snapshot.py`.
- References below to `replay`, `ops_visibility.py`, or other removed surfaces are historical planning context unless explicitly updated.

## Executive Summary

The repo already contains the hard infrastructure.

We do not need to rebuild:

- Postgres as the source of truth
- Redis queues, leases, and pub/sub
- ARQ scheduler and worker lanes
- the recorder-owned Alpaca option stream path
- a real execution ledger with broker sync and position reconciliation
- a usable CLI and historical-evaluation surface

The main problem is not missing infrastructure. The main problem is that the current product/runtime shape is still collector-centric and partially duplicated.

The right implementation approach is:

1. keep the infrastructure, recorder, execution ledger, control plane, and CLI
2. stop adding logic to legacy scan-history and collector-cycle paths
3. repurpose the collector and worker lanes around discovery, target planning, and bot runtime responsibilities
4. refactor discovery so it only discovers and persists opportunities
5. add a bot-owned decision layer above the current opportunity store
6. map the new product model onto the current ledger and broker-sync foundations
7. then delete or demote the old parallel paths

## What Already Exists

### 1. Runtime Infrastructure Is Real

The system already runs as a modular monolith with:

- Postgres-backed repositories and state tables
- Redis-backed queues, leases, and pub/sub
- ARQ scheduler and worker lanes
- a Typer CLI
- a thin FastAPI surface over backend services

Key modules:

- `packages/core/jobs/scheduler.py`
- `packages/core/jobs/worker.py`
- `packages/core/jobs/registry.py`
- `packages/core/storage/context.py`
- `packages/core/storage/factory.py`
- `packages/core/cli/main.py`

### 2. Market Data Capture Is Already On The Right Boundary

The system already has one canonical rule that we should keep:

- `services/market_recorder.py` is the sole Alpaca option websocket owner in normal runtime

That is the correct boundary for Alpaca and should remain intact.

Key modules:

- `packages/core/services/market_recorder.py`
- `packages/core/services/live_recovery.py`
- `packages/core/storage/recovery_repository.py`

### 3. There Is Already A Canonical Opportunity Layer

The repo already has a better state model than the older scan-history path:

- `signal_states`
- `signal_state_transitions`
- `opportunities`

Key modules:

- `packages/core/services/signal_state.py`
- `packages/core/services/opportunities.py`
- `packages/core/storage/signal_models.py`
- `packages/core/storage/signal_repository.py`

This is the strongest current foundation for the new automation system.

### 4. The Execution And Portfolio Stack Is Already Material

The repo already has:

- execution submission and refresh
- persisted broker order and fill history
- open-position ownership
- broker reconciliation and mark refresh
- risk and exit management

Key modules:

- `packages/core/services/execution.py`
- `packages/core/services/execution_portfolio.py`
- `packages/core/services/broker_sync.py`
- `packages/core/services/risk_manager.py`
- `packages/core/services/exit_manager.py`
- `packages/core/storage/execution_models.py`

### 5. Operator Surfaces Already Exist

The CLI is already real and useful.

Shipped command families include:

- `status`
- `trading`
- `pipelines`
- `opportunities`
- `positions`
- `jobs`
- `backtest`
- `uoa`

Key modules:

- `packages/core/cli/main.py`
- `packages/core/cli/ops.py`
- `packages/core/cli/runtime.py`
- `packages/core/cli/backtest.py`
- `packages/core/services/ops/`

## What Is Structurally Wrong Today

### 1. `live_collector.py` Owns Too Much

It still owns too many responsibilities in one loop:

- scanning
- enrichment
- promotion
- signal sync
- capture targeting
- live action gating
- alerting
- optional automatic execution

That is the wrong center of gravity for the new system.

### 2. Legacy And Newer State Paths Coexist

The repo currently persists similar runtime facts into multiple places:

- `scan_runs` and `scan_candidates`
- `collector_cycles` and `collector_cycle_candidates`
- `signal_states` and `opportunities`

That split makes implementation and historical evaluation harder than it should be.

### 3. Product Vocabulary Is Still Old

The runtime still speaks heavily in terms of:

- `pipeline`
- `session`
- `board`
- `watchlist`

The new automation system should instead center on:

- `Bot`
- `StrategyConfig`
- `Automation`
- `Opportunity`
- `OpportunityDecision`
- `StrategyPosition`

## Naming Migration

We should not collapse `bot`, `pipeline`, and `job` into one term.

They represent different layers.

### Keep `job` As A Scheduling Primitive

`job` should remain the scheduler and worker noun.

It means:

- scheduled work
- queue routing
- payload
- run history

It should not become the product/runtime ownership noun.

Current anchors:

- `job_definitions`
- `job_runs`
- `job_leases`
- `packages/core/jobs/registry.py`

### Demote `pipeline` To A Compatibility View

`pipeline` is close to the current operational container, but it is still tied to the collector-centric architecture.

For the migration, keep it as:

- a runtime projection
- a CLI/API compatibility surface
- a bridge to current session and cycle reads

But do not keep it as the long-term core noun.

### Promote `bot` To The Core Runtime Object

`bot` should become the operator-facing and runtime-facing ownership object.

It means:

- capital bucket
- open-position limits
- attached automations
- pause or resume state
- approval and execution mode
- ownership of open `StrategyPosition`s

That is materially more than the current meaning of `pipeline`.

### Recommended Mapping

Use this target model:

```text
Bot
  -> has many Automations
Automation
  -> is scheduled by JobDefinition
JobDefinition
  -> produces JobRuns
JobRun
  -> produces Opportunities and actions
```

### Migration Table

| Current term | Target term | Recommendation |
|---|---|---|
| `job_definition` | `job_definition` | keep as-is; scheduler primitive |
| `job_run` | `job_run` | keep as-is; execution record of scheduled work |
| `pipeline` | `bot` | migrate product/runtime ownership semantics here |
| `pipeline_cycles` | bot runtime views | keep temporarily only as compatibility/session projection |
| `session_id` | bot run scope | keep temporarily where needed, but stop making it the product noun |
| `board` / `watchlist` | opportunity states/views | keep only as compatibility labels if needed |

### Practical Rule

Use these meanings consistently:

- operator-managed thing that trades: `Bot`
- scheduled unit of work: `JobDefinition` and `JobRun`
- temporary read-model for old runtime/session aggregation: `pipeline`

### 4. The Scanner Boundary Is Weak

`scanner.py` still acts as a large mixed surface for:

- Alpaca REST integration
- candidate building
- scoring
- profile policy
- strategy merging

It works, but it is too monolithic to serve as the long-term automation core.

### 5. Compatibility Fields Still Leak Through The Stack

The codebase already has better `legs[]` support, but legacy fields still shape too much of the runtime:

- `short_symbol`
- `long_symbol`
- special-case vertical assumptions

That will block clean multi-leg automation if we keep leaning on them.

### 6. Target Planning Is Missing As Its Own Layer

The repo has a recorder and it has collectors, but it does not yet have a clean owner for:

- quote-budget allocation
- `cold` / `warm` / `hot` contract promotion
- recorder-target eviction policy
- prioritizing open-risk monitoring over discovery

That layer is required for the two-stage scanner to work in production.

## Keep, Refactor, Retire

### Keep

- scheduler and worker runtime: `jobs/scheduler.py`, `jobs/worker.py`, `jobs/registry.py`
- current process topology: `api`, `scheduler`, `worker-runtime`, `worker-discovery`
- Postgres repository pattern: `storage/context.py`, `storage/factory.py`
- recorder-owned stream model: `services/market_recorder.py`
- signal and opportunity store: `services/signal_state.py`, `storage/signal_models.py`
- execution ledger and broker sync: `services/execution.py`, `services/broker_sync.py`, `storage/execution_models.py`
- control, risk, and exits: `services/control_plane.py`, `services/risk_manager.py`, `services/exit_manager.py`
- CLI and historical-evaluation foundations: `cli/*.py`, `services/ops/`, `backtest/`

### Refactor Hard

- `jobs/live_collector.py` into a discovery-only runtime stage
- `jobs/registry.py` and job definitions so discovery jobs, bot-runtime jobs, and maintenance jobs are clearly separate
- `services/scanner.py` into smaller shortlist, chain-enrichment, and builder boundaries
- `services/live_runtime.py`, `services/pipelines.py`, and `services/ops/` away from pipeline/session ownership toward bot/automation/runtime ownership
- `portfolio_positions` semantics into `StrategyPosition` semantics, even if the first cut is an adapter over current tables
- current pipeline/session naming into bot/automation naming at the service layer while keeping pipeline reads as a compatibility projection
- ops and backtest surfaces so they consume bot decisions and execution truth rather than collector-centric status
- current backtest surface so it evaluates the new automation path, not legacy board/watchlist assumptions

### Retire Or Demote

- `services/analysis.py` as an active planning surface; keep only as legacy reporting
- `storage/run_history_repository.py` and `scan_runs`/`scan_candidates` as canonical runtime truth
- `collector_cycle_candidates` as a long-term selection surface
- board/watchlist as core logic terms; keep only if needed as compatibility views
- any reintroduction of API-owned or multi-owner option stream capture

## Recommended Product Mapping Onto The Current Backend

Use this mapping for the first implementation:

| New concept | Current-system anchor | Recommendation |
|---|---|---|
| `Strategy` | `services/option_structures.py` plus builder logic in `services/scanner.py` | keep code-backed builders, split scanner monolith over time |
| `StrategyConfig` | new checked-in YAML | add as config, do not create a DB table in `v1` |
| `Automation` | job payloads plus new YAML | add as config and schedule through current job system |
| `Bot` | current pipeline identity, not job identity | introduce as a new first-class noun and adapt current pipeline reads temporarily |
| `Opportunity` | existing `opportunities` table | reuse and evolve; do not create another parallel opportunity store |
| `OpportunityDecision` | new `opportunity_decisions` table written by a bot-owned decision engine | adopt from the collector-decision redesign as the canonical per-opportunity planning truth |
| `ExecutionIntent` | new `execution_intents` table linked to the existing execution ledger | use as the universal action handoff noun; do not make `opportunity_execution` the system-wide noun |
| `StrategyPosition` | current `portfolio_positions` | adapt first, rename physically later if useful |
| OMS ledger | `execution_attempts`, `execution_orders`, `execution_fills` | reuse the ledger instead of rewriting it |

## Decision Artifact Naming

The collector-decision redesign has one naming idea we should adopt directly:

- `OpportunityDecision` is the right noun for per-opportunity planning truth

That should become the canonical answer to:

- was this opportunity selected, rejected, blocked, or superseded
- under which policy stack and planning scope
- with which explicit reasons

### Keep `decision_engine` As An Internal Boundary

`decision_engine.py` is still a good internal runtime boundary.

But in the new system it should be bot-owned, not collector-owned.

Its planning scope should be:

- bot + automation + current market scope

not:

- collector label + cycle alone

### Do Not Promote `opportunity_execution` As The Universal Action Noun

`opportunity_execution` is useful for the older redesign because it cleanly described selected entry actions coming out of an opportunity set.

But it is too narrow for the new system because not every action starts from a fresh opportunity.

Examples:

- entry actions do start from an `Opportunity`
- management and close actions may start from an open `StrategyPosition`
- pause or suppress actions may come from bot policy without producing any entry candidate

So the newer system should prefer:

- `OpportunityDecision` for entry-selection truth
- `ExecutionIntent` for the universal action handoff into the OMS

### Recommended Relationship

Use this relationship:

```text
Bot + Automation
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
 execution_attempts / orders / fills
        |
        v
 StrategyPosition
```

For management paths, the left side becomes:

```text
Bot + Automation + StrategyPosition
```

which is why `ExecutionIntent` generalizes better than `opportunity_execution`.

### Immediate Persistence Choice

Create these immediately:

- `opportunity_decisions`
- `execution_intents`
- `execution_intent_events`

Migration rules:

- use `OpportunityDecision` as the new canonical decision artifact
- interpret `opportunity_execution` in the older redesign docs as an entry-specific precursor concept
- link `ExecutionIntent` rows to the existing `execution_attempts`, `execution_orders`, and `execution_fills` ledger rather than replacing that ledger

### `OpportunityDecision` Minimum Contract

`OpportunityDecision` should be a first-class persisted artifact.

Minimum fields:

- `opportunity_decision_id`
- `opportunity_id`
- `bot_id`
- `automation_id`
- `run_key`
- `scope_key`
- `policy_ref`
- `state`
- `score`
- `rank`
- `reason_codes[]`
- `superseded_by_id`
- `decided_at`
- `payload`

Initial states:

- `selected`
- `rejected`
- `blocked`
- `superseded`

Runtime rules:

- every completed decision pass should write one `OpportunityDecision` outcome for every in-scope opportunity
- rejection and blocking reasons must be explicit, never inferred from missing rows
- `run_key + opportunity_id` should be unique within a completed planning pass

### `ExecutionIntent` Minimum Contract

`ExecutionIntent` is the universal action handoff into the OMS.

Minimum fields:

- `execution_intent_id`
- `bot_id`
- `automation_id`
- `opportunity_decision_id`
- `strategy_position_id`
- `action_type`
- `slot_key`
- `claim_token`
- `policy_ref`
- `state`
- `expires_at`
- `superseded_by_id`
- `payload`
- `created_at`
- `updated_at`

Initial states:

- `pending`
- `claimed`
- `submitted`
- `partially_filled`
- `filled`
- `canceled`
- `expired`
- `revoked`
- `failed`

First implementation rule:

- `execution_intents` is a first-class table immediately and links into the existing `execution_attempts` ledger

Source rule:

- entry intents should point to `opportunity_decision_id`
- management and close intents should point to `strategy_position_id`

### Decision Safety Rules

- only one active `ExecutionIntent` should exist per mutually exclusive `slot_key`
- execution must atomically claim an `ExecutionIntent` before broker submission
- newer decision passes must supersede stale pending intents before they can be consumed
- submitted or partially filled intents must reconcile against broker state before any replacement intent becomes active

## Config Model

Use checked-in YAML for operator-defined behavior and Postgres for runtime state.

Recommended config roots:

- `packages/config/strategies/*.yaml`
- `packages/config/automations/*.yaml`
- `packages/config/bots/*.yaml`

### `StrategyConfig`

Required config keys:

- `strategy_config_id`
- `strategy_id`
- `builder_params`
- `entry_recipe_refs[]`
- `management_recipe_refs[]`
- `liquidity_rules`
- `risk_defaults`
- `enabled`

### `Automation`

Required config keys:

- `automation_id`
- `strategy_config_id`
- `automation_type`
- `schedule`
- `universe_ref`
- `trigger_policy`
- `approval_mode`
- `execution_mode`
- `enabled`

### `Bot`

Required config keys:

- `bot_id`
- `name`
- `capital_limit`
- `max_open_positions`
- `max_daily_actions`
- `automation_ids[]`
- `paused`

### Validation And Versioning

- config should be schema-validated before job registration or runtime use
- semantic validation should check cross-references such as `automation.strategy_config_id` and `bot.automation_ids[]`
- every automation run, `OpportunityDecision`, and `ExecutionIntent` should record a resolved `config_hash`
- secrets remain environment-backed, not stored in checked-in YAML

### Example YAML

`StrategyConfig` example:

```yaml
strategy_config_id: short_dated_index_put_credit
strategy_id: put_credit_spread
enabled: true
builder_params:
  dte_min: 5
  dte_max: 10
  short_delta_min: 0.18
  short_delta_max: 0.28
  width_points: [2, 3, 5]
entry_recipe_refs:
  - trend_support
management_recipe_refs:
  - take_profit_50pct
  - max_loss_2x_credit
  - expiry_day_exit
liquidity_rules:
  min_open_interest: 200
  max_leg_spread_pct_mid: 0.15
  max_quote_age_seconds: 30
risk_defaults:
  max_risk_per_trade: 500
  max_credit_slippage_pct: 0.25
```

`Automation` example:

```yaml
automation_id: index_put_credit_entry
strategy_config_id: short_dated_index_put_credit
automation_type: entry
schedule:
  cadence: 5m
  market_hours_only: true
  start_time_et: "09:45"
  end_time_et: "14:30"
universe_ref: liquid_index_etfs
trigger_policy:
  min_opportunity_score: 70
  replan_on_new_cycle: true
approval_mode: manual
execution_mode: paper
enabled: true
```

`Bot` example:

```yaml
bot_id: short_dated_index_credit_bot
name: Short-Dated Index Credit Bot
capital_limit: 5000
max_open_positions: 3
max_daily_actions: 6
automation_ids:
  - index_put_credit_entry
  - index_put_credit_manage
paused: false
```

## Recommended Runtime Shape

The new runtime should be:

```text
market_recorder
      |
      v
target planner / quote budget
      |
      +------------------+
      |                  |
      v                  v
discovery jobs      live contract coverage
      |                  |
      v                  |
signal state + opportunities
      |
      v
bot runtime / decision engine
      |
      v
opportunity_decisions
      |
      v
execution intents
      |
      v
execution ledger
      |
      v
strategy positions + broker sync + backtest
```

That means:

- recorder coverage is planned, not implied by collector loops
- discovery persists opportunities
- a separate bot runtime writes `OpportunityDecision` truth
- decisioning emits `ExecutionIntent` records into the OMS path
- execution consumes those actions
- positions and backtest read the same persisted truth

## Collector And Infra Migration

The current service topology can stay. The ownership model cannot.

### Keep The Current Deployment Shape

Keep:

- `api`
- `scheduler`
- `worker-runtime`
- `worker-discovery`
- Postgres and Redis
- `market_recorder` as the sole Alpaca option stream owner

Do not add more long-running services in `v1` unless the refactor proves a real need.

### Demote API To A Read And Debug Surface

Keep `api`, but do not let it own runtime orchestration.

Rules:

- jobs and services are the canonical runtime owners
- CLI is the primary operator surface
- API routes may expose read/debug views and narrow operator actions, but they should not own decisioning, target planning, or execution flow

### Change Worker Responsibilities

Use this split:

- `worker-discovery`: shortlist generation, targeted option enrichment, structure building, recorder-target refresh
- `worker-runtime`: bot entry evaluation, bot management evaluation, approvals, execution submission, broker sync, exit handling

The important change is that `worker-discovery` stops being the place where trade decisions and auto-execution originate.

### Add A Recorder Target Planner

Add a dedicated service boundary for recorder-target planning.

It should own:

- `cold` / `warm` / `hot` target state
- quote-budget allocation
- promotion and demotion rules
- eviction order under pressure
- recorder target TTL and priority updates

Its inputs should be:

- open `StrategyPosition`s
- active working orders
- top-ranked opportunities
- discovery candidate demand

Its persisted surface should start from:

- `market_recorder_targets` as the canonical current target set
- derived runtime summaries for `cold`, `warm`, and `hot` coverage

Persistence and TTL rules:

- `cold` is derived only from universes and shortlist inputs; do not persist per-contract target rows for it
- `warm` persists shortlisted names with lightweight contract seeds and a default TTL of `300s`
- `hot` persists concrete live contract coverage with a default discovery TTL of `90s`
- `hot` rows driven by open risk or active working orders get a refreshed TTL of `120s` and remain active while the source condition exists

Priority rules:

- `100`: open-risk monitoring
- `90`: active working orders
- `70`: selected or approved entry candidates
- `50`: top-ranked discretionary discovery candidates
- `30`: warm shortlist coverage

Eviction rules:

1. evict lowest-priority `warm` rows first
2. then evict discretionary `hot` discovery rows without approval activity
3. never evict `hot` rows tied to open positions or active working orders while those conditions remain true

Minimum target fields:

- `scope_key`
- `symbol`
- `contract_symbols[]`
- `target_state`
- `priority`
- `reason`
- `expires_at`
- `updated_at`

Its most important rule is:

- open-risk monitoring always outranks new-idea discovery

### Rebuild Collector Semantics

`jobs/live_collector.py` should become discovery infrastructure, not product decision infrastructure.

It should own:

- stock-first shortlist generation
- targeted chain enrichment
- supported-structure construction
- opportunity persistence
- cycle summary capture

It should not own:

- inline automatic execution
- final portfolio selection
- control-plane style action gating as the final arbiter of whether to trade

### Shift Decisioning Into Bot Runtime Jobs

Introduce explicit job types for:

- bot entry evaluation
- bot management evaluation
- backtest evaluation
- recorder target refresh

These should be scheduled through the existing job system and persisted through the existing `job_definitions` and `job_runs` tables.

### Move Ops And Backtest Up A Layer

Runtime views and backtest should stop treating collector internals as the main business truth.

They should instead derive from:

- promoted opportunities
- `OpportunityDecision` outcomes
- execution lifecycle changes
- reconciliation failures

## Decision Trigger Model

The system needs explicit triggers for discovery, planning, and target refresh.

### Primary Planning Trigger

- successful discovery cycle that changed current opportunity truth materially

### Secondary Planning Triggers

- low-frequency fallback decision tick during market hours
- restart or recovery replan
- control or policy change that invalidates current decisions
- meaningful open-order or open-position lifecycle change for management automations

### Target Planner Triggers

- after each successful discovery cycle
- after each material decision pass
- after order or position lifecycle changes that affect live coverage priority
- periodic refresh while any open risk or working order exists

### Concurrency Rules

- decision jobs should serialize by `bot_id + automation_id + planning_scope`
- target-planner refresh should have one active owner per planning scope
- collector jobs may run concurrently, but they must not submit orders directly

### Idempotency Rules

- planning idempotency should key off planning scope, source change set, `policy_ref`, and `config_hash`
- target-planner refresh should skip writes when the computed target set is unchanged

## Proposed Implementation Approach

### Phase 1: Freeze Canonical Truth Boundaries

Before adding the new automation model, stop expanding the wrong paths.

Rules:

- `opportunities` becomes the canonical current candidate store
- `signal_states` remains the canonical current signal-truth layer
- `scan_runs`, `scan_candidates`, and `collector_cycle_candidates` stop gaining new product meaning
- `portfolio_positions` remains the current mutable position store until `StrategyPosition` fully replaces it

Practical effect:

- no new runtime features should be built on top of `analysis.py`
- no new selection logic should depend on board/watchlist ranking as its primary truth

### Phase 2: Introduce Config-Backed Strategy, Automation, And Bot Definitions

Add checked-in config for:

- `strategies/*.yaml`
- `automations/*.yaml`
- `bots/*.yaml`

Use the current CLI and job system to load and validate them.

Implementation notes:

- `Strategy` stays in code
- `StrategyConfig` and `Automation` stay in YAML
- job definitions schedule automation runs
- bot state and runtime artifacts stay in Postgres
- config loads must be schema-validated before registration
- every runtime artifact records the resolved `config_hash`

This gives us the new product model without rewriting the scheduler.

### Phase 3: Split Discovery From Action And Rebuild Collector Ownership

Refactor `jobs/live_collector.py` so it only does discovery work:

- run the stock-first shortlist
- perform targeted option enrichment
- build supported structures
- persist opportunities
- record cycle summaries

Add a dedicated target-planning step that:

- computes `cold`, `warm`, and `hot` target demand
- reserves quote budget for open positions and working orders first
- promotes top current opportunities into live coverage
- evicts stale discovery contracts when budget is tight
- updates recorder targets with TTL and priority metadata
- persists the canonical current target set through `market_recorder_targets`

It should stop doing:

- inline automatic execution
- final portfolio selection
- being the place where the system decides what to trade

This phase is the most important architectural correction.

### Phase 4: Add A Bot Runtime Above Opportunities

Create a new runtime stage that reads:

- active opportunities
- bot config
- automation config
- open positions
- control state
- risk state

Its job is to:

- decide whether a bot should open, manage, or close
- choose the best current opportunity for that bot
- persist `OpportunityDecision` rows with explicit outcome reasons
- emit `ExecutionIntent` requests into the current OMS path

This is where the product actually becomes an automation system instead of a collector with side effects.

Infra consequence:

- `worker-runtime` should become the lane that runs bot entry and management jobs
- `job_definitions` should schedule these explicitly rather than hiding them inside collector behavior
- decision runs should serialize by bot, automation, and planning scope

### Phase 5: Reuse The Existing OMS Instead Of Rewriting It

Do not rebuild the broker ledger first.

For the first cut:

- add `opportunity_decisions`, `execution_intents`, and `execution_intent_events` immediately
- link `execution_intents` into the existing `execution_attempts` ledger
- keep `execution_orders` and `execution_fills` as the broker interaction log
- keep `broker_sync.py` as the reconciliation path
- add adapter code so new bot/runtime actions submit through the existing execution boundary

This avoids replacing the most dangerous part of the runtime while the product model is still changing.

### Phase 6: Introduce `StrategyPosition` Semantics At The Service Layer

The new product model should expose `StrategyPosition`, but the first migration does not need to rename all physical storage immediately.

Recommended order:

1. add `StrategyPosition` service-layer semantics over `portfolio_positions`
2. update CLI and runtime readers to speak in terms of strategy positions
3. only later decide whether the physical table should be renamed or replaced

This keeps the user-facing and operator-facing model clean without forcing an early storage rewrite.

### Phase 7: Delete The Parallel Legacy Paths

After the new bot-runtime path is live and backtestable, remove or demote:

- legacy post-close planning reliance on `analysis.py`
- legacy scan-history dependency for current runtime decisions
- old board/watchlist-driven auto-execution assumptions
- collector-cycle candidate ranking as a primary source of truth
- pipeline/session-centric runtime views as the primary operator surface

If we do not delete these paths, they will keep attracting new logic and recreate the same split system.

## Recommended Module Direction

Prefer this module direction over adding more logic to existing mixed files.

### Keep As Canonical

- `services/market_recorder.py`
- `services/signal_state.py`
- `services/opportunities.py`
- `services/execution.py`
- `services/broker_sync.py`
- `services/control_plane.py`
- `services/risk_manager.py`
- `jobs/registry.py`

### Split Or Replace Over Time

- `jobs/live_collector.py`
- `services/scanner.py`
- `services/live_runtime.py`
- `services/pipelines.py`
- `services/ops/`

### Add For The New Model

- `services/strategy_configs.py`
- `services/automations.py`
- `services/bots.py`
- `services/shortlist.py`
- `services/target_planner.py`
- `services/bot_runtime.py`
- `services/decision_engine.py`
- `services/execution_intents.py`
- `services/strategy_positions.py`

These names are illustrative, but the boundary matters more than the exact filenames.

## What This Means For The Existing Tables

### Keep And Evolve

- `signal_states`
- `signal_state_transitions`
- `opportunities`
- `opportunity_decisions`
- `execution_intents`
- `execution_intent_events`
- `execution_attempts`
- `execution_orders`
- `execution_fills`
- `portfolio_positions`
- `position_closes`
- `job_definitions`
- `job_runs`
- `job_leases`
- `market_recorder_targets`
- `account_snapshots`
- `broker_sync_state`
- `risk_decisions`
- `control_state`
- `policy_rollouts`

### Transitional Or Historical

- `collector_cycles`
- `collector_cycle_candidates`
- `collector_cycle_events`
- `pipeline_cycles`
- `scan_runs`
- `scan_candidates`

The important distinction is not whether these legacy tables physically remain. The distinction is whether they still own live product truth. They should not.

## Recommended First Vertical Slice

Build the first `v1` automation path on top of the current backend in this order:

1. one discovery job plus recorder target planner
2. one bot
3. one entry automation
4. one management automation
5. one short-dated strategy, then expand to the rest of the initial set

Chosen first cutover slice:

- `Strategy`: `put_credit_spread`
- `StrategyConfig`: `short_dated_index_put_credit`
- `Bot`: `short_dated_index_credit_bot`
- entry automation on `SPY`, `QQQ`, and `IWM`
- management automation for profit-take, max-loss, and expiry-day exits
- manual approval first, paper execution first

Why this slice first:

- current code already has the strongest support for two-leg vertical spread semantics
- the existing quote helpers and execution assumptions are closest to credit-spread entry
- it proves discovery, decisioning, target planning, execution, and position lifecycle without taking on condor complexity immediately

Concrete implementation order:

1. add YAML loaders and validators for `StrategyConfig`, `Automation`, and `Bot`
2. add `opportunity_decisions`, `execution_intents`, and `execution_intent_events`
3. refactor `live_collector.py` to publish only vertical `Opportunity` rows for `SPY`, `QQQ`, and `IWM`
4. add `target_planner.py` and persist `warm` and `hot` recorder targets
5. add `decision_engine.py` to write `OpportunityDecision` rows for the entry automation
6. link `ExecutionIntent` submission into the existing `execution_attempts` path
7. add one management automation over existing `StrategyPosition` state
8. run backtest and paper validation before enabling live submission

## Non-Goals For This Implementation Track

- any web-first design work
- any multi-user account model
- any attempt to preserve every historical naming choice
- a full rewrite of scheduler, workers, or broker sync
- another temporary parallel runtime path

## Recommendation

Treat this as a current-system refactor, not a greenfield rewrite.

The infrastructure and ledger layers are already strong enough.

The correct move is to:

- keep the runtime foundations
- replace the product and decision model above them
- aggressively retire the legacy parallel paths once the new path is live

That is the fastest route to a durable options automation system without trapping the repo in one more transitional architecture.
