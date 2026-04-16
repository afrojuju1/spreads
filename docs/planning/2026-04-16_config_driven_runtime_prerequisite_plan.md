# Config-Driven Runtime Prerequisite Plan

Status: proposed

As of: Thursday, April 16, 2026

Related:

- [Backtest System Recommendation](./2026-04-16_backtest_system_recommendation.md)
- [Current-System Options Automation Implementation Approach](./2026-04-15_current_system_options_automation_implementation_approach.md)
- [Alpaca Options Automation System Architecture](./2026-04-15_alpaca_options_automation_system_architecture.md)
- [Alpaca Options Automation Schema](./2026-04-15_alpaca_options_automation_schema.md)

## Goal

Architect the prerequisite implementation needed before the improved backtest can be considered valid.

That prerequisite is:

- make the live runtime genuinely config-driven rather than merely config-loaded

The implementation should fit the current workspace split:

- `packages/core`
- `packages/api`
- `packages/config`

and should extend the current bot/runtime model rather than replace it.

## Recommendation Summary

Keep the current deployment shape and most current nouns.

Do not keep the current planning path.

The required implementation change is:

1. keep shared market-data discovery and collector jobs
2. stop using merged scanner args as the canonical planning model
3. introduce typed runtime specs derived from `Bot + Automation + StrategyConfig`
4. generate opportunities per automation using exact config, while reusing shared market snapshots
5. persist config lineage through `AutomationRun`, `Opportunity`, `OpportunityDecision`, `ExecutionIntent`, and `StrategyPosition`
6. make entry and management recipe refs executable code-backed registries, not passive strings

This is the smallest durable change that makes an improved backtest worth trusting.

## Current State

## What is already good

The repo already has the right top-level product/runtime nouns:

- `BotConfig` in `packages/core/services/bots.py`
- `AutomationConfig` in `packages/core/services/automations.py`
- `StrategyConfig` in `packages/core/services/strategy_configs.py`
- `OpportunityDecisionModel` in `packages/core/storage/signal_models.py`
- `ExecutionIntentModel` in `packages/core/storage/execution_models.py`
- `portfolio_positions` as the current `StrategyPosition` adapter

The repo also already has the right overall runtime split:

- collector/discovery work
- decisioning
- execution intents
- execution ledger
- positions

That means the prerequisite is not a clean-sheet rewrite.

## What is still wrong

### 1. Config is loaded, but planning is still arg-driven

Current config loading is real:

- `packages/core/services/strategy_configs.py`
- `packages/core/services/automations.py`
- `packages/core/services/bots.py`

But the live discovery path is still centered on scanner args and collector payloads.

Current path:

```text
job payload
   -> live_collector argparse namespace
   -> scanner args
   -> scanner builders
   -> live_selection thresholds
   -> collector opportunities
   -> signal opportunities
   -> decision engine matches by symbol + strategy family
```

Concrete examples:

- `jobs/live_collector.py` builds `scanner_args` from job payload and runs `run_universe_cycle()`
- `services/scanner.py` still uses `argparse.Namespace` as its effective internal contract
- `services/bots.py::build_collector_scope()` only converts config into merged scanner arguments, not a real per-automation planning path

### 2. Builder params are merged across automations instead of executed exactly

`build_collector_scope()` in `packages/core/services/bots.py` currently unions active entry automations and derives one merged scanner surface:

- min and max DTE
- min and max short delta
- min and max width
- min open interest
- max relative spread

That is useful for collector scoping, but it is the wrong canonical planning model.

It means the runtime currently treats:

- exact `StrategyConfig.builder_params`

as:

- one coarse merged discovery envelope

That is config-informed discovery, not config-driven planning.

### 3. Entry and management recipe refs are not executable runtime inputs yet

Current YAML contains:

- `entry_recipe_refs`
- `management_recipe_refs`

But there is no recipe registry or recipe execution boundary in `packages/core/services`.

Right now:

- entry selection is mostly hardwired through `services/live_selection.py`, `services/opportunity_scoring.py`, and `services/decision_engine.py`
- management is mostly hardwired through `services/strategy_positions.py` and `services/exit_manager.py`

So config names the recipes, but runtime does not actually execute recipes from config.

### 4. Opportunities are still generic collector outputs, not config-owned runtime artifacts

This is the biggest missing boundary.

Current `OpportunityModel` in `packages/core/storage/signal_models.py` does not persist:

- `bot_id`
- `automation_id`
- `strategy_config_id`
- `automation_run_id`
- `config_hash`

Current `decision_engine.py` therefore selects opportunities by:

- `market_date`
- `symbols`
- `strategy_family`
- active collector labels

That works only while the system assumes one coarse opportunity pool per strategy/profile lane.

It breaks down once multiple automations need:

- different builder params
- different recipe sets
- different trigger policies
- the same symbol and strategy family on the same date

### 5. Policy still leaks through legacy pipeline/profile helpers

`services/runtime_identity.py::resolve_pipeline_policy_fields()` derives:

- `style_profile`
- `horizon_intent`
- `product_class`

from legacy profile and label context.

That is still pipeline-centric compatibility logic.

For a truly config-driven runtime, the canonical policy context should come from resolved runtime spec and strategy config, not from legacy pipeline naming.

### 6. Management is closer than entry, but still not config-driven enough

`run_management_automation_decision()` in `packages/core/services/strategy_positions.py` already starts from:

- bot
- automation
- open positions
- current marks

That is good.

But it still does not evaluate `management_recipe_refs` directly as config-owned behavior.

It calls hardcoded exit-policy evaluation instead.

## Design Goals

The prerequisite implementation should satisfy these goals.

### 1. Shared data, exact planning

Market data should be shared.

Planning should be exact per automation.

That means:

- one collector/discovery pass may gather market inputs for several automations
- but each automation must run its own exact builder and recipe path over that shared market slice

### 2. One canonical runtime path for live and backtest

The same config-driven planning kernels must be callable from:

- live runtime
- bootstrap simulation
- improved backtest

### 3. Explicit config lineage on runtime artifacts

Every runtime artifact that matters for audit or backtest should know:

- which bot
- which automation
- which strategy config
- which config hash
- which automation run

### 4. Typed internal specs, not `argparse.Namespace`

CLI args can remain as an adapter surface.

They should stop being the core internal planning contract.

### 5. Code-backed strategy and recipe registries first

Do not build a generic DSL first.

For this prerequisite, keep:

- strategy definitions in code
- recipe implementations in code
- selection of those implementations in YAML

That is enough to become truly config-driven without building a no-code product.

## Recommended Architecture

## High-Level Runtime Shape

Use this target runtime flow:

```text
checked-in config
  -> Bot + Automation + StrategyConfig
  -> resolved runtime specs
  -> shared market slice
  -> per-automation opportunity generation
  -> persisted automation runs + opportunities
  -> entry planner
  -> OpportunityDecision
  -> ExecutionIntent
  -> execution ledger
  -> StrategyPosition
  -> management planner
```

## Key Design Choice

Do not make one merged collector scope equal one planning spec.

Instead:

- keep merged collector scopes only for shared market-data collection and quote budgeting
- run exact planning per resolved automation runtime inside that shared collector work

This removes the main distortion in the current runtime.

## Workspace Roles

Use the existing workspace split directly.

### `packages/config`

Owns checked-in definitions:

- `strategies/*.yaml`
- `automations/*.yaml`
- `bots/*.yaml`
- `universes/*.yaml`

This prerequisite should make those definitions executable runtime inputs. It should not create a second parallel config system.

### `packages/core`

Owns all planning, persistence, and runtime orchestration changes described in this plan.

### `packages/api`

Remains a thin adapter.

No planning logic should move into API handlers as part of this prerequisite. If API changes happen later, they should only expose the runtime state already owned by `packages/core`.

## Module Boundaries

All new business logic should live in `packages/core/services`.

### 1. Runtime spec resolution

Add:

- `packages/core/services/automation_runtime.py`

This module should resolve config into typed runtime specs.

Recommended shapes:

```text
EntryRuntime
  - bot
  - automation
  - strategy_config
  - symbols
  - strategy_family
  - build_settings
  - entry_recipe_refs
  - trigger_policy
  - config_hash

ManagementRuntime
  - bot
  - automation
  - strategy_config
  - symbols
  - management_recipe_refs
  - config_hash
```

This should become the canonical internal contract that live runtime and backtest both consume.

### 2. Strategy registry

Add:

- `packages/core/services/strategy_registry.py`

Purpose:

- map `strategy_id` to code-backed builder implementation
- stop treating `scanner.py` as the only strategy-definition surface

Recommended contract:

```text
StrategyDefinition
  - strategy_id
  - strategy_family
  - build_candidates(market_slice, build_settings) -> list[OpportunityDraft]
```

First cut should wrap existing builder logic from `services/scanner.py` rather than rewriting builders immediately.

### 3. Recipe registries

Add:

- `packages/core/services/entry_recipes.py`
- `packages/core/services/management_recipes.py`

Purpose:

- make `entry_recipe_refs` and `management_recipe_refs` executable

Recommended contract:

```text
EntryRecipe
  - recipe_ref
  - evaluate(candidate, runtime_context) -> EntryRecipeResult

ManagementRecipe
  - recipe_ref
  - evaluate(position, runtime_context) -> ManagementRecipeResult
```

First implementation should keep these code-backed and map current refs such as:

- entry:
  - `trend_support`
  - `trend_resistance`
  - `neutral_range`

- management:
  - `take_profit_50pct`
  - `max_loss_2x_credit`
  - `expiry_day_exit`

### 4. Opportunity generation service

Add:

- `packages/core/services/opportunity_generation.py`

Purpose:

- run exact per-automation planning over a shared market slice

Responsibilities:

- accept `EntryRuntime`
- invoke strategy builder using exact `build_settings`
- run entry recipes
- produce ranked `OpportunityDraft`s with explicit config lineage and reason codes

This should absorb the canonical planning responsibility currently split across:

- `services/scanner.py`
- `services/live_selection.py`
- parts of `jobs/live_collector.py`

### 5. Shared planner kernels

Add or extract:

- `packages/core/services/entry_planner.py`
- `packages/core/services/management_planner.py`

Purpose:

- pure decision kernels used by live runtime and backtest

Responsibilities:

- `entry_planner.py`
  - choose among in-scope opportunities for one bot + automation
  - apply bot-level limits and trigger policy
  - return decision outcomes to be persisted as `OpportunityDecision`

- `management_planner.py`
  - evaluate open positions for one bot + automation
  - execute management recipes
  - return management actions to be persisted as `ExecutionIntent`

### 6. Discovery orchestration

Add:

- `packages/core/services/discovery_runtime.py`

Purpose:

- orchestrate shared market-data collection and per-automation generation inside collector jobs

Responsibilities:

- resolve active entry runtime specs for the collector scope
- gather shared market slice once
- run `opportunity_generation` once per runtime spec
- persist automation runs and opportunities

`jobs/live_collector.py` should become a thin worker entrypoint over this service.

## Storage Changes

Storage changes should stay in `packages/core/storage` and be exposed through existing repositories.

## 1. Add `automation_runs`

Add a new runtime table and repository methods.

Recommended fields:

- `automation_run_id`
- `bot_id`
- `automation_id`
- `strategy_config_id`
- `trigger_type`
- `job_run_id`
- `session_date`
- `started_at`
- `completed_at`
- `status`
- `result_json`
- `config_hash`

Why:

- distinguish scheduler job runs from product/runtime automation invocations
- give opportunities a concrete config-owned parent run
- align live runtime with the planned backtest artifact model

## 2. Extend `OpportunityModel`

Add config lineage and ownership fields.

Recommended minimum additions:

- `bot_id`
- `automation_id`
- `automation_run_id`
- `strategy_config_id`
- `strategy_id`
- `config_hash`
- `policy_ref_json`

Keep existing legacy fields during migration:

- `pipeline_id`
- `label`
- `profile`

These can remain as compatibility views for now.

Why this change is required:

- `decision_engine.py` must stop matching opportunities only by symbol and strategy family
- opportunities need exact automation ownership and config lineage

## 3. Extend `portfolio_positions` gradually

Recommended additions:

- `bot_id`
- `strategy_config_id`
- `strategy_id`
- `opening_execution_intent_id`

This is not the very first migration, but it should be part of the prerequisite rollout.

Why:

- management runtime should not have to infer config ownership only by reverse walking owner intent and attempt metadata
- backtest parity improves when `StrategyPosition` semantics are explicit on the position row itself

## 4. Repository changes

Recommended repository ownership:

- add automation-run methods to `signal_repository.py` or a new narrow runtime repository if it proves cleaner
- extend `SignalRepository.upsert_opportunity()` to accept the new config lineage fields
- add listing methods for opportunities scoped by bot and automation

Required new query shape for decisioning:

- list active opportunities for exactly one `bot_id + automation_id + session_date`

That should replace the current broad symbol/family matching in `decision_engine.py`.

## Internal Contract Changes

## 1. Replace internal scanner arg coupling

Introduce a typed builder spec and stop letting internal services depend on raw CLI args.

Recommended add:

- `StrategyBuildSettings`

Fields should cover the current runtime-relevant subset of:

- `builder_params`
- `liquidity_rules`
- market data policy needed by builders

Example:

```text
StrategyBuildSettings
  - strategy_id
  - dte_min
  - dte_max
  - short_delta_min
  - short_delta_max
  - width_points
  - symmetric_wings_only
  - min_open_interest
  - max_leg_spread_pct_mid
  - max_quote_age_seconds
```

Adapters should exist for:

- `StrategyConfig -> StrategyBuildSettings`
- legacy CLI args -> `StrategyBuildSettings`

The CLI can remain unchanged while internal services migrate.

## 2. Replace pipeline-policy derivation in planning paths

Add a strategy/runtime policy resolver that derives policy context from resolved runtime spec instead of pipeline label and profile.

Add:

- `packages/core/services/runtime_policy.py`

Responsibilities:

- resolve `style_profile`
- resolve `horizon_intent`
- resolve `product_class`
- expose consistent `policy_ref` for opportunities, decisions, intents, and positions

This should gradually replace `resolve_pipeline_policy_fields()` in planning-critical paths.

## 3. Make selection recipes the owner of selection thresholds

Current hardcoded thresholds in `services/live_selection.py` are useful, but they are still global profile rules.

Recommended migration:

- wrap current scoring and threshold logic behind code-backed entry recipes
- keep default thresholds in code for v1
- let `entry_recipe_refs` decide which logic stack applies

Do not move every threshold into YAML immediately.

That would increase configurability without improving architecture.

## Recommended Runtime Flow By Surface

## Discovery / collector

Target flow:

```text
live_collector job
  -> resolve collector scope for shared data only
  -> resolve active entry runtime specs
  -> collect shared market slice
  -> for each runtime spec:
       create automation_run
       generate opportunities with exact config
       persist config-owned opportunities
```

Important rule:

- collector scope remains a market-data optimization boundary, not the canonical planning boundary

## Entry decisioning

Target flow:

```text
entry job
  -> load EntryRuntime
  -> list active opportunities for that bot + automation
  -> apply bot metrics and trigger policy
  -> write OpportunityDecision rows
  -> create ExecutionIntent for the selected opportunity
```

## Management decisioning

Target flow:

```text
management job
  -> load ManagementRuntime
  -> load open StrategyPositions owned by bot/config
  -> evaluate management recipes
  -> create close or management ExecutionIntent rows
```

## Rollout Plan

## Phase 0: Freeze The Target Boundary

Goal:

- stop adding more planning behavior to `argparse` and merged collector scope code

Work:

- document `build_collector_scope()` as data-scope only, not planning-truth
- document `scanner.py` arg objects as compatibility surfaces, not target internal contracts

## Phase 1: Introduce Typed Runtime Specs And Registries

Goal:

- create the internal contracts needed for config-driven execution without changing behavior yet

Work:

- add `automation_runtime.py`
- add `strategy_registry.py`
- add `entry_recipes.py`
- add `management_recipes.py`
- add `runtime_policy.py`
- add adapters from existing config and current scanner args

Validation:

- current live jobs still run unchanged
- new unit tests cover config resolution and registry lookup

## Phase 2: Add Automation Runs And Opportunity Lineage

Goal:

- give runtime artifacts explicit config ownership

Work:

- add `automation_runs`
- extend `opportunities` with config lineage fields
- write automation-run records and enriched opportunity rows from collector runtime
- add scoped repository queries

Validation:

- one collector run writes opportunities with `bot_id`, `automation_id`, `strategy_config_id`, and `config_hash`
- decision engine can query those scoped rows

## Phase 3: Move Per-Automation Planning Into Shared Discovery Runtime

Goal:

- replace merged scanner-arg planning with exact config execution over shared data

Work:

- add `discovery_runtime.py`
- add `opportunity_generation.py`
- make `jobs/live_collector.py` call the new orchestration path
- reuse shared market slice while running exact builders per runtime spec

Validation:

- two automations with the same family but different params can produce different scoped opportunities on the same session
- collector no longer depends on merged scanner args as planning truth

## Phase 4: Switch Entry Decisioning To Automation-Scoped Opportunities

Goal:

- make decisioning consume config-owned opportunities

Work:

- extract pure entry planner
- update `decision_engine.py` to query automation-scoped opportunities
- persist richer `policy_ref` and config lineage through decisions and intents

Validation:

- `run_entry_automation_decision()` only considers exact in-scope opportunities for its automation
- decision runs are reproducible from config lineage and opportunity rows

## Phase 5: Wire Management Recipes And Position Lineage

Goal:

- make management runtime truly config-driven too

Work:

- extract pure management planner
- map `management_recipe_refs` to executable recipe evaluators
- add config lineage to positions as they are created and updated

Validation:

- management behavior is driven by referenced recipe set, not just hardcoded exit manager wiring

## Phase 6: Retire Compatibility-Critical Distortions

Goal:

- leave compatibility views in place, but stop depending on them for planning truth

Work:

- stop relying on `resolve_pipeline_policy_fields()` in planning-critical code
- stop treating collector label/profile as the main business identity
- keep pipeline/session reads only as compatibility projections for ops and history

## Done Criteria

The prerequisite should be considered complete when all of these are true.

- `StrategyConfig.builder_params` drive real opportunity generation
- `entry_recipe_refs` and `management_recipe_refs` execute code-backed recipe logic
- collector scope is only a shared data boundary, not the planning truth
- opportunities carry `bot_id`, `automation_id`, `strategy_config_id`, and `config_hash`
- decision engine selects from automation-scoped opportunities, not generic strategy-family pools
- management runtime can evaluate recipe-driven behavior against config-owned positions
- the same planning kernels are callable by live runtime and backtest runtime

At that point, the improved backtest can be built against the actual intended runtime model instead of a partially reconstructed one.

## Final Recommendation

Do not treat this prerequisite as a light cleanup.

This is the boundary fix that makes the future backtest meaningful.

The most important implementation decisions are:

1. use shared discovery data but exact per-automation planning
2. introduce typed runtime specs and code-backed registries
3. add explicit config lineage to opportunities and automation runs
4. move planning truth away from merged scanner args and legacy pipeline/profile helpers

That is the right architecture for the prerequisite.
