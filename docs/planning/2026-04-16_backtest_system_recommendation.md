# Backtest System Recommendation

Status: implemented direction; retained as design background for the backtest cutover

As of: Thursday, April 16, 2026

Related:

- [Config-Driven Runtime Prerequisite Plan](./2026-04-16_config_driven_runtime_prerequisite_plan.md)
- [Current-System Options Automation Implementation Approach](./2026-04-15_current_system_options_automation_implementation_approach.md)
- [Alpaca Options Automation System Architecture](./2026-04-15_alpaca_options_automation_system_architecture.md)
- [Alpaca Options Automation Schema](./2026-04-15_alpaca_options_automation_schema.md)
- [Fresh Spread Opportunity System Design](./2026-04-11_fresh_spread_system_design.md)
- [Alpaca Capabilities Statement](../research/alpaca_capabilities_statement.md)

## Goal

Define a new config-driven backtest system that fits the current repo layout and the current options-automation runtime model.

The recommendation should optimize for runtime parity with this repo's actual system, not for generic backtesting convenience.

Implementation note:

- the recommended cutover has since landed
- the current public CLI surface is `packages/core/cli/backtest.py`
- the canonical historical-evaluation engine now lives in `packages/core/backtest/`
- references below to the old `replay` CLI/service describe the pre-cutover baseline that motivated this recommendation unless explicitly updated

## Code Anchors Investigated

Current recommendation is grounded in these code paths:

- current backtest CLI: `packages/core/cli/backtest.py`
- canonical backtest engine: `packages/core/backtest/`
- current config loading: `packages/core/services/bots.py`, `packages/core/services/automations.py`, `packages/core/services/strategy_configs.py`
- current entry decision path: `packages/core/services/decision_engine.py`
- current management decision path: `packages/core/services/strategy_positions.py`
- current intent dispatch path: `packages/core/services/execution_intents.py`
- current execution ledger and position creation: `packages/core/services/execution.py`
- current position adapter: `packages/core/services/positions.py`
- current runtime storage models: `packages/core/storage/signal_models.py`, `packages/core/storage/execution_models.py`
- current historical quote and trade store: `packages/core/storage/models.py`, `packages/core/storage/run_history_repository.py`
- current market recorder: `packages/core/services/market_recorder.py`

## (1) Historical Evaluation Before The Cutover

Before the cutover, the `replay` system was an offline reconstruction and evaluation surface, not a true backtest engine.

### What it currently does

Before the cutover, `uv run spreads replay` called `build_opportunity_replay()` in `packages/core/services/opportunity_replay.py`.

That service:

- resolves a historical collector target from `session_id` or `label + session_date`
- loads persisted collector cycle candidates, or recovers top candidates from `scan_runs` and `scan_candidates`
- rebuilds a session-level offline decision view:
  - `RegimeSnapshot`
  - `StrategyIntent`
  - `HorizonIntent`
  - `Opportunity`
  - provisional allocation decisions
  - provisional offline `ExecutionIntent`
- compares modeled outcomes with post-market analysis and, when possible, actual traded positions
- exports human-readable summaries plus flattened analytics rows

This is useful for:

- operator audit
- historical diagnosis
- comparing modeled allocator choices against legacy promotable baselines
- understanding why a past session looked good or bad

### Why it is not a backtest

It is not driven by the current bot runtime model.

Specifically, current replay does not:

- start from `Bot` + `StrategyConfig` + `Automation`
- run automation schedules across an arbitrary historical date range
- create real runtime `OpportunityDecisionModel` rows or the equivalent lifecycle in memory
- create runtime `ExecutionIntentModel` semantics with claim, submit, reprice, fill, cancel, and expiry behavior
- manage `StrategyPosition` lifecycle from entry through exit
- simulate bot limits and management automations as first-class run state

It is also tied to collector history and post-market analysis targets. The main input is a historical collector cycle, not a resolved bot configuration.

### Important current mismatch

The replay surface already uses some of the right nouns, but not the same runtime path.

In particular:

- replay builds lightweight domain `ExecutionIntent` objects in `packages/core/domain/opportunity_models.py`
- live automation uses persisted `execution_intents` rows in `packages/core/storage/execution_models.py`
- replay emits a provisional offline execution plan from `packages/core/services/opportunity_execution_plan.py`
- live automation creates real `OpportunityDecisionModel` and `ExecutionIntentModel` rows from `packages/core/services/decision_engine.py`

That means replay is already helpful, but extending it into the core backtest engine would deepen the wrong boundary.

## Current Runtime Baseline

The repo already has the right top-level runtime objects for backtesting:

- `BotConfig` in `packages/core/services/bots.py`
- `AutomationConfig` in `packages/core/services/automations.py`
- `StrategyConfig` in `packages/core/services/strategy_configs.py`
- `OpportunityDecisionModel` in `packages/core/storage/signal_models.py`
- `ExecutionIntentModel` in `packages/core/storage/execution_models.py`
- `portfolio_positions` as the current `StrategyPosition` storage adapter in `packages/core/storage/execution_models.py`

The live automation path already looks structurally like this:

```text
Bot + Automation + StrategyConfig
    -> select an Opportunity
    -> write OpportunityDecision
    -> create ExecutionIntent
    -> dispatch execution
    -> create/manage StrategyPosition
```

That is the path the new backtest system should mirror.

## Current Gaps That Matter For Backtest Design

There is one important architectural gap in the current code: config exists, but the runtime is not fully config-driven yet.

Observed gaps:

- `run_entry_automation_decision()` in `packages/core/services/decision_engine.py` filters the existing `opportunities` table; it does not build opportunities from `StrategyConfig`
- `StrategyConfig.builder_params` are currently loaded, but in practice they mostly feed collector scope derivation in `packages/core/services/bots.py`
- `StrategyConfig.entry_recipe_refs` and `management_recipe_refs` are loaded but are not yet the canonical execution path for entry and management planning
- `run_management_automation_decision()` in `packages/core/services/strategy_positions.py` is closer to the target because it already works from bot, automation, positions, and exit-policy evaluation

This matters because the final backtest should be config-driven, but phase 1 should not pretend that the live runtime already is.

## Required Prerequisite

For the improved backtest we actually want, this gap is not just context. It is a required implementation dependency.

The repo can build a temporary bootstrap simulator before this work is finished, but the canonical improved backtest should not be considered complete until the runtime is genuinely config-driven.

Required prerequisite before the canonical backtest:

- `StrategyConfig` must drive historical opportunity generation rather than merely loading config fields
- `Automation` must drive planning scope, schedule semantics, and trigger behavior in a reusable way
- entry and management recipe refs must become real planning inputs rather than passive config payload
- the same config-driven planning path should be callable from both live runtime and backtest runtime

Without that prerequisite, a backtest would still be testing a partially reconstructed system rather than the intended automation runtime.

## (2) What Backtest Should Be

The new backtest system should be a deterministic historical simulation engine for the current automation runtime.

It should run this canonical flow:

```text
Bot + StrategyConfig + Automation
        |
        v
historical market data and quote state
        |
        v
opportunity generation
        |
        v
OpportunityDecision lifecycle
        |
        v
ExecutionIntent lifecycle
        |
        v
paper execution and repricing
        |
        v
StrategyPosition lifecycle
        |
        v
per-run metrics, audit logs, and exported analytics rows
```

### Required properties

The backtest system should:

- be config-driven from checked-in `packages/config` YAML
- reuse the same product nouns as live trading
- support event-driven multi-leg options behavior
- distinguish selection quality from execution quality
- be explicit about fidelity when quote history is incomplete
- be deterministic for the same config, data refs, and run window
- preserve `legs[]` as the canonical structure path end to end

### What it should not be

It should not:

- keep a thin alias over the old replay surface and call that sufficient
- write simulated rows into the live `opportunity_decisions`, `execution_intents`, or `portfolio_positions` tables by default
- require translation into another engine's order model as the canonical runtime path
- assume bar-only simulation is sufficient for multi-leg option execution quality

## (3) Recommended Architecture

## Recommendation Summary

Use a custom in-repo event-driven backtest engine in `packages/core/backtest/`.

Do not keep `replay` as a public sibling product surface.

Do not rebuild or extend the removed `packages/core/services/opportunity_replay.py` boundary.

### Why this is the right architecture

The repo's main problem is not lack of a generic backtesting framework. The repo's main problem is that the current live runtime and current replay path are not yet the same system.

The best backtest architecture is therefore:

- keep the current live nouns
- extract the pure runtime kernels that matter
- run them against historical data in a deterministic engine
- keep live broker orchestration and historical simulation as separate adapters over shared decision logic

That gives the highest parity with the least model distortion.

### Boundary Recommendation

Use these boundaries.

#### `packages/config`

Owns checked-in definitions only.

Keep existing:

- `strategies/*.yaml`
- `automations/*.yaml`
- `bots/*.yaml`
- `universes/*.yaml`

Add:

- `backtests/*.yaml`

`backtests/*.yaml` should be thin run specs, not a second strategy-definition system.

Recommended contents:

- `backtest_id`
- `bot_id`
- optional `automation_ids`
- date range
- data-source policy
- fill-model policy
- output/export settings
- optional config overrides for controlled research runs

The canonical strategy definition should still come from existing `Bot`, `Automation`, and `StrategyConfig` YAML.

#### `packages/core`

Owns all backtest business logic.

Recommended module layout:

```text
packages/core/services/
  entry_planner.py
  management_planner.py
  execution_policy.py
  backtest/
    specs.py
    data.py
    engine.py
    position_book.py
    execution_simulator.py
    metrics.py
    artifacts.py
```

Recommended responsibilities:

- `entry_planner.py`
  - pure entry planning from bot + automation + opportunity set + bot metrics
  - this is the boundary extracted from `decision_engine.py`

- `management_planner.py`
  - pure management planning from bot + automation + open positions + marks
  - this is the boundary extracted from `strategy_positions.py`

- `execution_policy.py`
  - pure pricing, repricing, timeout, and cancellation rules
  - shared by live intent dispatch and backtest execution simulation

- `backtest/specs.py`
  - parse resolved `BacktestSpec`

- `backtest/data.py`
  - historical data loading
  - quote and trade window lookup from `RunHistoryRepository`
  - underlying bars and option-bar fallback where needed

- `backtest/engine.py`
  - event loop and deterministic clock
  - automation cadence scheduling
  - per-session orchestration

- `backtest/position_book.py`
  - in-memory `StrategyPosition` state
  - marks, realized PnL, remaining quantity, expiry handling

- `backtest/execution_simulator.py`
  - convert `ExecutionIntent` into simulated fills, cancels, reprices, and close attempts

- `backtest/metrics.py`
  - summary metrics, funnel metrics, drawdowns, per-bot and per-automation outputs

- `backtest/artifacts.py`
  - JSON and CSV export
  - optional parquet later if row volume grows

#### `packages/api`

Stay a thin adapter.

Phase 1 recommendation:

- no new API surface required

Later recommendation:

- add read-only list and detail endpoints for persisted backtest run summaries if the CLI export flow proves useful enough to operationalize

Do not put simulation logic in API handlers.

### Runtime Shape

The backtest engine should use in-memory state for simulated decisions, intents, attempts, and positions.

Do not reuse live tables as the active simulation store.

Reason:

- live and simulated rows should not be mixed
- backtest runs need isolated deterministic state
- simulation often needs extra fidelity metadata that does not belong in live runtime tables

The engine can still emit rows shaped like the live runtime nouns.

Recommended output artifacts:

- `summary.json`
- `events.jsonl`
- `opportunity_decisions.csv`
- `execution_intents.csv`
- `strategy_positions.csv`
- `fills.csv`
- `metrics.json`

If persistent run history becomes important later, add a small `backtest_runs` metadata layer and store artifact refs instead of stuffing large payloads into Postgres rows.

### Shared-Kernel Strategy

Do not try to reuse the current live entry and management services directly by faking the entire storage layer.

That would preserve surface parity but would lock the backtest engine to current orchestration and wall-clock assumptions.

Instead:

1. keep `decision_engine.py`, `strategy_positions.py`, and `execution_intents.py` as live orchestration adapters
2. extract their pure planning logic into shared modules
3. let live services call those pure kernels against Postgres-backed state
4. let backtest call those same pure kernels against in-memory simulation state

That is the cleanest parity boundary in this repo.

### Historical Data Contract

The engine should consume historical data in this priority order.

#### Selection and strategy generation

- underlying bars and event context
- option contract metadata
- historical option quote and trade windows when recorded
- existing opportunity or candidate history only as an interim bootstrap path

#### Execution and fill simulation

- recorded option quote events from `option_quote_events`
- recorded option trade events from `option_trade_events`
- snapshot or option-bar-derived fallback marks when quote history is missing

### Fidelity Model

The backtest system must label fidelity explicitly.

This is required because Alpaca does not document a general historical option quote REST surface, and repo-recorded quote coverage will be uneven.

Recommended labels:

- `high`
  - recorded option quote windows exist for the legs being simulated

- `medium`
  - entry selection is data-backed, but execution uses snapshot or option-bar fallback for some periods

- `reduced`
  - fills and marks depend materially on synthetic midpoint estimates or sparse historical coverage

This should be visible in every backtest run summary.

### Opportunity Source Recommendation

Long-term target:

- rebuild opportunities from historical data using `Bot` + `Automation` + `StrategyConfig`

Short-term bootstrap:

- allow a temporary mode that starts from persisted `opportunities` or recovered `scan_candidates`

This is the key rollout compromise.

It lets the repo validate decision, intent, execution, and position parity before the upstream strategy-builder path is fully extracted from the current collector and scanner system.

### What Not To Reuse As The Core

Do not make any of these the canonical engine:

- `opportunity_replay.py`
- `analysis.py`
- pipeline/session compatibility surfaces

Those are the wrong center of gravity for a bot-driven backtest.

## (4) Recommended Libraries And Non-Recommended Ones

## Recommended Core Choice

Use no external backtesting framework as the core engine.

This is the recommendation.

### Why custom is better here

- current repo nouns already match the target runtime
- current repo already has multi-leg execution intent and position lifecycle semantics
- current repo needs parity with bot scheduling, gating, repricing, and management logic more than it needs generic portfolio backtesting features
- external engines would require translation away from `Bot`, `Automation`, `OpportunityDecision`, `ExecutionIntent`, and `StrategyPosition`

## Recommended Helper Libraries

These may help, but none should be the core engine.

### Keep using `pandas-market-calendars`

Already in the repo and appropriate for trading-session scheduling.

### Consider `polars` later for analytics only

Use case:

- larger row exports
- faster post-run aggregation
- parameter-comparison reports

Do not make it the simulation core.

### Consider `quantstats` or `empyrical` later for reporting only

Use case:

- return-series metrics
- tear-sheet style reporting

These are reporting helpers, not runtime helpers.

## Non-Recommended As Core Engines

### `vectorbt`

Recommendation:

- not recommended as the core backtest engine
- acceptable later as an analytics layer over exported run rows

Why it does not fit as core:

- strongest for vectorized strategy sweeps, not stateful automation lifecycles
- poor fit for multi-leg intent state, repricing, cancellation, and management automations
- would encourage bar-based shortcuts exactly where this repo needs event-driven parity
- would require flattening multi-leg options state into a simpler portfolio abstraction and then translating back

### `backtrader`

Recommendation:

- not recommended as the core engine

Why it does not fit:

- event-driven, but built around generic bars and broker abstractions rather than this repo's intent model
- weak native support for realistic multi-leg options automation
- likely to force synthetic-instrument or adapter-heavy modeling for spreads and condors
- would add framework constraints without giving the repo a better parity story than a custom engine

### `QuantConnect LEAN`

Recommendation:

- not recommended as the embedded core engine for this repo
- useful only as external reference material

Why it is tempting:

- stronger option support than most Python backtesting libraries
- event-driven model
- better idea of assignment, expiry, and option-universe handling than generic retail libraries

Why it is still the wrong core choice here:

- it is a full separate platform with its own algorithm lifecycle and data model
- it would create a second architecture beside this repo rather than extending this repo's runtime
- backtest parity would become parity with LEAN adapters, not parity with the current bot runtime
- the translation cost is especially high for current repo concepts like `Automation`, `OpportunityDecision`, and `ExecutionIntent`

## Bottom Line On Libraries

No external library genuinely solves the hard part of this repo's problem.

The hard part is:

- config-driven strategy generation
- deterministic bot scheduling
- multi-leg option fill simulation under imperfect quote history
- parity with current intent and position semantics

That should stay in-repo.

## (5) Phased Implementation Plan

## Phase 0: Extract Shared Runtime Kernels

Goal:

- separate pure planning logic from live orchestration

Work:

- extract pure entry planning from `decision_engine.py`
- extract pure management planning from `strategy_positions.py`
- extract shared pricing and reprice policy from `execution_intents.py`
- define a small in-memory `StrategyPosition` shape aligned with `portfolio_positions`

Success criteria:

- live services still behave the same
- backtest can call the same pure kernels without depending on wall-clock storage orchestration

## Phase 1: Make Runtime Truly Config-Driven

Goal:

- make config the canonical driver of planning behavior instead of a mostly loaded-but-partially-unused definition layer

Work:

- extract strategy-builder logic from the current scanner and collector path into reusable services
- make `StrategyConfig.builder_params` part of a real generation path rather than mostly collector-scope derivation
- wire `entry_recipe_refs` into actual entry planning semantics
- wire `management_recipe_refs` into actual management planning semantics
- make `Automation` scheduling and trigger policy reusable across both live and historical runtime

Success criteria:

- same `Bot` + `Automation` + `StrategyConfig` can drive both live planning and historical planning
- the runtime is no longer only config-loaded; it is materially config-driven
- the improved backtest can evaluate the actual intended automation path rather than a bootstrap approximation

## Phase 2: Bootstrap Backtest From Existing Opportunity History

Goal:

- build a real decision-to-position simulation before rebuilding the full upstream opportunity-generation path

Scope:

- one lane only: `put_credit_spread`
- one bot spec from `packages/config/bots`
- one or more entry and management automations
- historical window limited to dates with useful quote coverage

Inputs:

- existing `opportunities` rows or recovered candidate history
- recorded quote and trade events when available
- current bot limits and exit-policy logic

Outputs:

- simulated `OpportunityDecision`
- simulated `ExecutionIntent`
- simulated `StrategyPosition`
- run metrics and fidelity labels

Why this phase is worth doing:

- it validates the downstream runtime model now
- it can still help while the upstream historical generation path matures
- it produces an immediately useful research tool that is already materially beyond replay

Important constraint:

- this remains a bootstrap backtest mode, not the final improved backtest

## Phase 3: Build The Improved Config-Driven Backtest Path

Goal:

- stop depending on persisted opportunity history as the canonical backtest input

Work:

- make backtest rebuild historical opportunity sets directly from config and historical data
- reuse the config-driven planning kernels from Phase 1 instead of replay-style reconstruction
- keep persisted opportunities and recovered candidates only as compatibility/bootstrap inputs

Success criteria:

- current replay remains available for audit, but improved backtest no longer depends on replay-style reconstruction
- improved backtest is evaluating the intended runtime model, not a historical artifact adapter

## Phase 4: Expand Strategy Coverage

Order:

1. `call_credit_spread`
2. `iron_condor`

Reason for this order:

- call-credit is structurally close to put-credit and validates symmetry
- iron-condor adds the real four-leg complexity and should come only after the execution simulator and position book are stable

Work:

- expand fill-model support for four-leg structures
- validate `legs[]` pricing and mark aggregation for condors
- add more exit-path coverage and expiry handling

## Phase 5: Comparison Workflows And Thin API Surface

Goal:

- make backtests easy to compare without turning API into a second engine

Work:

- add CLI compare commands around exported runs
- optionally persist small `backtest_runs` metadata rows plus artifact refs
- add API list/detail routes only if there is a real consumer

Optional extras in this phase:

- parameter sweeps
- analytics helpers using `polars`
- tear-sheet reporting using `quantstats` or `empyrical`

## Phase 6: Retire Temporary Bootstrap Paths

Goal:

- make config-driven historical opportunity generation the canonical backtest path

Work:

- demote persisted-opportunity bootstrap mode to compatibility only
- keep audit and reconstruction as report or input-adapter layers inside `backtest`, not as sibling products
- stop treating session-centric collector artifacts as the center of research workflows

## Final Recommendation

The repo should have one historical-evaluation surface: `backtest`.

Use `backtest` for:

- config-driven historical simulation
- bot-runtime parity
- decision-to-intent-to-position research
- execution-quality and management-quality evaluation
- audit or reconstruction adapters when those inputs are needed

Required dependency for that improved backtest:

- first make the runtime truly config-driven rather than merely config-loaded

The best path for this repo is:

1. make `backtest` the only public historical-evaluation surface
2. keep the custom event-driven `backtest` engine inside `packages/core/backtest/`
3. extract shared pure kernels from the current live runtime instead of reusing live orchestration directly
4. make the runtime genuinely config-driven before calling the improved backtest complete
5. use `packages/config/backtests/*.yaml` as thin run specs over the existing bot, automation, and strategy config system
6. treat `vectorbt`, `backtrader`, and LEAN as non-core tools because they reduce parity instead of improving it

That is the highest-confidence path to a real backtest system for this repo.
