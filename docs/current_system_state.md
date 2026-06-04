# System Architecture

This document is the canonical source of truth for the current `spreads` runtime architecture, ownership vocabulary, and service boundaries.

It describes the system as it exists in code today. Planning documents can describe history or target states, but when they disagree with this file, this file wins.

Last updated: 2026-06-04

## Top-Level Boundaries

| Boundary | Current owner | Notes |
|---|---|---|
| Operator interfaces | `packages/web`, `packages/api`, `packages/core/cli` | Web, API, and CLI are adapters over service-owned state. They must not own trading logic. |
| Trading strategy config | `packages/config/trading_strategies`, `services/trading_strategies.py`, `services/trading_strategy_runtime.py` | A `trading_strategy` is the product/operator owner for source, trade structure, entry routine, management routine, risk, limits, and execution settings. |
| Scheduling and workers | `packages/config/jobs`, `packages/core/jobs`, `services/runtime_policy.py` | Declared jobs and generated trading-strategy jobs are the scheduler source of truth. Runtime workers execute broker sync, strategy entry/manage, dispatch, recovery, and alert jobs. |
| Dynamic ticker sources | `packages/config/ticker_sources`, `services/ticker_sources.py` | Ticker sources materialize reusable underlying lists. `finviz_momentum` feeds `momentum_long_calls`. |
| Market data capture | `services/trading_engine/capture_targets.py`, `services/market_recorder.py`, `storage/capture_repository.py` | `DataEngine` owns desired capture state in `capture_targets`; `market_recorder.py` is the normal Alpaca option websocket owner and reconciles the prioritized target set into quote/trade events plus `capture_summaries`. |
| Discovery and scanning | `services/scanners/`, `services/discovery_runs/`, `services/live_selection.py`, `services/opportunity_scoring.py`, `services/candidate_policy.py` | Discovery scans strategy scopes, ranks candidates, captures diagnostics, and persists discovery-owned cycle state. |
| Signal and opportunity state | `services/signal_state.py`, `services/opportunity_generation.py`, `services/opportunities.py`, `storage/signal_repository.py` | Owns signal states, strategy runs, opportunities, and strategy-owned runtime projections. |
| Execution and portfolio state | `services/execution/`, `services/execution_intents/`, `services/session_positions.py`, `services/broker_sync.py`, `services/risk_manager.py`, `services/exit_manager.py` | Owns intent dispatch, broker submission, order/fill facts, position attribution, reconciliation, and close behavior. |
| Operator read models | `services/live_runtime.py`, `services/discovery_run_health/`, `services/pipelines.py`, `services/ops/` | Read models compose persisted state for operator status, jobs, trading health, audit, positions, and opportunities. |
| Research AI layer | `services/tradingagents_scan.py`, `packages/config/jobs/tradingagents_scan_finviz_momentum.yaml`, `external/TradingAgents` | Spreads owns orchestration, job config, artifacts, alerts, and visibility. The external TradingAgents repo owns its own agent internals. |
| Persistence and transport | Postgres, Redis | Postgres is source of truth. Redis handles queues, leases, and pub/sub fanout. |

## Non-Negotiable Boundary Rules

- `trading_strategy_id` is the canonical runtime owner for strategy-owned opportunities, decisions, intents, attempts, and positions.
- Authored trading strategy config lives in `packages/config/trading_strategies`. Do not recreate legacy wrapper directories around it.
- Strategy routines generate jobs named `trading_strategy:<strategy_id>:entry` and `trading_strategy:<strategy_id>:manage`.
- `execution_intent_dispatch:global` owns the global pending-intent dispatch loop.
- `trade_structure` names reusable option construction behavior, such as `long_call`, `call_credit_spread`, `iron_condor`, or `short_put`.
- `source` names the candidate source for a strategy. Current source types are `static` and `dynamic`.
- Discovery runs are diagnostic/scanning surfaces. They are not the product owner of execution.
- Strategy-owned opportunities are projections over scan/feed candidates and are persisted with `trading_strategy_id` and `strategy_run_id`.
- `pipeline_id` remains discovery lineage and compatibility identity, not the primary runtime owner.
- Capture is desired state, not a discovery side effect. The priority order is open positions, working intents/attempts, selected candidates, then watch candidates.
- `services/market_recorder.py` is the sole Alpaca option websocket owner in normal runtime. It reads `capture_targets` by priority and records `capture_summaries`.
- `execution_intents` is the control-plane handoff boundary. It selects an execution runtime before broker submission.
- `alpaca_direct` is the active Python-native runtime for equity, single-leg option, and Alpaca order-payload submission.
- `session_positions` owns day/session position attribution. Broker positions are reconciliation input, not the sole position truth.
- Spreads is the active trading-ops and research-orchestration home. The old `trading_operator` wrapper repo is not an active hub for future operator guidance.
- `external/TradingAgents` is a symlink to `/home/ade/Projects/TradingAgents`. Spreads may orchestrate research jobs against it, but does not own the external repo's internals.

## Domain Ownership Map

| Domain object | Meaning | Source of truth / owner | Must not own |
|---|---|---|---|
| Trading strategy | Operator/product trading unit with source, trade structure, routines, risk, limits, execution settings, and config hash. | `packages/config/trading_strategies`, `services/trading_strategies.py` | Discovery-session identity, broker facts, or dashboard-only state. |
| Trade structure | Reusable option construction family. | `services/strategy_builders.py`, `services/option_structures.py`, scanner builders | Runtime owner identity. |
| Routine | Scheduled strategy behavior such as entry or manage. | `services/trading_strategy_runtime.py`, generated job specs | Broker submission facts. |
| Ticker source | Reusable dynamic symbol list. | `packages/config/ticker_sources`, `services/ticker_sources.py`, `ticker_source:*` jobs | Execution ownership or position attribution. |
| Discovery run | Scanner/capture cycle for a strategy scope. | `packages/config/discovery_runs`, `services/discovery_runs/`, `discovery_runs` tables | Product/runtime ownership. |
| Strategy run | One persisted strategy-runtime sync pass for a strategy/cycle. | `strategy_runs`, `storage/signal_repository.py` | Broker facts or position PnL. |
| Signal | Normalized market/setup observation that may become an opportunity. | `signal_states`, `signal_state_transitions`, `services/signal_state.py` | Alerts, broker sync, or frontend state. |
| Opportunity | Candidate trade row, either discovery-owned or strategy-owned. | `opportunities`, `services/opportunity_generation.py`, `services/opportunities.py` | Order/fill truth or account policy. |
| Decision | Strategy/lifecycle choice such as selected, skipped, blocked, or no-entry. | Strategy and lifecycle services that create opportunity decisions and intent payloads | Alert delivery or dashboard-only read models. |
| Admission | Account/risk/policy answer to whether an approved idea can be carried now. | `services/risk_manager.py`, `services/execution/`, admission payloads | Account snapshots alone. |
| Intent | Control-plane request to open, manage, or close. | `execution_intents`, `services/execution_intents/` | Broker order/fill persistence. |
| Attempt | Broker-facing submission/refresh/cancel lifecycle for an intent. | `execution_attempts`, `services/execution/` | Session position attribution. |
| Order | Broker order fact attached to an attempt. | `execution_orders`, broker refresh paths | Opportunity selection. |
| Fill | Broker fill fact attached to an order/attempt. | `execution_fills`, broker refresh paths | Strategy selection. |
| Position | Day/session-local ownership and PnL projection. | `services/session_positions.py`, `portfolio_positions`, close records | Broker inventory as independent truth. |
| Close | Decision, intent, attempt, and fill path that reduces or exits a position. | `services/exit_manager.py`, `services/execution_intents/`, `services/execution/` | Separate close-only bypasses. |
| Broker sync | Poll-first broker/account health and fact ingestion. | `services/broker_sync.py`, `broker_sync_state`, `account_snapshots` | Trading decisions or owner attribution. |
| Capture target | Desired option contract capture need with owner, reason, priority, TTL, and quote/trade flags. | `services/trading_engine/capture_targets.py`, `capture_targets`, `storage/capture_repository.py` | Scanner diagnostics or broker order truth. |
| Capture summary | Market-recorder iteration summary for target pressure, captured rows, groups, and errors. | `services/market_recorder.py`, `capture_summaries` | Raw quote/trade event retention. |
| Trading ops state | Operator-facing trading health: market, control, scheduler/workers, strategies, decisions, intents, attempts, positions, exits, risk, and attention. | `services/ops/`, `services/live_runtime.py`, `services/pipelines.py` | Frontend stitching or live Alpaca calls during default dashboard render. |
| Storage ops state | Operator-facing retention/storage health. | `services/retention.py`, storage ops surfaces | Live trading decisions. |
| Research scan | Batch TradingAgents research run over a bounded ticker list. | `services/tradingagents_scan.py`, `outputs/tradingagents/`, `external/TradingAgents` | Live execution admission. |

## Runtime Stack

```text
Operator
  |
  +--> Browser -> Next.js web -> FastAPI -> Postgres/Redis
  |
  +--> `uv run spreads ...` CLI -> services -> Postgres/Redis/Alpaca

Scheduler
  |
  +--> declared YAML jobs + generated strategy routine jobs
  |
  +--> Redis queues

ARQ workers
  |
  +--> runtime lane: broker sync, trading strategy entry/manage, intent dispatch, recovery, alerts
  +--> discovery lane: discovery runs, ticker sources
  +--> valuation lane: company valuation jobs
  +--> research lane: TradingAgents jobs when enabled

Market recorder
  |
  +--> prioritized capture_targets -> Alpaca option websocket -> option quote/trade tables + capture_summaries

Postgres = source of truth
Redis = queues, leases, pub/sub
```

## Current Runtime Jobs

Current main job types:

- `ticker_source`
- `discovery_run`
- `broker_sync`
- `discovery_recovery`
- `trading_strategy_entry`
- `trading_strategy_manage`
- `execution_intent_dispatch`
- `alert_delivery`
- `alert_reconcile`
- `company_valuation_bootstrap`
- `company_valuation_screen_materialize`
- `company_valuation_resolve_unresolved`
- `tradingagents_scan`

The research lane is disabled by default in the deployed compose stack. It can be enabled intentionally when research jobs are needed.

## Trading Strategy Ownership

Trading strategies are authored as one file per strategy in `packages/config/trading_strategies`.

Each strategy owns:

- `trading_strategy_id`
- `trade_structure`
- candidate `source`
- scanner/build settings
- entry and management routine schedules
- runtime controls
- risk and limit policy references
- execution mode, approval mode, environment, and runtime
- `config_hash`

Current active strategies:

- `momentum_long_calls`
- `short_dated_earnings_call_debit`
- `short_dated_earnings_long_straddle`
- `short_dated_earnings_long_strangle`
- `short_dated_earnings_put_debit`
- `short_dated_etf_short_put`
- `short_dated_index_call_credit`
- `short_dated_index_iron_condor`
- `short_dated_index_put_credit`

`momentum_long_calls` is the Finviz-fed long-call strategy. It consumes `ticker_source:finviz_momentum`, enters during market hours on a 2-minute cadence, and manages during market hours on a 1-minute cadence.

## Discovery And Opportunity State

Discovery runs still scan and persist diagnostic cycle state. They are useful for scanner health, capture health, candidate diagnostics, and research/operator inspection.

Strategy-owned entry state is persisted separately through:

- `strategy_runs`
- `opportunities.trading_strategy_id`
- `opportunities.strategy_run_id`
- `opportunity_decisions.trading_strategy_id`
- `execution_intents.trading_strategy_id`
- `portfolio_positions.trading_strategy_id`

This removes the old split between direct source jobs and config-wrapper runtime jobs. Dynamic-source and static strategies both flow through the same strategy ownership model.

## Execution Domain

`execution_intents` records pending open/manage/close work and dispatch state. The dispatch job claims pending intents and routes them to `services/execution/`.

`services/execution/` records immutable broker-facing facts in:

- `execution_attempts`
- `execution_orders`
- `execution_fills`

`services/session_positions.py` and `services/exit_manager.py` own position attribution and close policy. Close actions go through intents and attempts; they should not bypass the execution lifecycle.

## Operator Read Models

Operator views should read service-owned state through:

- `services/ops/`
- `services/live_runtime.py`
- `services/pipelines.py`
- `services/opportunities.py`
- `services/positions.py`
- `services/execution/runtimes.py`

The dashboard should show strategy-owned runtime state, not recreate old runtime pages or infer business logic in the frontend.

## Rollout Notes

- After schema changes, run `uv run alembic upgrade head`.
- After declared job YAML or strategy config changes, restart the scheduler and affected workers so they reload config.
- After code imported by runtime/discovery workers changes, restart those containers before trusting live behavior.
- Default validation is live/runtime checks through shipped CLIs and operator reads. Do not add automated tests unless explicitly requested.
