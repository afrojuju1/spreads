# System Architecture

This document is the canonical source of truth for the current `spreads` runtime architecture, ownership vocabulary, and service boundaries.

It describes the system as it exists in code today. Planning documents can describe history or target states, but when they disagree with this file, this file wins.

Last updated: 2026-06-06

## Top-Level Boundaries

| Boundary | Current owner | Notes |
|---|---|---|
| Operator interfaces | `packages/web`, `packages/api`, `packages/core/cli` | Web, API, and CLI are adapters over service-owned state. They must not own trading logic. Top-level CLI commands such as `spreads status`, `spreads trading`, `spreads storage`, `spreads jobs`, and `spreads logs` are the operator-friendly entrypoints over canonical services. |
| Trading strategy config | `packages/config/trading_strategies`, `services/trading_strategies.py`, `services/trading_strategy_runtime.py` | A `trading_strategy` is the product/operator owner for source, trade structure, entry routine, management routine, risk, limits, and execution settings. |
| Scheduling and workers | `packages/config/jobs`, `packages/core/jobs`, `services/runtime_policy.py` | Declared jobs and generated trading-strategy jobs are the scheduler source of truth. Runtime workers execute broker sync, strategy entry/manage, dispatch, and alert jobs; data workers execute ticker sources. Research and valuation workers are optional lanes, disabled by default, and not part of live trading health. |
| Dynamic ticker sources | `packages/config/ticker_sources`, `services/ticker_sources.py` | Ticker sources materialize reusable underlying lists. `finviz_momentum` feeds `momentum_long_calls`. |
| Market data capture | `services/trading_engine/capture_targets.py`, `services/market_recorder.py`, `storage/capture_repository.py` | `DataEngine` owns desired capture state in `capture_targets`; `market_recorder.py` is the normal Alpaca option websocket owner and reconciles the prioritized target set into option quote/trade ticks plus `capture_summaries`. |
| Engine data and candidate building | `services/trading_engine/data_runtime.py`, `services/strategy_builders.py`, stale `services/scanners/` internals | DataEngine resolves ticker sources/static sources and builds strategy-owned candidate inputs. The current `services/scanners/` package is retained only as stale candidate-builder implementation infrastructure; do not expand it as a product surface, orchestration path, or ownership boundary. Scanner math, ranking-policy, and runtime candidate filters are delegated build policy and persist diagnostics under engine-owned candidate facts. |
| Strategy signals and decisions | `services/trading_engine/strategy_runtime.py`, `services/live_selection.py`, `services/entry_planner.py`, `services/trading_engine/facts.py`, `storage/engine_fact_repository.py` | StrategyEngine owns entry orchestration: ticker resolution, candidate build, live signal selection, trade decisions, admission handoff, and intent creation. Helper modules are pure policy delegates, not alternate orchestration paths. |
| Execution and portfolio state | `services/trading_engine/portfolio_runtime.py`, `services/trading_engine/close_policy.py`, `services/trading_engine/risk_runtime.py`, `services/execution_intents/`, `services/execution/`, `services/session_positions.py`, `services/broker_sync.py`, `services/risk_manager.py`, `services/exit_manager.py` | PortfolioEngine owns close decisions and close-policy evaluation. RiskEngine-owned close admission validates position/reconciliation/order readiness. The manage job refreshes marks, applies close admission, and creates close intents; execution services dispatch intents and persist broker attempts/orders/fills. |
| Operator read models | `services/ops/`, `services/positions.py`, `services/execution/runtimes.py` | Read models compose persisted engine, jobs, trading health, positions, execution, account, retention, and capture state. Operator surfaces should project current domain facts instead of reintroducing removed product pages. |
| Company valuation lane | `services/company_valuation/`, `packages/config/company_valuation`, optional `worker-valuation` | Company valuation is an offline research/maintenance lane. It can support future analysis, but live strategy selection, admission, execution, and position management must not depend on it by default. |
| Research AI lane | `services/tradingagents_scan.py`, `packages/config/jobs/tradingagents_scan_finviz_momentum.yaml`, optional `worker-research`, `external/TradingAgents` | Spreads owns orchestration, job config, artifacts, alerts, and visibility. The external TradingAgents repo owns its own agent internals. This lane is disabled by default and is not a live execution dependency. |
| Persistence and transport | Postgres, Redis | Postgres is source of truth. Redis handles queues, leases, and pub/sub fanout. |

## Non-Negotiable Boundary Rules

- `trading_strategy_id` is the canonical runtime owner for strategy-owned candidates, signals, decisions, intents, attempts, and positions.
- Authored trading strategy config lives in `packages/config/trading_strategies`. Do not recreate legacy wrapper directories around it.
- Strategy routines generate jobs named `trading_strategy:<strategy_id>:entry` and `trading_strategy:<strategy_id>:manage`.
- `execution_intent_dispatch:global` owns the global pending-intent dispatch loop.
- `trade_structure` names reusable option construction behavior, such as `long_call`, `call_credit_spread`, `iron_condor`, or `short_put`.
- `source` names the candidate source for a strategy. Current source types are `static` and `dynamic`.
- Current runtime identity is `trading_strategy_id`, `ticker_source`, candidate runs, trade signals, trade decisions, admissions, intents, attempts, and positions. Do not add compatibility wrappers outside that model.
- Trade candidates, trade signals, trade decisions, and admission decisions are the active strategy-entry facts.
- Capture is desired state, not a scanner side effect. The priority order is open positions, working intents/attempts, selected candidates, then watch candidates.
- `services/market_recorder.py` is the sole Alpaca option websocket owner in normal runtime. It reads `capture_targets` by priority and records `capture_summaries`.
- `execution_intents` is the control-plane handoff boundary. It selects an execution runtime before broker submission.
- `alpaca_direct` is the active Python-native runtime for equity, single-leg option, and Alpaca order-payload submission.
- `session_positions` owns day/session position attribution. Broker positions are reconciliation input, not the sole position truth.
- Spreads is the active trading-ops and research-orchestration home. The old `trading_operator` wrapper repo is not an active hub for future operator guidance.
- `external/TradingAgents` is a symlink to `/home/ade/Projects/TradingAgents`. Spreads may orchestrate research jobs against it, but does not own the external repo's internals.
- Company valuation and TradingAgents research are optional offline lanes. Default trading health, live strategy entry/manage, execution admission, and close management must stay healthy without these workers or jobs enabled.

## Domain Ownership Map

| Domain object | Meaning | Source of truth / owner | Must not own |
|---|---|---|---|
| Trading strategy | Operator/product trading unit with source, trade structure, routines, risk, limits, execution settings, and config hash. | `packages/config/trading_strategies`, `services/trading_strategies.py` | Discovery-session identity, broker facts, or dashboard-only state. |
| Trade structure | Reusable option construction family. | `services/strategy_builders.py`, `services/option_structures.py`, scanner builders | Runtime owner identity. |
| Routine | Scheduled strategy behavior such as entry or manage. | `services/trading_strategy_runtime.py`, generated job specs | Broker submission facts. |
| Ticker source | Reusable static or dynamic symbol source. | `packages/config/ticker_sources`, `services/ticker_sources.py`, `ticker_source:*` jobs | Execution ownership or position attribution. |
| Ticker source run | One materialized ticker-source refresh plus selected, observed, and filtered ticker observations. | `ticker_source_runs`, `ticker_source_observations`, `ticker_source_state`, `services/ticker_sources.py` | Strategy candidate ownership, execution ownership, or broker facts. |
| Candidate run | One strategy candidate-build pass over resolved tickers. | `candidate_runs`, `trade_candidates`, `services/trading_engine/facts.py` | Broker facts or position PnL. |
| Trade signal | Normalized market/setup observation from a candidate. | `trade_signals`, `services/trading_engine/facts.py` | Broker sync or frontend state. |
| Trade decision | Strategy/lifecycle choice such as selected, skipped, blocked, or no-entry. | `trade_decisions`, `services/trading_engine/strategy_runtime.py` | Alert delivery or dashboard-only read models. |
| Admission | Account/risk/policy answer to whether an approved idea can be carried now. | `services/risk_manager.py`, `services/execution/`, admission payloads | Account snapshots alone. |
| Intent | Control-plane request to open, manage, or close. | `execution_intents`, `services/execution_intents/` | Broker order/fill persistence. |
| Attempt | Broker-facing submission/refresh/cancel lifecycle for an intent. | `execution_attempts`, `services/execution/` | Session position attribution. |
| Order | Broker order fact attached to an attempt. | `execution_orders`, broker refresh paths | Strategy selection. |
| Fill | Broker fill fact attached to an order/attempt. | `execution_fills`, broker refresh paths | Strategy selection. |
| Position | Day/session-local ownership and PnL projection. | `services/session_positions.py`, `portfolio_positions`, close records | Broker inventory as independent truth. |
| Close | Decision, admission, intent, attempt, and fill path that reduces or exits a position. | `services/trading_engine/portfolio_runtime.py`, `services/trading_engine/close_policy.py`, `services/trading_engine/risk_runtime.py`, `services/exit_manager.py`, `services/execution_intents/`, `services/execution/` | Direct broker-submit bypasses from management jobs or dashboard-only close decisions. |
| Broker sync | Poll-first broker/account health and fact ingestion. | `services/broker_sync.py`, `broker_sync_state`, `account_snapshots` | Trading decisions or owner attribution. |
| Capture target | Desired option contract capture need with owner, reason, priority, TTL, and quote/trade flags. | `services/trading_engine/capture_targets.py`, `capture_targets`, `storage/capture_repository.py` | Scanner diagnostics or broker order truth. |
| Capture summary | Market-recorder iteration summary for target pressure, captured rows, groups, and errors. | `services/market_recorder.py`, `capture_summaries` | Raw quote/trade tick retention. |
| Trading ops state | Operator-facing trading health: market, control, scheduler/workers, sources, candidates, signals, decisions, intents, attempts, positions, exits, risk, capture, and attention. | `services/ops/` | Frontend stitching or live Alpaca calls during default dashboard render. |
| Storage ops state | Operator-facing retention/storage health. | `services/retention.py`, storage ops surfaces | Live trading decisions. |
| Company valuation | Offline issuer valuation, ownership resolution, and research datasets. | `services/company_valuation/`, `packages/config/company_valuation`, optional `worker-valuation` | Live strategy entry, live execution admission, or position close management. |
| Research scan | Batch TradingAgents research run over a bounded ticker list. | `services/tradingagents_scan.py`, `outputs/tradingagents/`, optional `worker-research`, `external/TradingAgents` | Live strategy entry or live execution admission. |

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
  +--> runtime lane: broker sync, trading strategy entry/manage, intent dispatch, alerts
  +--> data lane: ticker sources
  +--> optional valuation lane: company valuation jobs when enabled
  +--> optional research lane: TradingAgents jobs when enabled

Market recorder
  |
  +--> prioritized capture_targets -> Alpaca option websocket -> option_quote_ticks / option_trade_ticks + capture_summaries

Postgres = source of truth
Redis = queues, leases, pub/sub
```

## Current Runtime Jobs

Default live trading job types:

- `ticker_source`
- `broker_sync`
- `trading_strategy_entry`
- `trading_strategy_manage`
- `execution_intent_dispatch`
- `alert_delivery`
- `alert_reconcile`

Optional offline job types, disabled by default:

- `company_valuation_bootstrap`
- `company_valuation_screen_materialize`
- `company_valuation_resolve_unresolved`
- `tradingagents_scan`

The valuation and research lanes are disabled by default in job config and deploy target config. `worker-valuation` and `worker-research` are compose profiles with zero replicas unless intentionally enabled. The live `TradingOpsState` health path should stay focused on trading/data/runtime lanes; optional offline lanes should appear as idle or disabled, not blocked live trading dependencies.

## Runtime Resource Policy

Always-on runtime:

- Postgres, Redis, API, web, scheduler, and the logging/metrics stack stay up so operator reads, dashboards, leases, queues, and storage maintenance remain available.
- Runtime workers stay up for broker/account sync, intent dispatch, alert reconciliation, and strategy routines, but market-only jobs are expressed in their job schedules instead of waking and skipping all night.
- `alert_reconcile` is intentionally allowed off-hours so pending notifications can recover without waiting for the next session.

Market-window runtime:

- Ticker sources with `allow_off_hours: false`, including `ticker_source:finviz_momentum`, refresh only inside the configured market calendar window.
- Trading strategy entry and manage routines compile `market_hours_only: true` into generated job payloads with `allow_off_hours: false`.
- Broker sync and execution dispatch remain schedule-gated with a short close grace period where configured.
- `market_recorder.py` stays deployed as the sole option-stream owner, but it idles outside regular market hours. It checks the market calendar cheaply, throttles idle logs, and does not refresh capture targets, open the Alpaca option websocket, or write capture summaries while closed unless explicitly run with `--no-market-hours-only`.

Scale defaults:

- The active live deploy target runs one data worker by default. The current data lane only owns ticker-source jobs, so extra always-warm data workers add memory pressure without improving the normal live path.
- Optional valuation and research workers remain profile-gated with zero replicas until intentionally enabled.

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

Current default-enabled strategy:

- `momentum_long_calls`

Available but disabled-by-default strategy configs:

- `short_dated_earnings_call_debit`
- `short_dated_earnings_long_straddle`
- `short_dated_earnings_long_strangle`
- `short_dated_earnings_put_debit`
- `short_dated_etf_short_put`
- `short_dated_index_call_credit`
- `short_dated_index_iron_condor`
- `short_dated_index_put_credit`

`momentum_long_calls` is the Finviz-fed long-call strategy. It consumes `ticker_source:finviz_momentum`, enters during market hours on a 2-minute cadence, and manages during market hours on a 1-minute cadence.

Disabled strategy configs are kept as authored strategy definitions, but they do not generate default scheduler jobs until intentionally re-enabled.

The long-vol strategy configs are disabled and shadow-mode by default as operator policy. The Spreads execution path itself supports their two-long-leg `mleg` debit order shape when a strategy is intentionally enabled for paper/live; long-vol must not be blocked by vertical-only width or return-on-risk validation.

## Engine Entry State

Strategy entry follows the Nautilus-shaped spine:

`DataEngine -> engine facts/read models -> StrategyEngine -> RiskEngine -> ExecutionEngine -> PortfolioEngine -> Ops projections`.

The active entry owner is `PostgresStrategyEngine` in `services/trading_engine/strategy_runtime.py`. It resolves tickers through DataEngine, builds candidates once, turns selected candidates into trade signals, plans trade decisions, runs admission, and creates execution intents. DataEngine records runtime-filter and ranking-policy diagnostics during candidate build; StrategyEngine does not run a second candidate-filter pass.

Active entry facts are persisted through:

- `ticker_source_runs`
- `ticker_source_observations`
- `ticker_source_state`
- `candidate_runs`
- `trade_candidates`
- `trade_signals`
- `trade_decisions`
- `trade_admissions`
- `execution_intents.trading_strategy_id`
- `portfolio_positions.trading_strategy_id`

Dynamic-source and static-source strategies both flow through the same strategy ownership model.

## Execution Domain

`execution_intents` records pending open/manage/close work and dispatch state. The dispatch job claims pending intents and routes them to `services/execution/`.

`services/execution/` records immutable broker-facing facts in:

- `execution_attempts`
- `execution_orders`
- `execution_fills`

Execution modules have explicit owners: `direct_orders.py` owns direct equity/option attempt creation, `position_close.py` owns close-by-position attempt creation, `submit.py` owns queued broker submission, `sync.py` owns refresh/cancel reconciliation, `attempts.py` owns attempt/order/fill payload sync helpers, `admission.py` owns execution admission payload helpers, and `order_requests.py` owns order payload construction plus live quote quality gates. Do not route new callers through the package root.

`services/session_positions.py` owns position attribution. `PostgresPortfolioEngine` owns close decisions and `services/trading_engine/close_policy.py` owns reusable profit/stop/force-close policy math. `services/trading_engine/risk_runtime.py` owns close admission checks for position status, reconciliation freshness, broker symbols, and order validity. `services/exit_manager.py` is the manage-job adapter: it refreshes marks, applies broker/active-close guards plus close admission, and creates close intents for selected closes. Close actions go through intents and attempts; they should not bypass the execution lifecycle.

## Hot-Path Abstraction Audit

Current audit result for the live trading hot path:

- Keep the thin engine contract modules in `services/trading_engine/`. They are ownership boundaries and typed payload shapes, not a second runtime, bus, actor framework, or alternate store.
- Keep `execution_submit` as the broker-submit isolation job. It gives each claimed intent a durable attempt/job lifecycle and lets ops distinguish dispatch, broker submission, and unknown-submit outcomes.
- Keep scanner/build policy helpers under DataEngine ownership. `services/scanners/` is stale implementation infrastructure retained for current candidate math and diagnostics; it should be redesigned/replaced in a future DataEngine-centered pass, not expanded. It must not become an alternate orchestration path, product surface, CLI flow, or persistence owner.
- Merge management scheduling into `trading_strategy_manage` only. The standalone `position_exit_manager` job type is retired as an active worker surface; `services/exit_manager.py` remains the Strategy/Portfolio manage adapter.
- Do not add a message bus, second database, actor framework, or compatibility wrapper around removed runtime surfaces.

## Operator Read Models

Operator views should read service-owned state through:

- `services/ops/`
- `services/positions.py`
- `services/execution/runtimes.py`

The dashboard should show strategy-owned runtime state, not recreate old runtime pages or infer business logic in the frontend.

## Rollout Notes

- After schema changes, run `uv run alembic upgrade head`.
- After declared job YAML or strategy config changes, restart the scheduler and affected workers so they reload config.
- After code imported by runtime/data workers changes, restart those containers before trusting live behavior.
- Default validation is live/runtime checks through shipped CLIs and operator reads. Do not add automated tests unless explicitly requested.
