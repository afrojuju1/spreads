# System Architecture

This document is the canonical source of truth for the current `spreads` runtime architecture, ownership vocabulary, and service boundaries.

It describes the system as it exists in code today. Planning documents can describe history or target states, but when they disagree with this file, this file wins.

Last updated: 2026-07-15

## Top-Level Boundaries

| Boundary | Current owner | Notes |
|---|---|---|
| Operator interfaces | `packages/web`, `packages/api`, `packages/core/cli` | Web, API, and CLI are adapters over service-owned state. They must not own trading logic. Canonical on-box CLI visibility lives under `spreads ops state`, `spreads ops storage`, `spreads jobs`, `spreads execution list`, `spreads execution positions`, and `spreads execution runtimes`. Backend historical evaluation is exposed through the narrow plural `spreads backtests run` adapter over `BacktestEngine`. Remote target reads go through `spreads deploy exec --env <target> -- ...`; command-level `--env` passthrough is intentionally not shipped. On-box logs use Docker Compose directly; remote deployment logs live under `spreads deploy logs`. |
| Trading strategy config | `packages/config/strategies/catalog.yaml`, `packages/config/strategies/profiles.yaml`, `services/trading_strategies.py`, `services/trade_structure_specs.py`, `services/trading_strategy_runtime.py` | A `trading_strategy` is the product/operator owner for source, trade structure, entry routine, management routine, risk, limits, and execution settings. Reusable trade-structure construction lives in code; authored strategy runtime config composes from the catalog and profiles only. |
| Workflow runtime | `packages/config/jobs`, `packages/core/jobs`, `core.workflow_runtime`, `core.workflows` | Declared routines and generated trading-strategy routines are reconciled into first-class routine schedules. Market-session/open/close schedules materialize an exchange-calendar-exact rolling 400-day horizon, including holidays and early closes; the maintenance lane reconciles every six hours to keep that horizon current. Runtime, data, maintenance, valuation, and research workers each bind the stable `run_scheduled_job_activity` provider name to an exact lane-local handler registry. Temporal owns routine identity, overlap, activity retry/backoff, heartbeat timeout, cancellation, recovery, and authoritative execution state. `RoutineActivityRunner` writes `job_runs` only as durable domain/operator projections; `retry_count` projects the Temporal activity attempt and is never an application requeue counter. Completed workflows return a versioned projection envelope with exact job/run identity, terminal job status, attempt, and bounded result. Canonical operator state reads Temporal visibility/description data so healthy pollers cannot mask stuck workflow tasks, Activity retries, retired queues, schedule failures, or terminal-provider/stale-Postgres mismatches. `spreads jobs repair-projection` can repair only an exact terminal run and refuses active or mismatched work; it never leases, requeues, or starts workflows. Scheduled and ad-hoc starts share the same workflow/activity path. Lifecycle broker activities and the capture-session workflow remain separate. Lifecycle, runtime, data, maintenance, and capture workflow lanes are required; research and valuation lanes are explicitly optional and disabled by default. The provider adapter is isolated under `core.workflow_runtime`. |
| Dynamic ticker sources | `packages/config/ticker_sources`, `services/sources/` | Ticker sources materialize reusable underlying lists. `finviz_momentum` feeds `momentum_long_calls` and filters Finviz rows through the strategy's target-DTE optionability/expected-move requirements before marking symbols selected. `earnings_event_window` reads `earnings_event_consensus`, applies Alpaca tradable/optionable/price/volume/target-DTE/expected-move checks, and persists selected plus filtered earnings-event observations for the earnings-source cutover. Filtered observations remain visible with stable reason codes. |
| Calendar events and earnings consensus | `integrations/calendar_events`, `storage/calendar_models.py`, `CalendarEventStore` | `calendar_events` stores normalized provider event facts. `provider_fetch_audit` stores bounded provider fetch/cache/error summaries. `earnings_event_consensus` stores derived earnings facts separately from provider rows. `calendar_event_refresh:earnings_30d` is the data-lane provider-fetch entrypoint for yfinance, Alpha Vantage, DoltHub, and sparse Finviz enrichment. Strategy runtime must not call yfinance, Alpha Vantage, DoltHub, or Finviz directly. |
| Market data capture | `services/trading_engine/capture_targets.py`, `services/market_capture.py`, `workflow_runtime/capture.py`, `storage/capture_repository.py`, `storage/market_data_store.py` | `DataEngine` owns desired capture state in `capture_targets`; the workflow-supervised capture session is the sole Alpaca option websocket owner and reconciles the prioritized target set into ClickHouse option quote/trade ticks plus Postgres `capture_summaries`. |
| Shared market context | `services/trading_engine/market_context.py`, `services/trading_engine/market_context_runtime.py`, `storage/engine_fact_repository.py`, `market_context_snapshots` | `MarketContextEngine` computes strategy-independent `MarketContextSnapshot` and `RegimeSnapshot` facts from benchmark `SymbolMarketSlice` inputs or stored benchmark facts. The snapshots describe observed broad-market regime, risk posture, trend strength, volatility state, benchmark evidence, freshness, confidence, data quality, and fidelity labels, then persist in Postgres as engine facts. They do not choose trades, inspect account state, or submit orders. Entry quality, allocation, ops, and backtests consume context references without recomputing regime. |
| Engine data and candidate building | `services/trading_engine/data_runtime.py`, `services/strategy_builders.py`, `services/strategy_candidate_builders/` | DataEngine resolves ticker sources/static sources and builds strategy-owned candidate inputs. `services/strategy_candidate_builders/` owns market slices, option construction, ranking policy, and diagnostics under engine-owned candidate facts. Candidate generation consumes market data through the `MarketSliceProvider` boundary; live behavior uses `AlpacaMarketSliceProvider` by default, while backtest/rerun work uses `HistoricalMarketSliceProvider` under `services/backtest/` for point-in-time ClickHouse/Postgres slices. There is no separate candidate-building CLI flow or orchestration boundary. |
| Strategy signals and decisions | `services/trading_engine/strategy_runtime.py`, `services/trading_engine/entry_selection.py`, `services/quality/`, `services/live_selection.py`, `services/entry_planner.py`, `services/trading_engine/facts.py`, `storage/engine_fact_repository.py` | StrategyRuntime owns entry orchestration and persistence. `EntrySelectionEngine` owns account-agnostic entry quality analysis, candidate filtering, selected/monitored/rejected candidate output, and live signal selection. Admission handoff and intent creation remain after selection. Helper modules are pure policy delegates, not alternate orchestration paths. |
| Execution, exits, and portfolio state | `services/trading_engine/portfolio_runtime.py`, `services/trading_engine/exit_runtime.py`, `services/trading_engine/close_policy.py`, `services/trading_engine/risk_runtime.py`, `services/execution_intents/`, `services/execution/`, `services/session_positions.py`, `services/broker_sync.py`, `services/risk/`, `services/exit_manager.py`, `core.workflows`, `core.engine` | PortfolioEngine owns position projection. ExitEngine owns position-exit snapshots, close-policy evaluation, and durable CloseDecision facts. `trade_admissions` is the immutable admission fact; only approved admissions create an `execution_intents` row, in the same transaction. `execution_intents` is the sole executable state and claim authority, and `execution_attempts.execution_intent_id` owns the child link. Execution lifecycle start creates deterministic trade/close lifecycle workflows from pending intents. Versioned intent transitions append `engine_events` and `engine_outbox` rows transactionally; broker attempt/order/fill persistence remains in `services/execution/` as workflow activity helpers. |
| Money and premium arithmetic | `money.py` | `core.money` is the canonical helper layer for USD Money construction, Decimal quantization, option premium/limit-price rounding, contract notionals, spread exposure, close PnL, and repricing tick math. Runtime services may still persist floats for compatibility, but they should not add new local `_round_money` or ad hoc premium/notional helpers. |
| Operator read models | `services/ops/`, `services/positions.py`, `services/execution/runtimes.py` | Read models compose persisted engine, jobs, trading health, positions, execution, account, storage, and capture state. Operator surfaces should project current domain facts instead of reintroducing removed product pages. |
| Backtest evaluation | `services/backtest/` | Backend-only BacktestEngine primitive exposed by `spreads backtests run`. Shipped modes are `stored_facts`, `strategy_rerun`, `execution_simulation`, `portfolio_simulation`, and `parameter_sweep`. `stored_facts` evaluates bounded date windows over the current ticker-source, candidate, signal, decision, admission, intent, attempt, position, and ClickHouse market-data model. `strategy_rerun` reruns current strategy config through historical source scope, `HistoricalMarketSliceProvider`, candidate builders, entry quality, entry selection, decision planning, and protection/portfolio admission artifacts while deferring broker/allocation capacity to execution simulation. `execution_simulation` composes over strategy reruns to emit isolated simulated intents, attempts, orders, and fills using executor profile TTL/repricing policy, structure validation, and historical quote snapshots/trade ticks with explicit fill-model fidelity. `portfolio_simulation` projects simulated or artifact-shaped stored fills into isolated positions, evaluates current close policy against historical quote marks, computes realized/unrealized PnL with `core.money`, and emits performance metrics with explicit metrics-engine fidelity. `parameter_sweep` expands bounded current-config overlays for profile/source/ranking/exit and related strategy parameters, validates each variant as a `TradingStrategyConfig`, runs a rerun or simulation base mode, ranks variants by the requested metric, and persists per-variant artifacts/results. Backtest persists isolated `backtest_runs`, `backtest_artifacts`, and `backtest_variant_results` rows plus ignored local result artifacts under `outputs/backtest_runs` by default. `HistoricalMarketSliceProvider` is the historical sibling of `AlpacaMarketSliceProvider`; it returns `SymbolMarketSlice` inputs from ClickHouse option quote/trade data plus Postgres source, candidate diagnostic, stored candidate, and earnings consensus facts with explicit fidelity labels. It compares current catalog strategy/profile/source variants from persisted facts and labels source, candidate, decision, execution, fill, position, exit, PnL, metrics, sweep, and market-data fidelity explicitly. It is not an operator app UI and does not revive removed replay/audit/analyze wrappers or the removed historical singular `spreads backtest` command. |
| Company valuation lane | `services/company_valuation/`, `packages/config/company_valuation`, optional `workflow-valuation` | Company valuation is an offline research/maintenance lane. It can support future analysis, but live strategy selection, admission, execution, and position management must not depend on it by default. |
| Research AI lane | `services/tradingagents_scan.py`, `packages/config/jobs/tradingagents_scan_finviz_momentum.yaml`, optional `workflow-research`, `external/TradingAgents` | Spreads owns orchestration, job config, artifacts, alerts, and visibility. The external TradingAgents repo owns its own agent internals. This lane is disabled by default and is not a live execution dependency. |
| Persistence and transport | Postgres, ClickHouse, Redis, workflow provider, NATS JetStream | Postgres is source of truth for durable domain and ops state, including projected job outcomes, engine events/outbox, calendar provider facts, provider fetch audit, and earnings event consensus. ClickHouse owns high-volume raw market-data ticks and compact quote snapshots. Redis handles pub/sub fanout and short-lived provider cache/backoff state. Temporal is the routine and lifecycle orchestration authority through the provider adapter. The Postgres `job_leases` table is retained only for the capture session's cross-workflow websocket-owner exclusion; routine execution does not acquire a lease. The `engine_outbox_publish` runtime routine publishes pending Postgres outbox rows to NATS JetStream and marks rows published only after JetStream acknowledges them. |

## Non-Negotiable Boundary Rules

- `trading_strategy_id` is the canonical runtime owner for strategy-owned candidates, signals, decisions, intents, attempts, and positions.
- Authored trading strategy config lives in `packages/config/strategies/catalog.yaml` and `packages/config/strategies/profiles.yaml`. Do not recreate per-strategy runtime YAML, paper-specific config directories, or compatibility wrappers around it.
- Strategy routines generate routine schedules named `trading_strategy:<strategy_id>:entry` and `trading_strategy:<strategy_id>:manage`.
- `execution_lifecycle_start:global` owns pending-intent workflow starts and uses deterministic lifecycle workflow IDs.
- `trade_structure` names reusable option construction behavior, such as `long_call`, `call_credit_spread`, `iron_condor`, or `short_put`.
- Money, premium, limit-price, notional, exposure, PnL, and repricing tick math belongs in `core.money`. Keep strategy/risk/execution services focused on policy decisions instead of reimplementing rounding and contract-multiplier arithmetic.
- `source` names the candidate source for a strategy. Current source types are `static` and `dynamic`.
- Current runtime identity is `trading_strategy_id`, `ticker_source`, candidate runs, trade signals, trade decisions, admissions, intents, attempts, positions, and close decisions. Do not add compatibility wrappers outside that model.
- Trade candidates, trade signals, trade decisions, and admission decisions are the active strategy-entry facts.
- Backtest evaluation is a service-owned backend primitive under `services/backtest/`, with the narrow `spreads backtests run` CLI as its current adapter. It must consume the current fact spine and ClickHouse market-data stores with explicit fidelity labels; do not reintroduce removed replay, audit, analyze, old singular backtest, or post-market analyze wrapper commands as backtest shortcuts.
- Capture is desired state, not a candidate-build side effect. The priority order is open positions, working intents/attempts, selected candidates, then watch candidates.
- The workflow-supervised capture session is the sole Alpaca option websocket owner in normal runtime. Its activity reads `capture_targets` by priority and records durable `capture_summaries` and workflow heartbeats.
- `trade_admissions` is the immutable admission boundary. Blocked and unknown outcomes remain admission facts and must not create executable work.
- Approved admission and `execution_intents` creation are one transaction. `execution_intents` is the sole executable state/claim authority and carries the resolved executor-profile snapshot before lifecycle workflow start and broker submission.
- Intent transitions are compare-and-swap updates over `state` and `state_version`; the matching `engine_events` and `engine_outbox` records are written in the same transaction.
- `execution_attempts.execution_intent_id` is the canonical intent-to-attempt link. Do not add reverse attempt pointers or local intent-event streams.
- Executor profiles are the strategy-owned contract for order style, quote freshness, submit TTL, cancel/reprice policy, max concession, stale-order handling, open/close lifecycle policy, and fail-closed unsupported-structure behavior.
- `alpaca_direct` is the active Python-native runtime for equity, single-leg option, and Alpaca order-payload submission.
- `session_positions` owns day/session position attribution. Broker positions are reconciliation input, not the sole position truth.
- Spreads is the active trading operations and research-orchestration home. The old `trading_operator` wrapper repo is not an active hub for future operator guidance.
- `external/TradingAgents` is a symlink to `/home/ade/Projects/TradingAgents`. Spreads may orchestrate research jobs against it, but does not own the external repo's internals.
- Company valuation and TradingAgents research are optional workflow lanes. Default trading health, live strategy entry/manage, execution admission, and close management must stay healthy without these lanes enabled.

## Domain Ownership Map

| Domain object | Meaning | Source of truth / owner | Must not own |
|---|---|---|---|
| Trading strategy | Operator/product trading unit with source, trade structure, routines, risk, limits, execution settings, and config hash. | `packages/config/strategies/catalog.yaml`, `packages/config/strategies/profiles.yaml`, `services/trading_strategies.py` | Discovery-session identity, broker facts, or dashboard-only state. |
| Trade structure | Reusable option construction family. | `services/strategy_builders.py`, `services/strategy_candidate_builders/`, `services/option_structures.py` | Runtime owner identity. |
| Routine | Scheduled strategy behavior such as entry or manage. | `services/trading_strategy_runtime.py`, generated job specs | Broker submission facts. |
| Ticker source | Reusable static or dynamic symbol source. | `packages/config/ticker_sources`, `services/sources/`, `ticker_source:*` jobs | Execution ownership or position attribution. |
| Ticker source run | One materialized ticker-source refresh plus selected, observed, and filtered ticker observations. | `ticker_source_runs`, `ticker_source_observations`, `ticker_source_state`, `services/sources/` | Strategy candidate ownership, execution ownership, or broker facts. |
| Calendar event | Normalized provider event fact such as earnings, dividends, splits, or macro events. | `calendar_events`, `integrations/calendar_events` adapters, `CalendarEventStore` | Derived consensus, strategy candidate ownership, or raw provider replay storage. |
| Provider fetch audit | Bounded durable summary of provider fetch/cache/error state. | `provider_fetch_audit`, `CalendarEventStore`, provider cache helpers | Raw provider payload archival, secrets, or strategy runtime truth. |
| Earnings event consensus | Derived per-symbol earnings event fact with event date, session timing, source support, conflicts, confidence, and stale-after time. | `earnings_event_consensus`, `integrations/calendar_events/consensus.py`, `CalendarEventStore` | Fake provider rows in `calendar_events`, direct external provider calls, or Alpaca actionability decisions. |
| Market context snapshot | Strategy-independent observed market context for the entry cycle or context job, including benchmark evidence, freshness, data quality, fidelity labels, and the current `RegimeSnapshot`. | `services/trading_engine/market_context.py`, `services/trading_engine/market_context_runtime.py`, `market_context_snapshots`, `EngineFactRepository`. | Strategy-specific scoring, account capacity, portfolio admission, broker submission, or alert delivery. |
| Regime snapshot | Shared broad-market regime classification with risk posture, trend strength, volatility state, confidence, source evidence, and fidelity labels. | `services/trading_engine/market_context.py`, computed by `MarketContextEngine` in `services/trading_engine/market_context_runtime.py`. | Per-strategy benchmark fetching, entry-quality-local regime calculation, allocation, execution admission, or order lifecycle policy. |
| Candidate run | One strategy candidate-build pass over resolved tickers. | `candidate_runs`, `trade_candidates`, `services/trading_engine/facts.py` | Broker facts or position PnL. |
| Trade signal | Normalized market/setup observation from an account-agnostic selected or monitored candidate. | `trade_signals`, `services/trading_engine/entry_selection.py`, `services/trading_engine/facts.py` | Broker sync, frontend state, or account-capacity checks. |
| Trade decision | Strategy/lifecycle choice such as selected, skipped, blocked, or no-entry. | `trade_decisions`, `services/trading_engine/strategy_runtime.py` | Alert delivery or dashboard-only read models. |
| Admission | Immutable account/risk/policy answer to whether an approved idea can be carried now. Entry admission is an `entry_capacity_precheck`; final quote, broker, and submit readiness stays in execution. Only approved admission creates an intent, atomically; blocked or unknown admission creates none. | `trade_admissions`, `services/risk/admission.py`, `services/execution/` | Executable lifecycle state, account snapshots alone, or strategy selection. |
| Intent | Sole executable request, state, claim, workflow-correlation, version, and supersession authority for open or close work. | `execution_intents`, `services/execution_intents/` | Admission outcomes, reverse attempt ownership, broker order/fill persistence, or a second local event log. |
| Attempt | Broker-facing submission/refresh/cancel lifecycle for an intent. The attempt owns the nullable `execution_intent_id` child link. | `execution_attempts`, `services/execution/` | Intent state/claim ownership or session position attribution. |
| Order | Broker order fact attached to an attempt. | `execution_orders`, broker refresh paths | Strategy selection. |
| Fill | Broker fill fact attached to an order/attempt. | `execution_fills`, broker refresh paths | Strategy selection. |
| Position | Day/session-local ownership and PnL projection. | `services/session_positions.py`, `portfolio_positions`, close records | Broker inventory as independent truth. |
| Close | Decision, admission, intent, attempt, and fill path that reduces or exits a position. | `services/trading_engine/exit_runtime.py`, `services/trading_engine/close_policy.py`, `services/trading_engine/risk_runtime.py`, `trade_close_decisions`, `services/exit_manager.py`, `services/execution_intents/`, `services/execution/` | Direct broker-submit bypasses from management jobs, portfolio-owned policy evaluation, or dashboard-only close decisions. |
| Broker sync | Poll-first broker/account health and fact ingestion. | `services/broker_sync.py`, `broker_sync_state`, `account_snapshots` | Trading decisions or owner attribution. |
| Capture target | Desired option contract capture need with owner, reason, priority, TTL, and quote/trade flags. | `services/trading_engine/capture_targets.py`, `capture_targets`, `storage/capture_repository.py` | Candidate diagnostics or broker order truth. |
| Capture summary | Capture-session iteration summary for target pressure, captured rows, groups, and errors. | `services/market_capture.py`, `capture_summaries` | Raw quote/trade tick storage or retention. |
| Trading ops state | Operator-facing trading health: market, control, routine schedules/workflow lanes, sources, candidates, signals, decisions, intents, attempts, positions, exits, risk, capture, and attention. | `services/ops/` | Frontend stitching or live Alpaca calls during default dashboard render. |
| Storage ops state | Operator-facing storage health for ClickHouse market data and Postgres capture summaries. | `services/ops/storage/state.py`, storage ops surfaces | Live trading decisions. |
| Backtest evaluation | Bounded backtest artifact for current-model historical windows, including request/config snapshots, run state, candidate productivity, selection quality, admission outcomes, execution/fill assumptions, exit-decision outcomes, position/PnL labels, admission risk context, MarketContext references, regime-bucket metrics, reason-code attribution, ClickHouse coverage/fidelity, artifact pointers, and per-strategy current-catalog variant metrics. | `services/backtest/`, `backtest_runs`, `backtest_artifacts`, `backtest_variant_results` | Alternate execution orchestration, broker submission, operator UI, live candidate/decision/execution/position writes, local regime recomputation, or legacy replay/audit/analyze wrappers. |
| Company valuation | Offline issuer valuation, ownership resolution, and research datasets. | `services/company_valuation/`, `packages/config/company_valuation`, optional `workflow-valuation` | Live strategy entry, live execution admission, or position close management. |
| Research scan | Batch TradingAgents research run over a bounded ticker list. | `services/tradingagents_scan.py`, `outputs/tradingagents/`, optional `workflow-research`, `external/TradingAgents` | Live strategy entry or live execution admission. |

## Runtime Stack

```text
Operator
  |
  +--> Browser -> Next.js web -> FastAPI -> Postgres/ClickHouse/Redis
  |
  +--> `uv run spreads ...` CLI -> services -> Postgres/ClickHouse/Redis/Alpaca

Routine schedules
  |
  +--> declared YAML jobs + generated strategy routine jobs
  |
  +--> workflow lanes

Workflow workers
  |
  +--> runtime lane: broker sync, trading strategy entry/manage, execution lifecycle start, alerts
  +--> data lane: ticker sources, calendar event refreshes
  +--> maintenance lane: routine reconciliation, backups, health snapshots, log retention
  +--> capture lane: workflow-supervised market capture
  +--> optional valuation lane: company valuation jobs when enabled
  +--> optional research lane: TradingAgents jobs when enabled

Market capture
  |
  +--> prioritized capture_targets -> Alpaca option websocket -> ClickHouse option_quote_ticks / option_trade_ticks + Postgres capture_summaries

Postgres = domain and ops source of truth
ClickHouse = high-volume raw market-data ticks and quote snapshots
Redis = pub/sub plus short-lived provider cache/backoff
Workflow runtime = Temporal-owned routine schedules, retries, execution, close, capture, and maintenance orchestration
Postgres job_leases = capture websocket-owner exclusion only; never routine orchestration
NATS JetStream = event projection fanout from engine_outbox via engine_outbox_publish
```

## Current Runtime Jobs

Default live trading job types:

- `ticker_source`
- `calendar_event_refresh`
- `broker_sync`
- `trading_strategy_entry`
- `trading_strategy_manage`
- `execution_lifecycle_start`
- `engine_outbox_publish`
- `alert_delivery`
- `alert_reconcile`
- `routine_schedule_reconcile`
- `postgres_backup`
- `ops_health_snapshot`
- `ops_log_retention`

Optional offline job types, disabled by default:

- `company_valuation_bootstrap`
- `company_valuation_screen_materialize`
- `company_valuation_resolve_unresolved`
- `tradingagents_scan`

The valuation and research workflow lanes are disabled by default in job config and deploy target config. `workflow-valuation` and `workflow-research` are compose profiles with zero replicas unless intentionally enabled. The live `TradingOpsState` health path stays focused on required workflow lanes; optional offline lanes appear as idle or disabled, not blocked live trading dependencies.

## Runtime Resource Policy

Always-on runtime:

- Postgres, ClickHouse, Redis, API, web, workflow lanes, and the logging/metrics stack stay up so operator reads, dashboards, leases, schedules, and market-data storage remain available.
- Runtime workers stay up for broker/account sync, execution lifecycle start, alert reconciliation, engine outbox publishing, and strategy routines, but market-only jobs are expressed in their routine schedules instead of waking and skipping all night.
- `alert_reconcile` is intentionally allowed off-hours so pending notifications can recover without waiting for the next session.
- `TradingOpsState` keeps broker-sync age and stale position marks visible after market close, but expected off-hours staleness is not degraded when the latest sync was healthy and there are no queued attempts, missing marks, broker quote errors, or reconciliation mismatches.

Market-window and data-refresh runtime:

- Ticker sources with `allow_off_hours: false`, including `ticker_source:finviz_momentum`, refresh only inside the configured market calendar window.
- `ticker_source:earnings_event_window` is allowed off-hours because it is a persisted source refresh over consensus facts and Alpaca actionability checks; strategy entry routines remain market-hours gated separately.
- `calendar_event_refresh:earnings_30d` is allowed off-hours and uses Redis TTL/backoff plus Postgres provider-fetch audit to avoid provider-call storms.
- Trading strategy entry and manage routines compile `market_hours_only: true` into generated job payloads with `allow_off_hours: false`.
- Broker sync and execution lifecycle start remain schedule-gated with a short close grace period where configured.
- The capture workflow lane stays deployed as the sole option-stream owner, but its capture session idles outside regular market hours. It checks the market calendar cheaply, emits workflow heartbeats, throttles idle logs, and does not refresh capture targets, open the Alpaca option websocket, or write capture summaries while closed unless explicitly run with `--no-market-hours-only`.

Scale defaults:

- The active live deploy target runs one data worker by default. The current data lane only owns ticker-source jobs, so extra always-warm data workers add memory pressure without improving the normal live path.
- Optional valuation and research workers remain profile-gated with zero replicas until intentionally enabled.

## Trading Strategy Ownership

Trading strategies are authored through a single catalog/profile model under `packages/config/strategies`:

- `catalog.yaml` owns strategy identity, activation, execution mode, thesis, archetype, trade structure, structure model reference, portfolio model reference, and thesis-level overrides.
- `profiles.yaml` owns reusable source models, archetypes, routine profiles, liquidity profiles, structure models, portfolio models, protection models, executor profiles, and exit controllers. Executor profiles own broker-order lifecycle policy; exit controllers own why/when to close.

`services/trading_strategies.py` composes the catalog and profiles into the runtime `TradingStrategyConfig` objects consumed by generated workflow-runtime strategy routines. There is no per-strategy runtime YAML path and no paper-specific config namespace. `paper`, `shadow`, and `live` are execution posture values under `execution.mode`, not separate files or directories.

`services/trade_structure_specs.py` owns reusable code-level trade-structure specs for candidate builders. These are not authored strategy configs and must not become a second strategy catalog.

Each strategy owns:

- `trading_strategy_id`
- `trade_structure`
- candidate `source`
- candidate-build settings
- entry and management routine schedules
- entry quality profile and quality overrides when configured
- runtime controls
- risk and limit policy references
- execution posture, approval mode, observed broker environment, runtime, and executor lifecycle profile
- `config_hash`

Current default-enabled strategies:

- `momentum_long_calls`
- `short_dated_index_call_credit`
- `short_dated_earnings_call_debit`
- `short_dated_earnings_long_straddle`
- `short_dated_earnings_long_strangle`
- `short_dated_earnings_put_debit`
- `short_dated_etf_short_put`
- `short_dated_index_iron_condor`
- `short_dated_index_put_credit`

`momentum_long_calls` is the Finviz-fed long-call strategy. It consumes `ticker_source:finviz_momentum`, applies `entry.quality_profile: momentum_long_call_v1`, enters during market hours on a 2-minute cadence, and manages during market hours on a 1-minute cadence.

`short_dated_index_call_credit` is the first deliberately enabled non-long-call paper proof family. It consumes the static `liquid_index_etfs` source, applies `entry.quality_profile: call_credit_spread_v1`, and remains subject to AllocationPlan, protection admission, portfolio admission, and execution-submit structure guards before any Alpaca paper submission.

`short_dated_index_put_credit` and `short_dated_index_iron_condor` consume the same `liquid_index_etfs` source and run defined-risk index premium strategies through `put_credit_spread_v1` and `iron_condor_v1`.

`short_dated_earnings_call_debit`, `short_dated_earnings_put_debit`, `short_dated_earnings_long_straddle`, and `short_dated_earnings_long_strangle` consume the dynamic `earnings_event_window` source backed by cached `earnings_event_consensus` rows, then run paper-mode earnings-oriented debit and long-vol structures through their family quality profiles.

`short_dated_etf_short_put` consumes `liquid_etf_short_puts` and runs the `short_put_v1` quality profile with explicit cash-secured short-put portfolio caps.

There are currently no disabled-by-default authored strategy configs. Disabled strategy configs may still be kept as authored definitions in the future; if disabled, they must not generate routine schedules until intentionally re-enabled.

`TradingOpsState.details.strategy_breadth` projects every authored strategy config, including active, disabled paper, or disabled shadow families, as operator-visible breadth. Disabled strategy projection is observation-only: it may show source, trade structure, routine cadence, execution posture, environment compatibility, and the reason the strategy is not active, but it must not create routine schedules, candidate runs, decisions, intents, attempts, or broker submissions. `TradingOpsState.details.trading_flows` remains the active lifecycle-flow surface.

`spreads ops strategy-ledger --date <YYYY-MM-DD>` is the shipped daily evidence ledger. It reports every active strategy's source, candidate, signal, decision, admission, intent, attempt, order/fill, position, close, mark, PnL, blocker, config hash, latest lifecycle ID evidence, and MarketContext snapshot links for one market date. Source evidence includes `source_evidence_state`, so static configured universes, dynamic source runs with symbols, missing recent source runs, and empty source runs are distinct. Candidate evidence includes diagnostic symbol counts, diagnostic status counts, raw/postprocess/runtime/returned candidate totals, persisted trade-candidate count, `candidate_productivity_state`, raw chain rejection counts, data-quality status/reason counts, calendar-policy status/reason counts, ranking-policy status/blocker counts, market-context/regime-fit status/reasons, and market-data coverage totals. Use that split to distinguish no source symbols, no raw candidates, data/chain gaps, shared context mismatch, data-quality filtering, calendar filtering, ranking filtering, and true no-trade market conditions before changing thresholds. Use the ledger as the first tuning surface for catalog/profile changes instead of changing thresholds from vibes.

The 2026-06-15 non-momentum evidence showed all active non-momentum families had recent source and candidate-run facts, and every scoped family produced raw candidates. Index credit/condor and ETF short-put families were filtered primarily by expected-value, slippage-adjusted expected-value, model-IV, macro-calendar, and raw chain/liquidity evidence; earnings debit/long-vol families were filtered primarily by slippage-adjusted EV, entry slippage, model-IV, and low earnings calendar-confidence evidence. No fake rows, forced selections, or broad profile loosening should be used to advance those families.

`services.backtest.BacktestEngine` is the backend current-model backtest evaluator. `stored_facts` rolls daily ledger facts across a bounded date window, compares current catalog strategy/profile/source variants by config hash, aggregates candidate productivity, selection quality, admissions, execution/fill evidence, position/PnL outcomes, admission notional/max-loss context, and reason-code attribution, then checks ClickHouse option quote/trade coverage for option symbols referenced by stored candidate/signal/decision/attempt/position facts. `strategy_rerun` reruns active entry strategies from current config against point-in-time source scope and historical market slices, then emits isolated candidate, signal, decision, and admission artifacts without writing live candidate, decision, admission, intent, attempt, or position rows. It reuses shared entry signal shaping, entry selection, decision identity, and protection/portfolio admission helpers; broker buying-power and allocation capacity are explicitly deferred to execution simulation. Before selection, it replays the latest fresh stored `MarketContextSnapshot` as of the historical session, passes that full shared context payload into entry quality, and records expired or missing context references with explicit fidelity instead of recomputing regime inside the rerun path. If ClickHouse chain coverage is too thin for a current candidate rebuild, it may fall back to stored trade-candidate payloads and labels that path as `stored_trade_candidate_fallback` rather than pretending a full rebuild occurred. `execution_simulation` composes over strategy reruns and creates isolated simulated intent, attempt, order, and fill artifacts from selected decisions. It validates broker order shape with the same execution structure guard used by live submit, uses executor profile quote freshness, TTL, stale-order action, repricing step/count, and max concession, then applies a `quote_touch_with_executor_repricing` fill model against historical ClickHouse quote snapshots and trade ticks. `portfolio_simulation` composes over execution simulation, turns simulated or artifact-shaped stored fills into isolated position artifacts, evaluates current close policy against historical quote marks, projects close decisions and position closes, and computes realized/unrealized PnL, return on risk, win rate, profit factor, max drawdown, exposure time, fill rate, selection rate, admission approval rate, quote coverage, and market-context regime-bucket metrics. `parameter_sweep` accepts `BacktestSweepConfig` with bounded dimensions, base mode, max variants, and rank metric; overlays validate through the same strategy DomainModels and then run `strategy_rerun`, `execution_simulation`, or `portfolio_simulation` as the comparison engine. Sweep comparisons include per-variant regime-bucket metrics so outcomes can be read by shared market context bucket rather than only aggregate rank. Metrics currently use pandas/numpy internals because quantstats and empyrical are not installed, and sweep acceleration labels `not_installed_polars_vectorbt` because Polars/vectorbt are not installed; artifacts carry those labels rather than pretending a standard/vectorized library was used. Simulated fills, positions, exits, PnL, market-context replay, and sweeps are labeled separately from broker/lifecycle facts, and no broker submit, intent dispatch, or live execution/position table write occurs. Each run records the request, strategy config snapshot, run state, summary, fidelity labels, result artifact pointer, and per-strategy current-catalog variant metrics in isolated backtest tables; result artifacts live outside Postgres under `outputs/backtest_runs` by default. `HistoricalMarketSliceProvider` supplies point-in-time `SymbolMarketSlice` objects for rerun work from ClickHouse quote snapshots/trade ticks and Postgres source/candidate/calendar facts. It labels partial states such as OCC-parsed contract metadata without open interest, missing underlying bars, missing/partial Greeks, quote fallback scope, and missing calendar consensus instead of treating those as full historical chain coverage. Profile/source edits are not silently re-run in stored-facts mode; the artifact labels that comparison fidelity as `stored_facts_current_model_no_profile_rerun`.

`spreads execution list --date <YYYY-MM-DD>` is the shipped daily execution activity printout. It reports attempts with nested parent/leg order rows and leg fill rows for attempts whose `market_date` equals the date or whose `requested_at` falls inside that UTC activity day. Use `spreads execution inspect <execution_attempt_id>` for full single-attempt broker detail, refresh, or cancel decisions.

The long-vol strategy configs run in paper mode by default after the 2026-06-11 multi-strategy activation. The Spreads execution path supports their two-long-leg `mleg` debit order shape; long-vol must not be blocked by vertical-only width or return-on-risk validation.

## Multi-Strategy Activation Contract

Authored strategy breadth is not automatic strategy rotation. Spreads may carry disabled, shadow, paper, or future live strategy definitions, but no inactive strategy may create routine schedules, natural candidate runs, selected decisions, intents, attempts, positions, or broker submissions until it is deliberately activated through config and worker rollout.

Config keeps three concerns separate:

- `activation.state`: whether the strategy is active and allowed to generate workflow-owned entry/manage routines. Inactive definitions are visible breadth only.
- `activation.paused`: operator/runtime pause state for an otherwise active strategy.
- `execution.mode`: `shadow`, `paper`, or `live` execution posture for the canonical lifecycle.

An active shadow strategy may persist analysis-only evidence, but it must not produce selected entry decisions or execution intents. An active paper strategy may submit only when its execution posture, observed broker environment, approval mode, AllocationPlan, protection admission, portfolio admission, execution admission, and risk gates all allow it. Live mode is reserved for an explicitly approved live-money rollout using the same lifecycle plus live deployment guards.

For non-long-call families, the required gate order is:

```text
quality profile -> account-agnostic selection -> AllocationPlan -> protection admission -> portfolio admission -> execution admission -> lifecycle workflow activity
```

`quality profile` proves the candidate is structurally and economically worth considering for that family. `selection` chooses the best account-agnostic idea per strategy. `AllocationPlan` ranks observed same-day selected decisions across active strategies under shared market-context/regime fit, capital, buying power, strategy budgets, duplicate/correlation exposure, active intents/attempts, open positions, and schedule evidence; stale or explicitly blocked context expires the selected decision at this boundary. `protection admission` consumes the selected allocation and evaluates account, portfolio, strategy, event/calendar, duplicate exposure, option scenario, and emergency-stop protections before portfolio admission. `portfolio admission` consumes the plan decision and persists the allocation evidence alongside cap checks. `execution admission` validates broker-submission readiness, including leg shape, net debit/credit sign, quote freshness, and adapter support. Pending intents are handed to `execution_lifecycle_start`, which starts deterministic trade/close lifecycle workflows; those workflows prepare or reuse broker attempts and submit them through idempotent broker activities.

Protection admission is evaluated after AllocationPlan and before portfolio admission. Protection rules live under `protection_models.<id>.rules`, separate from source selection, entry quality, portfolio allocation caps, and executor lifecycle policy. Supported rules are `account_emergency_stop`, `daily_drawdown_halt`, `rolling_drawdown_halt`, `loss_streak_cooldown`, `strategy_family_cooldown`, `event_calendar_block`, `duplicate_underlying_theme_cap`, and `options_exposure_scenario_cap`. A blocked or unknown protection emits `admission_boundary: protection_admission`, durable reason codes, blockers, metrics, and evidence on the trade admission, and revokes the pending intent before portfolio admission or broker submission.

Portfolio admission is evaluated after protection admission allows a natural entry decision and before pending intent creation. It reads current portfolio positions, open entry attempts, active entry intents, and the current allocation decision, then persists a `portfolio_admission` sub-payload on the trade admission alongside `capacity_admission`, `protection_admission`, `allocation_plan`, and deferred `execution_readiness`. The policy blocks duplicate symbol/family exposure, strategy and family caps, daily new-entry caps, correlated broad-index ETF crowding, and total strategy max-loss exposure when the strategy exposes a computable risk budget. Active strategies must declare these caps under `risk.limits.portfolio_admission`; runtime fallback defaults exist only for older configs and must not be used to justify enabling a second non-long-call family. `TradingOpsState` projects the resolved strategy risk config, protection admission, allocation decision, and portfolio admission state separately under each trading flow, and summarizes protection and portfolio block counts/reasons separately from quality blockers and execution-submit guards.

Shadow and paper are distinct activation modes. A shadow strategy may persist analysis-only evidence, but it must not produce selected entry decisions or execution intents. A paper strategy may submit only when its execution posture, observed broker environment, approval mode, AllocationPlan, protection admission, portfolio admission, execution admission, and risk gates all allow it. Do not use disabled strategy breadth as a hidden auto-allocator.

## Paper Execution Contract

Paper mode is part of the normal execution lifecycle, not a separate engine. Keep three axes separate in code, docs, and operator state:

- Spreads execution posture: authored strategy intent from `execution.mode`.
- Broker environment: observed Alpaca account/API target from broker/account state.
- Validation provenance: why a lifecycle row exists.

Execution posture values:

- `shadow`: build candidates, signals, decisions, and analysis-only evidence without automatic broker submission.
- `paper`: submit only through the canonical lifecycle against an Alpaca paper broker environment when approval and risk gates allow it.
- `live`: submit through the same lifecycle with live-trading guards. On an Alpaca paper broker environment this is only broker-paper rehearsal evidence, not live-money proof.

Broker environment is an observed fact, never a strategy config knob. The canonical normalized values for paper/lifecycle snapshots are:

- `alpaca_paper`
- `alpaca_live`

Use `alpaca_custom` or `unknown` only when the Alpaca base URL is nonstandard or environment resolution fails. Existing account snapshots may still expose raw `paper`, `live`, or `custom`; new lifecycle and ops projections should normalize those into `broker_environment`.

Executor profiles are part of the normal paper/live lifecycle contract. A natural strategy open or close intent carries an `executor_profile` snapshot plus `execution_policy` and `repricing_policy` payloads. That snapshot owns broker order style, quote freshness, submit TTL, reprice/cancel behavior, max concession, stale-order action, runtime adapter, approval mode, and fail-closed unsupported-structure behavior. Strategy selection and exit controllers decide what to trade and when to close; execution uses the executor profile to decide how broker work is submitted, refreshed, canceled, or repriced.

Validation provenance values:

- `natural_strategy`: emitted by the scheduled strategy entry/manage flow from real ticker-source, candidate, signal, decision, admission, intent, attempt, order, fill, and position facts.
- `synthetic_validation`: emitted by an operator-run smoke harness for paper lifecycle mechanics. It must be visibly labeled and cannot satisfy natural selected-trade validation beads.
- `operator_direct`: emitted by deliberate operator actions such as direct order helpers, refresh, cancel, or other manually requested lifecycle work.

The shipped synthetic paper harness is `spreads lifecycle paper-smoke`. It is an operator validation path over the normal lifecycle, not a strategy-selection path. `paper-smoke open` is preview-first and requires `--execute` before it creates an intent; it checks market hours, the control plane and kill switch, Alpaca paper environment, exact contract and underlying allowlists, a total debit cap, and intent TTL before it writes. It can optionally auto-select a quoted SPY/QQQ contract under the debit cap for preview, but execution still requires the exact selected contract to be allowlisted. Synthetic open intents carry `validation_provenance=synthetic_validation`, `execution_mode=paper`, `approval_mode=auto`, and `profile=paper_smoke`, then request `execution_lifecycle_start` to start the matching lifecycle workflow. Broker submission, refresh, cancel, and close reprice handoff run through broker activities owned by that workflow. `paper-smoke close` only closes positions whose opening attempt is also `synthetic_validation`; it creates a close intent and uses the same lifecycle workflow start path. `paper-smoke status` inspects intent, attempt, order, fill, position, and close evidence for the run.

The expected environment/provenance/executor snapshot shape is:

- `execution_posture`
- `approval_mode`
- `execution_runtime`
- `executor_profile`
- `execution_policy`
- `repricing_policy`
- `broker`
- `broker_environment`
- `broker_environment_source`
- `environment_compatible`
- `environment_mismatch_reason`
- `validation_provenance`
- `strategy_run_id`
- `trade_decision_id`
- `execution_intent_id`
- `execution_attempt_id`
- `observed_at`

`TradingOpsState` is the canonical operator surface for this contract. Its summary exposes the primary active strategy posture, approval mode, runtime, normalized `broker_environment`, mode/environment compatibility, mismatch reason, and latest natural/synthetic lifecycle timestamps. Its details expose `execution_contract.strategy_contracts`, `execution_contract.primary_strategy_contract`, and `execution_contract.latest_lifecycle_evidence`. CLI, API, Today, and Ops dashboard views consume that read model directly; do not add a separate paper-mode dashboard or frontend-only environment inference.

Mismatch behavior:

- `paper` posture with `alpaca_live` must block automatic broker submission and raise operator attention.
- `live` posture with `alpaca_live` must still pass the existing live-trading deployment guard before broker submission.
- `live` posture with `alpaca_paper` may be useful rehearsal, but operator state must not present it as real-money proof.
- Missing or unknown broker environment should block automatic broker submission until the environment is resolved.

Legacy posture flags are not active config. Execution decisions must use `execution.mode`, observed `broker_environment`, approval mode, risk/admission gates, and the existing live-trading guard. Do not introduce replacement control flags such as `real_money_enabled` or `broker_submission_enabled`.

## Engine Entry State

Strategy entry follows the current Spreads-owned lifecycle spine:

`DataEngine -> engine facts/read models -> StrategyEngine -> AllocationPlan -> protection/risk admission -> PortfolioEngine -> execution services -> Ops projections`.

The active entry owner is `StrategyEngine` in `services/trading_engine/strategy_runtime.py`. It resolves tickers through DataEngine, builds candidates once, delegates account-agnostic quality and selection to `EntrySelectionEngine`, plans trade decisions, runs admission, and creates execution intents.

Current entry selection runtime:

- DataEngine builds `FeatureSnapshot` rows from the resolved ticker set and candidate-build result through a registry keyed by `trade_structure` and `quality_profile_id`.
- `MarketContextEngine` builds `MarketContextSnapshot` and `RegimeSnapshot` facts from shared benchmark market slices or stored benchmark facts. DataEngine materializes and persists the shared context snapshot during live entry candidate building, then passes the snapshot payload forward as candidate-build evidence. Entry quality consumes that shared context for regime fit; the models and engine do not choose trades or evaluate account-aware admission.
- Strategy structure models can declare `market_context` policy using domain names such as `allowed_regime_labels`, `preferred_regime_labels`, `blocked_risk_postures`, `min_confidence`, and watch/block reactions. `EntryRuntime` projects that policy into `market_context_regime_fit`; strategies declare regime fit preferences but do not compute regime.
- `trading_feature_snapshots` is the trading-owned durable feature store for those rows. It is written from the same `persist_entry_engine_facts` choke point as candidate diagnostics, trade candidates, and trade signals, with `trading_strategy_id`, `trade_structure`, `quality_profile_id`, `config_hash`, ticker-source context, candidate-run lineage, optional trade-candidate lineage, point-in-time `observed_at`, and versioned source/underlying/chain/premium/quality payloads. It is intentionally separate from the company-valuation `feature_snapshots` table.
- `trading_feature_snapshots.market_data_quality_json` is the canonical entry-time market-data SLA label surface. It labels source readiness, quote freshness, chain completeness, Greeks, expected-move availability, source-specific confidence, and ClickHouse coverage state. Live entry persistence records ClickHouse coverage as explicitly `not_checked`; backtests and diagnostics that probe ClickHouse may upgrade coverage labels rather than pretending live selection used historical tick coverage.
- Trading feature snapshots are retained with candidate-run facts through the `candidate_runs` foreign-key cascade. No separate cleanup loop, replay wrapper, or Postgres tick-retention path owns them. Future rollups should read this table and ClickHouse directly, then write named analytical rollups by question.
- Candidate generation consumes normalized `SymbolMarketSlice` inputs through `MarketSliceProvider`; live Alpaca calls are isolated behind `AlpacaMarketSliceProvider`.
- Candidate diagnostics enrich `underlying_setup` with candidate-vs-SPY/QQQ relative-strength evidence only. Broad-market regime lives in the shared `MarketContextSnapshot`/`RegimeSnapshot` payload produced by `MarketContextEngine`, not in setup metrics. Sector ETF comparison is intentionally deferred until sector mapping exists cleanly.
- `EntryQualityPipeline` resolves configured quality-profile thresholds and operator overrides, evaluates pre-selection quality for candidate filtering, and evaluates post-selection readiness only after live signal fields exist.
- `EntrySelectionEngine` owns quality analysis, candidate quality filtering, live signal selection, and selected/monitored/rejected candidate output. It does not inspect account buying power, broker positions, or execution runtime state.
- Allocation and entry admission in `services/risk/` are the first account-aware boundaries. `risk/portfolio.py` ranks observed selected decisions across active strategies, `risk/protection.py` applies protection gates, `risk/sizing.py` resolves quantity policy, and `risk/admission.py` emits stable `allocation_plan`, `portfolio_admission`, `capacity_admission`, `execution_readiness`, `reason_codes`, and `blockers` evidence; final execution readiness is explicitly deferred to the execution submit path.

The quality pipeline does not rebuild candidates, fetch market data, or own execution admission.

Current entry-quality snapshot builders:

- `momentum_long_call_v1` for `momentum_long_calls`
- a generic option-structure snapshot builder registered for planned vertical, condor, short-put, and long-vol quality profiles. It reads canonical `legs[]`, order payloads, economics, liquidity, DTE, greeks, expected-move, ranking evidence, and missing-structure reason codes from candidate payloads. This enables observation/profile work for non-long-call families without creating admission, intents, attempts, or broker submissions.

Implemented quality profiles:

- `momentum_long_call_v1`
- `call_credit_spread_v1`
- `put_credit_spread_v1`
- `call_debit_spread_v1`
- `put_debit_spread_v1`
- `iron_condor_v1`
- `short_put_v1`
- `long_straddle_v1`
- `long_strangle_v1`

Implemented quality stages include source preflight, underlying setup, chain viability, contract fit, premium quality, and selection. `momentum_long_call_v1` includes target-DTE chain viability, SPY/QQQ relative strength, and the shared `market_context_regime_fit` filter backed by `MarketContextSnapshot` / `RegimeSnapshot` facts. The non-long-call family profiles consume the generic structure snapshot and require canonical `legs[]`, family/leg-mix match, common expiration, spread/condor width where applicable, liquidity, premium/economics, return-on-risk, ranking-policy, and selection-readiness evidence. Long-vol profiles keep their two-long-leg debit shape and are not forced through vertical-only width validation. `TradingOpsState` projects the latest entry-quality waterfall from persisted engine facts so the CLI and dashboard show stage counts, top blocker reasons, selection counts, admission counts, and market-context fit from the same read model. The displaced replay/runtime filter helpers have been removed, and the `spr-34u` cutover was live-validated during the 2026-06-09 market session. First selected-order lifecycle validation remains a separate live-validation task that should wait for an actual selected decision.

Active entry facts are persisted through:

- `ticker_source_runs`
- `ticker_source_observations`
- `ticker_source_state`
- `market_context_snapshots`
- `candidate_runs`
- `trade_candidates`
- `trade_signals`
- `trade_decisions`
- `trade_admissions`
- `execution_intents.trading_strategy_id`
- `portfolio_positions.trading_strategy_id`

Dynamic-source and static-source strategies both flow through the same strategy ownership model.

## Execution Domain

`trade_admissions` records every immutable entry or close admission outcome. Approved admission and initial `execution_intents` creation commit together; blocked and unknown outcomes create no intent. `execution_intents` records the sole pending/claimed/executing/terminal lifecycle state, explicit workflow correlation, successor-owned supersession lineage, and a monotonically increasing `state_version`. State transitions use compare-and-swap semantics and append the same-version `engine_events` plus `engine_outbox` row in the same transaction.

The `execution_lifecycle_start` routine starts deterministic trade/close lifecycle workflows for pending intents and claims them through that transition API. The lifecycle workflow lane runs trade/close workflows and broker activities. Broker activities prepare or reuse the child `execution_attempts` row through `execution_attempts.execution_intent_id`, submit pending attempts to Alpaca through `alpaca_direct`, refresh/cancel broker state, persist attempt/order/fill rows, and transition linked intents. Close workflows continue refreshing broker state on durable workflow timers until the attempt is terminal or stale-policy handling cancels, fails closed, leaves working, or creates a separately admitted deterministic successor intent.

`TradingOpsState.details.engine` exposes lifecycle event health from `engine_events` and `engine_outbox`, including workflow event counts, recent engine events, pending outbox count, and retrying outbox count. The scheduled `engine_outbox_publish` job drains pending outbox rows into the `spreads_engine_lifecycle` JetStream stream for projection consumers. Retired lifecycle dispatch/submit job types are filtered from current job health summaries so historical runs do not look like active operator work.

`services/execution/` records immutable broker-facing facts in:

- `execution_attempts`
- `execution_orders`
- `execution_fills`

Execution modules have explicit owners: `direct_orders.py` owns attempt construction used by workflow activities, `position_close.py` owns close-by-position attempt construction, `submit.py` owns broker submission service logic used by workflow activities, `sync.py` owns refresh/cancel reconciliation helpers, `attempts.py` owns attempt/order/fill payload sync helpers, `admission.py` owns execution admission payload helpers, and `order_requests.py` owns order payload construction plus live quote quality gates. Natural strategy open intents start lifecycle workflows through `execution_lifecycle_start`; broker workflow activities own attempt preparation, submit, refresh, cancel, and order/fill sync. Mutating refresh/cancel commands are not exposed as direct CLI paths. Before broker submission, the broker activity still runs option-structure guards over canonical legs, family support, executor order style, Alpaca `mleg` usage, net credit/debit sign, max-risk resolvability, quantity, and executor-profile quote freshness; blocked attempts fail with stable execution-admission reason codes.

Operators inspect individual attempts through `spreads execution inspect <execution_attempt_id>`. Broker refresh, cancel, and reconciliation mutations are owned by lifecycle workflow activities and are not exposed as direct CLI paths.

`services/session_positions.py` owns position attribution. `PortfolioEngine` owns open-position projection only. `ExitEngine` owns `PositionExitSnapshot` construction, close-policy evaluation, and durable `trade_close_decisions` writes. `services/trading_engine/close_policy.py` owns reusable profit/stop/force-close/underlying-invalidation policy math. `services/trading_engine/risk_runtime.py` owns close admission checks for position status, reconciliation freshness, active close work, broker symbols, and order validity. `services/exit_manager.py` exposes `run_trading_strategy_manage`, the manage-job adapter: it refreshes marks, records hold/blocked/selected close decisions, applies close admission, and creates close intents for approved selected closes. Close actions go through intents and attempts; they should not bypass the execution lifecycle.

## Hot-Path Abstraction Audit

Current audit result for the live trading hot path:

- Keep the thin engine contract modules in `services/trading_engine/`. They are ownership boundaries and typed payload shapes, not a second runtime, bus, actor framework, or alternate store.
- Broker submit, refresh, cancel, and order/fill sync are lifecycle workflow activities. Do not recreate a parallel submit lane or direct mutating CLI path.
- Keep candidate-build policy helpers under DataEngine ownership. `services/strategy_candidate_builders/` is the only active candidate-construction package; it must not become an alternate orchestration path, product surface, CLI flow, or persistence owner.
- Keep `EntrySelectionEngine` as the canonical account-agnostic strategy-selection service. Do not put account capacity, broker submit readiness, or alert delivery back into selection.
- Merge management scheduling into `trading_strategy_manage` only. There is no standalone position-exit job type; `services/exit_manager.py` exposes the strategy manage adapter that wires PortfolioEngine position projection, ExitEngine close decisions, `risk_runtime.py` close admission, and ExecutionIntent close handoff.
- Do not add a second trading ledger, actor framework, or compatibility wrapper around removed runtime surfaces. Projection fanout is through `engine_outbox`.

## Operator Read Models

Operator views should read service-owned state through:

- `services/ops/`
- `services/positions.py`
- `services/execution/runtimes.py`

The dashboard should show strategy-owned runtime state, not recreate old runtime pages or infer business logic in the frontend.

`TradingOpsState` exposes current shared market context through `summary.market_context_*` fields and `details.market_context`, including regime label, risk posture, confidence, freshness, data quality, benchmark evidence, supportive/blocking benchmark counts, and missing/stale states. It exposes healthy no-entry rationale through each trading flow's `entry_posture`, `details.strategy_no_entry_summary`, and the primary summary fields `primary_entry_state`, `primary_entry_message`, `primary_entry_blocker_groups`, and `strategy_no_entry_category_counts`. A flat strategy can therefore be explicitly healthy when source and candidate runs are fresh but account-agnostic quality filters, such as shared market-context fit or target-DTE chain viability, blocked all candidates.

`TradingOpsState.details.trading_flows[].source_state` is source-type-aware. Static strategy sources use configured-universe evidence (`source_basis: configured_universe`, `source_evidence_state: static_symbols_configured`) instead of requiring a persisted ticker-source run. Dynamic sources use ticker-source-run freshness, selected-symbol evidence, and persisted degradation details. Missing, stale, provider-degraded, normalization-degraded, and partially degraded dynamic runs therefore remain visible as source problems through stable `source_evidence_state` values instead of being presented as healthy empty universes. A completed dynamic run with valid provider evidence and zero symbols after ordinary source filters remains a healthy `no_source_symbols` outcome. No-entry summaries should therefore classify static-source skips by candidate, data-quality, ranking, policy, admission, or selection evidence rather than by missing ticker-source runs.

`spreads execution positions` separates live close work from historical close accounting. `close_lifecycle.live_action_*` and the CLI `Live Close Work` row answer whether a date-scoped or unfiltered position view has actionable pending, active, failed, or anomalous close work. `accounting_*` fields and the CLI `Close Accounting` row can show retained historical close evidence without implying live close work is waiting.

`TradingOpsState.details.strategy_breadth` is the canonical operator inventory for authored strategy breadth. Disabled paper/shadow strategies can appear there as `paper_observation_candidate` or `shadow_observation_candidate`, but their breadth contracts force effective automatic submission off unless the strategy is actually active through the workflow-owned lifecycle spine.

Entry planning treats non-live signal eligibility, including `analysis_only` emitted by shadow-mode strategy runs, as observation evidence rather than selected-entry eligibility. Those rows may be persisted for regime comparison, but they must not create selected entry decisions or execution intents.

Disabled strategy breadth can be run explicitly through `spreads lifecycle observe-strategy <trading_strategy_id>`. Observation runs resolve authored strategy config without enabling routine schedules, run the normal ticker-source, candidate-build, entry-quality, signal, and decision persistence spine, force `analysis_only`/`observation_only` provenance, and stop before admission or execution-intent creation. `TradingOpsState.details.strategy_breadth[].latest_observation` exposes the latest observation evidence for each authored strategy.

`TradingOpsState.details.broker_exposure` classifies the latest broker account snapshot positions by ownership against canonical open Spreads position legs. Broker option legs should be labeled as `spreads_managed`, `spreads_synthetic_validation`, or `external_manual` instead of being hidden behind raw broker account positions.

`StorageOpsState.summary` uses `market_data_*` fields for ClickHouse market-data aggregates and `storage_*` fields for combined ClickHouse plus Postgres capture-summary storage aggregates. Avoid generic storage totals that hide which database class owns the bytes or row estimates.

## Rollout Notes

- After schema changes, run `uv run alembic upgrade head`.
- The maintenance lane reconciles routine schedules every six hours. After declared job YAML or strategy config changes, run `uv run spreads runtime routine-schedules` for immediate reconciliation and restart affected workflow lanes so they reload imported config.
- After code imported by workflow workers changes, restart those containers before trusting live behavior.
- Default validation is live/runtime checks through shipped CLIs and operator reads. Do not add automated tests unless explicitly requested.
