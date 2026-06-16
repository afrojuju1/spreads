# System Architecture

This document is the canonical source of truth for the current `spreads` runtime architecture, ownership vocabulary, and service boundaries.

It describes the system as it exists in code today. Planning documents can describe history or target states, but when they disagree with this file, this file wins.

Last updated: 2026-06-15

## Top-Level Boundaries

| Boundary | Current owner | Notes |
|---|---|---|
| Operator interfaces | `packages/web`, `packages/api`, `packages/core/cli` | Web, API, and CLI are adapters over service-owned state. They must not own trading logic. Canonical on-box CLI visibility lives under `spreads ops state`, `spreads ops storage`, `spreads jobs`, `spreads execution list`, `spreads execution positions`, and `spreads execution runtimes`. Remote target reads go through `spreads deploy exec --env <target> -- ...`; command-level `--env` passthrough is intentionally not shipped. On-box logs use Docker Compose directly; remote deployment logs live under `spreads deploy logs`. |
| Trading strategy config | `packages/config/strategies/catalog.yaml`, `packages/config/strategies/profiles.yaml`, `services/trading_strategies.py`, `services/trade_structure_specs.py`, `services/trading_strategy_runtime.py` | A `trading_strategy` is the product/operator owner for source, trade structure, entry routine, management routine, risk, limits, and execution settings. Reusable trade-structure construction lives in code; authored strategy runtime config composes from the catalog and profiles only. |
| Scheduling and workers | `packages/config/jobs`, `packages/core/jobs`, `services/runtime_policy.py` | Declared jobs and generated trading-strategy jobs are the scheduler source of truth. Runtime workers execute broker sync, strategy entry/manage, dispatch, and alert jobs; data workers execute ticker sources and calendar event refreshes. Research and valuation workers are optional lanes, disabled by default, and not part of live trading health. |
| Dynamic ticker sources | `packages/config/ticker_sources`, `services/ticker_sources.py` | Ticker sources materialize reusable underlying lists. `finviz_momentum` feeds `momentum_long_calls` and filters Finviz rows through the strategy's target-DTE optionability/expected-move requirements before marking symbols selected. `earnings_event_window` reads `earnings_event_consensus`, applies Alpaca tradable/optionable/price/volume/target-DTE/expected-move checks, and persists selected plus filtered earnings-event observations for the earnings-source cutover. Filtered observations remain visible with stable reason codes. |
| Calendar events and earnings consensus | `integrations/calendar_events`, `storage/calendar_models.py`, `CalendarEventStore` | `calendar_events` stores normalized provider event facts. `provider_fetch_audit` stores bounded provider fetch/cache/error summaries. `earnings_event_consensus` stores derived earnings facts separately from provider rows. `calendar_event_refresh:earnings_30d` is the data-lane provider-fetch entrypoint for yfinance, Alpha Vantage, DoltHub, and sparse Finviz enrichment. Strategy runtime must not call yfinance, Alpha Vantage, DoltHub, or Finviz directly. |
| Market data capture | `services/trading_engine/capture_targets.py`, `services/market_recorder.py`, `storage/capture_repository.py`, `storage/market_data_store.py` | `DataEngine` owns desired capture state in `capture_targets`; `market_recorder.py` is the normal Alpaca option websocket owner and reconciles the prioritized target set into ClickHouse option quote/trade ticks plus Postgres `capture_summaries`. |
| Engine data and candidate building | `services/trading_engine/data_runtime.py`, `services/strategy_builders.py`, `services/strategy_candidate_builders/` | DataEngine resolves ticker sources/static sources and builds strategy-owned candidate inputs. `services/strategy_candidate_builders/` owns market slices, option construction, ranking policy, and diagnostics under engine-owned candidate facts. Candidate generation consumes market data through the `MarketSliceProvider` boundary; live behavior uses `AlpacaMarketSliceProvider` by default. There is no separate candidate-building CLI flow or orchestration boundary. |
| Strategy signals and decisions | `services/trading_engine/strategy_runtime.py`, `services/trading_engine/entry_selection.py`, `services/trading_engine/entry_quality_pipeline.py`, `services/live_selection.py`, `services/entry_planner.py`, `services/trading_engine/facts.py`, `storage/engine_fact_repository.py` | StrategyRuntime owns entry orchestration and persistence. `EntrySelectionEngine` owns account-agnostic entry quality analysis, candidate filtering, selected/monitored/rejected candidate output, and live signal selection. Admission handoff and intent creation remain after selection. Helper modules are pure policy delegates, not alternate orchestration paths. |
| Execution and portfolio state | `services/trading_engine/portfolio_runtime.py`, `services/trading_engine/close_policy.py`, `services/trading_engine/risk_runtime.py`, `services/execution_intents/`, `services/execution/`, `services/session_positions.py`, `services/broker_sync.py`, `services/risk_manager.py`, `services/exit_manager.py` | PortfolioEngine owns close decisions and close-policy evaluation. RiskEngine-owned close admission validates position/reconciliation/order readiness. The manage job refreshes marks, applies close admission, and creates close intents; execution services dispatch intents and persist broker attempts/orders/fills. |
| Money and premium arithmetic | `money.py` | `core.money` is the canonical helper layer for USD Money construction, Decimal quantization, option premium/limit-price rounding, contract notionals, spread exposure, close PnL, and repricing tick math. Runtime services may still persist floats for compatibility, but they should not add new local `_round_money` or ad hoc premium/notional helpers. |
| Operator read models | `services/ops/`, `services/positions.py`, `services/execution/runtimes.py` | Read models compose persisted engine, jobs, trading health, positions, execution, account, storage, and capture state. Operator surfaces should project current domain facts instead of reintroducing removed product pages. |
| Historical strategy evaluation | `services/strategy_lab/historical_evaluator.py` | Backend-only strategy-lab primitive. `build_historical_strategy_evaluation` evaluates bounded date windows over the current ticker-source, candidate, signal, decision, admission, intent, attempt, position, and ClickHouse market-data model. The first shipped mode is `stored_facts_current_model`: it compares current catalog strategy/profile/source variants from persisted facts and labels source, candidate, decision, execution, PnL, and market-data fidelity explicitly. It is not an operator app UI and does not revive removed replay/audit/backtest/analyze commands. |
| Company valuation lane | `services/company_valuation/`, `packages/config/company_valuation`, optional `worker-valuation` | Company valuation is an offline research/maintenance lane. It can support future analysis, but live strategy selection, admission, execution, and position management must not depend on it by default. |
| Research AI lane | `services/tradingagents_scan.py`, `packages/config/jobs/tradingagents_scan_finviz_momentum.yaml`, optional `worker-research`, `external/TradingAgents` | Spreads owns orchestration, job config, artifacts, alerts, and visibility. The external TradingAgents repo owns its own agent internals. This lane is disabled by default and is not a live execution dependency. |
| Persistence and transport | Postgres, ClickHouse, Redis | Postgres is source of truth for durable domain and ops state, including calendar provider facts, provider fetch audit, and earnings event consensus. ClickHouse owns high-volume raw market-data ticks and compact quote snapshots. Redis handles queues, leases, pub/sub fanout, and short-lived provider cache/backoff state. |

## Non-Negotiable Boundary Rules

- `trading_strategy_id` is the canonical runtime owner for strategy-owned candidates, signals, decisions, intents, attempts, and positions.
- Authored trading strategy config lives in `packages/config/strategies/catalog.yaml` and `packages/config/strategies/profiles.yaml`. Do not recreate per-strategy runtime YAML, paper-specific config directories, or compatibility wrappers around it.
- Strategy routines generate jobs named `trading_strategy:<strategy_id>:entry` and `trading_strategy:<strategy_id>:manage`.
- `execution_intent_dispatch:global` owns the global pending-intent dispatch loop.
- `trade_structure` names reusable option construction behavior, such as `long_call`, `call_credit_spread`, `iron_condor`, or `short_put`.
- Money, premium, limit-price, notional, exposure, PnL, and repricing tick math belongs in `core.money`. Keep strategy/risk/execution services focused on policy decisions instead of reimplementing rounding and contract-multiplier arithmetic.
- `source` names the candidate source for a strategy. Current source types are `static` and `dynamic`.
- Current runtime identity is `trading_strategy_id`, `ticker_source`, candidate runs, trade signals, trade decisions, admissions, intents, attempts, and positions. Do not add compatibility wrappers outside that model.
- Trade candidates, trade signals, trade decisions, and admission decisions are the active strategy-entry facts.
- Historical strategy evaluation is a service-owned backend primitive under `services/strategy_lab/`. It must consume the current fact spine and ClickHouse market-data stores with explicit fidelity labels; do not reintroduce removed replay, audit, backtest, analyze, or post-market analyze wrapper commands as historical-evaluation shortcuts.
- Capture is desired state, not a candidate-build side effect. The priority order is open positions, working intents/attempts, selected candidates, then watch candidates.
- `services/market_recorder.py` is the sole Alpaca option websocket owner in normal runtime. It reads `capture_targets` by priority and records `capture_summaries`.
- `execution_intents` is the control-plane handoff boundary. It selects an execution runtime before broker submission.
- `alpaca_direct` is the active Python-native runtime for equity, single-leg option, and Alpaca order-payload submission.
- `session_positions` owns day/session position attribution. Broker positions are reconciliation input, not the sole position truth.
- Spreads is the active trading operations and research-orchestration home. The old `trading_operator` wrapper repo is not an active hub for future operator guidance.
- `external/TradingAgents` is a symlink to `/home/ade/Projects/TradingAgents`. Spreads may orchestrate research jobs against it, but does not own the external repo's internals.
- Company valuation and TradingAgents research are optional offline lanes. Default trading health, live strategy entry/manage, execution admission, and close management must stay healthy without these workers or jobs enabled.

## Domain Ownership Map

| Domain object | Meaning | Source of truth / owner | Must not own |
|---|---|---|---|
| Trading strategy | Operator/product trading unit with source, trade structure, routines, risk, limits, execution settings, and config hash. | `packages/config/strategies/catalog.yaml`, `packages/config/strategies/profiles.yaml`, `services/trading_strategies.py` | Discovery-session identity, broker facts, or dashboard-only state. |
| Trade structure | Reusable option construction family. | `services/strategy_builders.py`, `services/strategy_candidate_builders/`, `services/option_structures.py` | Runtime owner identity. |
| Routine | Scheduled strategy behavior such as entry or manage. | `services/trading_strategy_runtime.py`, generated job specs | Broker submission facts. |
| Ticker source | Reusable static or dynamic symbol source. | `packages/config/ticker_sources`, `services/ticker_sources.py`, `ticker_source:*` jobs | Execution ownership or position attribution. |
| Ticker source run | One materialized ticker-source refresh plus selected, observed, and filtered ticker observations. | `ticker_source_runs`, `ticker_source_observations`, `ticker_source_state`, `services/ticker_sources.py` | Strategy candidate ownership, execution ownership, or broker facts. |
| Calendar event | Normalized provider event fact such as earnings, dividends, splits, or macro events. | `calendar_events`, `integrations/calendar_events` adapters, `CalendarEventStore` | Derived consensus, strategy candidate ownership, or raw provider replay storage. |
| Provider fetch audit | Bounded durable summary of provider fetch/cache/error state. | `provider_fetch_audit`, `CalendarEventStore`, provider cache helpers | Raw provider payload archival, secrets, or strategy runtime truth. |
| Earnings event consensus | Derived per-symbol earnings event fact with event date, session timing, source support, conflicts, confidence, and stale-after time. | `earnings_event_consensus`, `integrations/calendar_events/consensus.py`, `CalendarEventStore` | Fake provider rows in `calendar_events`, direct external provider calls, or Alpaca actionability decisions. |
| Candidate run | One strategy candidate-build pass over resolved tickers. | `candidate_runs`, `trade_candidates`, `services/trading_engine/facts.py` | Broker facts or position PnL. |
| Trade signal | Normalized market/setup observation from an account-agnostic selected or monitored candidate. | `trade_signals`, `services/trading_engine/entry_selection.py`, `services/trading_engine/facts.py` | Broker sync, frontend state, or account-capacity checks. |
| Trade decision | Strategy/lifecycle choice such as selected, skipped, blocked, or no-entry. | `trade_decisions`, `services/trading_engine/strategy_runtime.py` | Alert delivery or dashboard-only read models. |
| Admission | Account/risk/policy answer to whether an approved idea can be carried now. Entry admission is an `entry_capacity_precheck`; final quote, broker, and submit readiness stays in execution. | `services/risk_manager.py`, `services/execution/`, admission payloads | Account snapshots alone or strategy selection. |
| Intent | Control-plane request to open, manage, or close. | `execution_intents`, `services/execution_intents/` | Broker order/fill persistence. |
| Attempt | Broker-facing submission/refresh/cancel lifecycle for an intent. | `execution_attempts`, `services/execution/` | Session position attribution. |
| Order | Broker order fact attached to an attempt. | `execution_orders`, broker refresh paths | Strategy selection. |
| Fill | Broker fill fact attached to an order/attempt. | `execution_fills`, broker refresh paths | Strategy selection. |
| Position | Day/session-local ownership and PnL projection. | `services/session_positions.py`, `portfolio_positions`, close records | Broker inventory as independent truth. |
| Close | Decision, admission, intent, attempt, and fill path that reduces or exits a position. | `services/trading_engine/portfolio_runtime.py`, `services/trading_engine/close_policy.py`, `services/trading_engine/risk_runtime.py`, `services/exit_manager.py`, `services/execution_intents/`, `services/execution/` | Direct broker-submit bypasses from management jobs or dashboard-only close decisions. |
| Broker sync | Poll-first broker/account health and fact ingestion. | `services/broker_sync.py`, `broker_sync_state`, `account_snapshots` | Trading decisions or owner attribution. |
| Capture target | Desired option contract capture need with owner, reason, priority, TTL, and quote/trade flags. | `services/trading_engine/capture_targets.py`, `capture_targets`, `storage/capture_repository.py` | Candidate diagnostics or broker order truth. |
| Capture summary | Market-recorder iteration summary for target pressure, captured rows, groups, and errors. | `services/market_recorder.py`, `capture_summaries` | Raw quote/trade tick storage or retention. |
| Trading ops state | Operator-facing trading health: market, control, scheduler/workers, sources, candidates, signals, decisions, intents, attempts, positions, exits, risk, capture, and attention. | `services/ops/` | Frontend stitching or live Alpaca calls during default dashboard render. |
| Storage ops state | Operator-facing storage health for ClickHouse market data and Postgres capture summaries. | `services/ops/storage_ops_state.py`, storage ops surfaces | Live trading decisions. |
| Historical evaluation | Bounded strategy-lab artifact for current-model historical windows, including candidate productivity, selection quality, admission outcomes, execution/fill assumptions, position/PnL labels, admission risk context, reason-code attribution, and ClickHouse coverage/fidelity. | `services/strategy_lab/historical_evaluator.py` | Alternate execution orchestration, broker submission, operator UI, or legacy replay/backtest wrappers. |
| Company valuation | Offline issuer valuation, ownership resolution, and research datasets. | `services/company_valuation/`, `packages/config/company_valuation`, optional `worker-valuation` | Live strategy entry, live execution admission, or position close management. |
| Research scan | Batch TradingAgents research run over a bounded ticker list. | `services/tradingagents_scan.py`, `outputs/tradingagents/`, optional `worker-research`, `external/TradingAgents` | Live strategy entry or live execution admission. |

## Runtime Stack

```text
Operator
  |
  +--> Browser -> Next.js web -> FastAPI -> Postgres/ClickHouse/Redis
  |
  +--> `uv run spreads ...` CLI -> services -> Postgres/ClickHouse/Redis/Alpaca

Scheduler
  |
  +--> declared YAML jobs + generated strategy routine jobs
  |
  +--> Redis queues

ARQ workers
  |
  +--> runtime lane: broker sync, trading strategy entry/manage, intent dispatch, alerts
  +--> data lane: ticker sources, calendar event refreshes
  +--> optional valuation lane: company valuation jobs when enabled
  +--> optional research lane: TradingAgents jobs when enabled

Market recorder
  |
  +--> prioritized capture_targets -> Alpaca option websocket -> ClickHouse option_quote_ticks / option_trade_ticks + Postgres capture_summaries

Postgres = domain and ops source of truth
ClickHouse = high-volume raw market-data ticks and quote snapshots
Redis = queues, leases, pub/sub, short-lived provider cache/backoff
```

## Current Runtime Jobs

Default live trading job types:

- `ticker_source`
- `calendar_event_refresh`
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

- Postgres, ClickHouse, Redis, API, web, scheduler, and the logging/metrics stack stay up so operator reads, dashboards, leases, queues, and market-data storage remain available.
- Runtime workers stay up for broker/account sync, intent dispatch, alert reconciliation, and strategy routines, but market-only jobs are expressed in their job schedules instead of waking and skipping all night.
- `alert_reconcile` is intentionally allowed off-hours so pending notifications can recover without waiting for the next session.
- `TradingOpsState` keeps broker-sync age and stale position marks visible after market close, but expected off-hours staleness is not degraded when the latest sync was healthy and there are no queued attempts, missing marks, broker quote errors, or reconciliation mismatches.

Market-window and data-refresh runtime:

- Ticker sources with `allow_off_hours: false`, including `ticker_source:finviz_momentum`, refresh only inside the configured market calendar window.
- `ticker_source:earnings_event_window` is allowed off-hours because it is a persisted source refresh over consensus facts and Alpaca actionability checks; strategy entry routines remain market-hours gated separately.
- `calendar_event_refresh:earnings_30d` is allowed off-hours and uses Redis TTL/backoff plus Postgres provider-fetch audit to avoid provider-call storms.
- Trading strategy entry and manage routines compile `market_hours_only: true` into generated job payloads with `allow_off_hours: false`.
- Broker sync and execution dispatch remain schedule-gated with a short close grace period where configured.
- `market_recorder.py` stays deployed as the sole option-stream owner, but it idles outside regular market hours. It checks the market calendar cheaply, throttles idle logs, and does not refresh capture targets, open the Alpaca option websocket, or write capture summaries while closed unless explicitly run with `--no-market-hours-only`.

Scale defaults:

- The active live deploy target runs one data worker by default. The current data lane only owns ticker-source jobs, so extra always-warm data workers add memory pressure without improving the normal live path.
- Optional valuation and research workers remain profile-gated with zero replicas until intentionally enabled.

## Trading Strategy Ownership

Trading strategies are authored through a single catalog/profile model under `packages/config/strategies`:

- `catalog.yaml` owns strategy identity, activation, execution mode, thesis, archetype, trade structure, structure model reference, portfolio model reference, and thesis-level overrides.
- `profiles.yaml` owns reusable source models, archetypes, routine profiles, liquidity profiles, structure models, portfolio models, protection models, executor profiles, and exit controllers.

`services/trading_strategies.py` composes the catalog and profiles into the runtime `TradingStrategyConfig` objects consumed by scheduler-generated strategy routines. There is no per-strategy runtime YAML path and no paper-specific config namespace. `paper`, `shadow`, and `live` are execution posture values under `execution.mode`, not separate files or directories.

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
- execution posture, approval mode, observed broker environment, and runtime
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

`short_dated_index_call_credit` is the first deliberately enabled non-long-call paper proof family. It consumes the static `liquid_index_etfs` source, applies `entry.quality_profile: call_credit_spread_v1`, and remains subject to portfolio admission plus execution-submit structure guards before any Alpaca paper submission.

`short_dated_index_put_credit` and `short_dated_index_iron_condor` consume the same `liquid_index_etfs` source and run defined-risk index premium strategies through `put_credit_spread_v1` and `iron_condor_v1`.

`short_dated_earnings_call_debit`, `short_dated_earnings_put_debit`, `short_dated_earnings_long_straddle`, and `short_dated_earnings_long_strangle` consume the dynamic `earnings_event_window` source backed by cached `earnings_event_consensus` rows, then run paper-mode earnings-oriented debit and long-vol structures through their family quality profiles.

`short_dated_etf_short_put` consumes `liquid_etf_short_puts` and runs the `short_put_v1` quality profile with explicit cash-secured short-put portfolio caps.

There are currently no disabled-by-default authored strategy configs. Disabled strategy configs may still be kept as authored definitions in the future; if disabled, they must not generate scheduler jobs until intentionally re-enabled.

`TradingOpsState.details.strategy_breadth` projects every authored strategy config, including active, disabled paper, or disabled shadow families, as operator-visible breadth. Disabled strategy projection is observation-only: it may show source, trade structure, routine cadence, execution posture, environment compatibility, and the reason the strategy is not active, but it must not create scheduler jobs, candidate runs, decisions, intents, attempts, or broker submissions. `TradingOpsState.details.trading_flows` remains the active lifecycle-flow surface.

`spreads ops strategy-ledger --date <YYYY-MM-DD>` is the shipped daily evidence ledger. It reports every active strategy's source, candidate, signal, decision, admission, intent, attempt, order/fill, position, close, mark, PnL, blocker, config hash, and latest lifecycle ID evidence for one market date. Source evidence includes `source_evidence_state`, so static configured universes, dynamic source runs with symbols, missing recent source runs, and empty source runs are distinct. Candidate evidence includes diagnostic symbol counts, diagnostic status counts, raw/postprocess/runtime/returned candidate totals, persisted trade-candidate count, `candidate_productivity_state`, raw chain rejection counts, data-quality status/reason counts, calendar-policy status/reason counts, ranking-policy status/blocker counts, and market-data coverage totals. Use that split to distinguish no source symbols, no raw candidates, data/chain gaps, data-quality filtering, calendar filtering, ranking filtering, and true no-trade market conditions before changing thresholds. Use the ledger as the first tuning surface for catalog/profile changes instead of changing thresholds from vibes.

The 2026-06-15 non-momentum evidence showed all active non-momentum families had recent source and candidate-run facts, and every scoped family produced raw candidates. Index credit/condor and ETF short-put families were filtered primarily by expected-value, slippage-adjusted expected-value, model-IV, macro-calendar, and raw chain/liquidity evidence; earnings debit/long-vol families were filtered primarily by slippage-adjusted EV, entry slippage, model-IV, and low earnings calendar-confidence evidence. No fake rows, forced selections, or broad profile loosening should be used to advance those families.

`services.strategy_lab.build_historical_strategy_evaluation` is the backend current-model historical evaluator. It rolls daily ledger facts across a bounded date window, compares current catalog strategy/profile/source variants by config hash, aggregates candidate productivity, selection quality, admissions, execution/fill evidence, position/PnL outcomes, admission notional/max-loss context, and reason-code attribution, then checks ClickHouse option quote/trade coverage for option symbols referenced by stored candidate/signal/decision/attempt/position facts. Its first mode is `stored_facts_current_model`, so profile/source edits are not silently re-run; the artifact labels that comparison fidelity as `stored_facts_current_model_no_profile_rerun`.

`spreads execution list --date <YYYY-MM-DD>` is the shipped daily execution activity printout. It reports attempts with nested parent/leg order rows and leg fill rows for attempts whose `market_date` equals the date or whose `requested_at` falls inside that UTC activity day. Use `spreads execution inspect <execution_attempt_id>` for full single-attempt broker detail, refresh, or cancel decisions.

The long-vol strategy configs run in paper mode by default after the 2026-06-11 multi-strategy activation. The Spreads execution path supports their two-long-leg `mleg` debit order shape; long-vol must not be blocked by vertical-only width or return-on-risk validation.

## Multi-Strategy Activation Contract

Authored strategy breadth is not automatic strategy rotation. Spreads may carry disabled, shadow, paper, or future live strategy definitions, but no inactive strategy may create scheduler jobs, natural candidate runs, selected decisions, intents, attempts, positions, or broker submissions until it is deliberately activated through config and worker rollout.

Config keeps three concerns separate:

- `activation.state`: whether the strategy is active and allowed to generate scheduler-owned entry/manage jobs. Inactive definitions are visible breadth only.
- `activation.paused`: operator/runtime pause state for an otherwise active strategy.
- `execution.mode`: `shadow`, `paper`, or `live` execution posture for the canonical lifecycle.

An active shadow strategy may persist analysis-only evidence, but it must not produce selected entry decisions or execution intents. An active paper strategy may submit only when its execution posture, observed broker environment, approval mode, portfolio admission, execution admission, and risk gates all allow it. Live mode is reserved for an explicitly approved live-money rollout using the same lifecycle plus live deployment guards.

For non-long-call families, the required gate order is:

```text
quality profile -> account-agnostic selection -> portfolio admission -> execution admission -> queued broker submission
```

`quality profile` proves the candidate is structurally and economically worth considering for that family. `selection` chooses the best account-agnostic idea. `portfolio admission` decides whether the account should add this exposure now. `execution admission` validates broker-submission readiness, including leg shape, net debit/credit sign, quote freshness, and adapter support. Broker submission remains behind `execution_intent_dispatch` and `execution_submit`.

Portfolio admission is evaluated after a selected natural entry decision and before pending intent creation. It reads current portfolio positions, open entry attempts, and active entry intents, then persists a `portfolio_admission` sub-payload on the trade admission alongside `capacity_admission` and deferred `execution_readiness`. The policy blocks duplicate symbol/family exposure, strategy and family caps, daily new-entry caps, correlated broad-index ETF crowding, and total strategy max-loss exposure when the strategy exposes a computable risk budget. Active strategies must declare these caps under `risk.limits.portfolio_admission`; runtime fallback defaults exist only for older configs and must not be used to justify enabling a second non-long-call family. `TradingOpsState` projects the resolved strategy risk config and portfolio admission state separately under each trading flow, and summarizes portfolio block counts/reasons separately from quality blockers and execution-submit guards.

Shadow and paper are distinct activation modes. A shadow strategy may persist analysis-only evidence, but it must not produce selected entry decisions or execution intents. A paper strategy may submit only when its execution posture, observed broker environment, approval mode, portfolio admission, execution admission, and risk gates all allow it. Do not use disabled strategy breadth as a hidden auto-allocator.

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

Validation provenance values:

- `natural_strategy`: emitted by the scheduled strategy entry/manage flow from real ticker-source, candidate, signal, decision, admission, intent, attempt, order, fill, and position facts.
- `synthetic_validation`: emitted by an operator-run smoke harness for paper lifecycle mechanics. It must be visibly labeled and cannot satisfy natural selected-trade validation beads.
- `operator_direct`: emitted by deliberate operator actions such as direct order helpers, refresh, cancel, or other manually requested lifecycle work.

The shipped synthetic paper harness is `spreads lifecycle paper-smoke`. It is an operator validation path over the normal lifecycle, not a strategy-selection path. `paper-smoke open` is preview-first and requires `--execute` before it creates an intent; it checks market hours, the control plane and kill switch, Alpaca paper environment, exact contract and underlying allowlists, a total debit cap, and intent TTL before it writes. It can optionally auto-select a quoted SPY/QQQ contract under the debit cap for preview, but execution still requires the exact selected contract to be allowlisted. Synthetic open intents carry `validation_provenance=synthetic_validation`, `execution_mode=paper`, `approval_mode=auto`, `profile=paper_smoke`, and `queue_submission=true`, then rely on `execution_intent_dispatch` and `execution_submit` for broker submission. `paper-smoke close` only closes positions whose opening attempt is also `synthetic_validation`; it creates a close intent and uses the same queued close attempt path. `paper-smoke status` inspects intent, attempt, order, fill, position, and close evidence for the run.

The expected environment/provenance snapshot shape is:

- `execution_posture`
- `approval_mode`
- `execution_runtime`
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

`DataEngine -> engine facts/read models -> StrategyEngine -> RiskEngine -> ExecutionEngine -> PortfolioEngine -> Ops projections`.

The active entry owner is `PostgresStrategyEngine` in `services/trading_engine/strategy_runtime.py`. It resolves tickers through DataEngine, builds candidates once, delegates account-agnostic quality and selection to `EntrySelectionEngine`, plans trade decisions, runs admission, and creates execution intents.

Current entry selection runtime:

- DataEngine builds `FeatureSnapshot` rows from the resolved ticker set and candidate-build result through a registry keyed by `trade_structure` and `quality_profile_id`.
- Candidate generation consumes normalized `SymbolMarketSlice` inputs through `MarketSliceProvider`; live Alpaca calls are isolated behind `AlpacaMarketSliceProvider`.
- Candidate diagnostics enrich `underlying_setup` with SPY/QQQ relative-strength and broad-market regime facts from lightweight underlying benchmark slices. Sector ETF comparison is intentionally deferred until sector mapping exists cleanly.
- `EntryQualityPipeline` resolves configured quality-profile thresholds and operator overrides, evaluates pre-selection quality for candidate filtering, and evaluates post-selection readiness only after live signal fields exist.
- `EntrySelectionEngine` owns quality analysis, candidate quality filtering, live signal selection, and selected/monitored/rejected candidate output. It does not inspect account buying power, broker positions, or execution runtime state.
- Entry admission in `services/risk_manager.py` is the first account-aware boundary. It emits stable `capacity_admission`, `execution_readiness`, `reason_codes`, and `blockers` evidence; final execution readiness is explicitly deferred to the execution submit path.

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

Implemented quality stages include source preflight, underlying setup, chain viability, contract fit, premium quality, and selection. `momentum_long_call_v1` includes target-DTE chain viability plus SPY/QQQ relative-strength and market-regime filters. The non-long-call family profiles consume the generic structure snapshot and require canonical `legs[]`, family/leg-mix match, common expiration, spread/condor width where applicable, liquidity, premium/economics, return-on-risk, ranking-policy, and selection-readiness evidence. Long-vol profiles keep their two-long-leg debit shape and are not forced through vertical-only width validation. `TradingOpsState` projects the latest entry-quality waterfall from persisted engine facts so the CLI and dashboard show stage counts, top blocker reasons, selection counts, and admission counts from the same read model. The displaced replay/runtime filter helpers have been removed, and the `spr-34u` cutover was live-validated during the 2026-06-09 market session. First selected-order lifecycle validation remains a separate live-validation task that should wait for an actual selected decision.

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

Execution modules have explicit owners: `direct_orders.py` owns equity/option attempt construction for direct operator requests and intent-derived option opens, `position_close.py` owns close-by-position attempt creation, `submit.py` owns queued broker submission, `sync.py` owns refresh/cancel reconciliation, `attempts.py` owns attempt/order/fill payload sync helpers, `admission.py` owns execution admission payload helpers, and `order_requests.py` owns order payload construction plus live quote quality gates. Natural strategy open intents create `pending_submission` attempts and queue `execution_submit`; selected trade-decision dispatch reads canonical `execution_shape.legs[]` and `order_payload`, preserving multi-leg order class, leg roles/position intents, quantity, signed net limit price, validation provenance, and decision/intent refs into the queued attempt. Before broker submission, `execution_submit` runs an option-structure guard over canonical legs, family support, Alpaca `mleg` usage, net credit/debit sign, max-risk resolvability, quantity, and quote freshness; blocked attempts fail with stable execution-admission reason codes. The legacy long-call single-leg path still uses the single-leg helper; other natural option structures use the canonical structure-order helper. Synthetic smoke open intents opt into the same isolation with `queue_submission=true`; direct/manual option helpers remain direct unless they deliberately request queueing. Do not route new callers through the package root.

Operators inspect and reconcile individual attempts through `spreads execution inspect <execution_attempt_id>`, `spreads execution refresh <execution_attempt_id>`, and `spreads execution cancel <execution_attempt_id>`. These commands are thin adapters over `services/execution/sync.py`, support deploy targeting with `--env`, render attempt status plus order/fill counts and linked intent state, and refuse terminal cancels with `changed=false`.

`services/session_positions.py` owns position attribution. `PostgresPortfolioEngine` owns close decisions and `services/trading_engine/close_policy.py` owns reusable profit/stop/force-close policy math. `services/trading_engine/risk_runtime.py` owns close admission checks for position status, reconciliation freshness, broker symbols, and order validity. `services/exit_manager.py` exposes `run_trading_strategy_manage`, the manage-job adapter: it refreshes marks, applies broker/active-close guards plus close admission, and creates close intents for selected closes. Close actions go through intents and attempts; they should not bypass the execution lifecycle.

## Hot-Path Abstraction Audit

Current audit result for the live trading hot path:

- Keep the thin engine contract modules in `services/trading_engine/`. They are ownership boundaries and typed payload shapes, not a second runtime, bus, actor framework, or alternate store.
- Keep `execution_submit` as the broker-submit isolation job. It gives each claimed intent a durable attempt/job lifecycle and lets ops distinguish dispatch, broker submission, and unknown-submit outcomes.
- Keep candidate-build policy helpers under DataEngine ownership. `services/strategy_candidate_builders/` is the only active candidate-construction package; it must not become an alternate orchestration path, product surface, CLI flow, or persistence owner.
- Keep `EntrySelectionEngine` as the canonical account-agnostic strategy-selection service. Do not put account capacity, broker submit readiness, or alert delivery back into selection.
- Merge management scheduling into `trading_strategy_manage` only. There is no standalone position-exit job type; `services/exit_manager.py` exposes the Strategy/Portfolio manage adapter used by `trading_strategy_manage`.
- Do not add a message bus, second database, actor framework, or compatibility wrapper around removed runtime surfaces.

## Operator Read Models

Operator views should read service-owned state through:

- `services/ops/`
- `services/positions.py`
- `services/execution/runtimes.py`

The dashboard should show strategy-owned runtime state, not recreate old runtime pages or infer business logic in the frontend.

`TradingOpsState` exposes healthy no-entry rationale through each trading flow's `entry_posture` and the primary summary fields `primary_entry_state`, `primary_entry_message`, and `primary_entry_blocker_groups`. A flat strategy can therefore be explicitly healthy when source and candidate runs are fresh but account-agnostic quality filters, such as broad market regime or target-DTE chain viability, blocked all candidates.

`TradingOpsState.details.strategy_breadth` is the canonical operator inventory for authored strategy breadth. Disabled paper/shadow strategies can appear there as `paper_observation_candidate` or `shadow_observation_candidate`, but their breadth contracts force effective automatic submission off unless the strategy is actually active through the scheduler-owned lifecycle spine.

Entry planning treats non-live signal eligibility, including `analysis_only` emitted by shadow-mode strategy runs, as observation evidence rather than selected-entry eligibility. Those rows may be persisted for regime comparison, but they must not create selected entry decisions or execution intents.

Disabled strategy breadth can be run explicitly through `spreads lifecycle observe-strategy <trading_strategy_id>`. Observation runs resolve authored strategy config without enabling scheduler jobs, run the normal ticker-source, candidate-build, entry-quality, signal, and decision persistence spine, force `analysis_only`/`observation_only` provenance, and stop before admission or execution-intent creation. `TradingOpsState.details.strategy_breadth[].latest_observation` exposes the latest observation evidence for each authored strategy.

`TradingOpsState.details.broker_exposure` classifies the latest broker account snapshot positions by ownership against canonical open Spreads position legs. Broker option legs should be labeled as `spreads_managed`, `spreads_synthetic_validation`, or `external_manual` instead of being hidden behind raw broker account positions.

`StorageOpsState.summary` uses `market_data_*` fields for ClickHouse market-data aggregates and `storage_*` fields for combined ClickHouse plus Postgres capture-summary storage aggregates. Avoid generic storage totals that hide which database class owns the bytes or row estimates.

## Rollout Notes

- After schema changes, run `uv run alembic upgrade head`.
- After declared job YAML or strategy config changes, restart the scheduler and affected workers so they reload config.
- After code imported by runtime/data workers changes, restart those containers before trusting live behavior.
- Default validation is live/runtime checks through shipped CLIs and operator reads. Do not add automated tests unless explicitly requested.
