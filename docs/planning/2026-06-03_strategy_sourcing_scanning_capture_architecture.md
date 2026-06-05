# Strategy Sourcing, Candidate Scanning, And Capture Architecture

Date: 2026-06-03

Status: target-state proposal for epic `spr-cb5`.

Related:

- [System Architecture](../current_system_state.md)
- [Target Trading Lifecycle Object Model](./2026-06-03_target_trading_lifecycle_object_model.md)
- [Trading Lifecycle State Machines](./2026-06-03_trading_lifecycle_state_machines.md)
- [Nautilus Patterns Inside Spreads](./2026-06-03_nautilus_patterns_inside_spreads_architecture.md)
- [Alpaca Capabilities Statement](../research/alpaca_capabilities_statement.md)

## Decision Summary

The clean direction is to copy NautilusTrader's engine boundaries, then adapt them to Spreads' product/runtime shell.

NautilusTrader's useful spine is:

```text
DataEngine -> Cache -> Strategy -> RiskEngine -> ExecutionEngine -> Portfolio -> Events
```

Spreads should express the same architecture as:

```text
DataEngine
  -> EngineCache / durable read models
  -> StrategyEngine
  -> RiskEngine
  -> ExecutionEngine
  -> PortfolioEngine
  -> Ops projections
```

This is not a request to embed Nautilus, copy its actor runtime, add a new message bus, revive Rust bridge ownership, or create a second live database. The target is to keep Spreads' Postgres, Redis/ARQ, Docker deployment, CLI, API, dashboard, and logging stack, while making the trading internals as cleanly bounded as a real trading engine.

`trading_strategy` remains the product/runtime owner. A strategy declares where its tickers come from, what trade structure it builds, when entry runs, how management runs, and what risk/execution limits apply. Entry execution resolves tickers through one source resolver, builds trade candidates, normalizes them into `TradeSignal`, records `TradeDecision`, and then creates `ExecutionIntent` only when the strategy actually selects something.

The target should remove `discovery_run` as an active product surface. It should also remove `uoa_only` runtime behavior for now. UOA can come back later as a proper ticker/intel source or signal source, not as a special discovery lane.

The important boundary is:

```text
ticker source -> resolved ticker set -> trade candidate build -> TradeSignal -> TradeDecision -> ExecutionIntent
```

Market capture is separate:

```text
candidate / intent / position -> CaptureTarget -> market_recorder -> raw events -> CaptureSummary
```

The critical refinement is that `SourceRun`, `SourceTicker`, `CandidateRun`, and `TradeCandidate` are `DataEngine` facts. `TradeSignal` and `TradeDecision` are `StrategyEngine` facts. `AdmissionDecision` belongs to `RiskEngine`. `ExecutionIntent`, `ExecutionAttempt`, `BrokerOrder`, and `BrokerFill` belong to `ExecutionEngine`. `TradingPosition`, `CloseDecision`, `PositionClose`, and reconciliation belong to `PortfolioEngine`.

## Current Problem

The current system has the right ownership direction but still has old shapes underneath it:

- `trading_strategy` is the intended owner.
- `ticker_source` materializes dynamic ticker lists such as `finviz_momentum`.
- `discovery_run` still owns scanner cycles, candidate diagnostics, and some operator health.
- Static-universe strategies are included in generated discovery scopes.
- Symbol-feed strategies such as `momentum_long_calls` can have fresh tickers but still need a clean candidate-generation path before entry decisions can consume anything.
- Capture behavior is partly tied to scan/discovery cycles even though quote capture should be driven by current trading need and Alpaca subscription limits.

That makes the system harder to reason about because "where tickers come from", "how candidates are built", "what strategy decided", and "what we must capture now" are not first-class separate contracts.

## Naming

Use these target names going forward:

| Name | Meaning |
| --- | --- |
| `SpreadsKernel` | Logical engine boundary that coordinates jobs/services without introducing a new actor runtime. This can remain package/module organization rather than a singleton object. |
| `DataEngine` | Owns ticker source refresh, market-data normalization, market slices, candidate inputs, capture targets, and capture summaries. |
| `EngineCache` | Current-state read model backed by Postgres/Redis/in-memory request scope. It borrows Nautilus' cache-before-strategy idea without making memory the durable truth. |
| `StrategyEngine` | Owns entry/manage orchestration, signal normalization, and strategy decisions. |
| `RiskEngine` | Owns admission decisions before broker attempts. |
| `ExecutionEngine` | Owns intents, attempts, broker orders, fills, and adapter dispatch. |
| `PortfolioEngine` | Owns positions, close decisions, position closes, and reconciliation. |
| `TickerSource` | Strategy config declaration for where underlying symbols come from. This is config, not necessarily a table. |
| `SourceRun` | Durable fact for a dynamic source refresh, such as Finviz momentum. |
| `SourceTicker` | One normalized ticker from a `SourceRun`, with rank, reason, source metrics, and expiry. |
| `ResolvedTickerSet` | The ticker set an entry routine uses for one run after source resolution, limits, staleness checks, and fallback policy. This can be payload/evidence before it needs its own table. |
| `CandidateRun` | One candidate-build pass for a strategy entry run. |
| `TradeCandidate` | Option/equity candidate with legs, economics, liquidity, quote age, and builder diagnostics. |
| `TradeSignal` | Normalized setup/candidate fact that can receive a strategy decision. This is the lifecycle object that replaces the old `signal_states` plus `opportunities` split. |
| `TradeDecision` | Strategy verdict over one signal: skip, no-entry, selected, selected-blocked, superseded. |
| `ExecutionIntent` | Durable request to open, close, replace, or cancel. |
| `CaptureTarget` | Desired market-data capture need for an underlying or option contract. |
| `CaptureSummary` | Reduced quote/trade/mark quality facts used by decisions, exits, dashboards, and retention. |

Avoid carrying these names forward as active architecture:

- `discovery_run` as product owner
- `pipeline_id` as runtime owner
- `uoa_only`
- feed-direct trading paths
- scanner-specific strategy wrappers
- capture that only exists as a side effect of scanner diagnostics

## Target Runtime Flow

Strategy entry should be the orchestrator for opening trades.

```text
SpreadsKernel job tick
  |
  +--> StrategyEngine.entry(trading_strategy)
        |
        +--> DataEngine.resolve_tickers
        |     |
        |     +--> static universe from config
        |     +--> latest valid SourceRun / SourceTicker rows
        |
        +--> DataEngine.build_trade_candidates for the declared trade_structure
        |
        +--> StrategyEngine.normalize TradeSignal rows
        |
        +--> StrategyEngine.record TradeDecision rows with clear selected/skip/no-entry reasons
        |
        +--> ExecutionEngine.create ExecutionIntent rows only for selected decisions
```

Strategy management should not pull from ticker sources. Management is position-centric:

```text
open TradingPosition
  |
  +--> StrategyEngine.manage(trading_strategy)
        |
        +--> PortfolioEngine.read current position, order, quote, and capture summaries
        +--> PortfolioEngine.record CloseDecision or no-action reason
        +--> ExecutionEngine.create close ExecutionIntent when an exit rule fires
```

This keeps entry source selection out of exit logic and prevents a dynamic source from accidentally deciding whether an existing position should be managed.

## Source Model

Every entry strategy should declare a source through the same config contract.

Static source:

```yaml
source:
  type: static
  ref: short_dated_index_core
  max_symbols: 25
```

Dynamic materialized source:

```yaml
source:
  type: dynamic
  ref: finviz_momentum
  max_age_seconds: 300
  max_symbols: 15
  stale_behavior: skip
  fallback:
    type: static
    ref: momentum_fallback_watchlist
    mode: observe_only
```

The strategy entry code should not have separate paths for static and dynamic strategies. It should call one resolver:

```text
resolve_tickers(strategy.source, as_of)
  -> ResolvedTickerSet(symbols, source_refs, source_run_id, freshness, blockers, evidence)
```

### When To Use Static Lists

Use static lists for intentional coverage:

- index and ETF strategies
- curated earnings strategies
- high-conviction watchlists
- symbols that must be monitored even when they are not currently in a public mover/feed list
- strategy families where the edge comes from repeatable structure and risk control, not from dynamic discovery

Static does not mean primitive. Static still runs through the same candidate builder, signal normalization, decision records, capture targets, risk, and execution lifecycle.

### When To Use Dynamic Sources

Use dynamic sources when the edge starts with "what is moving now" or "what should we look at now":

- Finviz momentum
- Alpaca movers / most actives
- news-linked movers
- research AI output
- manual operator watchlists
- future UOA source after it is rebuilt

Dynamic sources should materialize `SourceRun` and `SourceTicker` rows when they are external, rate-limited, reusable, operator-visible, or expensive enough that entry jobs should not scrape/recompute them directly.

### Fallback Policy

Fallback must be explicit. A stale dynamic source should never silently become a different trading universe.

Recommended modes:

| Mode | Behavior |
| --- | --- |
| `skip` | Do not evaluate entry when the source is stale or unavailable. Best for momentum sources. |
| `static_tradeable` | Use a configured static fallback and allow normal trading. Best only for intentionally equivalent universes. |
| `observe_only` | Build diagnostics/signals but block intent creation. Useful during rollout. |
| `disabled` | No fallback. Equivalent to hard skip. |

For `momentum_long_calls`, the default should be `skip` or `observe_only`, not silent tradeable fallback. If Finviz is stale, the strategy should say exactly that and not create an intent.

## Candidate Build Model

The system does not need a separate generic "scan job" product on day one.

The simpler and cleaner pivot is:

- dynamic ticker sources refresh independently only when needed
- strategy entry jobs resolve tickers at runtime
- strategy entry jobs call the candidate builder for their trade structure
- candidate facts are persisted as part of the entry run
- decisions and intents stay owned by the strategy lifecycle

This avoids a new scheduler/control-plane layer while still giving us durable facts.

Target entry run:

```text
EntryRun(strategy_id, run_key, config_hash)
  |
  +--> ResolvedTickerSet
  +--> CandidateRun
  +--> TradeCandidate rows
  +--> TradeSignal rows
  +--> TradeDecision rows
  +--> ExecutionIntent rows
```

`CandidateRun` can be a persisted `DataEngine` fact without being a separately scheduled job type. If resource pressure later proves that shared candidate refresh must be decoupled from entry cadence, the same object can be produced by a dedicated refresh worker without changing strategy decisions.

## Capture Model

Capture should be a `DataEngine` desired-state controller, not a scanner side effect.

Alpaca option quotes have subscription limits, and option streaming cannot subscribe to every option symbol with a wildcard. The capture system should compute a prioritized desired subscription set from current trading need.

Priority order:

1. Open positions.
2. Pending, claimed, or working intents.
3. Selected or nearly selected candidates.
4. Watch candidates.
5. Raw source tickers without buildable candidates.

Target flow:

```text
TradeCandidate / TradeSignal / ExecutionIntent / TradingPosition
  |
  +--> CaptureTarget(contract_symbol, priority, reason, ttl, owner)
        |
        +--> market_recorder reconciles actual subscriptions
              |
              +--> option quote/trade ticks
              +--> CaptureSummary rows
```

Raw quote/trade ticks should have short partition retention. Decisions, exits, dashboards, and audits should lean on `CaptureSummary` and latest mark/quote read models whenever possible. This keeps the quote tables from growing forever while preserving the facts needed to explain trading behavior.

## Finviz Long Calls Target

The Finviz long-call flow should become:

```text
ticker_source:finviz_momentum
  |
  +--> SourceRun / SourceTicker rows

trading_strategy:momentum_long_calls:entry
  |
  +--> resolve latest valid finviz_momentum tickers
  +--> build long-call candidates for those tickers
  +--> normalize to TradeSignal
  +--> apply entry rules such as VWAP reclaim, spread, quote age, DTE, delta, open interest, and strategy limits
  +--> record skip/no-entry/selected decisions
  +--> create at most the allowed number of open intents

trading_strategy:momentum_long_calls:manage
  |
  +--> manage open positions only
  +--> close through the normal intent/attempt/fill/position lifecycle
```

This removes the ambiguity where a feed is fresh but the strategy has no candidate-generation run to consume.

## Static Strategy Target

Static-universe strategies should use the same entry pipeline:

```text
trading_strategy:short_dated_index_put_credit:entry
  |
  +--> resolve static universe short_dated_index_core
  +--> build candidates
  +--> normalize to TradeSignal
  +--> decide
  +--> intent
```

The difference is source resolution only. Static and dynamic strategies should not create parallel lifecycle objects or dashboard surfaces.

## Nautilus Inspiration Boundaries

The parts to copy from Nautilus are the system boundaries:

- data is normalized before strategy logic consumes it
- strategies read current state from cache/read models, not from ad hoc API calls
- strategies emit commands/intents, not broker facts
- risk/admission runs before broker submission
- execution owns broker attempts, orders, fills, and adapter translation
- portfolio owns positions and close/reconciliation state
- operator views read projections rather than owning business logic

The parts not to copy are the implementation substrate:

- no new actor framework
- no new message bus
- no Rust bridge ownership
- no separate live trading database
- no standalone Nautilus `TradingNode`
- no second scheduler or operator CLI

Spreads improves the pattern for this system by keeping Postgres as durable truth, making ticker/candidate sourcing explicit, and making operator visibility first-class.

## Critical Review

The earlier idea of a fully generic source graph plus scan-plan scheduler is correct at scale, but too much for the next refactor step.

What would be overbuilt now:

- persisted `StrategyPlan` tables before operators need to inspect compiled plans
- separate scheduled `scan_job` for every strategy/source combination
- generic graph orchestration before there are enough node types to justify it
- compatibility shims around `discovery_run`
- UOA rework inside the same cutover

What should happen now:

- keep authored strategy config as the source of runtime truth
- add one source resolver contract
- materialize dynamic ticker sources when they are external or reusable
- let strategy entry own candidate build and signal/decision production
- make capture a separate desired-state controller because it manages scarce live data resources
- delete old discovery-run product ownership instead of wrapping it

This gives us the architecture we want without adding a whole new operating model.

## Implementation Direction

Epic `spr-cb5` owns the implementation.

| Bead | Work |
| --- | --- |
| `spr-cb5.1` | Refine Nautilus-shaped engine architecture docs. |
| `spr-cb5.2` | Add `core.services.trading_engine` skeleton and typed contracts. |
| `spr-cb5.3` | Replace active ticker sources with ticker sources. |
| `spr-cb5.4` | Move candidate build into strategy entry runtime. |
| `spr-cb5.5` | Persist candidate runs and trade signals. |
| `spr-cb5.6` | Replace opportunity decisions with trade decisions. |
| `spr-cb5.7` | Add admission decisions as the risk boundary. |
| `spr-cb5.8` | Clean execution facts away from discovery references. |
| `spr-cb5.9` | Move management into `PortfolioEngine`. |
| `spr-cb5.10` | Implement desired-state capture targets. |
| `spr-cb5.11` | Cut ops dashboard and CLIs to engine surfaces. |
| `spr-cb5.12` | Delete legacy discovery and symbol-feed active paths. |
| `spr-cb5.13` | Deferred: rebuild UOA as a ticker/intel source. |

## Acceptance Criteria

The refactor is good when these are true:

- A strategy can use either a static or dynamic ticker source without a separate code path.
- `momentum_long_calls` can refresh Finviz tickers and build long-call candidates from them during its entry window.
- Static strategies still build candidates from authored universes through the same entry pipeline.
- Entry decisions explain every no-trade result with source, candidate, policy, and risk reason codes.
- Management decisions are position-centric and do not depend on current source membership.
- Capture targets are visible, prioritized, bounded, and reconciled by `market_recorder`.
- Raw quote growth is controlled by retention and summary tables.
- Old `discovery_run` ownership is deleted, not preserved behind new names.
- UOA is absent from active runtime until rebuilt as a proper source/intel lane.
