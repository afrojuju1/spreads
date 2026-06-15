# Trading Engine Feature Matrix

Date: 2026-06-13

Status: active planning companion for `spr-9v2`. This document is not the current-architecture source of truth.

Related:

- [Current System State](../current_system_state.md)
- [Trading Engine Inspiration Repos](./2026-06-08_trading_engine_inspiration_repos.md)
- [Entry Selection Engine End Goal Architecture](./2026-06-08_entry_selection_engine_end_goal.md)
- [Strategy Catalog And Profile Contract](./2026-06-11_strategy_archetype_profile_contract.md)
- Bead `spr-9v2`: Sequence post-inspiration trading-engine capability work

## Purpose

Keep one compact feature matrix for trading-engine capability planning.

Use this to answer:

- what exists now
- what is partial
- what is missing
- which inspiration pattern applies
- which bead or workstream should own the next step

Do not use this document to override [Current System State](../current_system_state.md). Current ownership, shipped runtime boundaries, and operator guidance still live there.

## Rules

- Backend/runtime only. New operator-app strategy-lab pages and UI experiment browsers are explicitly out of scope for `spr-9v2`.
- Do not revive removed `replay`, `audit`, `backtest`, `analyze`, or `post-market analyze` wrappers.
- Do not reintroduce per-strategy YAML, paper-specific config files, or compatibility loaders.
- Preserve the separation between source, candidate quality, strategy selection, portfolio admission, execution admission, execution lifecycle, and position management.
- Borrow boundaries from inspiration repos, not their whole runtimes.
- Do not treat natural lifecycle proof beads as executable while active strategies are producing no candidates, signals, or selected decisions. Candidate productivity and blocker attribution must come first.

## Status Legend

| Status | Meaning |
| --- | --- |
| Shipped | Present in the current runtime and documented in current-state docs. |
| Partial | Present, but too thin or not yet complete for the target capability. |
| Proof needed | Implemented enough to run, but still waiting for honest live or burn-in validation. |
| Gap | Missing as a shipped current-model capability. |
| Deferred | Intentionally future work or tracked in an existing deferred bead. |
| Out of scope | Not part of this backend/runtime roadmap. |

## Capability Matrix

| Capability area | Current Spreads feature | Status | Inspiration pattern | Gap or risk | Next owner |
| --- | --- | --- | --- | --- | --- |
| Strategy catalog and profiles | `packages/config/strategies/catalog.yaml` plus `profiles.yaml` compose runtime strategies. `execution.mode` owns paper/shadow/live posture. | Shipped | LEAN lifecycle split, Spreads-native strategy specs | Legacy `live_enabled` still leaks in config/runtime output and can mislead future agents. | `spr-9v2` cleanup child |
| Strategy activation | All 9 authored strategies validate as active paper strategies. Activation is deliberate through catalog config. | Shipped | LEAN algorithm activation discipline | Active breadth increases need for global allocation and proof discipline. | `spr-9zt`, then portfolio construction child |
| Source and universe models | Static universes plus one dynamic Finviz momentum source. Ticker source observations feed DataEngine. | Partial | LEAN universe selection, Freqtrade pairlists | Universe breadth is narrow; no current UOA/intel/event/factor source model in runtime. | `spr-cb5.13`, source expansion child |
| Market data capture | `market_recorder.py` owns Alpaca option websocket targets and writes raw option ticks/snapshots to ClickHouse. | Proof needed | Nautilus data/cache ownership | ClickHouse cutover needs burn-in over another regular-market window. | `spr-281` |
| Data quality gates | Entry quality and ops project blockers such as missing snapshots, greeks, expected move, and chain viability. | Partial | Zipline factor/filter separation | Data readiness is visible but not yet a durable market-data quality SLA surface across strategies. | feature/data-quality child |
| Feature snapshots | DataEngine builds `FeatureSnapshot` rows through a registry keyed by `trade_structure` and `quality_profile_id`. | Partial | Zipline computes factors once, then filters | Snapshot builders are useful, but not yet a persistent factor store for historical runs and cross-strategy research. | historical strategy lab plus feature-store child |
| Candidate generation | Candidate builders emit canonical option structures, legs, economics, liquidity, expected-move, and ranking evidence. | Partial | Spreads-native structure generation | Live runs can be mechanically healthy while producing zero candidates across active strategies; blocker attribution and candidate productivity are the immediate bottleneck. | `spr-9v2.1` |
| Entry quality profiles | Named quality profiles and ordered waterfall stages cover source preflight, setup, chain viability, contract fit, premium quality, and selection. | Shipped | Freqtrade ordered filters, but hidden behind named profiles | Richer EV, calibration, and outcome-tuned thresholds need historical evidence, not vibes. | historical strategy lab child |
| Per-strategy selection | `EntrySelectionEngine` performs account-agnostic quality analysis and selects per-strategy promotable ideas. | Partial | LEAN alpha/signal separation | Current proof is limited when no strategy produces natural candidates/signals; once that is fixed, active multi-strategy scale can still become first-scheduled-strategy-wins. | `spr-9v2.1`, then portfolio construction child |
| Portfolio admission | `portfolio_admission` blocks duplicate symbol/family exposure, caps, daily entries, correlated groups, and max risk. | Partial | LEAN portfolio construction plus Nautilus risk denials | It is a post-selection gate, not a cross-strategy allocation planner. | portfolio construction child |
| Portfolio construction | No global `AllocationPlan` ranks selected signals across strategies before admission. | Gap | LEAN portfolio construction | Without this, strategies compete by schedule timing instead of portfolio value. | portfolio construction child |
| Execution intent lifecycle | Natural selected entries create pending intents and queued `execution_submit` attempts; attempts/orders/fills are persisted. | Partial | Hummingbot controllers emit executor actions; Nautilus explicit execution lifecycle | The open and close lifecycle exists, but proof still depends on natural selected decisions and stale close orders. | `spr-ztc`, `spr-ds1`, `spr-9zt` |
| Executor profiles | `executor_profiles` currently resolve approval and runtime, with repricing mostly living elsewhere. | Partial | Hummingbot executors own lifecycle | TTL, quote freshness, cancel/reprice policy, max concession, and order style are not first-class executor-profile behavior. | executor-profile lifecycle child |
| Execution admission | `execution_submit` validates structure, quote quality, buying power, runtime compatibility, and broker submission readiness. | Shipped | Nautilus risk checks on submit path | Needs to stay separate from source, quality, and portfolio construction as executor policy grows. | executor-profile lifecycle child |
| Position and close management | PortfolioEngine evaluates close decisions; close intents/attempts go through execution lifecycle. | Proof needed | Hummingbot executor lifecycle, Nautilus portfolio state | Adaptive close repricing requires honest live proof with a real stale working close. | `spr-ds1` |
| Protections | Profile config has protection models and runtime controls such as cancel pending entries and flatten times. | Partial | Freqtrade protections | Missing richer drawdown halts, loss streaks, cooldowns, event blocks, options exposure/scenario limits, and account-level risk protections. | protection/risk child |
| Historical evaluation | No shipped historical-evaluation CLI or `packages/core/backtest` package. Older replay/backtest docs are historical. | Gap | LEAN backtests, Qlib recorder, vectorbt compact research | Largest missing feedback loop; strategy tuning cannot be systematic without a current-model evaluator, and live proof beads cannot progress while candidate production is barren. | `spr-9v2.1`, then historical strategy lab child |
| UOA/intel source | Old market-intel CLI was deleted; UOA reintroduction is deferred as a source/intel contract. | Deferred | Source model plus market-intel as input, not side product | Needs clean return through ticker/intel/source contracts, not discovery-run leftovers. | `spr-cb5.13` |
| Resource management | Current runtime keeps base services up and recorder idles outside market hours. Docker-backed runtime profiles are not implemented. | Deferred | Ops/resource control, not a trading-framework pattern | Market-aware container scaling is tracked separately and should not become a custom always-on ResourceController. | `spr-cgr` |
| Operator read models | `TradingOpsState` and `StorageOpsState` are canonical operator state. CLI/web consume these read models. | Shipped | Nautilus cache/read-state discipline | New backend capabilities must project through these read models; new UI pages are out of scope here. | current-state updates only when implemented |
| Operator app strategy lab | No dedicated web strategy-lab experiment or comparison UI. | Out of scope | Product workflow, not backend prerequisite | Intentionally skipped for `spr-9v2`; backend primitives must exist first. | future separate bead only |

## Inspiration Parity Matrix

| Inspiration | Pattern to borrow | Current parity | Missing Spreads-native capability | Do not copy |
| --- | --- | --- | --- | --- |
| QuantConnect LEAN | Universe, alpha/signal, portfolio construction, execution, risk split | DataEngine, EntrySelectionEngine, admission, execution, and portfolio close boundaries exist | Global portfolio construction and historical evaluation | LEAN framework, cloud assumptions, full algorithm runtime |
| Zipline Pipeline | Compute facts once, then screen/filter over facts | `FeatureSnapshot` and quality pipeline exist | Persistent point-in-time feature store for historical and cross-strategy research | Full Zipline live/research pipeline machinery |
| Freqtrade | Ordered pairlist/filter chains plus separate protections | Named quality profiles with ordered stages exist | Richer source chains and protection engine | Arbitrary configured filter mazes in live strategies |
| NautilusTrader | Explicit engine boundaries, central read state, submit-path denials | Strategy/Risk/Execution/Portfolio/Ops boundaries are largely present | Stronger lifecycle proof, cache/read-state maturity, and risk/protection breadth | Nautilus embedding, Rust bridge revival, second scheduler |
| Hummingbot V2 | Strategy controllers emit actions; executors own order lifecycle | Execution intents and attempts exist | ExecutorProfile as first-class lifecycle policy for open and close orders | Exchange-agnostic bot runtime |
| Qlib | Experiment recorder and research workflow | Offline company valuation has research lanes, trading has engine facts | Trading strategy experiment recorder and historical evaluator | ML-first live decision engine |
| vectorbt | Compact signal/position research semantics | Engine facts and execution ledger can provide inputs | Fast comparison layer over current-model decisions and outcomes | Treating vectorized research as live runtime truth |

## Roadmap Matrix

| Order | Workstream | Start condition | Target capability | Validation |
| --- | --- | --- | --- | --- |
| 0 | Restore candidate production feedback loop | Active strategies are healthy but producing no candidates/signals/selected decisions | Quantified attribution for data coverage gaps, source-universe gaps, builder gaps, profile threshold issues, and true market-condition rejections; fix the real blocker without fake rows or forced loosening | `spr-9v2.1`; strategy ledger shows explainable candidates/signals or quantified no-candidate evidence; config validate, ops state, and storage state remain healthy |
| 1 | Historical strategy lab/backend evaluator | Current live blocker attribution identifies what must be measured over history | One historical evaluation surface over current ticker-source, candidate, signal, decision, admission, intent, attempt, and position facts | Config validate, stored engine fact comparison, ClickHouse coverage/fidelity report, artifact output with no old wrapper resurrection |
| 2 | Finish proof prerequisites that require natural candidates | Candidate productivity exists naturally enough to exercise downstream lifecycle | Natural strategy lifecycle, multi-strategy paper lifecycle, adaptive close repricing, and ClickHouse storage are honestly proven | `uv run spreads ops state --json`, `uv run spreads ops storage --json`, `uv run spreads ops strategy-ledger --date YYYY-MM-DD --json`, `uv run spreads execution list --date YYYY-MM-DD --json` |
| 3 | Portfolio construction/allocation | Active multi-strategy facts are reliable enough to compare candidates | Cross-strategy `AllocationPlan` ranks selected signals under capital, risk, correlation, strategy caps, and schedule constraints before admission | Strategy ledger shows allocation decisions; portfolio admission consumes allocation output; no first-scheduled-strategy-wins behavior |
| 4 | Executor-profile lifecycle policy | Intent/attempt lifecycle and close repricing are proven | ExecutorProfile owns approval, runtime, order style, quote freshness, TTL, cancel/reprice, max concession, and open/close lifecycle policy | Open and close attempts show policy provenance; `execution_submit` and intent maintenance apply the same profile contract |
| 5 | Source, feature, and protection expansion | Historical evaluator can measure source/feature/protection changes | Named UOA/intel/event/factor sources, persisted feature/factor snapshots, and separate protection/risk policies | Source observations, feature rows, protection denials, and risk blocks project through TradingOpsState and strategy ledger |
| 6 | Stale runtime noun cleanup | New runtime paths no longer depend on legacy fields | Remove or stop emitting legacy `live_enabled` in active runtime/config surfaces; keep `execution.mode` authoritative | `uv run spreads config validate --json` shows no misleading legacy posture fields; docs and skills remain aligned |

## Minimal Validation Commands

Use these before changing behavior or closing implementation beads:

```bash
uv run spreads config validate --json
uv run spreads ops state --json
uv run spreads ops storage --json
uv run spreads ops strategy-ledger --date YYYY-MM-DD --json
uv run spreads execution list --date YYYY-MM-DD --json
uv run spreads execution positions --date YYYY-MM-DD --json
```

For market-data and storage changes, also inspect StorageOpsState and ClickHouse coverage before trusting historical evaluation results.
