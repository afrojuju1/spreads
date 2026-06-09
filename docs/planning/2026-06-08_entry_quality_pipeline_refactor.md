# Entry Quality Pipeline Refactor Plan

Date: 2026-06-08

Status: implemented and live-validated for `momentum_long_calls`. Beads `spr-34u.1` through `spr-34u.10` are shipped; first selected-order lifecycle validation remains separate under `spr-tlf`.

Related:

- [Current System State](../current_system_state.md)
- [Trading Engine Inspiration Repos](./2026-06-08_trading_engine_inspiration_repos.md)
- [Strategy Sourcing, Candidate Scanning, And Capture Architecture](./2026-06-03_strategy_sourcing_scanning_capture_architecture.md)
- [Target Trading Lifecycle Object Model](./2026-06-03_target_trading_lifecycle_object_model.md)

## Recommendation

Build one centralized `EntryQualityPipeline`, driven by one named `quality_profile` per entry routine. This is now the active path for `momentum_long_calls` with `entry.quality_profile: momentum_long_call_v1`.

Do not expose arbitrary strategy-level "N filters" as the primary interface. That would make every strategy a custom boolean maze and would keep today's scattered gates alive under a new name.

The live strategy config stays small:

```yaml
entry:
  quality_profile: momentum_long_call_v1
  quality_overrides:
    chain_viability:
      min_open_interest: 500
      max_relative_spread: 0.10
    premium_quality:
      max_entry_slippage_dollars: 25
```

The profile owns the ordered stages. Operators can review and tune named stage thresholds, but the system keeps one canonical entry-quality path.

## Target Flow

```text
TickerSource / Universe
  -> FeatureSnapshot
  -> EntryQualityProfile
  -> CandidateBuilder
  -> TradeSignal
  -> TradeDecision / Selection
  -> RiskAdmission
  -> ExecutionIntent
  -> Position / Portfolio
```

The important addition is `FeatureSnapshot`. The first shipped cutover computes feature snapshots from the resolved ticker set plus existing candidate-build diagnostics/candidates, then evaluates the quality profile without adding another data-fetching path.

Before the cutover, parts of source freshness, setup quality, option chain viability, liquidity, ranking, and entry eligibility were spread across source jobs, builders, ranking policy, ad hoc evaluation helpers, live selection, and admission. The refactor centralizes the facts first, then lets filters evaluate those facts.

## Why This Shape

- QuantConnect-style boundary: source/universe, signal, portfolio/execution, risk stay separate.
- Zipline-style factor/filter split: compute features once; filters do not fetch their own data.
- Freqtrade-style ordered filtering: order matters, but Spreads should hide chain composition inside named profiles.
- Nautilus-style risk discipline: risk/admission happens after strategy selection and before broker attempts.
- Hummingbot-style lifecycle split: strategy emits intent; execution owns order state and lifecycle.

## Core Objects

| Object | Owner | Meaning |
| --- | --- | --- |
| `TickerSource` | DataEngine | Config declaration for where symbols come from. |
| `ResolvedTickerSet` | DataEngine | Symbols selected for one entry run, with source/freshness evidence. |
| `FeatureSnapshot` | DataEngine | Point-in-time facts for a symbol and optional candidate context. |
| `EntryQualityProfile` | StrategyEngine | Named ordered pipeline for a strategy family. |
| `EntryQualityStage` | StrategyEngine | A stage such as source preflight, setup, chain viability, contract fit, or premium quality. |
| `EntryFilter` | StrategyEngine | Typed evaluator over a `FeatureSnapshot` and optional candidate. |
| `FilterResult` | StrategyEngine | `pass`, `watch`, or `block`, with reason codes, metrics, thresholds, and message. |
| `TradeCandidate` | DataEngine / StrategyEngine boundary | Built contract candidate with economics, legs, liquidity, and diagnostics. |
| `TradeSignal` | StrategyEngine | Normalized candidate/setup fact eligible for a decision. |
| `TradeDecision` | StrategyEngine | Selected, no-entry, skip, or blocked strategy verdict. |
| `AdmissionDecision` | RiskEngine | Broker/risk admissibility after strategy selection. |
| `ExecutionIntent` | ExecutionEngine | Durable request to open, close, replace, or cancel. |

## Pipeline Stages

### 1. Source Preflight

Goal: reject bad source inputs before spending option-chain work.

Examples:

- source run is fresh enough
- symbol is normalized and known
- symbol is optionable when the trade structure needs options
- source rank is inside the strategy's max universe size
- source degradation does not require observe-only or skip

### 2. Underlying Setup

Goal: decide whether the underlying is worth building option candidates for.

Examples:

- price and equity-volume floors
- relative volume
- VWAP reclaim or trend confirmation
- relative strength vs SPY/QQQ/sector
- market regime supportive enough for long calls
- quote age/spread sanity on the underlying

### 3. Chain Viability

Goal: reject symbols whose options chain is mechanically bad for this trade.

Examples:

- target DTE expirations exist
- snapshots are fresh
- Greeks are available where required
- enough bid/ask size exists
- open interest and option volume are usable
- relative and absolute bid/ask spread are within profile limits

This stage should catch today's `TNGX` / `NRIX` shape early: momentum ticker, but bad option chain.

### 4. Contract Fit

Goal: filter individual option contracts.

Examples:

- DTE range
- delta range
- OTM/ATM long-call policy
- expected-move profit viability
- minimum return-on-risk where applicable
- contract-level liquidity still sane

### 5. Premium Quality

Goal: reject contracts where the setup is real but the premium is a bad buy.

Examples:

- probability of profit above floor
- expected value above floor
- slippage-adjusted expected value above floor
- entry slippage dollars below ceiling
- model IV inside profile range
- IV/HV or IV percentile/rank not hostile to long premium

No hard max premium. Premium size belongs in risk budget, buying power, slippage, and EV. A high-premium option can be valid if the risk/reward and account admission make sense.

### 6. Selection

Goal: rank survivors and choose only allowed entries.

Examples:

- top candidate per symbol
- max selected candidates per run
- replacement/confirmation behavior
- "watch" candidates retained for diagnostics/capture but not selected

### 7. Admission

Goal: decide whether a selected strategy decision may create an executable intent.

Admission is not a quality filter. It is the `RiskEngine` boundary.

Examples:

- buying power
- max open positions
- max daily entries/actions
- per-symbol exposure
- active intent conflicts
- trading state: active, halted, reducing

## Existing Gate Migration

The active `momentum_long_call_v1` profile now carries the migrated gates plus target-DTE chain viability and SPY/QQQ relative-strength/regime filters. EV and richer premium-quality work remain separate future beads so the current cutover does not pretend those checks are active.

| Stage | Active filter ids | Existing evidence / reason codes |
| --- | --- | --- |
| `source_preflight` | `source_is_fresh` | `ResolvedTickerSet.blockers`, `ticker_source_ready`, `ticker_source_fallback`, stale/missing source reasons |
| `underlying_setup` | `setup_context_usable`, `relative_strength_supportive`, `market_regime_supportive` | setup status/score/reasons from candidate-builder diagnostics; SPY/QQQ benchmark return and relative-strength metrics; unfavorable setup is watch unless current scoring blocks it |
| `chain_viability` | `chain_data_available`, `option_snapshots_available`, `greeks_available`, `target_dte_chain_usable` | `data_unavailable`, `no_snapshot`, `no_delta`, target-DTE contract/snapshot/delta counts, raw viable-contract count, open-interest/size/spread chain blockers |
| `contract_fit` | `strategy_family_matches`, `dte_in_range`, `delta_in_range`, `entry_recipe_passed` | `strategy_family_mismatch`, `dte_below_min`, `dte_above_max`, `short_delta_below_min`, `short_delta_above_max`, `delta_outside_range`, recipe failure reasons |
| `premium_quality` | `open_interest_ok`, `relative_spread_ok`, `return_on_risk_ok`, `ranking_policy_passed` | `open_interest_below_min`, `open_interest_below_floor`, `relative_spread_above_max`, `relative_spread_above_ceiling`, `return_on_risk_below_min`, `return_on_risk_below_floor`, ranking policy blockers |
| `selection` | `selection_score_ok`, `selection_live_ready` | `scoring_state`, `scoring_blockers`, `execution_blockers`, `selection_state`, `eligibility` from live selection |

## Filter Contract

```python
class EntryFilter:
    id: str
    stage: EntryStage

    def evaluate(
        self,
        context: EntryContext,
        snapshot: FeatureSnapshot,
        candidate: TradeCandidate | None = None,
    ) -> FilterResult:
        ...
```

```python
@dataclass(frozen=True)
class FilterResult:
    status: Literal["pass", "watch", "block"]
    reason_codes: tuple[str, ...]
    metrics: Mapping[str, Any]
    thresholds: Mapping[str, Any]
    message: str
```

Rules:

- Filters evaluate facts. They do not fetch Alpaca, scrape Finviz, query calendars, or mutate storage.
- Every block has stable reason codes.
- Every threshold comes from the resolved quality profile.
- Every result can be persisted into candidate diagnostics and surfaced in ops.
- Missing data is explicit: `missing_greeks`, `stale_snapshot`, `calendar_unavailable`, not a generic failure.

## High-Level Pseudocode

```python
def run_strategy_entry(strategy_id: str, now: datetime) -> EntryRunResult:
    strategy = load_strategy(strategy_id)
    profile = load_quality_profile(strategy.entry.quality_profile)

    ticker_set = data_engine.resolve_tickers(strategy.source, as_of=now)
    run = strategy_engine.start_entry_run(strategy, ticker_set, profile, now)

    for symbol in ticker_set.symbols:
        symbol_snapshot = data_engine.build_feature_snapshot(symbol, strategy, now)
        symbol_waterfall = EntryQualityWaterfall(profile)

        if not symbol_waterfall.apply_stage("source_preflight", symbol_snapshot):
            persist_symbol_diagnostic(run, symbol, symbol_waterfall)
            continue

        if not symbol_waterfall.apply_stage("underlying_setup", symbol_snapshot):
            persist_symbol_diagnostic(run, symbol, symbol_waterfall)
            continue

        if not symbol_waterfall.apply_stage("chain_viability", symbol_snapshot):
            persist_symbol_diagnostic(run, symbol, symbol_waterfall)
            continue

        raw_candidates = candidate_builder.build(strategy.trade_structure, symbol_snapshot)

        for candidate in raw_candidates:
            candidate_snapshot = symbol_snapshot.with_candidate(candidate)
            candidate_waterfall = symbol_waterfall.copy()

            if not candidate_waterfall.apply_stage("contract_fit", candidate_snapshot, candidate):
                persist_trade_candidate(run, candidate, candidate_waterfall)
                continue

            if not candidate_waterfall.apply_stage("premium_quality", candidate_snapshot, candidate):
                persist_trade_candidate(run, candidate, candidate_waterfall)
                continue

            scored = strategy_engine.score_candidate(candidate, candidate_waterfall)
            persist_trade_candidate(run, scored, candidate_waterfall)

    signals = strategy_engine.create_signals_from_passed_candidates(run)
    decisions = strategy_engine.select_entries(signals, strategy.entry.selection)

    for decision in decisions:
        persist_trade_decision(decision)

        if decision.state != "selected":
            continue

        admission = risk_engine.evaluate_entry(decision)
        persist_admission_decision(admission)

        if admission.allowed:
            execution_engine.create_entry_intent(decision, admission)

    return summarize_entry_run(run)
```

## Storage And Ops Shape

Do not create a DB event log for filter results. Use structured app logs for runtime logs, and persist compact decision evidence on the domain facts.

Recommended persisted evidence:

- `CandidateRun.summary.filter_stage_counts`
- `TradeCandidate.evidence.quality_waterfall`
- `TradeSignal.evidence.quality_profile_id`
- `TradeDecision.reason_codes`
- `AdmissionDecision.reason_codes`

Ops should show a simple waterfall:

```text
momentum_long_calls entry
source_preflight   pass 5 / block 0
underlying_setup   pass 3 / watch 1 / block 1
chain_viability    pass 1 / block 4
contract_fit       pass 3 / block 165
premium_quality    pass 0 / block 3
selection          selected 0
admission          attempted 0
```

## Migration Plan

This is intentionally a full cleanup path, not a compatibility wrapper.

1. Done: add profile contracts and registry.
2. Done: add `FeatureSnapshot` builder for `momentum_long_calls`.
3. Done: move existing source/setup/chain/contract/ranking/runtime gates into the pipeline with no intended behavior change.
4. Done: persist the quality waterfall on candidate diagnostics and candidate evidence.
5. Done: cut over `momentum_long_calls` entry to `quality_profile: momentum_long_call_v1`.
6. Done: add target-DTE optionable chain viability early.
7. Done: add SPY/QQQ relative-strength and market-regime filters.
8. Done: update ops CLI/dashboard to show filter waterfall.
9. Done: remove stale duplicate gate plumbing once the pipeline is canonical.
10. Done: live-validate the cutover after the filter, ops, and cleanup beads. First selected-order lifecycle validation is tracked separately and should wait for an actual selected decision.

## First Profile

```python
QUALITY_PROFILES = {
    "momentum_long_call_v1": EntryQualityProfile(
        stages=[
            Stage("source_preflight", filters=[
                source_is_fresh,
                symbol_is_optionable,
            ]),
            Stage("underlying_setup", filters=[
                underlying_price_volume_ok,
                relative_strength_supportive,
                vwap_reclaim_confirmed,
                market_regime_supportive,
            ]),
            Stage("chain_viability", filters=[
                has_target_dte_chain,
                option_snapshots_fresh,
                greeks_available,
                chain_liquidity_usable,
            ]),
            Stage("contract_fit", filters=[
                dte_in_range,
                delta_in_range,
                otm_or_atm_call,
                expected_move_supports_profit,
            ]),
            Stage("premium_quality", filters=[
                spread_cost_ok,
                slippage_adjusted_ev_ok,
                probability_of_profit_ok,
                implied_volatility_price_ok,
            ]),
        ],
    )
}
```

## Pushback

Do not solve "not enough trades" by loosening all filters.

The live diagnostics showed a healthy system producing zero candidates because option chains were not good enough or premium quality failed. That is different from a broken strategy. The first improvement should be clearer, earlier, centralized rejection evidence. After that, tune one stage at a time.

Also do not add `max_premium` as a blunt quality filter. For long calls, high debit is not automatically wrong. The real checks are:

- risk per trade
- buying power
- entry slippage dollars
- slippage-adjusted expected value
- IV price quality
- exit liquidity

## Open Questions

1. Should `watch` candidates drive capture targets even when they are not selectable?
2. Should sector ETF comparison be added after clean sector mapping exists?
3. Should profile overrides remain numeric-threshold only, or eventually allow enabling/disabling individual filters?

Recommended answer for question 3: numeric thresholds only at first.
