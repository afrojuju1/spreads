# Strategy Archetype And Profile Contract

Date: 2026-06-11

Status: target contract for `spr-72o`; no runtime behavior changes in this bead.

Related:

- [Current System State](../current_system_state.md)
- [Trading Engine Inspiration Repos](./2026-06-08_trading_engine_inspiration_repos.md)

## Decision

Spreads should move from large strategy YAML files toward small `StrategySpec` files backed by reusable `StrategyArchetype` and profile definitions.

The target model borrows the useful boundaries from LEAN, Zipline Pipeline, Freqtrade protections, Nautilus execution/risk separation, and Hummingbot controller/executor split without importing those frameworks:

```text
StrategySpec
  -> StrategyArchetype
     -> UniverseModel
     -> SignalModel
     -> QualityProfile
     -> PortfolioModel
     -> ProtectionModel
     -> ExecutorProfile
     -> ExitController
  -> EvidenceLedger
```

`trading_strategy_id` remains the runtime identity. Archetypes and profiles are reusable contracts, not runtime owners and not broker facts.

## Target Objects

| Object | Owns | Does not own | Current or replacement owner |
|---|---|---|---|
| `StrategySpec` | One authored strategy identity, activation state, thesis, archetype reference, and small thesis-level overrides. | Repeated schedules, broker lifecycle, broad risk machinery, or copied quality thresholds. | Replaces most of each file in `packages/config/trading_strategies`. Loaded by `services/trading_strategies.py`. |
| `StrategyArchetype` | The reusable family contract: allowed structures, default source model, quality, portfolio, protection, executor, exit controller, and routine schedule profile. | `trading_strategy_id`, broker orders, fills, or account state. | New config owner under strategy-profile config. Composes into the current runtime strategy config. |
| `UniverseModel` | The source of underlyings and source freshness expectations. Can be static, dynamic ticker source, event/earnings source, or future factor-screen source. | Option-chain construction or trade selection. | Current owners: `packages/config/universes`, `packages/config/ticker_sources`, `services/ticker_sources.py`, `services/trading_engine/data_runtime.py`. |
| `SignalModel` | How candidate facts become trade-signal evidence for a thesis: directional, premium-selling, earnings long-vol, volatility contraction, etc. | Account capacity, portfolio caps, or broker submit readiness. | Current owner: `EntrySelectionEngine` and signal persistence in `services/trading_engine/strategy_runtime.py`; target owner remains StrategyEngine-side. |
| `QualityProfile` | Ordered account-agnostic filters and scoring stages over facts and candidate structure snapshots. | Buying power, open positions, submit admission, or exits. | Current owner: `services/trading_engine/entry_quality_pipeline.py`, `packages/core/domain/profiles.py`, `services/strategy_candidate_builders/`. |
| `PortfolioModel` | Account-aware sizing, family caps, symbol caps, daily entry caps, max risk, and admission evidence. | Entry signal quality or broker order construction. | Current owner: `services/risk_manager.py` entry admission plus `portfolio_admission` payloads. |
| `ProtectionModel` | Strategy/family run guards: cooldowns, stale-source/data halts, drawdown halts, loss streak halts, event-window blocks, and operator control-plane protections. | Candidate scoring or order lifecycle. | Partially owned today by runtime policy, entry blockers, and risk admission. Target owner is an explicit protection profile used before admission and projected in ops. |
| `ExecutorProfile` | Approval mode, execution posture, runtime adapter, order style, quote freshness gate, repricing/cancel policy, and structure-submit compatibility. | Strategy selection, position ownership, or PnL. | Current owners: `services/execution_intents/`, `services/execution/`, `execution_submit`, and `order_requests.py`. |
| `ExitController` | Manage cadence, mark refresh expectations, profit/stop/expiry recipes, close admission handoff, and close-intent policy. | Open-entry selection or broker sync truth. | Current owners: `services/exit_manager.py`, `services/trading_engine/portfolio_runtime.py`, `services/trading_engine/close_policy.py`, and `services/trading_engine/risk_runtime.py`. |
| `EvidenceLedger` | A compact per-strategy daily read model: source, candidates, blockers, signals, decisions, admissions, intents, attempts, positions, marks, closes, PnL, and config hash. | Persistence ownership of raw facts. | New ops projection built from existing facts in `services/ops/`, engine fact repositories, execution repositories, and positions read models. |

## Target Flow

```text
UniverseModel
  -> candidate build + FeatureSnapshot
  -> QualityProfile + SignalModel
  -> TradeSignal + TradeDecision
  -> ProtectionModel
  -> PortfolioModel admission
  -> ExecutionIntent
  -> ExecutorProfile
  -> Attempt + Order + Fill
  -> Position
  -> ExitController
  -> close admission + close intent + close attempt
  -> EvidenceLedger projection
```

This preserves the current runtime spine:

```text
DataEngine -> StrategyEngine -> RiskEngine -> ExecutionEngine -> PortfolioEngine -> Ops projections
```

## Target YAML Examples

These examples show the desired authored shape. They are not loaded by the runtime yet.

### Index Credit Vertical

```yaml
# packages/config/strategy_archetypes/index_credit_vertical.yaml
strategy_archetype_id: index_credit_vertical
allowed_trade_structures:
  - call_credit_spread
  - put_credit_spread
universe_model: liquid_index_etfs
signal_model: index_premium_selling_v1
quality_profile: index_credit_vertical_v1
portfolio_model: defined_risk_index_premium_paper_v1
protection_model: index_intraday_premium_protections_v1
executor_profile: alpaca_paper_mleg_limit_v1
exit_controller: short_dated_credit_exit_v1
routines:
  entry:
    cadence_minutes: 5
    market_hours_only: true
    window:
      start_et: "09:45"
      end_et: "15:45"
  manage:
    cadence_minutes: 1
    market_hours_only: true
    window:
      start_et: "09:45"
      end_et: "15:45"
```

```yaml
# packages/config/trading_strategies/short_dated_index_call_credit.yaml
trading_strategy_id: short_dated_index_call_credit
name: Short-Dated Index Call Credit
activation: paper_active
archetype: index_credit_vertical
trade_structure: call_credit_spread
thesis: Sell short-dated defined-risk premium on liquid index ETFs when skew, liquidity, and expected-value evidence clear the family profile.
overrides:
  structure:
    dte:
      min: 4
      max: 10
    short_delta:
      min: 0.10
      max: 0.28
    widths: [2, 3, 5]
  portfolio:
    max_strategy_open_positions: 2
    max_daily_new_entries: 2
    max_total_strategy_risk: 1000
```

### Earnings Long-Vol

```yaml
# packages/config/strategy_archetypes/earnings_long_vol.yaml
strategy_archetype_id: earnings_long_vol
allowed_trade_structures:
  - long_straddle
  - long_strangle
universe_model: liquid_earnings_stocks
signal_model: earnings_long_vol_v1
quality_profile: earnings_long_vol_v1
portfolio_model: defined_debit_event_risk_paper_v1
protection_model: earnings_event_protections_v1
executor_profile: alpaca_paper_mleg_limit_v1
exit_controller: short_dated_debit_exit_v1
routines:
  entry:
    cadence_minutes: 5
    market_hours_only: true
    window:
      start_et: "09:45"
      end_et: "14:30"
  manage:
    cadence_minutes: 1
    market_hours_only: true
    window:
      start_et: "09:45"
      end_et: "15:45"
```

```yaml
# packages/config/trading_strategies/short_dated_earnings_long_straddle.yaml
trading_strategy_id: short_dated_earnings_long_straddle
name: Short-Dated Earnings Long Straddle
activation: paper_active
archetype: earnings_long_vol
trade_structure: long_straddle
thesis: Buy short-dated two-long-leg volatility around liquid earnings names when event reward, debit risk, and liquidity clear the long-vol profile.
overrides:
  structure:
    dte:
      min: 2
      max: 10
    entry_delta:
      min: 0.45
      max: 0.55
      target: 0.50
  portfolio:
    max_strategy_open_positions: 1
    max_daily_new_entries: 1
    max_total_strategy_risk: 1500
```

## Current Config Concepts

Keep:

- `trading_strategy_id` as the canonical runtime owner.
- `trade_structure` names such as `long_call`, `call_credit_spread`, `long_straddle`, and `short_put`.
- `execution.mode`, `execution.approval`, and `execution.runtime` as execution posture inputs, eventually resolved through `ExecutorProfile`.
- `entry.quality_profile` identifiers while profiles are moved into reusable profile config.
- Canonical `execution_shape.legs[]`, signed net price, intent, attempt, order, fill, and position facts.
- `config_hash`, but compute it from the composed `StrategySpec + StrategyArchetype + profiles` runtime payload.

Rename or move:

- `source` -> `UniverseModel` reference plus source overrides.
- `build` -> structure parameters under the archetype or strategy `overrides.structure`.
- `entry.schedule` and `management.schedule` -> archetype routine schedule profile with rare spec-level overrides.
- `liquidity` -> quality-profile gates.
- `risk.limits.portfolio_admission` -> `PortfolioModel.admission`.
- `risk.sizing` -> `PortfolioModel.sizing`.
- `management.recipes` -> `ExitController` policy names.
- `runtime.extends` -> `ProtectionModel` and routine/runtime policy references.

Remove from authored strategy specs:

- Repeated threshold blocks copied across strategies when they belong to a named profile.
- Loose recipe strings that are not resolved by an exit controller.
- Strategy-level broker-environment declarations; broker environment remains observed state.
- Legacy `runtime.live_enabled` as an execution decision input.
- Vertical-only assumptions in long-vol, short-put, or other non-vertical families.
- Hidden analysis or shadow flags that can bypass admission or execution posture.

## Migration Notes

1. Add profile files beside current strategy config without changing scheduler behavior.
2. Build an evidence ledger before tuning so every active strategy can be compared by source, candidate, blocker, decision, admission, execution, position, and PnL evidence.
3. Compose the existing runtime strategy payload from `StrategySpec + StrategyArchetype + profiles`, then reduce strategy YAML files to thesis and overrides only.
4. Preserve deployed paper behavior unless the evidence ledger justifies a deliberate profile change.

The runtime should still expose the current object vocabulary to operators: `trading_strategy_id`, candidate runs, trade signals, trade decisions, admissions, intents, attempts, positions, and closes. Archetypes and profiles are authored configuration contracts, not a second live runtime.
