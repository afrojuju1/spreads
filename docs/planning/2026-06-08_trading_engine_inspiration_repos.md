# Trading Engine Inspiration Repos

Date: 2026-06-08

Status: active inspiration list for Spreads trading-engine architecture. These are not dependencies to embed.

Related:

- [Current System State](../current_system_state.md)
- [Strategy Sourcing, Candidate Scanning, And Capture Architecture](./2026-06-03_strategy_sourcing_scanning_capture_architecture.md)
- [Entry Quality Pipeline Refactor Proposal](./2026-06-08_entry_quality_pipeline_refactor.md)

## Decision

Keep the inspiration set small and opinionated. Spreads should copy boundaries and proven patterns, not framework bulk.

Primary inspiration repos/products:

1. QuantConnect LEAN: [QuantConnect/Lean](https://github.com/QuantConnect/Lean)
2. Zipline Pipeline: [stefan-jansen/zipline-reloaded](https://github.com/stefan-jansen/zipline-reloaded)
3. Freqtrade: [freqtrade/freqtrade](https://github.com/freqtrade/freqtrade)
4. NautilusTrader: [nautechsystems/nautilus_trader](https://github.com/nautechsystems/nautilus_trader)
5. Hummingbot V2: [hummingbot/hummingbot](https://github.com/hummingbot/hummingbot)

Secondary research tools to watch, not copy directly:

- Qlib for experiment/workflow and recorder ideas.
- vectorbt for compact signal/position research semantics.

## What To Copy

| Repo | Pattern To Borrow | Spreads Translation |
| --- | --- | --- |
| QuantConnect LEAN | Strong lifecycle split: universe selection, alpha/signal, portfolio construction, execution, risk. | `TickerSource -> FeatureSnapshot -> QualityProfile -> TradeSignal -> TradeDecision -> AdmissionDecision -> ExecutionIntent`. |
| Zipline Pipeline | Compute factors once, then apply filters/screens over those factors. | Add `FeatureSnapshot` as the centralized facts layer before filters. Filters should evaluate facts, not fetch data. |
| Freqtrade | Ordered pairlist/filter chains and separate protections. | Use ordered stages inside a named quality profile. Keep risk/admission separate from entry signal quality. |
| NautilusTrader | Explicit engine boundaries, central cache/read state, risk checks on submit/modify path with concrete denials. | Keep `StrategyEngine`, `RiskEngine`, `ExecutionEngine`, `PortfolioEngine`, and ops read models separate. |
| Hummingbot V2 | Controllers emit executor actions; executors own order lifecycle. | Strategy selects; execution owns intent dispatch, repricing, cancel, close, and order lifecycle. |

## What Not To Copy

- Do not add LEAN's full algorithm framework, cloud assumptions, or portfolio-construction complexity before Spreads needs it.
- Do not add Zipline-style research pipeline machinery to live trading. Borrow factor/filter separation only.
- Do not expose Freqtrade-style arbitrary configured filter chains at the live strategy layer. That would recreate config sprawl.
- Do not embed NautilusTrader or revive Rust bridge ownership. Nautilus remains architectural inspiration only.
- Do not move Spreads toward Hummingbot's exchange-agnostic bot runtime. Spreads is an operator-focused Alpaca options system first.

## Source Notes

- QuantConnect's Algorithm Framework names core modules as Universe Selection, Alpha Creation, Portfolio Construction, Execution, and Risk Management: [QuantConnect Algorithm Framework](https://www.quantconnect.com/docs/v1/algorithm-framework/overview).
- QuantConnect universes narrow the tradable basket and can be manual, fundamental, or scheduled: [QuantConnect Universe Selection](https://www.quantconnect.com/docs/v1/algorithm-framework/universe-selection).
- Zipline filters describe asset sets, combine via boolean operators, and can be used as pipeline screens: [Zipline Pipeline Filters](https://zipline.ml4trading.io/_modules/zipline/pipeline/filters/filter.html).
- Freqtrade pairlist handlers can be chained in configured order, while protections are a separate concept: [Freqtrade Plugins](https://docs.freqtrade.io/en/stable/plugins/).
- NautilusTrader's RiskEngine sits on the submit/modify path and emits denied/rejected outcomes with reasons: [Nautilus Execution](https://nautilustrader.io/docs/latest/concepts/execution/).
- NautilusTrader's cache centralizes market, order, position, account, instrument, and custom data: [Nautilus Cache](https://nautilustrader.io/docs/latest/concepts/cache/).
- Hummingbot V2 controllers emit executor actions, and executors manage order state/lifecycle: [Hummingbot Controllers](https://hummingbot.org/strategies/v2-strategies/controllers/) and [Hummingbot Executors](https://hummingbot.org/strategies/v2-strategies/executors/).

## Operating Rule

When a future design question comes up, ask:

1. Is this a universe/source problem, signal-quality problem, selection problem, admission problem, execution lifecycle problem, or portfolio/position problem?
2. Which of the five inspiration repos has the cleanest boundary for that problem?
3. What is the smallest Spreads-native version of that boundary?

If the answer requires adding a broad framework, the answer is probably too big.
