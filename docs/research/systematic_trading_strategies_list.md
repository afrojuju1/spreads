# Systematic Trading Strategies List

Status: working list

Purpose: define the strategy families worth researching for this system, plus the governance rules that every strategy should obey.

This is not a list of random setups. It is the strategy taxonomy for a stock-first, options-enriched research and alerting system.

## System Bias

For this system, the highest-fit strategies are:

- stock-first
- intraday or short-horizon
- event-aware
- session-aware
- liquidity-aware
- optionally confirmed by options flow or options positioning

Do not start with:

- deep options microstructure strategies
- L2/order-book strategies
- complex options flow strategies that require data Alpaca does not provide

## Evidence Status

This document is not claiming that every pattern below is already proven in this repo.

Current status:

- research-backed at the broad concept level for momentum, relative strength, news reaction, and the importance of liquidity and transaction costs
- market-practice-backed for many intraday chart patterns and level-based entries
- not yet repo-validated through the system's own evaluation engine

Use these evidence tiers:

- `Tier A`: supported by established research or strong market-structure literature
- `Tier B`: widely used in practice and structurally coherent, but needs repo validation
- `Tier C`: heuristic only and should not be treated as an edge without evaluation

## Evidence By Strategy Family

## Tier A

- relative strength / weakness
- momentum continuation
- catalyst and news reaction
- liquidity and tradeability filters
- event-aware governance

Why:

- momentum and relative-strength effects are well-documented in the literature
- transaction costs and liquidity materially affect whether apparent edges survive in practice
- news and announcement timing clearly matter for return jumps and intraday behavior

## Tier B

- opening range breakout / breakdown
- gap and go / gap fade
- VWAP reclaim / VWAP loss
- trend pullback continuation
- compression breakout
- failed breakout / trap reversal

Why:

- these are coherent price-structure patterns used heavily in practice
- some related intraday momentum and opening-range research exists
- but strategy quality is highly implementation-dependent and should be treated as system hypotheses until evaluated here

## Tier C

- indicator crossover systems by themselves
- RSI-only or MACD-only entries
- pattern calls without structure, liquidity, and event context

Why:

- standalone indicators are usually too weak and redundant to treat as a complete strategy
- they are better used as supporting evidence than as primary triggers

## Market-Cap And Liquidity Applicability

This framework does not apply equally well across the whole market.

## ETFs And Index Proxies

Best fit.

Why:

- strongest liquidity
- tighter spreads
- cleaner session structure
- easier relative-strength framing
- best options confirmation coverage

Default use:

- fully supported

## Mega Caps And Liquid Large Caps

Strong fit.

Why:

- good bar quality
- tighter spreads
- cleaner catalyst reaction than thinner names
- better execution quality

Default use:

- fully supported

## Mid Caps

Selective fit.

Why:

- some setups still work well
- but slippage, spread quality, and event distortion become more important

Default use:

- allowed only with stricter liquidity, participation, and catalyst rules

Best pattern families:

- catalyst continuation
- gap and go / gap fade
- opening range breakout
- relative strength continuation

## Small Caps

Separate policy, not default.

Why:

- wider spreads
- noisier bars
- higher halt and manipulation risk
- weaker reliability for options confirmation
- much higher sensitivity to transaction costs and execution quality

Default use:

- do not include in the default strategy framework
- only allow through small-cap-specific strategies with stricter rules

Best initial use:

- catalyst-led, liquidity-screened, news-driven strategies only

## Micro Caps / Penny Stocks

Out of scope.

Why:

- market quality is too inconsistent for this framework
- the system would need a different governance model

## Default Segment Policy

System default should be:

- ETFs and index proxies
- mega caps
- liquid large caps
- selected liquid mid caps

Do not default to:

- broad small-cap scanning
- low-priced names
- thin names with unstable spreads
- names without usable options context when the strategy expects options confirmation

## Strategy Governance

Every strategy must define these before it is considered real:

- market universe
- instrument type
- timeframe
- allowed session window
- allowed regime
- catalyst or event policy
- liquidity policy
- entry trigger
- invalidation level
- stop logic
- profit-taking logic
- time stop
- no-trade conditions
- evaluation windows

## Entry Governance

Every entry should require:

- clear structure
- clear trigger
- clear invalidation
- acceptable liquidity
- acceptable data quality

Preferred entry stack:

1. context
2. structure
3. trigger
4. confirmation
5. tradeability

Where:

- `context` = session, regime, catalyst, relative strength
- `structure` = key levels, pattern shape, trend state
- `trigger` = break, reclaim, rejection, pullback hold, expansion
- `confirmation` = volume, participation, relative strength persistence, options confirmation
- `tradeability` = spread quality, freshness, liquidity, slippage risk

Minimum entry rules:

- no entry without a defined invalidation level
- no entry on stale or low-confidence data
- no entry through major scheduled event windows unless the strategy explicitly allows it
- no entry if liquidity quality fails the strategy's floor

## Exit Governance

Every strategy should define all three exit classes:

- profit exit
- risk exit
- time exit

Preferred exit stack:

- primary target
- invalidation stop
- time stop
- forced session close if the strategy is not designed to hold overnight

Exit styles the system should support:

- fixed-R exits
- structure exits
- trailing exits
- VWAP-based exits
- end-of-day force close

Rules:

- profit target and stop must be defined before entry
- stops should anchor to structure or volatility, not arbitrary cents
- time stops matter; stale trades are still bad trades
- exit logic should be strategy-specific, not one global template

## Technical Indicators To Combine

Indicators should be combined by role, not collected blindly.

## Structure Indicators

- prior day high / low
- premarket high / low
- opening range
- VWAP
- weekly high / low
- gap fill
- local swing highs / lows

## Trend Indicators

- `9 EMA`
- `20 EMA`
- `50 EMA`
- anchored VWAP when relevant

## Participation Indicators

- relative volume
- trade count
- bar range expansion
- volume expansion versus recent bars

## Momentum Indicators

- rate of change
- RSI
- MACD histogram

## Volatility Indicators

- ATR
- compression versus recent true range
- opening range width

## Relative Indicators

- relative strength versus `SPY`
- relative strength versus `QQQ`
- sector ETF relative strength

## Options Confirmation Indicators

- unusual options activity
- recent option premium concentration
- call/put dominance
- volume versus open interest
- options spread quality

## Indicator Combination Rules

Good combinations:

- structure + participation + relative strength
- trend + pullback + volume confirmation
- catalyst + price reaction + options confirmation
- mean reversion + stretch + weakening participation

Bad combinations:

- too many momentum indicators saying the same thing
- using indicators without a structure anchor
- entering on indicator crossover alone

## Chart Pattern Families

These are the pattern families worth supporting.

## 1. Opening Range Breakout / Breakdown

- best in: strong open, clean participation
- trigger: break of opening range with volume and persistence
- invalidation: return through opening range
- best confirmations: relative strength, volume expansion, options confirmation

## 2. Gap And Go

- best in: fresh catalyst, strong premarket structure
- trigger: hold above premarket/opening support and resume trend
- invalidation: failure back through open or premarket pivot
- best confirmations: news, relative strength, early volume

## 3. Gap Fade / Failed Open

- best in: weak follow-through after a strong gap
- trigger: failed hold above gap structure, then breakdown
- invalidation: reclaim of open or failed level
- best confirmations: weak relative strength, shrinking participation

## 4. VWAP Reclaim / VWAP Loss

- best in: trend continuation or intraday trend reversal
- trigger: reclaim and hold of VWAP or decisive loss of VWAP
- invalidation: immediate loss of reclaim / regain after breakdown
- best confirmations: trend alignment, volume support, relative strength

## 5. Trend Pullback Continuation

- best in: established directional session
- trigger: pullback into VWAP / EMA / prior breakout area, then continuation
- invalidation: loss of pullback support
- best confirmations: strong relative strength, trend EMA alignment

## 6. Compression Breakout

- best in: coiled range, low realized volatility
- trigger: expansion out of a tight range
- invalidation: failed return into the base
- best confirmations: volume expansion, wider range bars, catalyst support

## 7. Failed Breakout / Trap Reversal

- best in: crowded breakout attempts
- trigger: break above key level, rejection, then loss of reclaim
- invalidation: successful reclaim and hold
- best confirmations: weak participation, relative weakness, reversal volume

## 8. Relative Strength Continuation

- best in: market mixed, symbol still leading
- trigger: leader pulls back lightly and resumes while market is flat or weaker
- invalidation: loss of leadership and structure
- best confirmations: sector alignment, options confirmation

## 9. Mean Reversion To VWAP / Range

- best in: range-bound or overextended names
- trigger: exhaustion away from VWAP or local range edge, then reversion signal
- invalidation: continued expansion away from mean
- best confirmations: stretched ATR, fading participation, no strong catalyst

## 10. Catalyst Continuation

- best in: real news with clean reaction
- trigger: reaction consolidates and continues in the direction of the catalyst
- invalidation: loss of reaction pivot
- best confirmations: options activity, relative strength, follow-through volume

## Strategy List For This System

These are the priority strategy groups.

## Tier 1

Best initial strategies because they fit Alpaca, fit the current roadmap, and can share infrastructure.

- opening range breakout / breakdown
- gap and go / gap fade
- VWAP reclaim / VWAP loss
- trend pullback continuation
- relative strength continuation
- catalyst continuation

## Tier 2

Very useful, but should follow after the first context layers are stable.

- compression breakout
- failed breakout / trap reversal
- mean reversion to VWAP / range
- option-positioning-confirmed directional continuation

## Tier 3

Research later, not early.

- pure options-structure entry strategies
- slower swing strategies that need richer overnight context
- strategies requiring deeper market structure than Alpaca provides

## Strategy Card Template

Every concrete strategy should eventually get a card with:

- name
- market type
- timeframe
- session window
- regime
- setup prerequisites
- trigger
- invalidation
- stop logic
- target logic
- time stop
- confirmations
- disqualifiers
- evaluation windows

## Recommendation

Build the shared layers first, then express strategies on top of them.

Recommended order:

1. price structure and key levels
2. session and regime context
3. relative strength
4. catalyst and event intelligence
5. tradeability and liquidity confidence
6. strategy-specific triggers and exits

That keeps the strategy library systematic instead of turning it into disconnected pattern detection.

## Sources

- [Are Momentum Profits Robust to Trading Costs?](https://www.kellogg.northwestern.edu/faculty/korajczy/htm/korajczyk%20sadka.jf2004.pdf)
- [Market Intraday Momentum](https://www.sciencedirect.com/science/article/pii/S0304405X18301351)
- [Assessing the Profitability of Intraday Opening Range Breakout Strategies](https://www.sciencedirect.com/science/article/pii/S1544612312000438)
- [The Arrival of News and Return Jumps in Stock Markets](https://arxiv.org/abs/1901.02691)
- [Federal Reserve Bank of New York: The Joint Dynamics of Liquidity, Returns, and Volatility across Small and Large Firms](https://www.newyorkfed.org/research/staff_reports/sr207.html)
- [SEC Market Structure Data Downloads](https://www.sec.gov/data-research/market-structure-data)
