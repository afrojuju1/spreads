# Options Scanner Research Log

This file tracks researched topics, findings, and current decisions for the Alpaca options scanner work.

## Topic: DTE Framework For Call Credit Spread Scanner

### Problem

We do **not** want one flat DTE rule for all call credit spreads.

Reason:

- short-dated options have faster theta decay
- short-dated options also have higher gamma risk
- event risk can dominate short-expiry pricing
- a `1 DTE` spread and a `35 DTE` spread should not be scored the same way

### Research Summary

- As expiration approaches, **theta** increases, which helps short premium.
- As expiration approaches, **gamma** also increases, which makes short options less forgiving.
- Short-expiry equity options can price in jump/event risk, especially around scheduled announcements.
- Common short-premium research and practitioner heuristics often center around **~45 DTE** as a strong baseline. Since this scanner is capped below that, the closest "core" replacement is the **22-35 DTE** bucket.

### Current Resolution

The scanner should support **1-35 DTE**, but should treat that as **multiple regimes**, not one bucket.

Proposed DTE profiles:

- `micro`: `1-3 DTE`
- `weekly`: `4-10 DTE`
- `swing`: `11-21 DTE`
- `core`: `22-35 DTE`

### Default Product Decision

- Default production profile should be **`core`**
- Broader default scan can be **`11-35 DTE`**
- `1-35 DTE` should be supported, but **not** used as one undifferentiated default scan

## Topic: DTE-Specific Delta Targets

### Problem

The current scanner tends to over-rank high-premium trades near the top of the allowed delta range.

From the latest SPY scan:

- top 10 average short delta was about `0.296`
- top 10 average short strike was about `2.44%` OTM

That is still too close to "maximize premium" instead of "best trade."

### Current Resolution

Use different short-call delta targets by DTE profile:

- `micro` (`1-3 DTE`): `0.05-0.12`
- `weekly` (`4-10 DTE`): `0.08-0.16`
- `swing` (`11-21 DTE`): `0.12-0.20`
- `core` (`22-35 DTE`): `0.15-0.22`

## Topic: Ranking Logic

### Problem

A single global ranking across all DTEs biases toward:

- higher delta shorts
- narrower widths
- higher raw return on risk

That can push riskier near-the-money structures above more practical spreads.

### Current Resolution

- Rank candidates **within each DTE profile**
- Only compare candidates across profiles after a profile-aware adjustment
- Do **not** sort primarily by raw return on risk

Preferred ranking inputs:

- short delta distance from target range
- OTM distance / breakeven cushion
- natural-to-mid fill quality
- open interest / quote quality
- width appropriateness
- DTE profile

## Topic: Event Risk Handling

### Problem

Short-dated options can reflect scheduled event risk that makes normal premium-selling logic unreliable.

This matters more for:

- earnings
- ex-dividend dates
- macro events

### Current Resolution

- Event-aware filtering should be part of the scanner design
- Single-stock scanners need stricter event handling than ETF/index scanners
- `micro` mode should initially focus on **very liquid ETFs / index proxies** like `SPY`, `QQQ`, and `IWM`

## Topic: Current Scanner Behavior

### Latest Observations

Source file:

- [spy_20260406_171544.csv](/Users/adeb/Projects/spreads/outputs/call_credit_spreads/spy_20260406_171544.csv)

Summary from that run:

- total candidates: `275`
- top 10 average short delta: about `0.296`
- top 10 average OTM distance: about `2.44%`
- top 10 were skewed toward `1-point` and `2-point` wide spreads

Conclusion:

- candidate generation is working
- liquidity is reasonable
- ranking is still too premium-seeking

## Topic: Profile Config Presets

### Current Resolution

Profiles should be treated as **presets**, not just DTE ranges.

Each profile should define:

- DTE window
- short delta range
- preferred delta target
- width constraints
- minimum credit
- liquidity thresholds
- max relative bid/ask spread
- event handling rules
- ranking emphasis

### Proposed Presets

`micro`

- use case: intraday / very short premium
- DTE: `1-3`
- short delta range: `0.05-0.12`
- preferred delta target: `0.08`
- width: `1-2`
- minimum credit: `0.10`
- minimum open interest: `1500`
- max relative spread: `0.10`
- initial scope: ETF / index proxies only
- notes: no overnight by default

`weekly`

- use case: short-duration premium
- DTE: `4-10`
- short delta range: `0.08-0.16`
- preferred delta target: `0.12`
- width: `1-3` for ETFs, `1-5` for large caps
- minimum credit: `0.18`
- minimum open interest: `750` for ETFs, `400` for stocks
- max relative spread: `0.12` for ETFs, `0.15` for stocks
- notes: no earnings before expiry

`swing`

- use case: balanced short premium
- DTE: `11-21`
- short delta range: `0.12-0.20`
- preferred delta target: `0.16`
- width: `1-5` for ETFs, `2-10` for stocks
- minimum credit: `0.25`
- minimum open interest: `500` for ETFs, `250` for stocks
- max relative spread: `0.18`
- notes: broad active-scanning profile

`core`

- use case: default production mode
- DTE: `22-35`
- short delta range: `0.15-0.22`
- preferred delta target: `0.18`
- width: `2-10`
- minimum credit: `0.35`
- minimum open interest: `300` for ETFs, `200` for stocks
- max relative spread: `0.20`
- notes: closest fit to classic short-premium workflow inside the `<= 35 DTE` cap

`auto`

- use case: symbol-aware preset selection
- behavior: map underlying type, event risk, liquidity, and requested holding style into one of the concrete profiles

## Topic: Profile Selection Rules

### Current Resolution

Use these high-level rules:

- if no profile is given, default to `core`
- if user asks for very short-term or same-week ideas, use `weekly`
- if requested DTE is `<= 3`, use `micro`
- if symbol is illiquid, reject
- if symbol has earnings before expiry, reject or downgrade for single-name short premium

## Next Improvements To Implement

- add DTE profiles to the scanner CLI
- add profile-specific delta defaults
- add profile-aware ranking
- add breakeven cushion scoring
- add expected-move filtering
- de-duplicate near-identical structures

## Sources

- [OIC: Theta](https://www.optionseducation.org/advancedconcepts/theta)
- [OIC: Volatility and The Greeks](https://www.optionseducation.org/advancedconcepts/volatility-the-greeks)
- [Review of Finance paper on short-expiry event risk](https://academic.oup.com/rof/article/29/4/963/8079062)
- [tastylive short premium research / heuristics](https://www.tastylive.com/shows/from-theory-to-practice/episodes/the-short-premium-trifecta-05-06-2022)
- [tastylive on short-term option selling risks](https://www.tastylive.com/news-insights/primary-risks-selling-short-term-options)
