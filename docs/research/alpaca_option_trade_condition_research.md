# Alpaca Option Trade Condition Research

Verified on April 9, 2026 using Alpaca's option trade-condition metadata plus recent OPRA option trades from the last completed NYSE session on April 8, 2026.

Purpose: decide which Alpaca option trade conditions should count toward UOA scoring.

## Method

- decoded condition codes from `GET /v1beta1/options/meta/conditions/trade`
- reviewed liquid `0-7 DTE` contracts across `SPY`, `QQQ`, `IWM`, `NVDA`, and `TSLA`
- checked both a broader contract cross-section and a more session-distributed sample so the result was not just open-driven

## What Showed Up

The dominant single-leg conditions on liquid contracts were:

- `I` = `AUTO`
- `a` = `SLAN`
- `S` = `ISOI`
- `b` = `SLAI` was valid but rare

The condition families that repeatedly showed up but should stay out of v1 UOA scoring were:

- `f` = `MLET`
- `g` = `MLAT`
- `j` = `MESL`
- `i` = `MLFT`
- `n` = `TLET`

These excluded families were not just theoretical. They appeared in liquid contracts and sometimes carried large premium.

## Sample Readout

- In a more session-distributed liquid sample across `SPY` and `QQQ`, the current allowlist covered about `86.8%` of trades and about `87.1%` of premium.
- In a broader contract cross-section across `SPY`, `QQQ`, `IWM`, `NVDA`, and `TSLA`, the same allowlist still covered most trades by count, but premium share dropped materially because excluded multi-leg and floor conditions contributed some very large prints.
- Practical takeaway: count share and premium share both matter. Premium share alone can overreact to complex-flow outliers.

## Key Findings

- The current strict trade-level allowlist is directionally correct.
- On liquid contracts, most prints by count were still `I`, `a`, or `S`.
- Excluded multi-leg and stock-option conditions can carry large notional. That is a reason to exclude them from UOA scoring, not a reason to include them.
- Case sensitivity matters. `J` and `j` are different codes.
- `J` is the documented reopening-after-halt condition.
- `j` is a multi-leg autoelectronic trade against single-legs.
- Do not lowercase or normalize condition codes.
- A contract's latest snapshot trade condition is not a contract-level ban signal.
- Active contracts can still be worth monitoring even if the most recent print on the snapshot is an excluded condition.
- Filter at the trade-event level, not by dropping the entire contract.

## Policy Decision

Keep the v1 anomaly-scoring allowlist as:

- `I`
- `J`
- `S`
- `a`
- `b`

Keep these rules:

- every condition flag on a trade must be allowlisted or the trade is excluded from anomaly scoring
- excluded trades can be stored raw for audit and future research
- excluded trades must not affect premium, volume, trade count, aggressiveness, or alert confidence

## Why This Policy Stands

- It matches the dominant single-leg flow actually observed on liquid contracts.
- It avoids misclassifying complex or multi-leg prints as single-contract unusual activity.
- It keeps rare, high-notional complex prints from distorting alert scores.

## Practical Notes For Implementation

- treat condition codes as case-sensitive values
- decode conditions before scoring, not after aggregation
- do not reject a contract just because `latestTrade.c` on a snapshot is excluded
- if a later product wants to study complex flow, store excluded trades separately instead of mixing them into v1 UOA signals

## Sources

- [Alpaca trade condition metadata](https://docs.alpaca.markets/reference/optionsmetaconditionstrade)
- [Alpaca historical option trades](https://docs.alpaca.markets/reference/optiontrades)
- [Alpaca real-time option data](https://docs.alpaca.markets/docs/real-time-option-data)
