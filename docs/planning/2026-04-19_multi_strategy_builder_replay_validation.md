# Multi-Strategy Builder Replay Validation

Status: active

As of: Sunday, April 19, 2026

Related:

- [System Architecture](../current_system_state.md)
- [Backtest System Recommendation](./2026-04-16_backtest_system_recommendation.md)
- [Current-System Options Automation Implementation Approach](./2026-04-15_current_system_options_automation_implementation_approach.md)
- [Options Automation TODO](./2026-04-16_options_automation_todo.md)
- [Alpaca Capabilities Statement](../research/alpaca_capabilities_statement.md)

## Goal

Define a reusable plan to confirm strategy builder behavior against historical data without creating a second historical-evaluation subsystem beside `packages/core/backtest/`.

The proposal should answer a broader question:

- can we prove that the current builder would have produced the same candidates and rankings for a historical scan
- can we separate exact confirmation from approximate replay
- can we do that with the current Alpaca capability surface plus repo-recorded data
- can one framework support multiple shipped strategy families without duplicating the backtest boundary

This document makes `call_credit_spread` the first implementation target, but not the final scope.

## Recommendation Summary

Yes, but only with explicit fidelity tiers.

- exact builder confirmation requires repo-owned scan-time inputs or new scan-time builder artifacts
- Alpaca historical data alone is sufficient for spread-path and exit replay, but not for exact builder confirmation
- the implementation should live under `packages/core/backtest/` as a replay mode, not as a new historical-evaluation subsystem
- the storage and replay interfaces should be strategy-agnostic from day one
- `call_credit_spread` should be phase 1, followed by other vertical families, then condors, then long-vol families

Current implementation status as of April 19, 2026:

- shipped exact replay for stored scan artifacts via `uv run spreads backtest replay`
- shipped stored-history range replay via `uv run spreads backtest replay-range --source stored`
- shipped Alpaca-backed reduced-fidelity range replay via `uv run spreads backtest replay-range --source alpaca`
- current Alpaca mode samples one market-close cycle per trading day, reconstructs quotes synthetically from option bars, and recomputes Greeks locally

## Strategy Applicability

The framework should be designed for all builder-backed strategy families, but not every family is equally ready on day one.

### Phase 1

- `call_credit_spread`

Why first:

- it is already an active rollout target
- it uses the shared vertical builder path
- its entry and management recipes are already defined in config
- its acceptance criteria are easy to state in operator terms

### Phase 2

- `put_credit_spread`
- `call_debit_spread`
- `put_debit_spread`

Why next:

- these families share the same canonical vertical builder path in `services/scanners/builders/verticals.py`
- they can reuse the same artifact schema, replay shell, and most of the diff engine
- the main additions are family-specific gate labels and economics interpretation

### Phase 3

- `iron_condor`

Why later:

- it already has a canonical builder path
- it still fits the same artifact-and-diff framework
- exact replay is harder because it requires synchronized four-leg quote state, condor-specific metrics, and condor-specific trace reporting

### Phase 4

- `long_straddle`
- `long_strangle`

Why later:

- the shared framework still applies
- these families need different economics expectations, fill interpretation, and lifecycle evaluation language than short-premium verticals
- the report layer should avoid short-premium assumptions when diffing long-vol candidates

### Blocked Or Deferred

- naked short calls or puts
- butterflies
- any strategy family whose live runtime path, exposure math, or canonical builder remains incomplete

These families should not be added to replay until they have one canonical runtime-owned builder and one canonical opportunity path.

## Current Baseline

The relevant current code path is already clean enough to anchor the proposal:

- runtime build settings are loaded from checked-in config in [packages/core/services/automation_runtime.py](../../packages/core/services/automation_runtime.py)
- runtime scan args are derived from those settings in [packages/core/services/strategy_builders.py](../../packages/core/services/strategy_builders.py)
- live market-slice assembly happens in [packages/core/services/scanners/runtime.py](../../packages/core/services/scanners/runtime.py)
- vertical candidate generation happens in [packages/core/services/scanners/builders/verticals.py](../../packages/core/services/scanners/builders/verticals.py)
- condor candidate generation happens in [packages/core/services/scanners/builders/iron_condors.py](../../packages/core/services/scanners/builders/iron_condors.py)
- long-vol candidate generation happens in [packages/core/services/scanners/builders/long_vol.py](../../packages/core/services/scanners/builders/long_vol.py)
- runtime opportunity filtering and entry-recipe gates happen in [packages/core/services/opportunity_generation.py](../../packages/core/services/opportunity_generation.py)
- canonical historical evaluation already lives in [packages/core/backtest/](../../packages/core/backtest/)

That matters because the right implementation is not to retype builder logic inside a new historical module. The right implementation is to replay the existing builder path against historical inputs, with one family adapter layer where metrics or trace labels differ.

## Current Constraint From Alpaca

The current documented Alpaca options surface is good, but not complete enough for exact builder truth on its own.

From [Alpaca Capabilities Statement](../research/alpaca_capabilities_statement.md) and Alpaca's official docs:

- Alpaca documents historical options data only since February 1, 2024
- Alpaca documents historical option bars and historical option trades
- Alpaca documents current snapshots with latest trade, latest quote, Greeks, IV, and bar fields
- Alpaca documents real-time option quotes and trades over WebSocket
- Alpaca does not currently document a historical option quotes REST endpoint for options

Implication:

- we can replay spread marks and lifecycle behavior from historical bars and trades
- we cannot prove exact scan-time bid, ask, midpoint, spread width, quote freshness, or snapshot delta values from Alpaca history alone

## Why Builder Confirmation Is Different From Existing Backtest

The existing backtest path already uses fidelity tiers for post-entry lifecycle evaluation.

That is visible in:

- [packages/core/backtest/service.py](../../packages/core/backtest/service.py), which prefers repo-recorded quote or trade windows and falls back to Alpaca history
- [packages/core/backtest/market_data.py](../../packages/core/backtest/market_data.py), which reconstructs structure marks from bars and trades

But builder confirmation is stricter than post-entry replay because builders filter on scan-time state such as:

- DTE window
- short-leg delta band
- width
- leg liquidity and relative spread
- midpoint credit and natural credit
- minimum return on risk
- expected-move inputs
- setup state and entry recipes

Those are live selection facts, not just later outcome facts.

Some families add more:

- condors need side-balance and wing-shape facts
- long-vol families need pair symmetry and premium-shape facts
- future families may need undefined-risk or multi-wing facts

That is why the framework should be generic, but family-aware.

## Important Current Gaps

The current repo can persist scan outputs, but not enough scan inputs to guarantee exact historical regeneration.

Current storage:

- `scan_runs` stores run metadata and filters
- `scan_candidates` stores flattened selected candidate fields
- `option_quote_events` stores recorded quote windows
- `option_trade_events` stores recorded trade windows

Current gaps:

- we do not persist the full chain snapshot or compact builder trace used to create a historical candidate set
- we do not currently have a first-class scan artifact reference model for replay
- we do not yet have a strategy-agnostic builder trace schema that can explain why a leg, pair, or structure was excluded

There is also one code-level issue that should be treated as part of this proposal:

- `max_quote_age_seconds` is loaded into `StrategyBuildSettings` in [packages/core/services/automation_runtime.py](../../packages/core/services/automation_runtime.py), but the scanner builder path does not currently appear to enforce that field during candidate generation

That means we should not claim builder confirmation for quote freshness until either:

- the rule is implemented in the live builder path, or
- the proposal explicitly marks that field as not yet enforceable

## Proposed Design

Implement a replay mode inside `packages/core/backtest/` with three fidelity tiers.

### Fidelity Tiers

#### High Fidelity

Inputs:

- repo-recorded scan-time builder artifact
- repo-recorded option quote windows
- repo-recorded option trade windows when available

What it can confirm:

- exact candidate existence
- exact pass or fail on builder gates
- exact candidate metrics from stored scan inputs
- exact ranking drift caused by code changes
- exact family-specific metric drift when the family adapter is implemented

Intended meaning:

- this is the only tier that can support the phrase "confirmed builder logic"

#### Medium Fidelity

Inputs:

- existing `scan_runs` and `scan_candidates`
- Alpaca historical option bars
- Alpaca historical option trades
- local Greek recomputation where needed

What it can confirm:

- approximate candidate region and structure economics
- rough selection drift and structural plausibility
- spread-path and lifecycle reasonableness after entry
- family-level plausibility when exact snapshot state is unavailable

What it cannot confirm:

- exact quote-spread gates
- exact quote freshness
- exact Alpaca snapshot deltas at scan time

Intended meaning:

- use for historical diagnosis when no high-fidelity artifact exists
- never label this as exact confirmation

#### Reduced Fidelity

Inputs:

- existing stored candidates only

What it can confirm:

- stored candidate identity and downstream outcome comparisons

What it cannot confirm:

- regenerated builder truth

Intended meaning:

- fallback reporting only

## Canonical Boundary

This work should stay inside the current historical-evaluation boundary:

- current-system ownership says `backtest/` owns canonical historical evaluation
- scanner services may provide adapters, but they should not become a second historical-evaluation owner

So the proposal is:

- add replay entrypoints under `packages/core/backtest/`
- keep the live builder logic in `services/scanners/`
- call that live builder logic from the backtest layer using historical inputs

Do not:

- add a new `services/replay/` package
- reintroduce `opportunity_replay.py`-style ownership
- duplicate the candidate builder in a separate historical implementation

## Builder Artifact Proposal

To support high-fidelity replay, persist one compact external artifact per scan run and reference it from Postgres.

Preferred storage shape:

- new `scan_run_artifacts` table
- artifact payload stored outside row bodies
- artifact rows linked to `scan_runs.run_id`

Reasoning:

- `scan_runs.setup_json` is too small and too vague for full builder provenance
- one nullable artifact column on `scan_runs` would work for the smallest patch, but it will not scale if we later want multiple artifact types per run
- a separate artifact table matches the existing planning rule that large replay artifacts should live outside table bodies with Postgres references

### Strategy-Agnostic Artifact Rules

The artifact model should be generic enough for every builder-backed family.

Required shape:

- normalized `strategy_family`
- normalized `legs[]` modeling where applicable
- family-specific metrics stored in a flexible payload rather than as hard-coded columns
- generic builder-trace events that can describe:
  - short-leg rejection
  - long-leg rejection
  - pair rejection
  - structure rejection
  - ranking contribution
  - dedup or tie-break decisions

The artifact should not be shaped around `call_credit_spread` field names only.

### Required Artifact Contents

Each high-fidelity builder artifact should store the minimum input set needed to replay the builder deterministically:

- resolved `config_hash`
- resolved scanner args after runtime overrides
- underlying spot price
- underlying setup inputs or a setup snapshot result
- option contract metadata used by the scan
- option snapshot fields used by the builder for every contract in scope:
  - bid
  - ask
  - midpoint
  - bid size
  - ask size
  - quote timestamp if available
  - delta
  - gamma
  - theta
  - vega
  - implied volatility
  - Greek source
- expected-move inputs and chosen ATM reference strike
- normalized candidate payloads produced at each major stage:
  - raw builder output
  - post-calendar output
  - post-data-quality output
  - ranked output
  - deduplicated output
- normalized `legs[]` payload for every stored candidate
- family-specific metrics payload for each candidate
- builder trace rows:
  - why a contract was skipped as a short leg
  - why a pair was skipped as a spread candidate
  - why a multi-leg structure was skipped
  - ranking component values
  - dedup decisions

This should be compact JSON or msgpack, optionally compressed.

## Family Adapters

The replay shell should be shared, but candidate comparison needs one adapter per family or family group.

### Vertical Adapter

Applies to:

- `call_credit_spread`
- `put_credit_spread`
- `call_debit_spread`
- `put_debit_spread`

Shared diff points:

- short leg identity
- long leg identity
- width
- midpoint and natural economics
- delta band pass or fail
- expected-move cushion

### Condor Adapter

Applies to:

- `iron_condor`

Additional diff points:

- short-call and short-put identity
- long-wing identity
- side balance
- wing symmetry
- condor-specific ranking and dedup reasons

### Long-Vol Adapter

Applies to:

- `long_straddle`
- `long_strangle`

Additional diff points:

- call and put pairing
- symmetry or skew metrics
- premium level and fill assumptions
- long-premium ranking and exclusion reasons

### Future Adapter Rule

Every new family should implement:

- a candidate identity comparer
- a family-specific economics comparer
- a family-specific trace label formatter

before it is marked supported in replay.

## Proposed CLI Shape

Add a replay surface under the canonical backtest CLI:

```text
uv run spreads backtest replay --run-id <scan-run-id>
uv run spreads backtest replay --symbol SPY --strategy call_credit --latest
uv run spreads backtest replay --bot-id short_dated_index_credit_bot --automation-id index_call_credit_entry --session-date 2026-04-16
uv run spreads backtest replay --family iron_condor --run-id <scan-run-id>
```

### Output Shape

Default text summary:

- target run
- strategy family
- fidelity tier
- exact, approximate, or unsupported status
- candidate-count diff
- top-ranked identity diff
- field drift summary
- blocker or gate drift summary

JSON export:

- stored candidate set
- replayed candidate set
- identity matches
- mismatches with reason codes
- per-field drift for top candidates
- fidelity explanation

Artifact output root:

- `outputs/backtests/replay/...`

## Replay Algorithm

### Phase 1: Exact Replay When Artifact Exists

1. Resolve the historical scan run.
2. Load the stored builder artifact.
3. Reconstruct a historical `SymbolMarketSlice` from the artifact rather than from current Alpaca snapshots.
4. Call the existing builder path.
5. Diff replayed candidates against stored candidates.
6. Report exact or changed status with explicit reason codes.

### Phase 2: Approximate Replay Without Artifact

1. Resolve the historical scan run.
2. Use stored scan metadata plus stored candidate rows to determine target universe and DTE window.
3. Pull Alpaca historical option bars and trades for the involved contracts or the relevant chain region.
4. Recompute Greeks locally where possible.
5. Reconstruct an approximate market slice.
6. Run the same builder path in medium-fidelity mode.
7. Report approximate drift with clear warnings.

### Phase 3: Optional Selection And Lifecycle Join

After exact or approximate replay exists, join it with:

- runtime opportunity filtering
- entry recipe results
- automation selection
- post-entry lifecycle backtest

That lets one report answer:

- would the builder have built it
- would the runtime have selected it
- how would it likely have behaved after entry

## What The Replay Must Diff

The report should compare three separate layers.

### Layer 1: Builder Output

- candidate identity by legs
- candidate rank
- candidate count
- top-N changes

### Layer 2: Candidate Economics

- midpoint credit
- natural credit
- return on risk
- max loss
- breakeven
- expected-move cushion
- family-specific metrics payload

### Layer 3: Gate Decisions

- delta band pass or fail
- width pass or fail
- open-interest pass or fail
- relative-spread pass or fail
- quote-age pass or fail when implemented
- setup and entry-recipe pass or fail
- family-specific exclusion or ranking reasons

This separation matters because one changed number should not be reported as a completely unexplained candidate mismatch.

## Strategy Support Matrix

| Strategy family | Shared framework | Family adapter needed | Exact replay possible | Medium replay useful | Initial phase |
|---|---|---|---|---|---|
| `call_credit_spread` | yes | vertical | yes | yes | 1 |
| `put_credit_spread` | yes | vertical | yes | yes | 2 |
| `call_debit_spread` | yes | vertical | yes | yes | 2 |
| `put_debit_spread` | yes | vertical | yes | yes | 2 |
| `iron_condor` | yes | condor | yes, harder | yes | 3 |
| `long_straddle` | yes | long-vol | yes | yes, weaker | 4 |
| `long_strangle` | yes | long-vol | yes | yes, weaker | 4 |
| butterfly families | not yet | future | not yet | not yet | blocked |
| naked short families | not yet | future | not yet | not yet | blocked |

## Acceptance Criteria

The shared framework should be considered successfully established when all of the following are true:

- a new `replay` CLI exists under `uv run spreads backtest`
- high-fidelity replay runs reuse the existing builder path instead of a duplicate implementation
- artifacts and diff payloads are strategy-agnostic
- family adapters can plug into one shared replay shell
- medium-fidelity replay is clearly labeled approximate
- unsupported sessions are explicitly labeled unsupported rather than silently treated as failures

The framework should be considered successfully implemented for a specific family when all of the following are true:

- the family has a registered adapter
- high-fidelity replay can label a run as exact or changed and show why
- family-specific metric drift is surfaced in the report
- quote-freshness confirmation is either implemented in the live builder path or clearly marked unsupported for that family

## Rollout Plan

### Step 1

- implement quote-age enforcement in the live builder path if we want it included in confirmation

### Step 2

- add `scan_run_artifacts` storage and persist high-fidelity builder artifacts from the existing runtime scan path
- make the artifact schema strategy-agnostic rather than call-credit-specific

### Step 3

- add `backtest replay` for exact replay from stored artifacts
- ship the first family adapter for `call_credit_spread`

### Step 4

- add medium-fidelity Alpaca fallback for artifact-less historical sessions after February 1, 2024
- add the shared vertical adapter path for the remaining vertical families

### Step 5

- add the condor adapter and condor-specific diff reporting

### Step 6

- add the long-vol adapter and long-vol-specific diff reporting

### Step 7

- wire replay outputs into the per-family rollout checklist and tuning workflow
- treat unsupported families as blocked until their runtime path is canonical

## Explicit Non-Goals

This proposal does not try to:

- replace the existing automation-run backtest
- prove full paper trading lifecycle correctness by itself
- reconstruct perfect scan-time quotes for old sessions where we never recorded them
- mark every existing strategy family as supported on day one

## Open Questions

- should high-fidelity builder artifacts be written for every scan run or only runtime-owned automation runs
- should the first artifact capture the full chain in scope or a compacted in-band chain plus exclusion trace
- do we want replay to diff against `scan_candidates` only, or also against later runtime-owned `opportunities`
- should family adapters register through strategy definitions, a replay registry, or the backtest package itself

## Decision

Proceed with a multi-strategy replay framework inside `packages/core/backtest/`, with explicit fidelity labels, a new scan artifact reference model, and one shared replay shell plus family adapters.

Use `call_credit_spread` as phase 1, but keep the storage model, CLI shape, and diff engine generic enough for the other builder-backed strategy families.

Do not rely on Alpaca historical options data alone for exact builder confirmation.
