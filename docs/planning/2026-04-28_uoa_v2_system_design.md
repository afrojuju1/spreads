# UOA V2 System Design

Status: implementation-ready design

As of: Tuesday, April 28, 2026

Related:

- [System Architecture](../current_system_state.md)
- [UOA Dedicated Pipeline Design](./2026-04-28_uoa_dedicated_pipeline_design.md)
- [Alpaca-Only Unusual Activity Scanner Design](./unusual_activity_scanner_design.md)
- [Alpaca Capabilities Statement](../research/alpaca_capabilities_statement.md)

## Role Of This Doc

This document is the implementation contract for **UOA v2 Phase 1**.

Use it for:

- the Phase 1 ownership boundary
- the Phase 1 payload contract
- the deterministic scoring model
- the scanner state model
- the module-by-module implementation order

It reuses the runtime topology from [Alpaca-Only Unusual Activity Scanner Design](./unusual_activity_scanner_design.md), but it supersedes that note for:

- the v2 scoring model
- the v2 state taxonomy
- the v2 payload fields
- the v2 compatibility and rollout plan

Use [System Architecture](../current_system_state.md) for current repo ownership and top-level runtime boundaries.

## Goal

Implement a **strategy-agnostic, bounded Alpaca-only UOA scanner** that produces:

- generic root-level scanner states
- generic root descriptors
- ranked supporting contracts
- operator-facing board and alert payloads

UOA v2 is **not** a strategy model. Downstream strategy services may consume scanner evidence, but UOA does not decide whether flow supports any specific structure.

## Non-Goals

Phase 1 does not attempt to provide:

- exchange-truth buy/sell side
- exchange-truth open/close classification
- participant-origin segmentation
- complex-trade or package linkage
- full options order book or depth
- whole-market reactive options monitoring
- strategy-fit labels such as "good for long straddle"
- a new scheduler, worker class, or parallel UOA pipeline

## Locked Phase 1 Decisions

These decisions are locked for implementation.

- UOA remains a **bounded** scanner. It uses a stock-first or prefiltered monitor set instead of whole-market options coverage.
- Phase 1 reuses the existing discovery-run and live-session capture path. No new worker or pipeline is introduced.
- `services/market_recorder.py` remains the sole Alpaca option websocket owner.
- Phase 1 keeps the existing three top-level UOA payloads:
  - `uoa_summary`
  - `uoa_quote_summary`
  - `uoa_decisions`
- Phase 1 does **not** add new first-class database tables for UOA.
- Phase 1 does **not** require a schema migration for `option_trade_events`.
- Aggressor side and signed notionals are derived at summary time, not persisted as source-of-truth trade columns.
- UOA scanner states use scanner-native names:
  - `none`
  - `emerging`
  - `notable`
  - `high`
  - `critical`
- Legacy UOA aliases remain readable during rollout:
  - `watchlist -> emerging`
  - `board -> notable`
  - `monitor -> emerging`
  - `promotable -> notable`
- Downstream strategy services remain downstream consumers. Phase 1 does not require them to change before UOA v2 ships.

## Current Runtime Shape

UOA v2 keeps the current runtime shape and upgrades the evidence and scoring model inside it.

Current path:

1. a bounded shortlist or monitor set is chosen for capture
2. recorder-backed option quotes and trades are captured for target contracts
3. trades are normalized and enriched
4. quote quality and surface state are summarized
5. root and contract scanner scores are produced
6. scanner states and alerts are projected from generic evidence
7. downstream consumers may read scanner evidence, but UOA does not own strategy interpretation

### System Diagram

```text
                               ALPACA
   +--------------------------------------------------------------+
   | stock data | option trades | option quotes | chain snapshots |
   +-------------------------------+------------------------------+
                                   |
                                   v
                    +--------------+---------------+
                    | bounded shortlist / monitor  |
                    | stock-first or prefiltered   |
                    +--------------+---------------+
                                   |
                                   v
                    +--------------+---------------+
                    | recorder-backed live capture |
                    | quotes + trades for targets  |
                    +--------------+---------------+
                                   |
                 +-----------------+------------------+
                 |                                    |
                 v                                    v
      +----------+-----------+             +----------+-----------+
      | trade enrichment     |             | quote + surface      |
      | aggressor inference  |             | liquidity + freshness|
      | signed notionals     |             | IV state             |
      +----------+-----------+             +----------+-----------+
                 \                                    /
                  \                                  /
                   v                                v
                    +--------------+---------------+
                    | root + contract scoring      |
                    | anomaly / directional /      |
                    | vol-demand / quote context   |
                    +--------------+---------------+
                                   |
                 +-----------------+------------------+
                 |                                    |
                 v                                    v
      +----------+-----------+             +----------+-----------+
      | scanner states       |             | supporting contracts |
      | none/emerging/       |             | ranked explanations  |
      | notable/high/critical|             +----------------------+
      +----------+-----------+
                 |
                 v
      +----------+-----------------------------------------------+
      | operator surfaces + alerts                               |
      | CLI / API / board / Discord                              |
      +----------+-----------------------------------------------+
                 |
                 v
      +----------+-----------------------------------------------+
      | downstream consumers outside UOA ownership               |
      | strategy evidence / policy / execution decisions         |
      +----------------------------------------------------------+
```

## Phase 1 Inputs And Outputs

### Inputs

Phase 1 uses only currently-available bounded-capture inputs:

- `capture_candidates`
- `contract_metadata_by_symbol`
- `stream_trade_records`
- `reactive_quote_records`
- existing trade baselines:
  - `rolling_5m`
  - `session_to_time`
  - `previous_session_same_time`

Phase 1 does not depend on:

- historical option quote backfill from Alpaca
- external options flow vendors
- full-chain whole-market options coverage

### Outputs

Phase 1 keeps the existing three UOA payload families and extends them in-place.

| Payload | Owner | Phase 1 role |
|---|---|---|
| `uoa_summary` | `packages/core/services/uoa_trade_summary.py` | trade-derived root and contract flow summary |
| `uoa_quote_summary` | `packages/core/services/uoa_quote_summary.py` | quote quality and surface-state summary |
| `uoa_decisions` | `packages/core/services/uoa_root_decisions.py` | scanner scores, states, descriptors, and supporting contracts |

No new top-level UOA payload is added in Phase 1.

## Alpaca Feasibility Check

As of April 28, 2026, Phase 1 is feasible inside the current Alpaca-based architecture.

Directly supported:

- live option trades
- live option quotes
- latest option quotes
- historical option trades
- historical option bars
- option chain snapshots with latest trade, latest quote, and Greeks
- contract metadata including `open_interest` and `open_interest_date`
- stock bars, trades, quotes, snapshots, auctions, and order imbalances

Supported by inference only:

- aggressor side
- signed delta notional
- signed vega notional
- signed gamma dollar exposure
- front-expiry ATM IV, term slope, and implied-move metrics

Not supported by Alpaca alone:

- buy/sell truth
- open/close truth
- participant origin
- complex-trade/package identifiers
- full options depth

Phase 1 accepts those limitations and does not block on them.

## Phase 1 Payload Contract

Phase 1 should be implemented without adding new storage tables. The contract is to enrich the existing payloads, not to create a parallel UOA persistence path.

### `uoa_summary`

Purpose:

- own trade-derived root and contract summaries
- own signed flow descriptors
- own volatility-demand aggregation inputs

Add to `uoa_summary.overview`:

- `aggressor_known_trade_count`
- `aggressor_unknown_trade_count`
- `aggressor_known_ratio`
- `signed_premium_total`
- `signed_delta_notional_total`
- `signed_vega_notional_total`
- `signed_gamma_dollar_total`
- `emerging_candidate_root_count`
- `front_expiry_root_count`

Add to each contract summary:

- `aggressor_known_trade_count`
- `buy_initiated_trade_count`
- `sell_initiated_trade_count`
- `unknown_initiated_trade_count`
- `signed_trade_count`
- `signed_size`
- `signed_premium`
- `signed_delta_notional`
- `signed_vega_notional`
- `signed_gamma_dollar_exposure`
- `gross_delta_notional`
- `gross_vega_notional`
- `gross_gamma_dollar_exposure`
- `aggressor_known_ratio`
- `atm_distance_pct`
- `atm_relevance_score`
- `expiry_bucket`
- `open_interest_date`
- `open_interest_age_days`
- `support_score_inputs`

Add to each root summary:

- `buy_initiated_trade_count`
- `sell_initiated_trade_count`
- `unknown_initiated_trade_count`
- `signed_trade_count`
- `signed_size`
- `signed_premium`
- `signed_delta_notional`
- `signed_vega_notional`
- `signed_gamma_dollar_exposure`
- `gross_delta_notional`
- `gross_vega_notional`
- `gross_gamma_dollar_exposure`
- `aggressor_known_ratio`
- `call_put_balance_score`
- `atm_concentration_score`
- `same_expiry_symmetry_score`
- `front_expiry_concentration_score`
- `positive_vega_share`
- `open_interest_freshness_score`
- `front_expiry`
- `top_expiry`
- `top_expiry_premium_share`

Keep the existing fields:

- `dominant_flow`
- `dominant_flow_ratio`
- `top_contracts`

Those remain important for downstream consumers and operator continuity.

### `uoa_quote_summary`

Purpose:

- own quote quality and liquidity context
- own surface-state metrics from bounded monitored contracts

Add to each contract summary:

- `gamma`
- `vega`
- `rho`
- `open_interest_date`
- `open_interest_age_days`
- `atm_distance_pct`
- `moneyness_bucket`
- `is_front_expiry`
- `is_next_expiry`

Add to each root summary:

- `surface_coverage_state`
- `front_expiry`
- `front_expiry_dte`
- `front_expiry_atm_call_symbol`
- `front_expiry_atm_put_symbol`
- `front_expiry_atm_iv`
- `next_expiry`
- `next_expiry_dte`
- `next_expiry_atm_iv`
- `front_next_term_slope`
- `front_atm_call_put_iv_gap`
- `front_expiry_implied_move_pct`
- `surface_score_inputs`

Optional Phase 1 descriptive fields:

- `wing_skew_metric`
- `butterfly_metric`

Those fields should be `null` when bounded coverage does not provide enough contracts. They are descriptive only and not required for Phase 1 state scoring.

### `uoa_decisions`

Purpose:

- own scanner-native states
- own continuous root scores
- own root descriptors
- own ranked supporting contracts for alerts and board rendering

Add to `uoa_decisions.overview`:

- `emerging_count`
- `notable_count`
- `high_count`
- `critical_count`
- `top_decision_shape`
- `top_decision_bias`

Add to each root decision:

- `root_interest_score`
- `directional_interest_score`
- `volatility_interest_score`
- `flow_anomaly_score`
- `directional_flow_score`
- `volatility_demand_score`
- `quote_context_score`
- `stock_context_score`
- `decision_state_pre_cap`
- `decision_state`
- `flow_shape`
- `directional_bias`
- `score_components`
- `driver_metrics`
- `top_supporting_contracts`

`top_supporting_contracts` should replace the implicit old preview behavior with explicit ranked support rows for operator surfaces and alerts.

## Phase 1 Enrichment And Scoring Contract

Phase 1 uses deterministic scoring only. No ML or fitted model is introduced.

### 1. Metadata Plumbing

Extend `build_option_symbol_metadata(...)` in [option_quote_records.py](../../packages/core/services/option_quote_records.py) to carry:

- `gamma`
- `vega`
- `rho`
- `open_interest_date`

Phase 1 does not require `theta` for UOA scoring.

### 2. Trade Enrichment

Create a new helper module:

- `packages/core/services/uoa_trade_enrichment.py`

This module should:

- take `stream_trade_records`
- take `reactive_quote_records`
- take `contract_metadata_by_symbol`
- return enriched trade rows for summary-time use

Phase 1 does **not** change `option_trade_events` storage schema. Raw trades remain raw. Enriched aggressor and signed-notional fields are derived in memory for `uoa_summary`.

#### Aggressor Inference Algorithm

For each trade:

1. find the latest same-symbol quote at or before `trade_timestamp` within `2s`
2. if not found, allow one fallback quote after the trade within `250ms`
3. if no usable quote exists, classify aggressor as `unknown`
4. if the quote is locked or crossed, classify aggressor as `unknown`
5. compute:
   - `spread = ask - bid`
   - `midpoint = (bid + ask) / 2`
   - `touch_epsilon = max(0.01, spread * 0.10)`
   - `midpoint_epsilon = max(0.005, spread * 0.15)`
6. classify:
   - `buy` if `trade_price >= ask - touch_epsilon`
   - `sell` if `trade_price <= bid + touch_epsilon`
   - `buy` if `trade_price > midpoint + midpoint_epsilon`
   - `sell` if `trade_price < midpoint - midpoint_epsilon`
   - otherwise `unknown`

Confidence labels:

- `high` for bid/ask-touch classification
- `medium` for midpoint-side classification
- `low` for fallback-after-quote classification
- `unknown` when no usable quote match exists

Required enriched trade fields:

- `aggressor_side`
- `aggressor_confidence`
- `quote_match_source`
- `quote_match_age_ms`
- `matched_bid`
- `matched_ask`
- `matched_midpoint`
- `matched_spread`

### 3. Signed Notional Formulas

For each enriched trade:

- `side_sign = +1` for `buy`, `-1` for `sell`, `0` for `unknown`
- `signed_contracts = side_sign`
- `signed_size = side_sign * size`
- `signed_premium = side_sign * premium`
- `signed_delta_notional = side_sign * delta * underlying_price * 100 * size`
- `signed_vega_notional = side_sign * vega * 100 * size`
- `signed_gamma_dollar_exposure = side_sign * gamma * underlying_price * underlying_price * 100 * size`

Use the signed option `delta` from metadata. Do not force call/put deltas to absolute values.

When a required input is missing:

- keep the field `null`
- do not substitute a fake zero
- do not drop the trade from the unsigned anomaly path

### 4. Quote And Surface Summary

`uoa_quote_summary` should continue to own quote quality and add bounded surface-state metrics.

Surface construction rules:

- only use contracts that pass the existing freshness and liquidity gates
- `front_expiry` is the nearest expiry with at least one usable call and one usable put
- `next_expiry` is the next later expiry meeting the same condition
- the ATM call and put for an expiry are the usable contracts with minimum `abs(percent_otm)`

Required root-level surface metrics:

- `front_expiry_atm_iv = mean(front_atm_call.iv, front_atm_put.iv)`
- `next_expiry_atm_iv = mean(next_atm_call.iv, next_atm_put.iv)` when available
- `front_next_term_slope = front_expiry_atm_iv - next_expiry_atm_iv`
- `front_atm_call_put_iv_gap = front_atm_call.iv - front_atm_put.iv`
- `front_expiry_implied_move_pct = front_expiry_atm_iv * sqrt(max(front_expiry_dte, 1) / 365.0)`

Optional descriptive metrics:

- wing-skew proxy
- butterfly proxy

Those remain optional because bounded monitored coverage may not provide enough wings every cycle.

### 5. Root Aggregation Features

Required root-level aggregation features:

- `call_put_balance_score`
  - `1 - abs(call_scoreable_premium - put_scoreable_premium) / max(scoreable_premium, 1)`
- `atm_concentration_score`
  - share of scoreable premium in the most ATM-relevant contracts
- `same_expiry_symmetry_score`
  - balance between call and put premium in the same front expiry
- `front_expiry_concentration_score`
  - front-expiry scoreable premium share of root total
- `positive_vega_share`
  - `max(signed_vega_notional, 0) / max(gross_vega_notional, 1)`
- `open_interest_freshness_score`
  - derived from `open_interest_age_days`

`open_interest_freshness_score` should be:

- `1.0` when median age is `0-1` days
- `0.5` when median age is `2` days
- `0.0` when median age is `>= 3` days or unavailable

### 6. Root Score Components

All root score components are normalized `0-100`.

#### Flow Anomaly Score

Purpose:

- answer "how abnormal is the activity"

Components:

- `35` points: premium-rate anomaly vs max of trade baselines, full at `5x`
- `25` points: trade-rate anomaly vs max of trade baselines, full at `4x`
- `15` points: scoreable contract breadth, full at `4` contracts
- `15` points: absolute scoreable premium using log scale, full at `$100k`
- `10` points: aggressor-known ratio

#### Directional Flow Score

Purpose:

- answer "how coherent is the directional expression"

Components:

- `35` points: `abs(signed_premium) / max(scoreable_premium, 1)`
- `35` points: `abs(signed_delta_notional) / max(gross_delta_notional, 1)`
- `15` points: premium-sign and delta-sign consistency
- `15` points: aggressor-known ratio

Directional bias:

- `bullish` when signed delta notional is materially positive
- `bearish` when signed delta notional is materially negative
- `mixed` otherwise

#### Volatility Demand Score

Purpose:

- answer "does the root look like long-vol demand rather than one-sided directional expression"

Components:

- `20` points: call-put balance score
- `20` points: ATM concentration score
- `15` points: same-expiry symmetry score
- `20` points: positive vega share
- `15` points: front-expiry concentration score
- `10` points: surface-state coverage and quality

#### Quote Context Score

Purpose:

- answer "is the current quoted market usable enough to trust the flow"

Components:

- `40` points: fresh contract coverage ratio
- `30` points: liquid contract coverage ratio
- `20` points: average contract quality score
- `10` points: open-interest freshness score

#### Stock Context Score

Purpose:

- normalize option activity against stock activity when stock context is available

Components when stock context exists:

- `40` points: option premium as share of stock dollar volume
- `40` points: absolute signed delta notional as share of stock dollar volume
- `20` points: option trade count as share of stock trade count

Fallback:

- if stock context is missing, set `stock_context_score = 50` and mark `stock_context_unavailable`
- this should be a neutral score, not a blocker

### 7. Interest Paths And Flow Shape

Phase 1 should produce two generic interest paths:

- `directional_interest_score`
- `volatility_interest_score`

Use weighted means across available components.

`directional_interest_score` weights:

- `flow_anomaly_score`: `45%`
- `directional_flow_score`: `35%`
- `stock_context_score`: `10%`
- `quote_context_score`: `10%`

`volatility_interest_score` weights:

- `flow_anomaly_score`: `40%`
- `volatility_demand_score`: `35%`
- `stock_context_score`: `10%`
- `quote_context_score`: `15%`

Then compute:

- `root_interest_score = max(directional_interest_score, volatility_interest_score)`

Set `flow_shape` as:

- `directional_bullish`
- `directional_bearish`
- `volatility_demand`
- `mixed`

Rules:

- if `directional_interest_score >= volatility_interest_score + 10` and bias is positive, use `directional_bullish`
- if `directional_interest_score >= volatility_interest_score + 10` and bias is negative, use `directional_bearish`
- if `volatility_interest_score >= directional_interest_score + 10`, use `volatility_demand`
- otherwise use `mixed`

### 8. Supporting Contract Ranking

Supporting contracts explain the root story. They do not predict which strategy structure should be used.

Base `support_score` components:

- `35` points: premium contribution share of root
- `20` points: signed size and trade participation
- `20` points: quote quality
- `15` points: ATM relevance
- `10` points: freshness and volume/OI support

Selection rules:

- for directional flow shapes, return top `3` contracts by `support_score`
- for `volatility_demand`, prefer:
  - up to `2` front-expiry ATM-near contracts on opposite option types
  - then the strongest remaining contributor

## Scanner State Model

### Canonical States

UOA scanner states are:

- `none`
- `emerging`
- `notable`
- `high`
- `critical`

Thresholds:

- `< 60`: `none`
- `60-74.9`: `emerging`
- `75-79.9`: `notable`
- `80-89.9`: `high`
- `>= 90`: `critical`

### Quote And Capture Caps

Apply these caps after the root score is computed:

- if `fresh_contract_count == 0`, cap to `emerging`
- if `liquid_contract_count == 0`, cap to `emerging`
- if `quote_context_score < 55`, cap to `emerging`
- if `quote_context_score < 70`, suppress `critical`
- if capture health is degraded, suppress `critical`

### Alert Policy

Phase 1 alert policy:

- update operator state for `emerging`, `notable`, `high`, and `critical`
- send Discord alerts only for `high`, `critical`, or valid escalation events
- cooldown is `15m` per root and alert type
- escalate when:
  - `root_interest_score` increases by at least `15`
  - `flow_shape` changes
  - front-expiry concentration or top supporting expiry changes materially

## Compatibility And Migration

### State Taxonomy Decoupling

Current code still couples UOA states to selection terms in [selection_terms.py](../../packages/core/services/selection_terms.py).

Phase 1 should break that coupling by creating:

- `packages/core/services/uoa_terms.py`

This module should own:

- canonical UOA scanner states
- legacy alias normalization
- UOA decision counting
- UOA decision ordering

`selection_terms.py` should no longer be the semantic owner of UOA scanner states.

### Backward-Compatible Payload Aliases

During rollout, expose both canonical and compatibility fields in `uoa_decisions.overview`:

- canonical:
  - `emerging_count`
  - `notable_count`
  - `high_count`
  - `critical_count`
- compatibility:
  - `watchlist_count = emerging_count`
  - `board_count = notable_count`
  - `monitor_count = emerging_count`
  - `promotable_count = notable_count`

Expose both list families during rollout:

- canonical:
  - `top_emerging_roots`
  - `top_notable_roots`
  - `top_high_roots`
  - `top_critical_roots`
- compatibility:
  - `top_watchlist_roots`
  - `top_board_roots`
  - `top_monitor_roots`
  - `top_promotable_roots`
  - `top_high_roots`

This keeps current ops and downstream consumers readable while the UI and CLI shift to scanner-native language.

### Downstream Consumers

Phase 1 should not require downstream strategy services to change.

Downstream services may continue to read:

- `dominant_flow`
- `dominant_flow_ratio`
- root quote quality
- top supporting contracts

They may adopt the new generic descriptors later:

- `flow_shape`
- `directional_bias`
- `directional_flow_score`
- `volatility_demand_score`

## Module-By-Module Implementation Plan

Implement in this order.

1. Extend [option_quote_records.py](../../packages/core/services/option_quote_records.py)
   - add `gamma`, `vega`, `rho`, and `open_interest_date` to symbol metadata

2. Add `packages/core/services/uoa_terms.py`
   - own scanner-native state names and compatibility aliases

3. Add `packages/core/services/uoa_trade_enrichment.py`
   - perform quote matching
   - infer aggressor side
   - compute signed notionals

4. Update [capture/runtime.py](../../packages/core/services/discovery_runs/capture/runtime.py)
   - enrich stream trades before building `uoa_summary`
   - keep the existing three top-level payloads

5. Update [uoa_trade_summary.py](../../packages/core/services/uoa_trade_summary.py)
   - aggregate enriched signed fields
   - add volatility-demand aggregation metrics
   - preserve existing `dominant_flow` outputs

6. Update [uoa_quote_summary.py](../../packages/core/services/uoa_quote_summary.py)
   - add surface-state metrics
   - add OI freshness metrics
   - keep quote-quality ownership here

7. Replace the scoring logic in [uoa_root_decisions.py](../../packages/core/services/uoa_root_decisions.py)
   - switch from selection-coupled `monitor/promotable/high`
   - emit scanner-native states
   - emit continuous component scores and flow descriptors

8. Update [discovery_run_health/enrichment.py](../../packages/core/services/discovery_run_health/enrichment.py)
   - normalize canonical and compatibility fields together

9. Update [uoa_state.py](../../packages/core/services/uoa_state.py), [ops/uoa.py](../../packages/core/services/ops/uoa.py), and [ops_render.py](../../packages/core/cli/ops_render.py)
   - render new scores, descriptors, state counts, and supporting contracts

10. Leave downstream strategy consumers unchanged for the initial ship
   - they may adopt the new generic fields later

## Phase 2

Phase 2 requires richer data than Alpaca alone.

The dedicated UOA owner pipeline is a separate target-state ownership change. It does not require the richer market-data scope described in this Phase 2 section. Use [UOA Dedicated Pipeline Design](./2026-04-28_uoa_dedicated_pipeline_design.md) for that pipeline split.

Use Phase 2 only if the product scope justifies the cost and operational complexity.

Phase 2 items:

- open/close truth
- participant-origin truth
- complex-trade linkage
- package-aware flow handling
- optional broader UOA discovery lane

Phase 2 is not a blocker for Phase 1.

## Rollout And Verification

### Rollout

Roll out Phase 1 in this order:

1. ship metadata plumbing and in-memory trade enrichment
2. ship enriched `uoa_summary` and `uoa_quote_summary`
3. ship new root scoring and canonical scanner states behind compatibility aliases
4. ship updated ops and CLI rendering
5. widen alert policy only after live operator review

### Verification

Use narrow runtime-safe verification only.

Required checks:

- `uv run ruff check packages/core/services/option_quote_records.py packages/core/services/uoa_trade_enrichment.py packages/core/services/uoa_trade_summary.py packages/core/services/uoa_quote_summary.py packages/core/services/uoa_root_decisions.py packages/core/services/uoa_terms.py packages/core/services/ops/uoa.py packages/core/cli/ops_render.py`
- `uv run python -m py_compile packages/core/services/option_quote_records.py packages/core/services/uoa_trade_enrichment.py packages/core/services/uoa_trade_summary.py packages/core/services/uoa_quote_summary.py packages/core/services/uoa_root_decisions.py packages/core/services/uoa_terms.py packages/core/services/ops/uoa.py packages/core/cli/ops_render.py`
- `uv run spreads uoa --json`
- `uv run spreads uoa --no-color`

Live-review expectations:

- root payloads expose canonical state names and compatibility aliases
- top supporting contracts explain the root score path
- high directional roots expose a clear bullish/bearish bias
- volatility-demand roots expose two-sided ATM/front-expiry concentration
- weak or stale quote context caps otherwise-high roots as expected

## Recommendation

Implement Phase 1 exactly as defined here:

- keep UOA scanner-native and strategy-agnostic
- keep the existing bounded runtime path
- keep the existing three top-level payloads
- add in-memory trade enrichment, surface-state metrics, and deterministic v2 scoring
- decouple UOA state taxonomy from selection terminology during rollout

This is sufficient to start implementation without further planning work.

## References

Implementation references:

- [packages/core/services/discovery_runs/capture/runtime.py](../../packages/core/services/discovery_runs/capture/runtime.py)
- [packages/core/services/option_quote_records.py](../../packages/core/services/option_quote_records.py)
- [packages/core/services/option_trade_records.py](../../packages/core/services/option_trade_records.py)
- [packages/core/services/uoa_trade_summary.py](../../packages/core/services/uoa_trade_summary.py)
- [packages/core/services/uoa_quote_summary.py](../../packages/core/services/uoa_quote_summary.py)
- [packages/core/services/uoa_trade_baselines.py](../../packages/core/services/uoa_trade_baselines.py)
- [packages/core/services/uoa_root_decisions.py](../../packages/core/services/uoa_root_decisions.py)
- [packages/core/services/uoa_state.py](../../packages/core/services/uoa_state.py)
- [packages/core/services/ops/uoa.py](../../packages/core/services/ops/uoa.py)
- [packages/core/cli/ops_render.py](../../packages/core/cli/ops_render.py)

External references:

- [Pan and Poteshman (2006), The Information in Option Volume for Future Stock Prices](https://academic.oup.com/rfs/article-abstract/19/3/871/1646711)
- [Pan and Poteshman (2008), Volatility Information Trading in the Option Market](https://www.mit.edu/~junpan/npp.pdf)
- [Muravyev (2015/2016), Order Flow and Expected Option Returns](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1963865)
- [Cboe Open-Close Volume Summary](https://datashop.cboe.com/cboe-options-open-close-volume-summary)
- [Cboe Trade-By-Trade Execution Detail Specification](https://cdn.cboe.com/resources/membership/US-Options-Trade-By-Trade-Execution-Detail-Specification.pdf)
