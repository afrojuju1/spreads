# Company Valuation Engine V1 Spec

Status: research-backed design spec

As of: Wednesday, April 29, 2026

Related:

- [System Architecture](../current_system_state.md)
- [Planning Docs](./README.md)
- [Features Tracker](../product/features_tracker.md)
- [Company Valuation Engine V1 Implementation Contract](./2026-04-29_company_valuation_engine_v1_implementation_contract.md)

## Role Of This Doc

This document is the design contract for **V1 of the company valuation engine**.

Use it for:

- the V1 product boundary
- the single-service deployable shape
- the industry-template contract
- the ownership-data contract
- the canonical evaluation payload
- the minimal API and screening surface

This is a **future product design** inside the broader spreads ecosystem. It is **not** the canonical source of truth for the current runtime architecture. Use [System Architecture](../current_system_state.md) for current repo ownership and top-level runtime boundaries.

## Terminology

This repo already uses `evaluation` heavily for:

- historical strategy evaluation
- backtest evaluation
- post-close outcome evaluation

This document uses `company valuation` to mean:

- issuer quality assessment
- intrinsic value assessment
- ownership-aware screening

To avoid collisions with the current historical-evaluation vocabulary, new code, tables, and payloads should use explicit nouns such as:

- `company_valuation`
- `issuer_evaluation`
- `intrinsic_value`
- `quality_score`

Do **not** use vague names like `evaluation_service` or `evaluation_snapshot` without the `company` or `issuer` qualifier.

## Goal

Build a **single-service V1 company valuation engine** for U.S. public companies that produces one canonical, AI-friendly valuation document per issuer.

That document should answer:

1. how strong is the business
2. what is its intrinsic long-term value
3. how does ownership strengthen or weaken the case
4. how confident is the system in that output
5. what changed since the last evaluation

V1 should be validated on 5 names first, but the design should support **full-universe screening**.

## Non-Goals

V1 does not attempt to provide:

- non-U.S. issuer coverage
- a second-purpose market-timing or 12-month target engine
- transcript NLP or narrative-heavy guidance parsing
- analyst-estimate-driven valuation
- real-time institutional ownership truth
- one generic template that pretends all sectors are comparable
- support for banks, insurers, REITs, or BDCs through the default operating-company templates

## Locked V1 Decisions

These decisions are locked for the spec.

- V1 is one deployable microservice: `company-valuation-service`.
- V1 is one engine, not a fleet of separate services.
- V1 uses raw official-source inputs first:
  - SEC 10-K / 10-Q / 8-K
  - SEC Forms 3 / 4 / 5
  - SEC Schedules 13D / 13G
  - SEC Form 13F datasets
  - U.S. Treasury yield curve
  - existing price / market-cap input service
- V1 returns **intrinsic long-term fair value** as the canonical valuation output.
- V1 keeps ownership inside the score.
- V1 prioritizes structured numbers over narrative text.
- V1 includes **industry templates from day one**.
- V1 stores full holder-level history, even if the first UI or AI surfaces summarize it.
- V1 is built for downstream AI consumption through a canonical structured evaluation document.
- V1 keeps the external API intentionally small.

## System Shape

`Company Valuation Engine` is the whole product.

`company-valuation-service` is the single deployable for V1.

Internal modules:

- `ingestion`
- `normalization`
- `template_registry`
- `point_in_time`
- `features`
- `ownership_signals`
- `quality`
- `valuation`
- `screening`
- `reports`

Supporting storage:

- `Postgres`
- optional object storage for raw SEC payloads, parse artifacts, and rendered reports

## High-Level Architecture

```text
External Inputs
---------------
SEC 10-K / 10-Q / 8-K
SEC Forms 3 / 4 / 5
SEC Schedules 13D / 13G
SEC Form 13F data sets
Treasury yield curve
Price / market-cap input service


Company Valuation Engine
========================================================================

                  +--------------------------------------+
                  |      company-valuation-service       |
                  |--------------------------------------|
                  | ingestion                            |
                  | normalization                        |
                  | template_registry                    |
                  | point_in_time                        |
                  | features                             |
                  | ownership_signals                    |
                  | quality                              |
                  | valuation                            |
                  | screening                            |
                  | reports + API                        |
                  +------------------+-------------------+
                                     |
                 +-------------------+-------------------+
                 |                                       |
                 v                                       v
       +------------------------+            +-------------------------+
       | Postgres               |            | Object Storage          |
       | canonical facts        |            | raw filings             |
       | ownership history      |            | parse artifacts         |
       | evaluation snapshots   |            | rendered reports        |
       | screening rows         |            +-------------------------+
       +------------------------+

========================================================================
```

## Runtime Flow

```text
raw filings / ownership / Treasury / price
        |
        v
ingestion
        |
        v
normalization
        |
        v
template assignment
        |
        v
point_in_time(as_of)
        |
        v
template-specific feature computation
        |
   +----+-------------------+
   |                        |
   v                        v
ownership signals      financial features
   |                        |
   +------------+-----------+
                |
                v
        quality scoring
                |
                +------------------+
                |                  |
                v                  v
         risk + reason codes   intrinsic valuation
                |                  |
                +---------+--------+
                          |
                          v
          canonical company valuation document
                          |
               +----------+-----------+
               |                      |
               v                      v
       screening materialization   API response / AI layer
```

## Point-In-Time Rule

This is non-negotiable.

For any `as_of = T`, the engine may only use records where:

- `available_at <= T`

Important timestamps:

- `period_end`
- `filed_at`
- `accepted_at`
- `available_at`
- `event_date`

Operational rules:

- 8-K earnings timing may make operating information public before the 10-Q or 10-K.
- amended and restated filings must not overwrite original truth
- ownership changes must use filing availability, not just stated transaction or event dates

## Industry Templates

Industry templates are part of V1 because they are foundational to:

- normalization
- feature definitions
- score weights
- valuation method mix
- risk handling
- cross-company screening

Each issuer must resolve to a `template_id`.

Template assignment inputs:

- SIC / NAICS
- issuer description from filings
- filing-language heuristics
- manual override when needed

### V1 Supported Templates

- `general_operating`
- `software_asset_light`
- `retail_consumer`
- `industrial_manufacturing`
- `energy_asset_heavy`

### V1 Unsupported Or Limited Templates

These should explicitly return `unsupported_template` or `limited_coverage`, not fake precision.

- `banking`
- `insurance`
- `reit`
- `bdc`

### Template Contract

Each template definition must include:

- `template_id`
- `template_version`
- `assignment_rules`
- `required_features`
- `optional_features`
- `quality_weight_map`
- `ownership_weight_map`
- `valuation_model_mix`
- `confidence_rules`
- `risk_rules`
- `unsupported_conditions`

Templates themselves should be config-backed in V1 rather than persisted as mutable Postgres rows.

### Template Behavior Examples

`software_asset_light`

- heavier weight on recurring-revenue proxies, FCF conversion, and margin ramp
- heavier penalty for persistent SBC dilution
- valuation emphasizes revenue-to-margin path and FCF

`industrial_manufacturing`

- heavier weight on ROIC, capex discipline, leverage, and cash conversion
- lower weight on SBC-related penalties
- valuation emphasizes EBIT and FCF anchors

`energy_asset_heavy`

- stricter cyclicality and leverage penalties
- lower valuation confidence by default
- heavier sensitivity to capex burden and commodity-linked instability

## Ownership Data Contract

Ownership is a first-class part of V1.

### V1 Ownership Inputs

- `Forms 3 / 4 / 5`
- `Schedules 13D / 13D-A`
- `Schedules 13G / 13G-A`
- `Form 13F`

Interpretation model:

- `Forms 3 / 4 / 5`
  - insider ownership and insider transaction behavior
- `13D`
  - activist, control-intent, strategic-stake, or event-driven beneficial ownership
- `13G`
  - passive or institutional beneficial ownership
- `13F`
  - holder-level institutional history and concentration

### Why 13D And 13G Are In V1

They are materially more useful now because:

- SEC filing deadlines were accelerated in 2023 and are in force now
- structured XML is now required for 13D and 13G
- they provide earlier and more interpretable ownership signals than 13F alone

### Ownership Parsing Contract

For each 13D / 13G filing, capture at minimum:

- `schedule_type`
- `accession_no`
- `issuer_cik`
- `issuer_ticker`
- `reporting_person_name`
- `reporting_person_cik` when present
- `reporting_person_type`
- `event_date`
- `filed_at`
- `accepted_at`
- `available_at`
- `percent_beneficial_ownership`
- `share_count_reported`
- `sole_voting_power`
- `shared_voting_power`
- `sole_dispositive_power`
- `shared_dispositive_power`
- `passive_flag`
- `control_intent_flag`
- `group_flag`
- `amendment_flag`
- `amendment_no`
- `prior_schedule_type`
- `item4_purpose_text`
- `item5_interest_text`
- `item6_derivative_or_arrangement_text`
- `source_xml_path`

For each Form 3 / 4 / 5 record, capture at minimum:

- `reporting_owner_name`
- `reporting_owner_cik`
- `officer_flag`
- `director_flag`
- `ten_percent_owner_flag`
- `transaction_date`
- `transaction_code`
- `security_type`
- `shares_delta`
- `shares_owned_after`
- `price`
- `footnote_refs`
- `filed_at`
- `accepted_at`
- `available_at`

For each 13F position record, capture at minimum:

- `manager_name`
- `manager_cik`
- `report_period`
- `filed_at`
- `available_at`
- `issuer_name`
- `cusip`
- `share_count`
- `market_value_reported`
- `discretion_type`
- `other_manager_refs`
- `voting_authority_sole`
- `voting_authority_shared`
- `voting_authority_none`

### Ownership Entity-Resolution Contract

V1 must not treat ownership as flat text rows only.

It needs canonical entities for:

- reporting person
- beneficial owner
- filing group
- institutional manager
- issuer

Required behaviors:

- resolve parent / subsidiary relationships where disclosed
- preserve joint-filing groups
- support one holder appearing through multiple filings and amendments
- allow one issuer to have many related holder entities

### Ownership Data Model

Add these canonical tables:

- `beneficial_ownership_filings`
- `beneficial_owners`
- `beneficial_owner_positions`
- `beneficial_owner_groups`
- `beneficial_owner_group_memberships`
- `insider_transactions`
- `institutional_holders`
- `institutional_positions`

Recommended key fields:

- `issuer_id`
- `holder_id`
- `group_id`
- `schedule_type`
- `template_id`
- `event_date`
- `accepted_at`
- `available_at`
- `share_count`
- `ownership_pct`
- `ownership_kind`
- `source_accession_no`
- `source_row_hash`

## Ownership Signal Contract

Ownership affects score, but it must not dominate the entire engine.

Split ownership into three sub-signals:

- `insider_signal`
- `beneficial_owner_signal`
- `institutional_holder_signal`

### Weighting Rules

- `insider_signal` gets the strongest ownership weight
- `beneficial_owner_signal` gets medium weight
- `institutional_holder_signal` gets lower capped weight

Practical implications:

- insider buying or selling can move the total score more than a stale 13F trend
- 13D is more meaningful than 13G for control and special-situation detection
- 13F concentration and accumulation matter, but with heavier freshness decay

### Activist And Control Policy

`13D` is not automatically positive or negative.

V1 should treat it as:

- an ownership modifier
- a risk modifier
- a special-situation indicator

Possible outcomes:

- constructive strategic ownership
- governance pressure on a weak operator
- control contest or uncertainty
- event-driven stake with low read-through for intrinsic quality

Template rules should control how activist or control ownership is interpreted.

### Freshness Decay Rules

Ownership signals should decay over time.

Relative decay strength:

- Forms 3 / 4 / 5: slowest decay among ownership event signals
- 13D / 13G: medium decay
- 13F: fastest decay

Freshness should be explicit in the evaluation payload, not hidden.

### Ownership Reason-Code Contract

Ownership logic must emit structured reason codes, not prose-only commentary.

V1 ownership reason-code families:

- `insider_net_buying_positive`
- `insider_net_selling_negative`
- `insider_execution_cluster_positive`
- `insider_execution_cluster_negative`
- `new_13d_filed_special_situation`
- `13d_control_intent_caution`
- `13d_constructive_strategic_owner_positive`
- `13g_large_passive_owner_positive`
- `13g_passive_holder_concentration_caution`
- `holder_concentration_risk_negative`
- `holder_base_broadening_positive`
- `institutional_accumulation_positive`
- `institutional_distribution_negative`
- `ownership_signal_stale_caution`
- `ownership_identity_unresolved_caution`
- `ownership_group_change_special_situation`

### AI-Facing Ownership Payload Contract

The canonical evaluation document should include:

```text
ownership
  insider_signal
    score
    confidence
    freshness_days
    reason_codes[]
    evidence[]

  beneficial_owner_signal
    score
    confidence
    freshness_days
    reason_codes[]
    evidence[]

  institutional_holder_signal
    score
    confidence
    freshness_days
    reason_codes[]
    evidence[]

  special_situations[]
  concentration_summary
  top_holders[]
```

`evidence[]` rows should be compact and machine-readable, for example:

- `new_13d_filed`
- `ownership_pct_change`
- `group_formed`
- `13g_to_13d_conversion`
- `insider_open_market_buy_cluster`

## Financial Feature Contract

Feature computation must be template-aware and point-in-time.

Core feature families:

- growth quality
- profitability quality
- cash-flow quality
- capital efficiency
- balance-sheet risk
- shareholder quality
- ownership quality
- reporting quality

Examples:

- TTM revenue growth
- gross margin trend
- operating margin stability
- FCF conversion
- capex intensity
- ROIC
- net leverage
- diluted share growth
- SBC burden
- buyback offset quality
- amendment frequency
- accrual heaviness proxies

## Quality-Scoring Contract

Quality scoring is template-aware and ownership-aware.

The output must include:

- `total_score`
- `sub_scores`
- `factor_contributions`
- `reason_codes`
- `confidence`

Recommended sub-score families:

- `growth_score`
- `profitability_score`
- `cash_flow_score`
- `capital_efficiency_score`
- `balance_sheet_score`
- `shareholder_score`
- `ownership_score`
- `reporting_quality_score`

Total score behavior:

- fundamentals dominate
- ownership modifies but does not dominate
- unsupported or low-confidence sectors do not get fake-normalized high precision

## Valuation Contract

The canonical valuation output is:

- `intrinsic long-term fair value`

It is not:

- a 12-month price target

V1 valuation methods:

- `DCF / owner-earnings style model`
- `historical multiples anchor`

Each template defines its own method emphasis.

Required output fields:

- `intrinsic_value_bear`
- `intrinsic_value_base`
- `intrinsic_value_bull`
- `intrinsic_value_mid`
- `valuation_gap`
- `valuation_confidence`
- `valuation_reason_codes`
- `assumption_summary`

Required confidence penalties:

- unstable or negative FCF
- high cyclicality
- repeated amendments or restatements
- weak template fit
- thin or noisy ownership resolution
- stale market inputs

## Canonical Evaluation Document Contract

This is the main V1 product surface.

It should be one structured document per issuer and `as_of`.

```text
company_valuation
  issuer
    issuer_id
    cik
    ticker
    company_name
    template_id
    template_version

  as_of
  freshness
    latest_filing_available_at
    latest_ownership_available_at
    latest_price_snapshot_at

  source_summary
    filings_used[]
    ownership_sources_used[]
    treasury_curve_date
    price_snapshot_ref

  quality
    total_score
    sub_scores
    factor_contributions
    reason_codes[]
    confidence

  valuation
    intrinsic_value_bear
    intrinsic_value_base
    intrinsic_value_bull
    intrinsic_value_mid
    current_price
    valuation_gap
    confidence
    reason_codes[]
    assumption_summary

  ownership
    insider_signal
    beneficial_owner_signal
    institutional_holder_signal
    special_situations[]
    concentration_summary
    top_holders[]

  risks
    key_risks[]
    accounting_flags[]
    model_limitations[]

  delta_summary
    changed_since_previous_flag
    score_delta
    value_delta
    ownership_delta
    top_change_reason_codes[]

  provenance
    accession_nos[]
    periods_used[]
    template_assignment_reason
    missing_data_flags[]
```

## Screening Contract

V1 is built for full-universe screening, so it should persist one screening row per issuer and evaluation date.

`screening_rows` should include at minimum:

- `issuer_id`
- `ticker`
- `template_id`
- `as_of`
- `quality_score`
- `intrinsic_value_mid`
- `valuation_gap`
- `quality_confidence`
- `valuation_confidence`
- `ownership_score`
- `ownership_special_situation_flag`
- `top_reason_codes`
- `limited_coverage_flag`

Screening rules:

- screen within template cohorts first
- allow cross-template screens, but mark them as lower-comparability views
- never compare unsupported templates as if they are normal V1 peers

## Minimal API

Keep the external surface intentionally small.

- `GET /companies/{ticker}/evaluation`
  - optional `as_of=...`
- `GET /screen`
  - filters by `template_id`, score bands, valuation gap, confidence, special situations
- `POST /internal/evaluations/{ticker}/recompute`

V1 should not create many narrow “explanation-only” endpoints. Explanations belong inside the canonical evaluation document.

## Canonical Tables

Keep the schema durable but small.

- `issuers`
- `securities`
- `filings`
- `xbrl_facts`
- `statement_period_snapshots`
- `market_snapshots`
- `beneficial_ownership_filings`
- `beneficial_owners`
- `beneficial_owner_positions`
- `beneficial_owner_groups`
- `beneficial_owner_group_memberships`
- `insider_transactions`
- `institutional_holders`
- `institutional_positions`
- `feature_snapshots`
- `company_valuation_snapshots`
- `screening_rows`

## Jobs

Required V1 jobs:

- `ingest_new_filings`
- `ingest_insider_forms`
- `ingest_beneficial_ownership_filings`
- `ingest_13f_quarter`
- `refresh_treasury_curve`
- `refresh_market_snapshots`
- `recompute_company_valuation_for_issuer`
- `refresh_screening_rows`

Recompute triggers:

- new 10-K / 10-Q / 8-K
- new Form 3 / 4 / 5
- new 13D / 13G
- new 13F quarter
- daily price refresh
- template override change

## Other Critical Considerations

These are not optional cleanups. They should be designed correctly in V1.

- `8-K timing`
  - public availability may arrive before periodic filings
- `restatement lineage`
  - original and amended truth must both remain queryable
- `issuer identity continuity`
  - ticker changes, spinoffs, mergers, share classes, and CIK continuity need durable modeling
- `ownership entity resolution`
  - parent funds, reporting groups, and joint filers cannot be flattened away
- `13D derivative disclosure`
  - derivative-linked exposure and arrangements should be preserved even if V1 uses them mainly for flags
- `confidence separation`
  - quality confidence and valuation confidence should be separate
- `AI consumption discipline`
  - downstream AI should consume canonical evaluation documents, not raw filing logic

## Recommended V1 Scope Boundary

Do now:

- one service
- one canonical evaluation document
- intrinsic-value-first valuation
- template-aware scoring and screening
- ownership stack with Forms 3 / 4 / 5, 13D / 13G, and 13F
- machine-readable reason codes and evidence fragments

Do later:

- analyst estimates
- transcript NLP
- market-implied 12-month target bands
- banking / insurance / REIT templates
- deeper peer-relative comp engines

## Research Constraints Behind The Spec

This design relies on current official SEC behavior as of April 29, 2026:

- Schedule 13D initial filings are due within five business days
- Schedule 13D amendments are due within two business days
- revised Schedule 13G deadlines have been in required compliance since September 30, 2024
- structured XML is required for 13D and 13G filings

Those changes make 13D and 13G materially more useful as V1 ownership inputs than in older workflows built around text filings and slower deadlines.

## Reference Sources

- [SEC EDGAR API entrypoint](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
- [SEC beneficial ownership modernization rule](https://www.sec.gov/rules-regulations/2023/10/33-11180)
- [SEC beneficial ownership modernization fact sheet](https://www.sec.gov/files/33-11253-fact-sheet.pdf)
- [SEC 13D / 13G interpretations](https://www.sec.gov/rules-regulations/staff-guidance/corporation-finance-interpretations/exchange-act-sections-13d-13g-regulation-13d-g-beneficial-ownership-reporting)
- [SEC EDGAR technical specifications](https://www.sec.gov/edgar/filer-information/current-edgar-technical-specifications)
- [SEC Form 13F datasets](https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets)
- [SEC Forms 3, 4, and 5 bulletin](https://www.sec.gov/files/forms-3-4-5.pdf)
- [U.S. Treasury daily yield curve rates](https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?type=daily_treasury_yield_curve)
