# Company Valuation Engine V1 Implementation Contract

Status: implementation-ready contract

As of: Wednesday, April 29, 2026

Related:

- [System Architecture](../current_system_state.md)
- [Company Valuation Engine V1 Spec](./2026-04-29_company_valuation_engine_v1_spec.md)
- [Planning Docs](./README.md)

## Role Of This Doc

This document locks the **implementation contract** for V1 of the company valuation engine.

Use it for:

- exact module boundaries
- exact config-backed template shape
- exact application-model contract
- exact Postgres table contract
- exact job contract
- exact API payloads
- implementation order

This document is intentionally more concrete than the V1 spec. If the two disagree, this document wins for V1 implementation details.

## Locked V1 Implementation Decisions

- The whole product is `company valuation engine`.
- The single deployable is `company-valuation-service`.
- V1 remains one service with one Postgres database.
- V1 keeps templates and issuer overrides as **checked-in config**, not database-backed editable state.
- V1 stores derived screening rows in Postgres rather than computing full-universe screens on demand.
- V1 stores one canonical evaluation document per issuer and `as_of`.
- V1 uses explicit prefixed text IDs instead of anonymous UUIDs.
- V1 uses raw official-source ingestion first:
  - SEC 10-K / 10-Q / 8-K
  - Forms 3 / 4 / 5
  - Schedules 13D / 13G
  - Form 13F
  - Treasury curve
  - price / market-cap service
- V1 uses config-backed templates under `packages/config`.
- V1 uses intrinsic long-term value as the canonical valuation output.
- V1 supports unsupported-template responses for sectors outside V1 coverage.

## Proposed Repo Layout

```text
packages/
  config/
    company_valuation/
      templates/
        general_operating.yaml
        software_asset_light.yaml
        retail_consumer.yaml
        industrial_manufacturing.yaml
        energy_asset_heavy.yaml
      issuer_overrides.yaml

  core/
    services/
      company_valuation/
        contracts.py
        ids.py
        templates.py
        point_in_time.py
        screening.py
        reports.py
        ingestion/
          sec_filings.py
          sec_insiders.py
          sec_beneficial_ownership.py
          sec_13f.py
          treasury.py
          market_inputs.py
        normalization/
          filings.py
          xbrl_facts.py
          ownership.py
        features/
          financials.py
          ownership.py
          quality_inputs.py
        quality/
          scoring.py
          reason_codes.py
        valuation/
          dcf.py
          multiples.py
          confidence.py

    storage/
      company_valuation_models.py
      company_valuation_repository.py

  api/
    app/
      routes/
        company_valuation.py
```

Rules:

- `templates/` and `issuer_overrides.yaml` are config-backed, not Postgres tables in V1.
- `contracts.py` owns API and internal document models.
- `company_valuation_models.py` owns persistence models only.
- routes stay thin and call service-layer entrypoints only.

## ID Contract

Use stable text IDs with prefixes.

- `issuer_id = issuer:<cik>`
- `security_id = security:<cik>:<ticker>`
- `filing_id = filing:<accession_no>`
- `holder_id = holder:<normalized-name-hash>`
- `group_id = group:<issuer-cik>:<group-hash>`
- `feature_snapshot_id = feature_snapshot:<issuer-cik>:<as-of>:<feature-version>`
- `company_valuation_snapshot_id = company_valuation:<issuer-cik>:<as-of>:<valuation-version>`
- `screening_row_id = screen:<issuer-cik>:<as-of>`

Do not use display labels as keys.

## Config-Backed Definitions

### Template Registry Contract

Templates live in:

- `packages/config/company_valuation/templates/*.yaml`

Each template file must include:

```yaml
template_id: general_operating
template_version: v1
status: active

assignment_rules:
  sic_prefixes: []
  naics_prefixes: []
  keyword_rules: []

required_features:
  - revenue_ttm_growth
  - gross_margin_ttm
  - operating_margin_ttm
  - free_cash_flow_ttm
  - diluted_share_growth_ttm

optional_features:
  - sbc_as_pct_revenue
  - inventory_turns
  - capex_intensity

quality_weight_map:
  growth_score: 18
  profitability_score: 18
  cash_flow_score: 16
  capital_efficiency_score: 14
  balance_sheet_score: 14
  shareholder_score: 10
  ownership_score: 6
  reporting_quality_score: 4

ownership_weight_map:
  insider_signal: 3
  beneficial_owner_signal: 2
  institutional_holder_signal: 1

valuation_model_mix:
  dcf_weight: 0.7
  historical_multiples_weight: 0.3
  primary_multiple_metrics:
    - ev_ebit
    - ev_fcf
    - pe
  terminal_growth_floor: 0.015
  terminal_growth_cap: 0.03
  discount_rate_spread_bps: 450

confidence_rules:
  low_confidence_if:
    - persistent_negative_fcf
    - repeated_restatements
    - missing_core_facts

risk_rules:
  add_flags_if:
    - dilution_accelerating
    - leverage_above_threshold
    - ownership_concentration_high

unsupported_conditions: []
```

Rules:

- `quality_weight_map` must sum to `100`.
- `ownership_weight_map` is expressed in score points inside `ownership_score`.
- `template_version` must be included in every feature and evaluation snapshot.

### Issuer Overrides Contract

Issuer overrides live in:

- `packages/config/company_valuation/issuer_overrides.yaml`

Contract:

```yaml
overrides:
  - issuer_cik: "0000320193"
    template_id: software_asset_light
    reason: "manual override after filing-language review"
    active: true
```

Rules:

- overrides are rare
- overrides require human-written `reason`
- overrides are part of provenance in the evaluation payload

## Application Model Contract

These are the proposed Python service models for `contracts.py`.

### `TemplateDefinition`

- `template_id: str`
- `template_version: str`
- `status: Literal["active", "inactive"]`
- `assignment_rules: dict[str, Any]`
- `required_features: list[str]`
- `optional_features: list[str]`
- `quality_weight_map: dict[str, int]`
- `ownership_weight_map: dict[str, int]`
- `valuation_model_mix: dict[str, Any]`
- `confidence_rules: dict[str, Any]`
- `risk_rules: dict[str, Any]`
- `unsupported_conditions: list[str]`

### `IssuerIdentity`

- `issuer_id: str`
- `cik: str`
- `ticker: str`
- `company_name: str`
- `template_id: str`
- `template_version: str`

### `FilingRef`

- `filing_id: str`
- `accession_no: str`
- `form_type: str`
- `accepted_at: datetime`
- `available_at: datetime`
- `period_end: date | None`

### `OwnershipEvidence`

- `source_type: Literal["form3", "form4", "form5", "13d", "13g", "13f"]`
- `holder_id: str | None`
- `group_id: str | None`
- `event_date: date | None`
- `available_at: datetime`
- `headline: str`
- `reason_code: str`
- `metrics: dict[str, float | int | str | None]`

### `OwnershipSignal`

- `score: float`
- `confidence: float`
- `freshness_days: int | None`
- `reason_codes: list[str]`
- `evidence: list[OwnershipEvidence]`

### `QualityBreakdown`

- `total_score: float`
- `sub_scores: dict[str, float]`
- `factor_contributions: dict[str, float]`
- `reason_codes: list[str]`
- `confidence: float`

### `ValuationSummary`

- `intrinsic_value_bear: float | None`
- `intrinsic_value_base: float | None`
- `intrinsic_value_bull: float | None`
- `intrinsic_value_mid: float | None`
- `current_price: float | None`
- `valuation_gap: float | None`
- `confidence: float`
- `reason_codes: list[str]`
- `assumption_summary: dict[str, Any]`

### `CompanyValuationDocument`

- `payload_version: str`
- `issuer: IssuerIdentity`
- `as_of: datetime`
- `freshness: dict[str, Any]`
- `source_summary: dict[str, Any]`
- `quality: QualityBreakdown`
- `valuation: ValuationSummary`
- `ownership: dict[str, Any]`
- `risks: dict[str, Any]`
- `delta_summary: dict[str, Any]`
- `provenance: dict[str, Any]`

### `ScreenRow`

- `screening_row_id: str`
- `issuer_id: str`
- `ticker: str`
- `template_id: str`
- `as_of: date`
- `quality_score: float | None`
- `intrinsic_value_mid: float | None`
- `current_price: float | None`
- `valuation_gap: float | None`
- `quality_confidence: float | None`
- `valuation_confidence: float | None`
- `ownership_score: float | None`
- `ownership_special_situation_flag: bool`
- `limited_coverage_flag: bool`
- `top_reason_codes: list[str]`

## Storage Model

### Storage Rules

- Postgres is the primary store.
- Raw SEC payloads may live in object storage, but their references must live in Postgres.
- Templates are config-backed, not Postgres-backed.
- Large derived evaluation documents may be stored in JSONB in V1.
- Screening reads should hit `screening_rows`, not recompute the world on request.

### Table List

Postgres tables for V1:

- `issuers`
- `securities`
- `filings`
- `xbrl_facts`
- `statement_period_snapshots`
- `treasury_curve_snapshots`
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

### `issuers`

| Column | Type | Notes |
|---|---|---|
| `issuer_id` | `text pk` | `issuer:<cik>` |
| `cik` | `text not null unique` | zero-padded SEC CIK |
| `company_name` | `text not null` | latest canonical name |
| `sic` | `text null` | raw SEC SIC |
| `sic_description` | `text null` | descriptive label |
| `naics` | `text null` | optional when resolved |
| `template_id` | `text not null` | active template |
| `template_version` | `text not null` | active template version |
| `template_assignment_source` | `text not null` | `derived` or `manual_override` |
| `template_assignment_reason` | `text not null` | short explanation |
| `limited_coverage_flag` | `boolean not null default false` | unsupported or partial coverage |
| `created_at` | `timestamptz not null` | row creation |
| `updated_at` | `timestamptz not null` | row update |

Indexes:

- unique `cik`
- index on `template_id`
- index on `limited_coverage_flag`

### `securities`

| Column | Type | Notes |
|---|---|---|
| `security_id` | `text pk` | `security:<cik>:<ticker>` |
| `issuer_id` | `text not null fk issuers` | issuer owner |
| `ticker` | `text not null` | symbol at the time |
| `share_class` | `text null` | share class label |
| `exchange` | `text null` | exchange code if known |
| `cusip` | `text null` | optional |
| `is_primary` | `boolean not null default false` | primary screenable security |
| `active_from` | `date null` | optional history |
| `active_to` | `date null` | optional history |
| `created_at` | `timestamptz not null` | row creation |

Indexes:

- unique `(issuer_id, ticker, coalesce(active_to, 'infinity'))` conceptually
- index on `(issuer_id, is_primary)`
- index on `cusip`

### `filings`

| Column | Type | Notes |
|---|---|---|
| `filing_id` | `text pk` | `filing:<accession_no>` |
| `issuer_id` | `text not null fk issuers` | issuer owner |
| `accession_no` | `text not null unique` | SEC accession number |
| `form_type` | `text not null` | `10-K`, `10-Q`, `8-K`, `4`, `SC 13D`, etc. |
| `period_end` | `date null` | fiscal period end when present |
| `filed_at` | `timestamptz not null` | SEC filed time when available |
| `accepted_at` | `timestamptz not null` | SEC acceptance timestamp |
| `available_at` | `timestamptz not null` | canonical PIT timestamp |
| `amendment_flag` | `boolean not null default false` | amendment row |
| `amendment_of_accession_no` | `text null` | source filing accession |
| `primary_document_url` | `text null` | SEC document URL |
| `primary_xml_url` | `text null` | XML document URL |
| `raw_storage_uri` | `text null` | object-storage reference |
| `raw_sha256` | `text null` | payload integrity |
| `parse_status` | `text not null` | `pending`, `parsed`, `failed` |
| `created_at` | `timestamptz not null` | row creation |

Indexes:

- unique `accession_no`
- index `(issuer_id, form_type, available_at desc)`
- index `(accepted_at desc)`
- index `(period_end desc)`

### `xbrl_facts`

| Column | Type | Notes |
|---|---|---|
| `fact_id` | `bigserial pk` | row id |
| `filing_id` | `text not null fk filings` | source filing |
| `issuer_id` | `text not null fk issuers` | issuer owner |
| `taxonomy` | `text not null` | e.g. `us-gaap` |
| `concept_name` | `text not null` | e.g. `RevenueFromContractWithCustomerExcludingAssessedTax` |
| `unit` | `text not null` | `USD`, `USD/shares`, `shares` |
| `period_start` | `date null` | null for instant facts |
| `period_end` | `date not null` | period or instant end |
| `instant_flag` | `boolean not null default false` | instant vs duration fact |
| `dimensions_json` | `jsonb not null default '{}'` | dimension members |
| `value_numeric` | `numeric null` | numeric value |
| `value_text` | `text null` | textual fact when needed |
| `decimals` | `text null` | XBRL decimals marker |
| `available_at` | `timestamptz not null` | PIT availability |
| `fact_hash` | `text not null` | uniqueness helper |

Indexes:

- unique `fact_hash`
- index `(issuer_id, concept_name, period_end desc)`
- gin index on `dimensions_json`

### `statement_period_snapshots`

This is the normalized per-period financial layer over raw XBRL.

| Column | Type | Notes |
|---|---|---|
| `snapshot_id` | `text pk` | stable normalized-period id |
| `issuer_id` | `text not null fk issuers` | issuer owner |
| `filing_id` | `text not null fk filings` | source filing |
| `period_type` | `text not null` | `quarterly` or `annual` |
| `fiscal_year` | `integer null` | fiscal year |
| `fiscal_period` | `text null` | `Q1`, `Q2`, `Q3`, `Q4`, `FY` |
| `period_start` | `date null` | period start |
| `period_end` | `date not null` | period end |
| `available_at` | `timestamptz not null` | PIT availability |
| `normalization_version` | `text not null` | normalized-contract version |
| `metrics_json` | `jsonb not null` | canonical normalized metrics |
| `source_fact_refs_json` | `jsonb not null` | supporting fact refs |

Indexes:

- unique `(issuer_id, filing_id, period_type, period_end, normalization_version)`
- index `(issuer_id, period_end desc)`

### `treasury_curve_snapshots`

| Column | Type | Notes |
|---|---|---|
| `curve_snapshot_id` | `text pk` | stable curve id |
| `curve_date` | `date not null unique` | Treasury curve date |
| `available_at` | `timestamptz not null` | when considered usable |
| `curve_points_json` | `jsonb not null` | tenor -> rate map |
| `source_url` | `text not null` | provenance |

Indexes:

- unique `curve_date`

### `market_snapshots`

| Column | Type | Notes |
|---|---|---|
| `market_snapshot_id` | `text pk` | stable snapshot id |
| `security_id` | `text not null fk securities` | security owner |
| `issuer_id` | `text not null fk issuers` | denormalized issuer owner |
| `captured_at` | `timestamptz not null` | source snapshot time |
| `available_at` | `timestamptz not null` | PIT availability |
| `price` | `numeric not null` | last or close price |
| `shares_outstanding_market` | `numeric null` | market-source shares if used |
| `market_cap` | `numeric null` | market cap |
| `enterprise_value` | `numeric null` | EV if computed upstream |
| `source` | `text not null` | input service source |

Indexes:

- index `(issuer_id, available_at desc)`
- index `(security_id, available_at desc)`

### `beneficial_ownership_filings`

1:1 extension over `filings` for 13D / 13G family.

| Column | Type | Notes |
|---|---|---|
| `filing_id` | `text pk fk filings` | extension key |
| `issuer_id` | `text not null fk issuers` | issuer owner |
| `schedule_type` | `text not null` | `13D`, `13D/A`, `13G`, `13G/A` |
| `event_date` | `date null` | reported event date |
| `passive_flag` | `boolean not null default false` | passive filer |
| `control_intent_flag` | `boolean not null default false` | control-intent signal |
| `group_flag` | `boolean not null default false` | group filing |
| `amendment_no` | `integer null` | amendment number when inferable |
| `prior_schedule_type` | `text null` | previous schedule type if switched |
| `item4_purpose_text` | `text null` | purpose text |
| `item5_interest_text` | `text null` | interest text |
| `item6_derivative_or_arrangement_text` | `text null` | derivative / arrangement text |
| `ownership_xml_version` | `text null` | SEC technical spec version if captured |

Indexes:

- index `(issuer_id, event_date desc)`
- index `(issuer_id, schedule_type, filing_id)`
- gin/trigram index on purpose text if needed later

### `beneficial_owners`

| Column | Type | Notes |
|---|---|---|
| `holder_id` | `text pk` | `holder:<hash>` |
| `canonical_name` | `text not null` | resolved display name |
| `normalized_name` | `text not null` | normalization key |
| `holder_cik` | `text null` | reporting person CIK when known |
| `holder_type` | `text null` | person / fund / advisor / parent / other |
| `parent_holder_id` | `text null fk beneficial_owners` | parent relationship |
| `created_at` | `timestamptz not null` | row creation |
| `updated_at` | `timestamptz not null` | row update |

Indexes:

- unique `(normalized_name, coalesce(holder_cik, ''))`
- index `parent_holder_id`

### `beneficial_owner_groups`

| Column | Type | Notes |
|---|---|---|
| `group_id` | `text pk` | `group:<issuer-cik>:<hash>` |
| `issuer_id` | `text not null fk issuers` | issuer owner |
| `group_name` | `text not null` | derived group label |
| `group_kind` | `text not null` | `joint_filing`, `acting_in_concert`, `derived` |
| `root_filing_id` | `text not null fk filings` | establishing filing |
| `effective_from` | `date null` | effective start |
| `effective_to` | `date null` | effective end |
| `created_at` | `timestamptz not null` | row creation |

Indexes:

- index `(issuer_id, effective_from desc)`

### `beneficial_owner_group_memberships`

| Column | Type | Notes |
|---|---|---|
| `group_id` | `text not null fk beneficial_owner_groups` | group owner |
| `holder_id` | `text not null fk beneficial_owners` | member holder |
| `filing_id` | `text not null fk filings` | source filing |
| `member_role` | `text null` | optional role label |
| `effective_from` | `date null` | effective start |
| `effective_to` | `date null` | effective end |

Primary key:

- `(group_id, holder_id, filing_id)`

Indexes:

- index `holder_id`

### `beneficial_owner_positions`

| Column | Type | Notes |
|---|---|---|
| `position_id` | `bigserial pk` | row id |
| `issuer_id` | `text not null fk issuers` | issuer owner |
| `holder_id` | `text not null fk beneficial_owners` | holder owner |
| `group_id` | `text null fk beneficial_owner_groups` | optional group owner |
| `filing_id` | `text not null fk filings` | source filing |
| `schedule_type` | `text not null` | 13D/13G family |
| `event_date` | `date null` | reported event date |
| `available_at` | `timestamptz not null` | PIT availability |
| `share_count_reported` | `numeric null` | reported shares |
| `ownership_pct` | `numeric null` | beneficial ownership pct |
| `sole_voting_power` | `numeric null` | voting power |
| `shared_voting_power` | `numeric null` | voting power |
| `sole_dispositive_power` | `numeric null` | dispositive power |
| `shared_dispositive_power` | `numeric null` | dispositive power |
| `passive_flag` | `boolean not null default false` | passive owner |
| `control_intent_flag` | `boolean not null default false` | control signal |
| `derivative_exposure_flag` | `boolean not null default false` | derivative-linked disclosure |
| `source_row_hash` | `text not null` | dedupe key |

Indexes:

- unique `source_row_hash`
- index `(issuer_id, available_at desc)`
- index `(holder_id, available_at desc)`
- index `(issuer_id, ownership_pct desc)`

### `insider_transactions`

| Column | Type | Notes |
|---|---|---|
| `insider_transaction_id` | `bigserial pk` | row id |
| `issuer_id` | `text not null fk issuers` | issuer owner |
| `holder_id` | `text null fk beneficial_owners` | resolved insider holder |
| `filing_id` | `text not null fk filings` | source Form 3/4/5 |
| `transaction_date` | `date null` | trade date |
| `transaction_code` | `text not null` | SEC code |
| `security_type` | `text null` | security label |
| `shares_delta` | `numeric null` | signed share change |
| `price` | `numeric null` | transaction price |
| `shares_owned_after` | `numeric null` | resulting owned shares |
| `ownership_nature` | `text null` | direct / indirect if available |
| `footnotes_json` | `jsonb not null default '[]'` | footnote refs |
| `available_at` | `timestamptz not null` | PIT availability |

Indexes:

- index `(issuer_id, available_at desc)`
- index `(holder_id, available_at desc)`
- index `(issuer_id, transaction_date desc)`

### `institutional_holders`

| Column | Type | Notes |
|---|---|---|
| `institutional_holder_id` | `text pk` | stable manager id |
| `manager_cik` | `text null` | manager CIK |
| `manager_name` | `text not null` | canonical manager name |
| `normalized_name` | `text not null` | normalization key |
| `created_at` | `timestamptz not null` | row creation |
| `updated_at` | `timestamptz not null` | row update |

Indexes:

- unique `(normalized_name, coalesce(manager_cik, ''))`

### `institutional_positions`

| Column | Type | Notes |
|---|---|---|
| `institutional_position_id` | `bigserial pk` | row id |
| `institutional_holder_id` | `text not null fk institutional_holders` | manager owner |
| `issuer_id` | `text not null fk issuers` | issuer owner |
| `filing_id` | `text not null fk filings` | source 13F filing |
| `report_period` | `date not null` | 13F report period |
| `available_at` | `timestamptz not null` | PIT availability |
| `cusip` | `text null` | reported CUSIP |
| `share_count` | `numeric null` | reported shares |
| `market_value_reported` | `numeric null` | reported market value |
| `discretion_type` | `text null` | reported discretion |
| `other_manager_refs_json` | `jsonb not null default '[]'` | manager refs |
| `voting_authority_sole` | `numeric null` | voting authority |
| `voting_authority_shared` | `numeric null` | voting authority |
| `voting_authority_none` | `numeric null` | voting authority |

Indexes:

- index `(issuer_id, report_period desc)`
- index `(institutional_holder_id, report_period desc)`

### `feature_snapshots`

| Column | Type | Notes |
|---|---|---|
| `feature_snapshot_id` | `text pk` | versioned feature snapshot id |
| `issuer_id` | `text not null fk issuers` | issuer owner |
| `template_id` | `text not null` | template in use |
| `template_version` | `text not null` | template version |
| `as_of` | `timestamptz not null` | PIT cutoff |
| `feature_version` | `text not null` | feature contract version |
| `financial_features_json` | `jsonb not null` | computed financial features |
| `ownership_features_json` | `jsonb not null` | computed ownership features |
| `dependency_refs_json` | `jsonb not null` | source refs |
| `computed_at` | `timestamptz not null` | computation time |

Indexes:

- unique `(issuer_id, as_of, feature_version)`
- index `(template_id, as_of desc)`

### `company_valuation_snapshots`

| Column | Type | Notes |
|---|---|---|
| `company_valuation_snapshot_id` | `text pk` | versioned valuation id |
| `issuer_id` | `text not null fk issuers` | issuer owner |
| `security_id` | `text null fk securities` | primary security when available |
| `template_id` | `text not null` | active template |
| `template_version` | `text not null` | active template version |
| `as_of` | `timestamptz not null` | PIT cutoff |
| `evaluation_version` | `text not null` | evaluation contract version |
| `quality_score` | `numeric null` | total quality score |
| `ownership_score` | `numeric null` | ownership subscore |
| `intrinsic_value_bear` | `numeric null` | bear case |
| `intrinsic_value_base` | `numeric null` | base case |
| `intrinsic_value_bull` | `numeric null` | bull case |
| `intrinsic_value_mid` | `numeric null` | midpoint |
| `current_price` | `numeric null` | current price |
| `valuation_gap` | `numeric null` | midpoint vs price |
| `quality_confidence` | `numeric null` | 0-1 confidence |
| `valuation_confidence` | `numeric null` | 0-1 confidence |
| `limited_coverage_flag` | `boolean not null default false` | unsupported or partial |
| `top_reason_codes_json` | `jsonb not null default '[]'` | high-level reason codes |
| `evaluation_json` | `jsonb not null` | canonical document body |
| `computed_at` | `timestamptz not null` | computation time |

Indexes:

- unique `(issuer_id, as_of, evaluation_version)`
- index `(issuer_id, as_of desc)`
- index `(template_id, as_of desc)`

### `screening_rows`

| Column | Type | Notes |
|---|---|---|
| `screening_row_id` | `text pk` | `screen:<issuer-cik>:<as-of>` |
| `issuer_id` | `text not null fk issuers` | issuer owner |
| `security_id` | `text null fk securities` | primary security |
| `ticker` | `text not null` | primary ticker |
| `template_id` | `text not null` | template cohort |
| `as_of` | `date not null` | screening date |
| `quality_score` | `numeric null` | quality score |
| `intrinsic_value_mid` | `numeric null` | intrinsic midpoint |
| `current_price` | `numeric null` | price |
| `valuation_gap` | `numeric null` | midpoint vs price |
| `quality_confidence` | `numeric null` | confidence |
| `valuation_confidence` | `numeric null` | confidence |
| `ownership_score` | `numeric null` | ownership subscore |
| `ownership_special_situation_flag` | `boolean not null default false` | activist/control/etc |
| `limited_coverage_flag` | `boolean not null default false` | unsupported/partial |
| `top_reason_codes_json` | `jsonb not null default '[]'` | screening explanations |
| `updated_at` | `timestamptz not null` | materialization time |

Indexes:

- unique `(issuer_id, as_of)`
- index `(template_id, as_of, quality_score desc)`
- index `(as_of, valuation_gap desc)`
- index `(as_of, ownership_special_situation_flag)`

## Minimal API Contract

### `GET /companies/{ticker}/evaluation`

Purpose:

- return one canonical evaluation document

Query params:

- `as_of` optional ISO timestamp or date

Response: `200`

```json
{
  "payload_version": "v1",
  "issuer": {
    "issuer_id": "issuer:0000320193",
    "cik": "0000320193",
    "ticker": "AAPL",
    "company_name": "Apple Inc.",
    "template_id": "general_operating",
    "template_version": "v1"
  },
  "as_of": "2026-04-29T00:00:00Z",
  "freshness": {
    "latest_filing_available_at": "2026-02-01T21:30:00Z",
    "latest_ownership_available_at": "2026-04-12T22:00:00Z",
    "latest_price_snapshot_at": "2026-04-29T20:00:00Z"
  },
  "source_summary": {
    "filings_used": [],
    "ownership_sources_used": ["forms_3_4_5", "13d_13g", "13f"],
    "treasury_curve_date": "2026-04-29",
    "price_snapshot_ref": "market_snapshot:..."
  },
  "quality": {
    "total_score": 77.4,
    "sub_scores": {},
    "factor_contributions": {},
    "reason_codes": [],
    "confidence": 0.84
  },
  "valuation": {
    "intrinsic_value_bear": 150.0,
    "intrinsic_value_base": 178.0,
    "intrinsic_value_bull": 205.0,
    "intrinsic_value_mid": 177.7,
    "current_price": 168.4,
    "valuation_gap": 0.0552,
    "confidence": 0.78,
    "reason_codes": [],
    "assumption_summary": {}
  },
  "ownership": {
    "insider_signal": {},
    "beneficial_owner_signal": {},
    "institutional_holder_signal": {},
    "special_situations": [],
    "concentration_summary": {},
    "top_holders": []
  },
  "risks": {
    "key_risks": [],
    "accounting_flags": [],
    "model_limitations": []
  },
  "delta_summary": {
    "changed_since_previous_flag": false,
    "score_delta": null,
    "value_delta": null,
    "ownership_delta": null,
    "top_change_reason_codes": []
  },
  "provenance": {
    "accession_nos": [],
    "periods_used": [],
    "template_assignment_reason": "sic-derived",
    "missing_data_flags": []
  }
}
```

Error behaviors:

- `404` when ticker is unknown
- `422` when `as_of` is invalid
- `409` when issuer exists but template is unsupported in V1 and no limited output is allowed

### `GET /screen`

Purpose:

- return template-aware screening rows

Query params:

- `as_of` required date
- `template_id` optional
- `min_quality_score` optional
- `min_valuation_gap` optional
- `min_quality_confidence` optional
- `min_valuation_confidence` optional
- `special_situations_only` optional boolean
- `limit` optional, default `100`, max `500`
- `cursor` optional

Response: `200`

```json
{
  "payload_version": "v1",
  "as_of": "2026-04-29",
  "template_id": "software_asset_light",
  "rows": [
    {
      "screening_row_id": "screen:issuer:0000000000:2026-04-29",
      "issuer_id": "issuer:0000000000",
      "ticker": "EXAMPLE",
      "template_id": "software_asset_light",
      "as_of": "2026-04-29",
      "quality_score": 81.2,
      "intrinsic_value_mid": 45.0,
      "current_price": 38.2,
      "valuation_gap": 0.177,
      "quality_confidence": 0.82,
      "valuation_confidence": 0.74,
      "ownership_score": 4.0,
      "ownership_special_situation_flag": false,
      "limited_coverage_flag": false,
      "top_reason_codes": ["margin_stability_positive", "insider_net_buying_positive"]
    }
  ],
  "next_cursor": null
}
```

Rules:

- `screen` reads materialized `screening_rows`
- cross-template screens are allowed, but template_id is still returned on every row

### `POST /internal/evaluations/{ticker}/recompute`

Purpose:

- enqueue or execute a recomputation for one issuer

Request body:

```json
{
  "as_of": "2026-04-29T00:00:00Z",
  "force": false,
  "reason": "manual_recompute"
}
```

Response: `202`

```json
{
  "status": "accepted",
  "ticker": "AAPL",
  "job_name": "recompute_company_valuation_for_issuer",
  "as_of": "2026-04-29T00:00:00Z"
}
```

## Job Contract

V1 jobs:

- `ingest_new_filings`
- `ingest_insider_forms`
- `ingest_beneficial_ownership_filings`
- `ingest_13f_quarter`
- `refresh_treasury_curve`
- `refresh_market_snapshots`
- `recompute_company_valuation_for_issuer`
- `refresh_screening_rows`

### Job Input Shape

`recompute_company_valuation_for_issuer`

- `issuer_id` or `ticker`
- `as_of`
- `force`
- `reason`

Rules:

- recompute must be idempotent for the same `(issuer_id, as_of, evaluation_version)`
- ingestion jobs must be safe to rerun from the same SEC source window
- `refresh_screening_rows` must not recompute fundamentals; it reads from existing evaluation snapshots

## Scoring Contract

### Score Scale

- all score families use `0.0` to `100.0`
- `confidence` uses `0.0` to `1.0`

### Total Quality Score

Quality-score composition is template-controlled, but all templates must fit these families:

- `growth_score`
- `profitability_score`
- `cash_flow_score`
- `capital_efficiency_score`
- `balance_sheet_score`
- `shareholder_score`
- `ownership_score`
- `reporting_quality_score`

### Ownership Score Cap

Lock these V1 global caps:

- total `ownership_score` contribution must not exceed `15` score points
- `insider_signal` cap: `6`
- `beneficial_owner_signal` cap: `5`
- `institutional_holder_signal` cap: `4`

This prevents ownership from overwhelming business quality.

## Ownership Reason-Code Contract

These codes are locked for V1 and must be emitted in machine-readable form.

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

Reason-code payload rules:

- top-level evaluation must include aggregate reason codes
- each ownership sub-signal must include its own reason-code list
- reasons must be stable enums, not prose labels

## Implementation Order

Build in this order.

### Phase 1. Contracts And Storage

- add template config files
- add issuer override config
- add service contracts in code
- add storage models and migrations

Deliverables:

- config files compile
- migrations for all V1 tables
- typed `CompanyValuationDocument` contract

### Phase 2. Filing And Ownership Ingestion

- SEC filings ingestion
- Forms 3 / 4 / 5 ingestion
- 13D / 13G ingestion
- 13F ingestion
- Treasury and market inputs ingestion

Deliverables:

- raw filing rows
- ownership rows
- repeatable reruns

### Phase 3. Normalization And PIT

- XBRL fact normalization
- per-period snapshots
- ownership entity resolution
- PIT resolver

Deliverables:

- canonical normalized facts
- PIT data assembly for any issuer and `as_of`

### Phase 4. Features, Quality, And Valuation

- template-aware feature computation
- ownership signal computation
- quality scoring
- valuation outputs

Deliverables:

- feature snapshots
- company valuation snapshots

### Phase 5. Screening And API

- screening materialization
- evaluation endpoint
- screen endpoint
- recompute endpoint

Deliverables:

- stable API
- template-aware screens

## Acceptance Criteria

V1 is implementation-complete when:

- one issuer can be ingested end to end from filings to evaluation payload
- PIT queries for the same issuer are deterministic for the same `as_of`
- ownership parsing supports Forms 3 / 4 / 5, 13D / 13G, and 13F
- templates drive scoring and valuation behavior
- unsupported templates return explicit limited-coverage outputs
- `GET /companies/{ticker}/evaluation` returns a stable document contract
- `GET /screen` returns materialized screen rows without recomputation

## Non-Negotiable Guardrails

- do not create multiple deployable services in V1
- do not store templates only in code
- do not flatten 13D / 13G groups away
- do not mix intrinsic value with 12-month target language in the canonical payload
- do not let ownership dominate total quality score
- do not hide missing-data or low-confidence states
