# Company Valuation Standards-Based Taxonomy Plan

Status: implemented checkpoint with curated-universe boundary; hold unless support scope expands

As of: Friday, May 1, 2026

Related:

- [Company Valuation Engine V1 Spec](./2026-04-29_company_valuation_engine_v1_spec.md)
- [Company Valuation Engine V1 Implementation Contract](./2026-04-29_company_valuation_engine_v1_implementation_contract.md)
- [Company Valuation Software Sub-Template Findings](./2026-04-30_company_valuation_software_subtemplate_findings.md)
- [Company Valuation Software Compounder Follow-Up Findings](./2026-04-30_company_valuation_software_compounder_followup_findings.md)

## Role Of This Doc

This document defines the **standards-based classification direction** for the company valuation engine.

It replaces the implicit assumption that we should invent most sector and industry buckets ourselves.

The core shift is:

- use **public classification standards first**
- keep **valuation templates** as a thin layer on top
- keep **stress / recovery / limited-coverage** as overlays, not taxonomy

## Why This Change Is Needed

The recent custom-template research was useful, but it also exposed the limit:

- industrial and energy refinements produced real value
- software quickly fragmented into multiple overlapping sub-regimes
- additional clustering started to look like re-creating a private market taxonomy instead of building a practical valuation engine

That is the point where the system should stop inventing ontology and anchor on standards.

## Research-Backed Standards View

### GICS

`GICS` is the equity-market standard most aligned with research, benchmark construction, and valuation workflows.

That is an inference from the official MSCI / S&P material:

- GICS was developed by `MSCI` and `S&P Dow Jones Indices`
- companies are assigned to a single sub-industry based on principal business activity
- the methodology is reviewed and revised over time to reflect market structure

But GICS is also a **licensed proprietary standard**.

So the correct architectural conclusion is:

- the engine should be **GICS-shaped**
- but it should not assume we can freely embed or distribute proprietary GICS reference data unless we have rights to do so

### SEC SIC

`SIC` is the cleanest public source we can rely on directly.

Strengths:

- it is already attached to SEC filers
- it is official and easy to source from SEC filings / metadata
- it is good enough for a first deterministic classification backbone

Weaknesses:

- it is older and coarser than modern equity-research sectoring
- it is not ideal as the final market-facing taxonomy by itself

### NAICS

`NAICS 2022` is the best public supplemental business-classification standard.

Strengths:

- modern
- hierarchical
- public
- broadly used by U.S. statistical agencies

Weaknesses:

- not the natural default for public-equity valuation/screening
- less aligned than GICS with how investors think about sectors and comp sets

## Core Recommendation

Use a **three-layer model**:

1. `source classification layer`
   - raw `SEC SIC`
   - raw `NAICS 2022` when available

2. `canonical market taxonomy layer`
   - internal taxonomy shaped like:
     - `sector`
     - `industry_group`
     - `industry`
     - `subindustry`
   - mapped from public standards
   - intentionally designed so a future licensed GICS feed can drop in cleanly later

3. `valuation overlay layer`
   - valuation templates
   - stress / recovery / limited-coverage flags
   - special-situation overlays

That is the right architecture if we want standards without depending on paid taxonomy feeds.

## What The Engine Should Stop Doing

Do **not** let clustering or manual overrides invent the primary company ontology.

Do **not** use valuation-template IDs as if they are the same thing as industry classification.

Do **not** let `stressed_operator` or `execution_recovery` become fake sectors.

Those are overlays, not classifications.

## Proposed Taxonomy Model

Each issuer should carry:

- `sic_code`
- `sic_title`
- `naics_code`
- `naics_title`
- `taxonomy_version`
- `canonical_sector_id`
- `canonical_industry_group_id`
- `canonical_industry_id`
- `canonical_subindustry_id`
- `classification_source`
- `classification_confidence`

Important distinction:

- `classification_source` tells us where the category came from
- `valuation_template_id` tells us how we value it
- `overlay_flags` tell us what special condition modifies interpretation

## Recommended Public-Source Implementation

Because we are not buying a taxonomy feed, V1.5 should work like this:

### Source Of Truth

- `SEC SIC` from issuer / filing metadata
- `NAICS 2022` when we can attach it reliably

### Canonical Internal Taxonomy

Maintain a checked-in mapping from:

- `SIC`
- `NAICS`
- issuer override when necessary

into a stable internal hierarchy:

- `sector`
- `industry_group`
- `industry`
- `subindustry`

This is not “inventing a new standard.”  
It is a public-data-backed internal market taxonomy designed to be:

- deterministic
- auditable
- compatible with future GICS-shaped usage

## What Survives From The Current Custom Templates

### Keep As Valuation Templates

These still make sense as valuation-policy objects:

- `platform_hardware`
- `retail_consumer`
- `energy_asset_heavy`
- `aerospace_defense_prime`
- `diversified_industrial_core`
- `software_asset_light`

But they should no longer be treated as the first classification layer.

They should map from canonical taxonomy plus rules.

### Reclassify As Overlays

These should become overlays, not industry buckets:

- `stressed_operator`
- future `execution_recovery`
- future `special_situation`
- `limited_coverage`

### Shrink Or Remove

`general_operating` should shrink dramatically.

It is useful as a temporary fallback, but it should not remain the silent default for broad parts of the universe once the classification layer exists.

## Example Shape

Correct model:

```text
issuer
  -> source classification
     -> sic = 7372
     -> naics = 513210

  -> canonical taxonomy
     -> sector = technology
     -> industry_group = software_and_services
     -> industry = application_software
     -> subindustry = enterprise_application_software

  -> valuation template
     -> software_asset_light

  -> overlays
     -> none
```

Another example:

```text
issuer
  -> source classification
     -> sic = 2911
     -> naics = 324110

  -> canonical taxonomy
     -> sector = energy
     -> industry_group = oil_gas_and_consumable_fuels
     -> industry = integrated_oil_gas_or_refining
     -> subindustry = refining_marketing_or_integrated

  -> valuation template
     -> energy_asset_heavy

  -> overlays
     -> stressed_operator = true/false
```

## Proposed Storage Additions

Add tables like:

- `taxonomy_nodes`
  - hierarchy nodes for sector / group / industry / subindustry
- `issuer_classifications`
  - raw SIC / NAICS plus current canonical mapping
- `taxonomy_mappings`
  - deterministic mapping rules from SIC/NAICS to canonical nodes
- `valuation_template_mappings`
  - canonical taxonomy -> default valuation template
- `issuer_overlay_flags`
  - `stressed_operator`, `limited_coverage`, `execution_recovery`, etc.

Minimal fields:

- `taxonomy_node_id`
- `taxonomy_level`
- `taxonomy_code`
- `taxonomy_name`
- `parent_taxonomy_node_id`
- `mapping_version`
- `source_standard`
- `effective_from`
- `effective_to`

## Runtime Decision Order

The engine should resolve classification in this order:

1. manual issuer classification override
2. deterministic `SIC + NAICS` mapping
3. deterministic `SIC-only` fallback
4. `general_operating` only as a last resort

Then:

1. map canonical taxonomy to default valuation template
2. apply issuer template override if explicitly configured
3. apply overlays independently

That order is much cleaner than the current “template first, sector second” behavior.

## What To Do With Existing Templates

Short-term:

- keep the existing active templates working
- do not break the current calibration gains

Medium-term:

- introduce the classification layer
- re-express templates as:
  - `default valuation template for canonical subindustry X`
  - not `primary industry label`

Long-term:

- reduce ad hoc issuer overrides
- use overrides mostly for:
  - unusual conglomerates
  - transition entities
  - edge-case taxonomy mismatches

## Recommended Migration

### Phase 1

Add the standards-based classification schema and config without touching valuation output.

### Phase 2

Populate raw `SIC` and `NAICS` consistently for the current issuer universe.

### Phase 3

Create the internal canonical taxonomy mapping file:

- public-source-backed
- auditable
- checked into config

### Phase 4

Map current active templates to canonical taxonomy defaults.

### Phase 5

Move `stressed_operator` out of the primary template family and into overlays.

## Practical Recommendation Right Now

The next implementation work should be:

1. add a classification data model
2. ingest and persist raw `SIC` and `NAICS`
3. define a canonical internal taxonomy hierarchy
4. map current templates onto that hierarchy
5. reclassify `stressed_operator` as an overlay

Do **not** continue broad unsupervised template discovery before this layer exists.

## Implementation Checkpoint

The core direction in this document has now been implemented far enough to serve as the active company-valuation routing model.

What landed:

- standards-based taxonomy foundation:
  - raw `SIC` / `NAICS` representation
  - canonical internal taxonomy nodes and mappings
  - taxonomy -> default template mappings
  - overlay flags
  - migration, storage models, resolver helpers, and shadow sync
- support-aware product boundary:
  - the engine is now treated as a valuation product with **explicit supported cohorts**
  - a curated `50`-name supported universe is the live scorecard
  - broad whole-market classification is no longer the main KPI
- live overlay correction:
  - `stressed_operator` is now modeled as `energy_asset_heavy + stressed_operator_flag`
  - it is no longer treated as a fake base industry
- support-aware runtime behavior:
  - screen materialization defaults to supported issuers
  - screen and document reads surface `support_status`, expected template, and effective template

## Current Operating Position

As of May 1, 2026:

- curated supported universe: `75/75`
- support-tier split: `core=50`, `expanded=25`
- supported names with documents, prices, and valuation gaps: `75/75`
- shadow expected-template mismatches inside the curated universe: `0`
- remaining effective-template mismatches are intentional stressed-overlay names, not taxonomy failures

Useful checkpoint artifacts:

- supported-universe freeze pack: [curated50_2026-05-01_checkpoint_freeze](../../outputs/company_valuation/supported_report_pack/curated50_2026-05-01_checkpoint_freeze/summary.md)
- stressed benchmark/prior pre-calibration: [stressed_operator_supported_pre_v1](../../outputs/company_valuation/benchmark_priors/stressed_operator_supported_pre_v1/summary.md)
- stressed benchmark/prior post-calibration: [stressed_operator_supported_post_v1](../../outputs/company_valuation/benchmark_priors/stressed_operator_supported_post_v1/summary.md)
- stressed benchmark/prior after curated-universe expansion to `75`: [stressed_operator_supported_post_v2_75](../../outputs/company_valuation/benchmark_priors/stressed_operator_supported_post_v2_75/summary.md)

## What We Learned

The most important product clarification was this:

- `company_valuation` is **not** a universal market-classification project
- it is a valuation engine with explicit supported cohorts and explicit `out_of_scope` names

That changes the scorecard completely.

The wrong KPI was:

- “how much of the full issuer universe classifies”

The right KPI is:

- “within supported cohorts, do names classify cleanly, route to the right default template, and produce coherent valuation output”

The current curated universe is no longer just the initial `50`.

It has now been expanded to `75` by adding:

- software names that already routed cleanly into `software_asset_light`
- retail names that already routed cleanly into `retail_consumer`
- energy names that already routed cleanly into `energy_asset_heavy`
- one additional industrial name and one additional aerospace name

The important boundary is unchanged:

- the expansion used names that already fit the supported template families
- it did **not** justify broadening unsupported sectors
- a few known noisy names still remain excluded even though they can technically classify
- the `75` expansion materially widened the supported stressed-energy cohort, which improved coverage but worsened stressed benchmark coherence

The live interpretation should now be:

- `core`
  - the original `50`
  - trusted as the primary supported valuation universe
- `expanded`
  - the `25` clean-routing additions
  - technically supported, but noisier and not yet as validated as the core set

## Benchmark-Gated Calibration Rule

Further template tuning should only happen when there is an explicit benchmark or analyst prior.

That rule is now active for stressed names:

- a checked-in benchmark/prior snapshot exists for the stressed basket
- the supported stressed cohort can be compared to external analyst targets
- one conservative stressed-template softening was applied against that benchmark

That pass improved the supported stressed cohort without changing the deeper conclusion:

- the stressed template was too bearish before the benchmark-gated pass
- it is still intentionally harsher than base energy after the pass
- the remaining noise is concentrated in names that are mostly `out_of_scope`, not proof that the supported stressed cohort needs more structural splitting today

## Stop Condition

Do **not** reopen broad taxonomy work or generic template tuning from this checkpoint by default.

Stop here unless one of these is true:

- the curated supported universe is expanding materially
- a new supported cohort is being added explicitly
- a template has a new external benchmark / analyst-prior pack
- a live supported cohort shows persistent benchmark divergence that is structural, not single-name noise

Specifically for stressed energy:

- do **not** split `stressed_operator` into stressed upstream vs stressed services/refining yet
- only make that split if those cohorts become explicitly supported with their own curated names and benchmark/prior pack

## What This Means For Current Research

The recent research is still useful.

It should now be interpreted as:

- evidence for where valuation templates help
- not evidence that we should let clustering define the whole category system

Examples:

- `aerospace_defense_prime`
  - keep as a valuation-policy candidate
  - anchor it under a standard aerospace/defense classification node
- `platform_suite_software`
  - keep as a research candidate
  - anchor it under a standard software/application-services classification node
- `stressed_operator`
  - keep as an overlay
  - do not treat it as a base industry

## Sources

- SEC SIC code list: [SEC SIC Code List](https://www.sec.gov/search-filings/standard-industrial-classification-sic-code-list)
- NAICS 2022 overview: [U.S. Census NAICS](https://www.census.gov/naics/?chart=2022&input=11)
- NAICS 2022 manual PDF: [2022 NAICS Manual](https://www.census.gov/naics/reference_files_tools/2022_NAICS_Manual.pdf)
- GICS overview: [MSCI GICS Overview](https://www.msci.com/indexes/index-resources/gics)
- GICS methodology: [MSCI GICS Methodology PDF](https://www.msci.com/downloads/web/msci-com/indexes/index-resources/gics/MSCI_Global_Industry_Classification_Standard_%28GICS%C2%AE%29_Methodology_20240801.pdf)
- S&P GICS overview: [S&P Dow Jones GICS](https://www.spglobal.com/spdji/en/landing/topic/gics/)
