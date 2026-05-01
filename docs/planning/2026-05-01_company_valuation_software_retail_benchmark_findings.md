# Company Valuation Software And Retail Benchmark Findings

Status: software structurally split; retail calibration retained

As of: Friday, May 1, 2026

Related:

- [Company Valuation Standards-Based Taxonomy Plan](./2026-04-30_company_valuation_standards_based_taxonomy_plan.md)
- [Expanded Support Review](./2026-05-01_company_valuation_expanded_support_review.md)
- [Software Benchmark Prior Report](../../outputs/company_valuation/benchmark_priors/software_asset_light_supported_v1/summary.md)
- [Software Franchise Benchmark Prior Report](../../outputs/company_valuation/benchmark_priors/software_franchise_platform_supported_v1b/summary.md)
- [Software Workflow Benchmark Prior Report](../../outputs/company_valuation/benchmark_priors/software_workflow_growth_supported_v1b/summary.md)
- [Software Mission-Critical Benchmark Prior Report](../../outputs/company_valuation/benchmark_priors/software_mission_critical_supported_v2/summary.md)
- [Retail Benchmark Prior Report](../../outputs/company_valuation/benchmark_priors/retail_consumer_supported_v1/summary.md)
- [Retail Compounder Benchmark Prior Report](../../outputs/company_valuation/benchmark_priors/retail_consumer_compounder_supported_v1/summary.md)

## Role Of This Doc

This note records the first benchmark-gated validation pass for the main non-stressed supported cohorts.

The question is not whether the architecture works.

It does.

The question is whether the current valuation outputs are strong enough to claim that the supported universe is fully trustworthy.

This pass says: not yet.

## What Was Added

Checked-in analyst-prior packs now exist for:

- `software_franchise_platform`
- `software_workflow_growth`
- `software_mission_critical`
- `retail_consumer`
- `retail_consumer_compounder`

All three are sourced from StockAnalysis forecast pages as of `2026-05-01`, where the site attributes price targets and analyst ratings to Benzinga / Wall Street analyst data.

## First-Pass Results

### Software, Before Structural Split

Artifact:

- [software_asset_light_supported_v1](../../outputs/company_valuation/benchmark_priors/software_asset_light_supported_v1/summary.md)

Headline metrics:

- rows compared: `17`
- mean valuation gap: `+0.0498`
- mean benchmark gap: `+0.5776`
- mean abs gap delta: `0.6174`
- sign mismatch count: `9`
- under benchmark count: `15`
- over benchmark count: `2`

Critical read:

- the software template is not obviously broken in isolation
- but it is materially below external analyst priors across most names
- the disagreement is too broad to dismiss as a few edge cases

Largest problem names:

- `GWRE`
- `NOW`
- `FICO`
- `SNPS`
- `CDNS`
- `ORCL`
- `MSFT`

Important nuance:

- `NOW` has an especially noisy benchmark surface:
  - average target: `184.84`
  - median target: `140`
  - low target: `85`
  - high target: `1,160`
- that spread is a warning that average-target priors can be brittle for some high-dispersion software names

### Retail Consumer

Artifact:

- [retail_consumer_supported_v1](../../outputs/company_valuation/benchmark_priors/retail_consumer_supported_v1/summary.md)

Headline metrics:

- rows compared: `9`
- mean valuation gap: `-0.0936`
- mean benchmark gap: `+0.0550`
- mean abs gap delta: `0.4714`
- sign mismatch count: `4`
- under benchmark count: `6`
- over benchmark count: `3`

Critical read:

- the broad retail template is directionally mixed, not uniformly too harsh or too generous
- several names still look badly misfit relative to external priors

Largest problem names:

- `KMX`
- `WMT`
- `TJX`
- `DLTR`
- `BBY`
- `CASY`

The big conclusion:

- `KMX` remains the clearest weak supported retail name
- `WMT`, `TJX`, and `CASY` suggest the current broad retail cohort is still too compressed for stronger operators

### Retail Consumer Compounder

Artifact:

- [retail_consumer_compounder_supported_v1](../../outputs/company_valuation/benchmark_priors/retail_consumer_compounder_supported_v1/summary.md)

Headline metrics:

- rows compared: `5`
- mean valuation gap: `-0.2624`
- mean benchmark gap: `+0.1812`
- mean abs gap delta: `0.4436`
- sign mismatch count: `5`
- under benchmark count: `5`
- over benchmark count: `0`

Critical read:

- this is the cleanest structural signal in the whole pass
- every compounder retail name is below the analyst prior
- that strongly suggests the current compounder template is still too conservative

Affected names:

- `AZO`
- `ORLY`
- `COST`
- `HD`
- `LOW`

## Overall Conclusion

The benchmark system is doing its job.

It shows that:

- the architecture is usable
- the supported-universe boundary is much healthier than before
- but software and retail are not yet benchmark-cleared valuation products

That means the honest current claim is:

- usable as an internal, constrained research engine
- not yet ready for a strong “absolutely usable” claim across the supported universe

## First Calibration Pass

One calibration pass has now been applied.

### Software

Updated artifact:

- [software_asset_light_supported_v2](../../outputs/company_valuation/benchmark_priors/software_asset_light_supported_v2/summary.md)

Result:

- mean abs gap delta: `0.6174 -> 0.5345`
- sign mismatch count: `9 -> 7`

Interpretation:

- this was a real improvement
- the software template is still not benchmark-cleared
- but the gap is now smaller, and the problem increasingly looks like a mix of:
  - residual conservatism for some names
  - benchmark dispersion in names like `NOW`
  - likely need for future segmentation inside software if we want much tighter fit

### Retail Consumer

Updated artifacts:

- [retail_consumer_supported_v2](../../outputs/company_valuation/benchmark_priors/retail_consumer_supported_v2/summary.md)
- [retail_consumer_supported_v3_67](../../outputs/company_valuation/benchmark_priors/retail_consumer_supported_v3_67/summary.md)

Result:

- broad retail tuning alone was not enough
- after demoting `KMX`, the supported-only cohort improved to:
  - mean abs gap delta: `0.4091`
  - sign mismatch count: `3`

Interpretation:

- the retail template is still not benchmark-cleared
- but `KMX` was the right name to remove
- the remaining retail cohort is cleaner and more informative

### Retail Consumer Compounder

Updated artifact:

- [retail_consumer_compounder_supported_v2](../../outputs/company_valuation/benchmark_priors/retail_consumer_compounder_supported_v2/summary.md)

Result:

- mean abs gap delta: `0.4436 -> 0.3150`
- sign mismatch count: `5 -> 3`

Interpretation:

- this was the most successful calibration pass
- the template still skews conservative
- but it now looks directionally closer to a defensible supported cohort

## Software Structural Split

The first calibration pass improved software, but the remaining pattern was no longer “one template is somewhat conservative.”

It was:

- mature franchise software names were mixed
- workflow and vertical software names were still systematically below analyst priors
- mission-critical software was still badly undervalued

That was enough evidence to stop broad tuning and split the supported software cohort structurally.

### New Supported Software Cohorts

Supported software now routes through:

- `software_franchise_platform`
  - `ADBE`, `CHKP`, `INTU`, `MSFT`, `ORCL`, `SSNC`
- `software_workflow_growth`
  - `ADSK`, `CRM`, `GWRE`, `HUBS`, `NOW`, `PAYC`, `TYL`, `VEEV`
- `software_mission_critical`
  - `CDNS`, `FICO`, `SNPS`

`software_asset_light` remains in config only as a generic fallback for unsupported software names outside the curated cohort.

### Split Results

Shadow routing stayed clean after the split:

- supported universe still: `67/67`
- shadow expected-template mismatches: `0`
- supported template mismatches: `0`
- artifact: [supported_software_split_v1](../../outputs/company_valuation/taxonomy_sync/supported_software_split_v1/summary.md)

Per-cohort benchmark results:

- franchise: [software_franchise_platform_supported_v1b](../../outputs/company_valuation/benchmark_priors/software_franchise_platform_supported_v1b/summary.md)
  - rows: `6`
  - mean abs gap delta: `0.4997`
  - sign mismatch count: `2`
- workflow: [software_workflow_growth_supported_v1b](../../outputs/company_valuation/benchmark_priors/software_workflow_growth_supported_v1b/summary.md)
  - rows: `8`
  - target field: `median_target`
  - mean abs gap delta: `0.3442`
  - sign mismatch count: `1`
- mission critical after one narrow calibration pass: [software_mission_critical_supported_v2](../../outputs/company_valuation/benchmark_priors/software_mission_critical_supported_v2/summary.md)
  - rows: `3`
  - mean abs gap delta: `0.6261 -> 0.3189`
  - sign mismatch count: `2`

Combined software result after the split:

- rows: `17`
- mean abs gap delta: `0.5345 -> 0.3946`
- sign mismatch count: `7 -> 5`

Critical read:

- the split was worth doing
- the workflow cohort is now much closer to a defensible supported state
- the mission-critical cohort materially improved after one targeted recalibration
- the franchise cohort is still the weakest structural holdout, mainly because `ADBE` screens much richer than the analyst prior while `MSFT` and `ORCL` still screen below it

That means the correct posture now is:

- keep the structural split
- keep the workflow and mission-critical calibrations
- do not do another broad software tuning pass by default
- only reopen software if the franchise cohort becomes important enough to justify another benchmark-gated review

## What To Do Next

Do **not** reopen taxonomy work.

Do **not** broaden the supported universe.

Do **not** do freehand template tuning without using these benchmark packs.

The right next sequence is now:

1. freeze the supported universe at `67`
2. keep the software structural split and the current workflow / mission-critical calibrations
3. keep the retail `v2` calibration only together with the `KMX` demotion
4. do not broaden support until the remaining weak supported names are benchmark-reviewed
5. only after more benchmark coverage in industrial, aerospace, and base energy, revisit whether the universe is close to fully signoff-ready
