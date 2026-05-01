# Company Valuation Industrial And Aerospace Benchmark Findings

Status: baseline benchmark coverage added; one calibration pass applied for diversified industrial and industrial compounder

As of: Friday, May 1, 2026

Related:

- [Company Valuation Standards-Based Taxonomy Plan](./2026-04-30_company_valuation_standards_based_taxonomy_plan.md)
- [Company Valuation Software And Retail Benchmark Findings](./2026-05-01_company_valuation_software_retail_benchmark_findings.md)

## Role Of This Doc

This note records the first benchmark-gated validation pass for the next three supported non-software cohorts:

- `diversified_industrial_core`
- `industrial_compounder`
- `aerospace_defense_prime`

The goal of this pass was not to tune templates immediately.

The goal was to answer:

- which cohorts are broadly coherent but conservative
- which cohorts are structurally mixed
- where further work should be calibration versus cohort surgery

All three packs use StockAnalysis analyst-prior snapshots captured on `2026-05-01`.

## Diversified Industrial Core

Artifact:

- [diversified_industrial_core_supported_v1](../../outputs/company_valuation/benchmark_priors/diversified_industrial_core_supported_v1/summary.md)

Headline metrics:

- rows compared: `7`
- mean valuation gap: `-0.4116`
- mean benchmark gap: `+0.0665`
- mean abs gap delta: `0.4781`
- sign mismatch count: `6`
- under benchmark count: `7`
- over benchmark count: `0`

Interpretation:

- this is a clean conservative-template signal
- the whole cohort is below benchmark
- the direction is too consistent to dismiss as source noise or single-name drift

Worst names:

- `ROK`
- `IR`
- `DOV`
- `HON`
- `ITW`

Decision:

- **one calibration pass is warranted**
- do not split the cohort yet
- this looks like the current template is simply too punitive for high-quality diversified industrial platforms

### Diversified Industrial Calibration Pass

Updated artifact:

- [diversified_industrial_core_supported_v2](../../outputs/company_valuation/benchmark_priors/diversified_industrial_core_supported_v2/summary.md)

Result:

- mean abs gap delta: `0.4781 -> 0.3447`
- mean valuation gap: `-0.4116 -> -0.2783`
- sign mismatch count: `6 -> 6`

Interpretation:

- the pass clearly helped
- but the cohort still screens uniformly below benchmark
- this no longer looks like a small-parameter miss; it still suggests the template is too punitive or the cohort definition is too broad for one template

Decision after the pass:

- **keep the v2 calibration**
- **do not do another immediate pass**
- revisit only after deciding whether this cohort should split or whether a few names should move elsewhere

## Industrial Compounder

Artifact:

- [industrial_compounder_supported_v1](../../outputs/company_valuation/benchmark_priors/industrial_compounder_supported_v1/summary.md)

Headline metrics:

- rows compared: `6`
- mean valuation gap: `-0.3475`
- mean benchmark gap: `+0.0254`
- mean abs gap delta: `0.3729`
- sign mismatch count: `3`
- under benchmark count: `6`
- over benchmark count: `0`

Interpretation:

- this is also a broad conservative-template signal
- not as severe as diversified industrial, but still directionally clear
- the cohort is coherent enough that a single pass is justified

Worst names:

- `CAT`
- `ETN`
- `EMR`
- `DE`
- `CMI`

Best-aligned name:

- `IEX`

Decision:

- **one calibration pass is warranted**
- keep the cohort intact for now
- this still looks like a template-level issue, not a support-boundary failure

### Industrial Compounder Calibration Pass

Updated artifact:

- [industrial_compounder_supported_v2](../../outputs/company_valuation/benchmark_priors/industrial_compounder_supported_v2/summary.md)

Result:

- mean abs gap delta: `0.3729 -> 0.2411`
- mean valuation gap: `-0.3475 -> -0.1841`
- sign mismatch count: `3 -> 2`

Interpretation:

- this was a successful pass
- the cohort is much closer to a defensible supported state
- a few names still screen conservative, especially `CAT` and `ETN`, but the broad template no longer looks fundamentally broken

Decision after the pass:

- **keep the v2 calibration**
- **stop tuning this cohort for now**
- only reopen it if later benchmark coverage shows persistent drift

## Aerospace Defense Prime

Artifact:

- [aerospace_defense_prime_supported_v1](../../outputs/company_valuation/benchmark_priors/aerospace_defense_prime_supported_v1/summary.md)

Headline metrics:

- rows compared: `6`
- mean valuation gap: `-0.2486`
- mean benchmark gap: `+0.1282`
- mean abs gap delta: `0.4347`
- sign mismatch count: `4`
- under benchmark count: `4`
- over benchmark count: `2`

Interpretation:

- this is **not** the same pattern as the other two cohorts
- `LMT`, `NOC`, `RTX`, and `LHX` screen far below benchmark
- `GD` and `TXT` already screen positive and close enough
- a broad loosening pass would risk helping the weak names by over-loosening the already-acceptable ones

Worst names:

- `NOC`
- `LMT`
- `RTX`
- `LHX`

Relatively acceptable names:

- `GD`
- `TXT`

Decision:

- **do not do a broad calibration pass first**
- treat this as a likely **cohort-mix problem**
- next work here should be a narrower review:
  - either split the prime cohort further
  - or decide whether one or two names are simply weak fits for the current supported template

### Aerospace Prime Structural Split

The mixed pattern held up under closer review:

- `GD` and `TXT` were already acceptable
- `LHX`, `LMT`, `NOC`, and `RTX` behaved like a separate defense-platform regime

That led to a structural split:

- keep `GD` and `TXT` in `aerospace_defense_prime`
- move `LHX`, `LMT`, `NOC`, and `RTX` into a new `defense_platform_systems` cohort

Shadow routing stayed clean after the split:

- [supported_aerospace_split_v1](../../outputs/company_valuation/taxonomy_sync/supported_aerospace_split_v1/summary.md)
- supported universe still `67/67`
- supported template mismatches: `0`

The old mixed-prime baseline split cleanly into:

- `GD/TXT` residual prime bucket:
  - mean abs gap delta: `0.0869`
  - sign mismatch count: `0`
- `LHX/LMT/NOC/RTX` defense-platform bucket:
  - mean abs gap delta: `0.6086`
  - sign mismatch count: `4`

That is exactly the pattern that justifies a structural split.

### Defense Platform Systems Calibration Pass

Baseline artifact:

- [defense_platform_systems_supported_v1b](../../outputs/company_valuation/benchmark_priors/defense_platform_systems_supported_v1b/summary.md)

Result before calibration:

- rows: `4`
- mean abs gap delta: `0.4455`
- sign mismatch count: `4`

One narrow calibration pass was then applied.

Updated artifact:

- [defense_platform_systems_supported_v2](../../outputs/company_valuation/benchmark_priors/defense_platform_systems_supported_v2/summary.md)

Result after calibration:

- mean abs gap delta: `0.4455 -> 0.2719`
- mean valuation gap: `-0.2805 -> -0.1069`
- sign mismatch count: `4 -> 3`

Interpretation:

- the split plus one pass was worth doing
- `LHX` is now close enough
- `RTX` is much closer
- `LMT` and `NOC` still screen materially conservative

Decision after the pass:

- **keep the split**
- **keep the v2 defense-platform template**
- **stop here for now**

This is the right stop because another loosening pass would increasingly become curve-fitting to four names.

## Source-Sanity Check

I also checked whether switching to `median_target` materially changes the picture.

It does not:

- diversified industrial median mean abs gap delta: `0.4712`
- industrial compounder median mean abs gap delta: `0.3889`
- aerospace prime median mean abs gap delta: `0.4574`

That matters because it means these are mostly **real model/cohort issues**, not just noisy analyst-average artifacts.

## Bottom Line

The three cohorts separate cleanly into two buckets:

1. **Calibrate next**
   - `diversified_industrial_core`
   - `industrial_compounder`

2. **Do not broadly tune yet**
   - `aerospace_defense_prime`

The right next sequence is:

1. keep `industrial_compounder` v2 as the current calibrated state
2. keep `diversified_industrial_core` v2, but treat it as still unresolved
3. keep the aerospace split and the `defense_platform_systems` v2 template
4. if aerospace is reopened later, review `LMT` and `NOC` first rather than globally loosening the cohort
5. only after that, decide whether diversified industrial also needs a structural split instead of more template tuning
