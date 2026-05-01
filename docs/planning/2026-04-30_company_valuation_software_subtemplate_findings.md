# Company Valuation Software Sub-Template Findings

Status: research findings

As of: Thursday, April 30, 2026

Related:

- [Company Valuation ML Template Discovery Plan](./2026-04-29_company_valuation_ml_template_discovery_plan.md)
- [Company Valuation Industrial And Energy Clustering Findings](./2026-04-30_company_valuation_industrial_energy_clustering_findings.md)

## Role Of This Doc

This document records the first focused software sub-template research pass for the company valuation engine.

It extends the earlier industrial and energy template work by asking a narrower question:

- is the broad `software_asset_light` bucket stable enough to keep as one active template
- or is there enough evidence to promote a deterministic software split now

This is still a research note, not a live runtime contract.

## Why This Pass Was Needed

The earlier 18-name software baseline showed a real but incomplete internal split:

- best `MiniBatchKMeans`: `k=3`
- one clear high-growth, lower-margin branch
- one broad profitable-compounder branch
- one much smaller mixed branch around `MSFT`, `ORCL`, and `PAYC`

That was enough to justify a focused candidate test, but not enough to justify immediate promotion.

## Software Calibration Basket

This pass used the same `18` U.S.-listed software and software-platform names already validated for 10-year PIT export depth:

- `MSFT`
- `ORCL`
- `CRM`
- `ADBE`
- `INTU`
- `NOW`
- `ADSK`
- `SNPS`
- `CDNS`
- `WDAY`
- `PANW`
- `FTNT`
- `HUBS`
- `VEEV`
- `TYL`
- `PAYC`
- `SSNC`
- `FICO`

## Research Config Roots

Broad software baseline root:

- [packages/config/company_valuation_research_software_baseline](/Users/adeb/Projects/spreads/packages/config/company_valuation_research_software_baseline)

Candidate split root:

- [packages/config/company_valuation_research_software_candidate](/Users/adeb/Projects/spreads/packages/config/company_valuation_research_software_candidate)

The candidate root adds two research-only template IDs:

- `software_compounder_core`
- `growth_transition_software`

## Candidate Split Applied

The candidate split intentionally stayed simple.

`growth_transition_software`:

- `CRM`
- `HUBS`
- `NOW`
- `PANW`
- `WDAY`

`software_compounder_core`:

- `ADBE`
- `ADSK`
- `CDNS`
- `FICO`
- `FTNT`
- `INTU`
- `MSFT`
- `ORCL`
- `PAYC`
- `SNPS`
- `SSNC`
- `TYL`
- `VEEV`

This was a deliberate `2`-way split, not a forced `3`-way promotion.

Reason:

- the high-growth branch had the clearest business identity
- the smaller `MSFT/ORCL/PAYC` branch was not clean enough to promote on its own

## Dataset Artifacts

Baseline export:

- [outputs/company_valuation/research_software_baseline_10y](/Users/adeb/Projects/spreads/outputs/company_valuation/research_software_baseline_10y)
- [baseline cluster_summary.json](/Users/adeb/Projects/spreads/outputs/company_valuation/research_software_baseline_10y/feature_version=v1/analysis/cluster_summary.json)

Candidate export:

- [outputs/company_valuation/research_software_candidate_10y](/Users/adeb/Projects/spreads/outputs/company_valuation/research_software_candidate_10y)
- [candidate cluster_summary.json](/Users/adeb/Projects/spreads/outputs/company_valuation/research_software_candidate_10y/feature_version=v1/analysis/cluster_summary.json)

Shared export stats:

- `10` years
- `721` PIT rows
- `18` issuers exported

Candidate split row counts:

- `520` software compounder core rows
- `201` growth-transition software rows

## Baseline Reminder

Refreshed baseline result:

- best `MiniBatchKMeans` for `software_asset_light`: `k=3`
- silhouette `0.241615`
- rolling stability ARI `0.65989`

Dominant baseline cluster patterns:

- profitable compounders:
  - `ADBE`
  - `CDNS`
  - `FICO`
  - `INTU`
  - `MSFT`
  - `SNPS`
  - `SSNC`
  - `TYL`
  - `VEEV`
- high-growth / margin-transition names:
  - `CRM`
  - `HUBS`
  - `NOW`
  - `PANW`
  - `WDAY`
- smaller mixed branch:
  - `MSFT`
  - `ORCL`
  - `PAYC`

Interpretation:

- one real split exists
- but the whole family is still not clean enough for a blind `3`-way promotion

## Candidate Split Findings

### 1. The growth-transition branch is directionally real

Candidate clustering inside `growth_transition_software`:

- rows: `201`
- issuers: `5`
- best `MiniBatchKMeans`: `k=2`
- silhouette `0.210329`
- rolling stability ARI `0.210176`

Interpretation:

- this branch is meaningfully different from the broad software baseline
- but it still has visible internal structure
- stability is too weak to call it fully mature

### 2. The software-compounder branch is still broad

Candidate clustering inside `software_compounder_core`:

- rows: `520`
- issuers: `13`
- best `MiniBatchKMeans`: `k=2`
- silhouette `0.140944`
- rolling stability ARI `0.131952`

Interpretation:

- the compounder core is still hiding at least one additional multiple regime
- likely around:
  - premium application compounders
  - platform / suite / infrastructure hybrids

This is the biggest reason not to promote the split yet.

### 3. The split reduced software optimism, but not cleanly enough

Latest filing-date snapshot medians across all `18` names:

- baseline quality:
  - `68.77`
- candidate quality:
  - `69.68`
- baseline intrinsic value:
  - `271.46`
- candidate intrinsic value:
  - `228.20`
- baseline valuation gap:
  - `+19.38%`
- candidate valuation gap:
  - `+17.33%`
- baseline valuation confidence:
  - `0.6423`
- candidate valuation confidence:
  - `0.6395`

Interpretation:

- the candidate split did reduce broad software optimism
- but the improvement is not large enough at the whole-basket level to justify active promotion

## Branch-Level Before / After

Using the latest filing-date snapshot per ticker:

### Growth Transition Software

- issuers: `5`
- baseline quality:
  - `67.89`
- candidate quality:
  - `67.03`
- baseline intrinsic value:
  - `270.97`
- candidate intrinsic value:
  - `226.14`
- baseline valuation gap:
  - `+44.07%`
- candidate valuation gap:
  - `+19.34%`
- baseline valuation confidence:
  - `0.6226`
- candidate valuation confidence:
  - `0.6103`

Interpretation:

- this is the clearest success in the software pass
- the growth-transition branch was meaningfully too optimistic under the broad template
- the candidate branch cut that optimism materially

### Software Compounder Core

- issuers: `13`
- baseline quality:
  - `76.28`
- candidate quality:
  - `76.96`
- baseline intrinsic value:
  - `271.96`
- candidate intrinsic value:
  - `255.40`
- baseline valuation gap:
  - `+15.75%`
- candidate valuation gap:
  - `+11.15%`
- baseline valuation confidence:
  - `0.6581`
- candidate valuation confidence:
  - `0.6538`

Interpretation:

- the compounder branch did not improve enough to justify a promotion on its own
- it still looks like at least two businesses are being mixed together

## Biggest Candidate Changes

Largest intrinsic-value reductions on the latest filing-date snapshot:

- `INTU`: `-65.89`
- `FICO`: `-59.02`
- `CRM`: `-47.72`
- `WDAY`: `-44.83`
- `PANW`: `-27.03`

Small positive changes:

- `ORCL`: `+5.38`
- `SSNC`: `+2.01`
- `PAYC`: `+0.20`

Interpretation:

- the split mostly acts as a de-optimism pass
- that is useful for the growth-transition names
- it is too blunt for the broader compounder group

## Conclusion

This software pass found one real idea and one unresolved problem.

The real idea:

- `growth_transition_software` is directionally valid

The unresolved problem:

- `software_compounder_core` is still too heterogeneous for promotion

That means the right decision today is:

- keep both software draft templates as research-only
- do not promote either into the active engine yet
- keep active `software_asset_light` in place for now

## Recommended Next Step

The next software pass should not be a blanket template promotion.

It should be a narrower follow-up on the compounder side:

- test whether `MSFT`, `ORCL`, and `PAYC` represent a real `platform_suite` or `platform_hybrid` branch
- test whether the remaining premium application names form a cleaner deterministic `software_compounder_core`

Until that is done, promoting the current software split would be premature.

## Sources

- Current software-sector benchmark framing and holdings context as of `April 29, 2026`: [iShares Expanded Tech-Software Sector ETF (IGV)](https://www.ishares.com/us/products/239771/ishares-north-american-techsoftware-etf)
