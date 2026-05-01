# Company Valuation Software Compounder Follow-Up Findings

Status: research findings

As of: Thursday, April 30, 2026

Related:

- [Company Valuation Software Sub-Template Findings](./2026-04-30_company_valuation_software_subtemplate_findings.md)
- [Company Valuation ML Template Discovery Plan](./2026-04-29_company_valuation_ml_template_discovery_plan.md)

## Role Of This Doc

This document records the narrow follow-up on the unresolved software compounder bucket.

The prior software pass found:

- a directionally valid `growth_transition_software` branch
- but an unresolved `software_compounder_core` bucket that still looked too broad

This follow-up asked one focused question:

- does that compounder bucket split cleanly enough into `platform_suite_software` and `premium_application_compounder` to justify promotion

## Focused Cohort

This pass narrowed the software compounder follow-up to `8` names:

- `MSFT`
- `ORCL`
- `PAYC`
- `ADBE`
- `INTU`
- `CDNS`
- `SNPS`
- `VEEV`

These were chosen because they were the highest-value unresolved names from the prior software pass.

## Research Config Roots

Broad follow-up baseline root:

- [packages/config/company_valuation_research_software_compounder_followup_baseline](/Users/adeb/Projects/spreads/packages/config/company_valuation_research_software_compounder_followup_baseline)

Candidate split root:

- [packages/config/company_valuation_research_software_compounder_followup_candidate](/Users/adeb/Projects/spreads/packages/config/company_valuation_research_software_compounder_followup_candidate)

The candidate root adds two research-only template IDs:

- `platform_suite_software`
- `premium_application_compounder`

## Candidate Split Applied

`platform_suite_software`:

- `MSFT`
- `ORCL`
- `PAYC`

`premium_application_compounder`:

- `ADBE`
- `INTU`
- `CDNS`
- `SNPS`
- `VEEV`

## Dataset Artifacts

Baseline export:

- [outputs/company_valuation/research_software_compounder_followup_baseline_10y](/Users/adeb/Projects/spreads/outputs/company_valuation/research_software_compounder_followup_baseline_10y)
- [baseline cluster_summary.json](/Users/adeb/Projects/spreads/outputs/company_valuation/research_software_compounder_followup_baseline_10y/feature_version=v1/analysis/cluster_summary.json)

Candidate export:

- [outputs/company_valuation/research_software_compounder_followup_candidate_10y](/Users/adeb/Projects/spreads/outputs/company_valuation/research_software_compounder_followup_candidate_10y)
- [candidate cluster_summary.json](/Users/adeb/Projects/spreads/outputs/company_valuation/research_software_compounder_followup_candidate_10y/feature_version=v1/analysis/cluster_summary.json)

Shared export stats:

- `10` years
- `319` PIT rows
- `8` issuers exported

Candidate split row counts:

- `120` platform-suite rows
- `199` premium-application rows

## Baseline Findings

The follow-up baseline result was more fragmented than expected.

Broad baseline result:

- best `MiniBatchKMeans`: `k=6`
- silhouette `0.233333`
- rolling stability ARI `0.663912`

Dominant mapping:

- `MSFT` was a clean standalone dominant branch
- `PAYC` was a clean standalone dominant branch
- `ORCL` was internally split
- `ADBE`, `INTU`, `CDNS`, `SNPS`, and `VEEV` did not collapse into one single clean premium branch

Interpretation:

- the unresolved compounder bucket is not just one hidden 2-way split
- it likely contains several distinct business/multiple regimes

## Candidate Split Findings

### 1. The platform-suite branch is directionally valid

Latest filing-date snapshot medians for the `3` platform-suite names:

- baseline quality:
  - `70.07`
- candidate quality:
  - `71.28`
- baseline intrinsic value:
  - `196.92`
- candidate intrinsic value:
  - `189.11`
- baseline valuation gap:
  - `+20.65%`
- candidate valuation gap:
  - `+5.18%`
- baseline valuation confidence:
  - `0.6368`
- candidate valuation confidence:
  - `0.6380`

Interpretation:

- this branch moved in the right direction
- the candidate template reduced optimism materially without damaging confidence
- `platform_suite_software` is now a legitimate future promotion candidate

### 2. The premium-application branch did not help enough

Latest filing-date snapshot medians for the `5` premium-application names:

- baseline quality:
  - `77.72`
- candidate quality:
  - `78.14`
- baseline intrinsic value:
  - `255.40`
- candidate intrinsic value:
  - `276.83`
- baseline valuation gap:
  - `+38.75%`
- candidate valuation gap:
  - `+50.39%`
- baseline valuation confidence:
  - `0.6538`
- candidate valuation confidence:
  - `0.6535`

Interpretation:

- this branch mostly made already-rich names look even richer
- that is the opposite of what we want from a calibration refinement
- `premium_application_compounder` is not ready for promotion

### 3. The overall candidate split is not strong enough to promote

Latest filing-date snapshot medians across the full `8`-name cohort:

- baseline quality:
  - `76.22`
- candidate quality:
  - `76.53`
- baseline intrinsic value:
  - `226.16`
- candidate intrinsic value:
  - `237.12`
- baseline valuation gap:
  - `+29.70%`
- candidate valuation gap:
  - `+27.79%`
- baseline valuation confidence:
  - `0.6480`
- candidate valuation confidence:
  - `0.6468`

Interpretation:

- the split helped one branch
- but the combined result is not strong enough to justify active promotion

## Per-Ticker Highlights

Names helped by the premium-application branch:

- `INTU`: intrinsic value `+87.04`
- `ADBE`: intrinsic value `+48.08`
- `VEEV`: intrinsic value `+21.43`
- `CDNS`: intrinsic value `+18.20`
- `SNPS`: intrinsic value `+12.25`

Names de-optimized by the platform-suite branch:

- `MSFT`: intrinsic value `-37.51`
- `ORCL`: intrinsic value `-23.11`
- `PAYC`: intrinsic value `-7.80`

Interpretation:

- the platform-suite direction looks useful
- the premium-application branch still needs a tighter rule set or a larger cohort before it can be trusted

## Conclusion

This follow-up did not produce a full software promotion decision.

What it did produce:

- a stronger case for `platform_suite_software`
- a weaker case for `premium_application_compounder`

That means the right decision is:

- keep both as research-only
- do not promote either into the active engine yet
- treat `platform_suite_software` as the next software branch worth revisiting when the cohort is wider

## Recommended Next Step

If software calibration continues later, the next useful move is:

- widen `platform_suite_software` beyond `MSFT`, `ORCL`, and `PAYC`
- add adjacent names that test whether the branch is actually a real business model group rather than a 3-name artifact

Do not do another premium-application rewrite until that branch can stop inflating already-rich names.
