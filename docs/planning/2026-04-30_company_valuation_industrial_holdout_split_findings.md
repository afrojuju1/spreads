# Company Valuation Industrial Holdout Split Findings

Status: research findings

As of: Thursday, April 30, 2026

Related:

- [Company Valuation Industrial And Energy Clustering Findings](./2026-04-30_company_valuation_industrial_energy_clustering_findings.md)
- [Company Valuation 30-Name Template Recalibration Findings](./2026-04-30_company_valuation_30_name_template_recalibration_findings.md)

## Role Of This Doc

This document records the focused follow-up pass on the unresolved industrial holdout bucket.

The prior 30-name industrial study showed that the full industrial cohort was too heterogeneous to justify one clean rewrite of `industrial_manufacturing`.

This pass narrowed the question to:

- the original 5-name industrial holdout
- adjacent aerospace and defense names
- adjacent diversified-industrial names

The goal was to test whether the unresolved industrial remainder splits more cleanly there than in the full 30-name industrial basket.

## Why This Follow-Up Was Needed

The broad 30-name industrial run said two things at once:

- the overall industrial bucket is too broad
- the remaining holdout slice still looked structurally different from the industrial core

That made a focused holdout-only pass the right next move.

Research inputs used to shape the narrower cohort:

- `XLI` industry allocation as of `April 24, 2026`, which still showed industrials heavily weighted toward `Aerospace & Defence` and a smaller but distinct `Industrial Conglomerates` sleeve
- `XAR` top holdings and benchmark definition as of `April 29, 2026`, which reinforced the current public aerospace and defense peer set
- current market-cap ranking checks for large U.S. aerospace names as of `April 2026`

## Focused Holdout Cohort

The focused basket was kept to `19` names:

- `GE`
- `RTX`
- `BA`
- `GD`
- `MMM`
- `HON`
- `LMT`
- `NOC`
- `LHX`
- `TDG`
- `HEI`
- `HWM`
- `TXT`
- `ITW`
- `DOV`
- `ROK`
- `IR`
- `FTV`
- `AME`

## Research Config Roots

Broad holdout baseline root:

- [packages/config/company_valuation_research_industrial_holdout_baseline](/Users/adeb/Projects/spreads/packages/config/company_valuation_research_industrial_holdout_baseline)

Candidate split root:

- [packages/config/company_valuation_research_industrial_holdout_candidate](/Users/adeb/Projects/spreads/packages/config/company_valuation_research_industrial_holdout_candidate)

The candidate root adds two research-only template IDs:

- `aerospace_defense_prime`
- `diversified_industrial_core`

## Dataset Artifacts

Baseline export:

- [outputs/company_valuation/research_industrial_holdout_baseline_10y](/Users/adeb/Projects/spreads/outputs/company_valuation/research_industrial_holdout_baseline_10y)
- [baseline cluster_summary.json](/Users/adeb/Projects/spreads/outputs/company_valuation/research_industrial_holdout_baseline_10y/feature_version=v1/analysis/cluster_summary.json)

Candidate export:

- [outputs/company_valuation/research_industrial_holdout_candidate_10y](/Users/adeb/Projects/spreads/outputs/company_valuation/research_industrial_holdout_candidate_10y)
- [candidate cluster_summary.json](/Users/adeb/Projects/spreads/outputs/company_valuation/research_industrial_holdout_candidate_10y/feature_version=v1/analysis/cluster_summary.json)

Shared export stats:

- `10` years
- `752` PIT rows
- `19` issuers exported

Candidate split row counts:

- `435` aerospace-defense rows
- `317` diversified-industrial rows

## Main Learnings

### 1. The focused holdout cohort does split more cleanly than the full 30-name industrial set

Baseline result:

- best `MiniBatchKMeans` for the full focused cohort: `k=2`

The dominant mapping was directionally clean:

- aerospace and defense names mostly sat together:
  - `BA`
  - `GD`
  - `GE`
  - `HWM`
  - `LHX`
  - `LMT`
  - `NOC`
  - `RTX`
  - `TXT`
- diversified names mostly sat together:
  - `AME`
  - `FTV`
  - `HON`
  - `IR`
  - `ITW`
  - `MMM`
  - `ROK`
- mixed bridge names remained:
  - `HEI`
  - `TDG`
  - parts of `DOV`

Interpretation:

- the broader industrial cohort was hiding a real structural split
- the unresolved industrial remainder is not one random bucket
- the cleanest deterministic next cut is aerospace/defense versus diversified industrials

### 2. The candidate split improved valuation medians in both branches

Latest-snapshot medians by candidate branch:

#### Aerospace Defense Prime

- issuers: `11`
- median quality:
  - baseline `56.12`
  - candidate `57.58`
- median intrinsic value:
  - baseline `108.56`
  - candidate `132.15`
- median valuation gap:
  - baseline `-66.02%`
  - candidate `-58.17%`

Interpretation:

- the branch moved in the right direction without blowing out quality scores
- the split helped the most on better aerospace and defense names where the broad industrial baseline was too blunt

#### Diversified Industrial Core

- issuers: `8`
- median quality:
  - baseline `58.34`
  - candidate `58.94`
- median intrinsic value:
  - baseline `111.95`
  - candidate `131.70`
- median valuation gap:
  - baseline `-48.53%`
  - candidate `-40.39%`

Interpretation:

- the diversified branch also improved, but less dramatically than aerospace/defense
- this looks like a real refinement, not just cluster relabeling

### 3. Aerospace and defense still has visible internal structure

Inside `aerospace_defense_prime`, the candidate clustering still found substructure:

- `TDG` stood out strongly on its own
- `HEI`, `HWM`, and `LHX` showed partial supplier/aftermarket behavior
- `LMT` produced a distinctive low-`ev_fcf`, high-`pb` shape in one sub-cluster

Interpretation:

- a future split between defense primes and aerospace suppliers/aftermarket may be real
- it is not necessary yet for the next deterministic template promotion

### 4. Diversified industrials also has a lighter internal multiple split

Inside `diversified_industrial_core`, the clustering still showed a smaller two-way shape:

- higher-multiple profiles around names like `FTV`, `IR`, and parts of `ROK`
- lower-multiple profiles around names like `DOV`, `HON`, `ITW`, and `MMM`

Interpretation:

- there may eventually be a premium-process/automation versus slower diversified split
- that is not yet strong enough to justify another template promotion from this sample alone

## Biggest Intrinsic-Value Changes

Largest positive candidate minus baseline intrinsic-value deltas on the latest snapshot:

- `TDG`: `+167.07`
- `NOC`: `+96.97`
- `LHX`: `+70.76`
- `GD`: `+54.47`
- `LMT`: `+51.28`
- `RTX`: `+27.02`
- `ROK`: `+25.18`
- `ITW`: `+24.52`

Smallest positive deltas:

- `FTV`: `+4.71`
- `IR`: `+5.12`
- `HWM`: `+13.76`
- `MMM`: `+16.70`
- `AME`: `+17.47`

## Conclusion

This focused pass is materially stronger than the broad 30-name industrial result.

The main takeaway is:

- `industrial_manufacturing` should not absorb the whole unresolved industrial remainder
- the next industrial refinement should be:
  - `aerospace_defense_prime`
  - `diversified_industrial_core`

This is now a better-supported promotion candidate than the earlier broad industrial rewrite.

## Recommended Next Step

Promote these as the next industrial research-approved template candidates:

- `aerospace_defense_prime`
- `diversified_industrial_core`

Do not split either branch further yet.

Keep these as watch items for the next pass:

- `TDG`, `HEI`, `HWM`, and `LHX` for a possible aerospace supplier/aftermarket branch
- `GE` because its post-breakup history still carries entity-transition noise
- `BA` because its operating profile is unusually distorted by execution recovery risk

## Sources

- `XLI` holdings and industry allocation as of `April 24, 2026`: [State Street Industrial Select Sector SPDR](https://www.ssga.com/mainfund/xli)
- `XAR` holdings and benchmark definition as of `April 29, 2026`: [State Street SPDR S&P Aerospace & Defense ETF](https://www.ssga.com/us/en/intermediary/etfs/state-street-spdr-sp-aerospace-defense-etf-xar)
- U.S. aerospace market-cap peer checks as of `April 2026`: [Largest aerospace companies by market cap](https://companiesmarketcap.com/aerospace/largest-companies-by-market-cap)
