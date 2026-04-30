# Company Valuation 30-Name Template Recalibration Findings

Status: research findings

As of: Thursday, April 30, 2026

Related:

- [Company Valuation ML Template Discovery Plan](./2026-04-29_company_valuation_ml_template_discovery_plan.md)
- [Company Valuation Industrial And Energy Clustering Findings](./2026-04-30_company_valuation_industrial_energy_clustering_findings.md)

## Role Of This Doc

This document records the first **30-name per industry** recalibration pass for the company valuation engine.

It extends the earlier 10-name industrial and energy clustering study by:

- expanding both cohorts to `30` names each
- holding the historical window at `10 years`
- testing a research-only broad-cohort baseline
- testing a research-only candidate sub-template split

This is still a research note, not a live runtime contract.

## Why The 30-Name Expansion Matters

The 10-name pilot was directionally useful, but too narrow to trust for promotion.

The larger 30-name baskets were chosen to be:

- `U.S.-domiciled`
- `currently active`
- `long-history` enough for 10-year point-in-time exports
- broad enough to stress template stability

Research inputs used to shape the larger baskets:

- current `XLI` top holdings and industry mix as of `April 29, 2026`
- current `XLE` top holdings and industry mix as of `April 29, 2026`
- current public status checks for names removed by M&A or too-recent rebrands such as `HES`, `MRO`, and `EXE`

## Expanded Calibration Baskets

Industrial basket:

- `CAT`
- `DE`
- `GE`
- `RTX`
- `BA`
- `HON`
- `PH`
- `PWR`
- `CMI`
- `GD`
- `EMR`
- `MMM`
- `ITW`
- `PCAR`
- `DOV`
- `ROK`
- `XYL`
- `AME`
- `HUBB`
- `NDSN`
- `FTV`
- `FAST`
- `GWW`
- `WAB`
- `URI`
- `IR`
- `IEX`
- `SNA`
- `AOS`
- `TXT`

Energy basket:

- `XOM`
- `CVX`
- `COP`
- `EOG`
- `OXY`
- `VLO`
- `MPC`
- `PSX`
- `WMB`
- `KMI`
- `OKE`
- `TRGP`
- `DVN`
- `APA`
- `EQT`
- `FANG`
- `HAL`
- `BKR`
- `LNG`
- `MUR`
- `MTDR`
- `SM`
- `AR`
- `CNX`
- `PBF`
- `PTEN`
- `NOV`
- `RRC`
- `HP`
- `DK`

## Research Config Roots

Broad-cohort baseline root:

- [packages/config/company_valuation_research_baseline_30](/Users/adeb/Projects/spreads/packages/config/company_valuation_research_baseline_30)

Candidate sub-template root:

- [packages/config/company_valuation_research_candidate_30](/Users/adeb/Projects/spreads/packages/config/company_valuation_research_candidate_30)

The candidate root adds:

- `industrial_core_compounder`
- `cyclical_capital_goods`
- `stressed_operator`

and reassigns only selected issuers into those draft templates.

## Dataset Artifacts

Broad baseline export:

- [outputs/company_valuation/research_calibration_30_baseline_10y](/Users/adeb/Projects/spreads/outputs/company_valuation/research_calibration_30_baseline_10y)
- [baseline cluster_summary.json](/Users/adeb/Projects/spreads/outputs/company_valuation/research_calibration_30_baseline_10y/analysis/cluster_summary.json)

Candidate export:

- [outputs/company_valuation/research_calibration_30_candidate_10y](/Users/adeb/Projects/spreads/outputs/company_valuation/research_calibration_30_candidate_10y)
- [candidate cluster_summary.json](/Users/adeb/Projects/spreads/outputs/company_valuation/research_calibration_30_candidate_10y/analysis/cluster_summary.json)

Broad baseline export stats:

- `2,245` PIT rows
- `60` issuers exported
- `1,148` industrial rows
- `1,097` energy rows

Candidate export stats:

- `2,245` PIT rows
- `60` issuers exported
- `690` industrial core compounder rows
- `256` cyclical capital goods rows
- `202` industrial holdout rows
- `682` broad energy rows
- `415` stressed-operator rows

## Main Learnings

### 1. The 30-name industrial result is weaker than the 10-name pilot

The earlier 10-name pilot suggested a clear industrial split.

The larger broad industrial cohort did **not** preserve that shape cleanly.

Broad baseline result:

- best KMeans selection for `industrial_manufacturing`: `k=2`
- one cluster was effectively a `URI` outlier
- the rest of the industrial basket collapsed into one dominant broad core

Interpretation:

- the original broad industrial bucket is too heterogeneous
- a 30-name industrial set that mixes aerospace/defense, compounders, capital goods, and rental/exposure names does not produce the same clean unsupervised split as the 10-name pilot
- the industrial sub-template draft should still be treated as **domain-guided**, not ML-proven

### 2. The 30-name energy result is stronger than the industrial result

Broad baseline result:

- best KMeans selection for `energy_asset_heavy`: `k=4`

Practical reading of those clusters:

- one clear healthy broad energy regime
- one clear stressed/operator regime
- some additional nuance around upstream/E&P versus refining/service-heavy names

Interpretation:

- `stressed_operator` remains a defensible draft promotion path
- energy still has more internal structure than the current single broad template captures
- the cleanest next deterministic refinement is still `healthy energy` vs `stressed operator`

## Broad Baseline Findings

### Industrial

Broad industrial clustering did **not** support promoting the entire 30-name basket into a clean deterministic 3-way split.

What it did support:

- keep a broad industrial holdout bucket for aerospace/defense and diversified names
- test a domain-guided industrial-core vs cyclical-capital-goods split in a research-only path

### Energy

Broad energy clustering did support:

- a real stressed branch
- additional structure inside the non-stressed energy set

That is enough to justify testing `stressed_operator` in a draft template path.

## Candidate Split Applied

Industrial candidate assignment:

- `industrial_core_compounder`: `18` issuers
- `cyclical_capital_goods`: `7` issuers
- `industrial_manufacturing` holdout: `5` issuers

Energy candidate assignment:

- `stressed_operator`: `11` issuers
- `energy_asset_heavy`: `19` issuers

## Before / After Outcome

Using the **latest filing-date snapshot per ticker**:

### Industrial Core Compounder

- issuers: `18`
- median quality:
  - baseline `61.31`
  - candidate `61.47`
- median intrinsic value:
  - baseline `123.77`
  - candidate `156.68`
- median valuation gap:
  - baseline `-53.10%`
  - candidate `-37.79%`

Interpretation:

- the industrial-core draft increased fair values materially without needing a quality-score rewrite
- this is the clearest success in the candidate split

### Cyclical Capital Goods

- issuers: `7`
- median quality:
  - baseline `57.05`
  - candidate `58.60`
- median intrinsic value:
  - baseline `211.20`
  - candidate `171.44`
- median valuation gap:
  - baseline `-56.39%`
  - candidate `-64.30%`

Interpretation:

- the cyclical draft pushed valuations down, which is directionally consistent with a more conservative capital-goods regime
- this looks believable for names like `CAT`, `DE`, `PCAR`, and `URI`
- it is still a candidate, not yet a promotion target

### Industrial Holdout

- issuers: `5`
- no change by design

Holdout names remain useful evidence that industrial should not yet be forced into only two templates.

### Broad Energy

- issuers: `19`
- no change by design

This was the control group.

### Stressed Operator

- issuers: `11`
- median quality:
  - baseline `53.75`
  - candidate `52.97`
- median intrinsic value:
  - baseline `20.38`
  - candidate `18.96`
- median valuation gap:
  - baseline `-17.93%`
  - candidate `-42.11%`

Interpretation:

- the stressed-operator draft did exactly what it should:
  - lower fair values
  - lower confidence posture
  - widen the apparent downside for weaker/high-beta energy names

## Candidate Clustering Readback

The candidate export shows better internal structure than the broad baseline:

- `industrial_core_compounder`
  - best KMeans: `k=3`
- `cyclical_capital_goods`
  - best KMeans: `k=2`
- `energy_asset_heavy`
  - best KMeans: `k=2`
- `stressed_operator`
  - best KMeans: `k=4`

This is not proof that the draft split is perfect.

It is evidence that the candidate template partition is more structurally coherent than the broad 30-name baseline buckets.

## Promotion Recommendation

Promote next:

- keep `stressed_operator` as the leading energy refinement candidate
- keep `industrial_core_compounder` as the leading industrial refinement candidate

Keep in research only for now:

- `cyclical_capital_goods`

Do not promote yet:

- a full rewrite of all industrial names into only `industrial_core_compounder` and `cyclical_capital_goods`
- a broad `integrated_energy` vs `upstream_energy` split

## What This Means

The larger 30-name pass changed the conclusion:

- the industrial opportunity is **not** “promote the old 10-name split exactly as-is”
- the energy opportunity **is** still “carve out stressed names explicitly”
- the industrial opportunity is more nuanced:
  - promote a higher-quality core candidate
  - keep a cyclical candidate
  - preserve a broad industrial holdout for names that do not fit cleanly yet

## Next Recommendation

Next best move:

1. keep these candidate templates research-only
2. expand the industrial holdout analysis specifically around:
   - aerospace/defense
   - diversified industrials
   - rental / non-manufacturing industrial exposure
3. add a small current-value comparison sheet for the 60 names so promotion decisions can be made issuer by issuer, not only at cohort median level

## Sources

- [State Street XLI holdings](https://www.ssga.com/us/en/intermediary/etfs/state-street-industrial-select-sector-spdr-etf-xli)
- [State Street XLE holdings](https://www.ssga.com/us/en/intermediary/etfs/state-street-energy-select-sector-spdr-etf-xle)
- [Chevron completed Hess acquisition on July 18, 2025](https://www.chevron.com/newsroom/2025/q3/chevron-completes-acquisition-of-hess-corporation)
- [ConocoPhillips completed Marathon Oil acquisition on November 22, 2024](https://www.conocophillips.com/news-media/story/conocophillips-completes-acquisition-of-marathon-oil-corporation/)
- [Expand Energy rebrand after Chesapeake/Southwestern merger on October 1, 2024](https://investors.expandenergy.com/news-releases/news-release-details/chesapeake-energy-and-southwestern-energy-complete-merger-and)
