# Company Valuation Industrial And Energy Clustering Findings

Status: research findings

As of: Thursday, April 30, 2026

Related:

- [Company Valuation ML Template Discovery Plan](./2026-04-29_company_valuation_ml_template_discovery_plan.md)
- [Company Valuation Engine V1 Spec](./2026-04-29_company_valuation_engine_v1_spec.md)
- [Company Valuation Engine V1 Implementation Contract](./2026-04-29_company_valuation_engine_v1_implementation_contract.md)

## Role Of This Doc

This document records the first **10-year offline clustering findings** for the company valuation engine using the two initial broad template cohorts:

- `industrial_manufacturing`
- `energy_asset_heavy`

The findings here are meant to:

- summarize the first offline template-discovery run
- identify what can be promoted into future checked-in template changes
- explicitly identify what is **not** yet stable enough to promote

This is a research findings document, not a new live runtime contract.

## Dataset Used

Research dataset:

- export root:
  - [outputs/company_valuation/research_calibration_10y](/Users/adeb/Projects/spreads/outputs/company_valuation/research_calibration_10y)
- parquet dataset:
  - [feature_version=v1/evaluation_version=v1](/Users/adeb/Projects/spreads/outputs/company_valuation/research_calibration_10y/feature_version=v1/evaluation_version=v1)
- clustering artifacts:
  - [cluster_summary.json](/Users/adeb/Projects/spreads/outputs/company_valuation/research_calibration_10y/analysis/cluster_summary.json)
  - [cluster_summary.md](/Users/adeb/Projects/spreads/outputs/company_valuation/research_calibration_10y/analysis/cluster_summary.md)
  - [cluster_assignments.parquet](/Users/adeb/Projects/spreads/outputs/company_valuation/research_calibration_10y/analysis/cluster_assignments.parquet)

Scope:

- `10 years`
- `399` PIT rows
- `199` industrial rows
- `200` energy rows
- `10` issuers total

Calibration basket:

- industrial:
  - `CAT`, `DE`, `PH`, `EMR`, `ETN`
- energy:
  - `XOM`, `CVX`, `COP`, `EOG`, `OXY`

## Method

Primary path:

- `MiniBatchKMeans`
- `k=2..6`
- selection by:
  - silhouette score
  - rolling-window ARI stability
  - explicit complexity penalty to avoid over-fragmenting a 5-name basket

Diagnostic path:

- `HDBSCAN`

Feature families used:

- growth
- margins
- FCF conversion
- capex intensity
- leverage
- returns on capital
- asset turns / inventory turns
- dilution / SBC / deferred revenue where present
- beneficial-owner and institutional-holder coverage features

## Main Result

The first run supports:

- a **real industrial split**
- a **stressed-vs-healthy energy split**

The first run does **not** yet support:

- a clean checked-in `integrated_energy` vs `upstream_energy` promotion

## Industrial Findings

Best KMeans selection:

- `k=3`
- silhouette: `0.146836`
- rolling ARI: `0.406265`

Candidate sub-templates:

1. `electrification_automation_compounder`
   - cluster leaning:
     - `EMR`, `PH`
   - profile:
     - higher gross margin
     - higher `ps`
     - strong FCF margin
     - better-quality platform / controls / automation profile

2. `industrial_core_compounder`
   - dominant broad cluster:
     - `ETN`, `PH`, `EMR`
     - plus many stronger rows from `CAT` and `DE`
   - profile:
     - strong `ROIC`
     - high asset turnover
     - solid FCF conversion
     - high `P/B` relative to the current template assumptions

3. `cyclical_capital_goods`
   - cluster leaning:
     - `CAT`, `DE`
     - some weaker / trough rows from `ETN`
   - profile:
     - weaker growth
     - materially higher leverage
     - lower quality score
     - distorted `EV/EBIT` behavior in weaker periods

### Industrial Interpretation

The broad current `industrial_manufacturing` template is too coarse.

The strongest signal from the first run is not “five separate issuer-specific archetypes.”  
It is:

- a healthier industrial core / compounder regime
- a more cyclical capital-goods regime

The `electrification_automation_compounder` finding is real, but with only 5 issuers it is still a smaller secondary refinement.

### Recommended Checked-In Changes For Industrial

Promote next:

- split `industrial_manufacturing` into:
  - `industrial_core_compounder`
  - `cyclical_capital_goods`

Defer one step:

- keep `electrification_automation_compounder` as a candidate override/sub-template until a broader industrial cohort confirms it outside the current 5-name basket

### Recommended Valuation Changes For Industrial

For `industrial_core_compounder`:

- raise multiple anchors materially versus the current broad industrial template
- likely anchor range direction:
  - `ev_ebit`: higher than current `14`
  - `ev_fcf`: higher than current `18`
  - `pb`: much higher than current `2.2`

For `cyclical_capital_goods`:

- do **not** trust `ev_ebit` as the primary anchor during weak periods
- emphasize:
  - `ev_fcf`
  - `pb`
  - wider confidence penalties

## Energy Findings

Best KMeans selection:

- `k=2`
- silhouette: `0.349468`
- rolling ARI: `0.382723`

Candidate sub-templates:

1. `healthy_energy_core`
   - dominant cluster:
     - `XOM`, `CVX`, `COP`, `EOG`
     - plus most `OXY` rows
   - profile:
     - positive operating margin
     - positive FCF margin
     - moderate leverage
     - healthy `P/B`
     - healthy `EV/FCF`

2. `stressed_operator`
   - cluster leaning:
     - mostly `OXY`
     - some stressed rows from `COP` and `EOG`
   - profile:
     - negative or distorted `EV/EBIT`
     - very high leverage
     - very low quality score
     - low `P/B`
     - poor or negative operating margin

### Energy Interpretation

The first stable split is:

- healthy energy core
- stressed operator

That is useful, but it is **not** the same thing as a clean integrated-major vs upstream-E&P split.

`HDBSCAN` does show finer structure:

- `5` clusters
- `35%` noise fraction

That is evidence that more than one energy sub-regime exists, but it is not clean enough yet for a checked-in template promotion.

### Recommended Checked-In Changes For Energy

Promote next:

- keep `energy_asset_heavy` as the broad healthy template for now
- add a distinct `stressed_operator` branch or override path for `OXY`-like names

Defer:

- do **not** yet split into:
  - `integrated_energy`
  - `upstream_energy`

Reason:

- the current 5-name calibration basket does not produce a stable-enough clean separation for that split

### Recommended Valuation Changes For Energy

For the broad healthy template:

- current empirical medians suggest a healthier regime than the current pessimistic valuation posture implies
- the cluster medians support keeping:
  - `ev_fcf`
  - `pb`
  - `ev_ebit`
  as useful anchors in healthy cases

For `stressed_operator`:

- `ev_ebit` can become unusable or misleading
- rely more on:
  - `pb`
  - downside confidence penalties
  - explicit low-confidence handling

## Promotion Recommendation

If only one structural change is promoted next, it should be:

- `industrial_manufacturing` -> split into:
  - `industrial_core_compounder`
  - `cyclical_capital_goods`

If only one energy refinement is promoted next, it should be:

- add `stressed_operator` handling
- do not yet split integrated vs upstream

## What This Means For The Live Templates

Near-term next template work should be:

1. add candidate configs for:
   - `industrial_core_compounder`
   - `cyclical_capital_goods`
   - `stressed_operator`
2. retune the current `industrial_manufacturing` and `energy_asset_heavy` anchor logic using the cluster medians
3. expand the research basket beyond 5 names per cohort before promoting `electrification_automation_compounder` or `integrated_energy` / `upstream_energy`

## Not Yet Promoted

This research run does **not** justify:

- replacing deterministic templates with ML
- promoting `integrated_energy` or `upstream_energy` as checked-in live templates yet
- creating one template per issuer family from a 5-name basket

## Next Step

The next implementation step should be:

- create draft candidate template configs for:
  - `industrial_core_compounder`
  - `cyclical_capital_goods`
  - `stressed_operator`

Then run the same valuation and screen materialization workflow on those candidate configs against the existing calibration baskets before promoting anything into the active template set.
