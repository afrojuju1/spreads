# Company Valuation ML Template Discovery Plan

Status: research-backed planning document

As of: Wednesday, April 29, 2026

Related:

- [System Architecture](../current_system_state.md)
- [Company Valuation Engine V1 Spec](./2026-04-29_company_valuation_engine_v1_spec.md)
- [Company Valuation Engine V1 Implementation Contract](./2026-04-29_company_valuation_engine_v1_implementation_contract.md)

## Role Of This Doc

This document defines the **offline ML-assisted template-discovery and calibration plan** for the company valuation engine.

It is intentionally separate from the V1 online valuation contract.

The online engine remains:

- deterministic
- point-in-time correct
- config-template driven

This plan is for using offline ML to:

- discover better sub-templates inside broad template families
- estimate market-implied multiple regimes more intelligently
- improve confidence and outlier handling

This plan does **not** make ML the source of truth for fair value.

## Locked Scope

- Initial research window: **5 years** of point-in-time data
- Storage and training shape must extend cleanly to **10 years or more**
- Design target must remain practical for **10k+ issuers**
- First research focus:
  - `industrial_manufacturing`
  - `energy_asset_heavy`
- Initial calibration baskets stay:
  - industrial: `CAT`, `DE`, `PH`, `EMR`, `ETN`
  - energy: `XOM`, `CVX`, `COP`, `EOG`, `OXY`

## What ML Should And Should Not Do

### Use ML For

- `archetype discovery`
  - find stable business sub-types inside broad templates
- `multiple-regime calibration`
  - estimate what valuation-anchor regime the market usually assigns to a feature profile
- `confidence and outlier flags`
  - identify names whose feature profile is sparse, unstable, or unlike the rest of the cohort

### Do Not Use ML For

- direct black-box intrinsic value prediction
- replacing point-in-time feature construction
- replacing deterministic quality scoring outright
- leaking future returns or future fundamentals into current labels

The online product should keep this split:

- deterministic online engine
- offline ML research lane
- explicit promotion of only reviewed outputs back into checked-in templates

## Core Recommendation

The first ML system should generate **labels**, not direct prices.

Recommended label families:

- `archetype_label`
  - example: premium industrial compounder, cyclical heavy equipment, integrated major, upstream E&P
- `multiple_regime_label`
  - example: low, medium, high valuation-regime cohort within a broad template
- `outlier_flag`
  - examples: sparse facts, atypical capital structure, event-driven ownership, low-coverage issuer
- `cluster_confidence`
  - how stable and interpretable the archetype assignment is

Those labels can later be promoted into:

- new checked-in templates
- template overrides
- template-specific multiple anchors
- stricter low-confidence rules

## Why 5 Years First

Five years is enough to:

- cover multiple annual and quarterly filing cycles
- capture post-COVID and higher-rate regime behavior
- keep the first dataset small enough to iterate quickly
- avoid turning the first pass into a bulk-history engineering project

At the same time, the data contract should not assume 5 years forever.

The storage, feature, and training layout should be built so that moving from 5 years to 10 years is:

- more rows
- same schema
- same partitioning strategy
- same modeling interfaces

## Scale Expectations

For one row per issuer per canonical quarter-end valuation snapshot:

- `10k issuers * 4 snapshots/year * 5 years ~= 200k rows`
- `10k issuers * 4 snapshots/year * 10 years ~= 400k rows`

If annual rows and a small number of extra filing-aligned rows are retained, the practical range is still roughly:

- `250k-500k rows` for 5 years
- `500k-1M rows` for 10 years

That size is large enough to justify:

- columnar storage
- batched training
- incremental preprocessing

But it is still small enough that standard, well-understood tooling is sufficient.

## Point-In-Time Dataset Contract

### Row Grain

Use one row per:

- `issuer_id`
- `as_of`
- `template_id`
- `feature_version`

Where `as_of` should be the first evaluation-ready timestamp after a canonical financial update.

For the first 5-year dataset, prefer:

- 10-Q derived snapshots
- 10-K derived snapshots

And avoid using thin 8-K-only rows as primary training rows unless they materially update the same numeric fields as a later 10-Q or 10-K.

This is important because the recent calibration work already showed that thin filing-derived rows can distort financial-window reconstruction.

### Minimum Columns

Each training row should include:

- issuer identity
  - `issuer_id`
  - `cik`
  - `ticker`
  - `template_id`
  - `template_version`
  - `sic`
  - `naics` when available
- point-in-time metadata
  - `as_of`
  - `period_end`
  - `accepted_at`
  - `available_at`
  - `statement_coverage_flags`
- financial features
  - growth
  - margins
  - cash conversion
  - capex intensity
  - leverage
  - returns on capital
  - dilution / buyback features
- ownership features
  - insider signal
  - beneficial-owner signal
  - 13F breadth / concentration
- market context
  - current price
  - market cap
  - enterprise value
  - contemporaneous multiples such as `ev_ebit`, `ev_fcf`, `pb`, `ps`
- quality metadata
  - missingness flags
  - fallback flags
  - limited-coverage flags

### Feature Handling Rules

- keep numeric features and missingness indicators separate
- do not silently zero-fill missing accounting fields
- winsorize heavy-tailed ratios before clustering
- apply log transforms where scale is naturally multiplicative
- standardize features within the broad template cohort, not across the entire universe

## Storage And Query Shape

The research dataset should be exported from Postgres into **partitioned Parquet** and queried with DuckDB for offline work.

Recommended partition keys:

- `template_id`
- `as_of_year`

Recommended file layout:

```text
company_valuation_research/
  feature_version=v1/
    template_id=industrial_manufacturing/
      as_of_year=2022/
      as_of_year=2023/
      ...
    template_id=energy_asset_heavy/
      as_of_year=2022/
      as_of_year=2023/
      ...
```

Why this shape:

- Parquet is efficient for columnar scans
- DuckDB supports filter and projection pushdown against Parquet
- year partitions make extension from 5 years to 10 years trivial
- template partitions keep cohort-level modeling simple

## Modeling Approach

### Stage 1: Deterministic Cohort Split

Do not cluster the whole market at once.

Start from the broad deterministic templates already in the engine:

- `industrial_manufacturing`
- `energy_asset_heavy`

Then do sub-template discovery **inside** each broad template.

That keeps the feature geometry more coherent and the resulting labels more interpretable.

### Stage 2: Preprocessing

Recommended preprocessing stack:

1. build the PIT feature matrix from normalized engine snapshots
2. remove columns that are effectively always null in the chosen cohort
3. cap/winsorize extreme ratio outliers
4. add missingness indicator columns
5. robust-scale or standard-scale numeric features
6. optionally reduce dimensionality with `IncrementalPCA`

Recommended first-pass dimensionality target:

- reduce to roughly `20-30` components only if the raw feature matrix is too noisy

Do not jump to nonlinear embeddings as the primary representation. Use them only for visualization if needed.

### Stage 3: Archetype Discovery

Recommended primary clustering path:

- `MiniBatchKMeans` per broad template

Reason:

- simple
- reproducible
- scales comfortably
- easy to re-run on rolling windows
- centroid labels are much easier to convert into checked-in templates

Recommended search range for the first pass:

- industrial: `k=3..8`
- energy: `k=2..6`

Recommended secondary diagnostic path:

- `HDBSCAN` on the same cohort

Use it to:

- see whether the cohort contains meaningful noise or variable-density structure
- identify outliers that should not be forced into centroid clusters
- test whether fixed-`k` clustering is hiding a real split

Do **not** make HDBSCAN the primary production-label generator in the first pass. It is more useful here as a structure-check and outlier detector.

### Stage 4: Optional Compression Path For Larger History

If the dataset becomes materially larger than expected when the window expands past 10 years, add:

- `Birch` as a memory-efficient pre-clustering or compression step

This is a deferred optimization, not the first choice.

For the expected 5-year and 10-year sizes, standard minibatch clustering should still be fine.

## Recommended Output Labels

Each research row should end with:

- `broad_template_id`
- `archetype_label_candidate`
- `archetype_cluster_id`
- `cluster_confidence`
- `outlier_flag`
- `multiple_regime_label`

These outputs should remain **offline artifacts** until reviewed.

## Market-Implied Multiple Calibration

After archetype discovery, train a second offline model per broad template to estimate the market-implied multiple regime.

Use the archetype-aware feature matrix as input.

Recommended targets:

- industrial:
  - `ev_ebit`
  - `ev_fcf`
  - `pb`
- energy:
  - `ev_fcf`
  - `pb`
  - `ev_ebit` where coverage is good

Recommended first model:

- `HistGradientBoostingRegressor`

Reason:

- fast on larger datasets
- handles nonlinearities better than a flat linear model
- supports missing values natively

This model should predict **anchor regimes**, not final fair value.

The online engine can then use those anchor regimes to refine:

- template multiple ranges
- quality premiums
- low-confidence penalties

## Validation Rules

### Time-Safe Validation

Use time-ordered validation only.

Recommended split method:

- `TimeSeriesSplit`

Do not randomly shuffle issuer-quarter rows across time. That would leak future regime structure backwards.

### Cluster Validation

Use multiple checks together:

- `silhouette_score`
  - measures within-cluster cohesion vs separation
- rolling-window `adjusted_rand_score`
  - measures whether clusters stay similar across retrains
- business sanity checks
  - cluster median margins
  - capex intensity
  - leverage
  - revenue cyclicality
  - ownership concentration

No single metric should be treated as decisive.

### Promotion Thresholds

Do not promote a cluster into a real template unless it passes all of these:

- enough coverage
  - at least `30 issuers`
  - at least `150 issuer-quarter rows`
- acceptable stability
  - rolling-window ARI roughly `>= 0.60`
- understandable business identity
  - humans can describe the cluster coherently
- valuation usefulness
  - the cluster materially reduces within-cohort multiple dispersion or improves anchor-model error

If a cluster is statistically present but not interpretable, keep it as a diagnostic label only.

## Proposed Research Sequence

### Phase 1: Dataset Builder

Build a research export path that writes:

- 5 years of PIT quarterly/annual feature rows
- partitioned Parquet
- one schema version

Do this for the two broad templates only:

- `industrial_manufacturing`
- `energy_asset_heavy`

### Phase 2: Archetype Discovery

For each broad template:

- generate the cleaned feature matrix
- run `MiniBatchKMeans` over a small `k` grid
- run `HDBSCAN` as a diagnostic comparison
- inspect cluster medoids and feature summaries

### Phase 3: Market-Implied Multiple Models

Within each broad template:

- train a multiple-regime model
- predict anchor multiple bands
- compare those bands to current checked-in template assumptions

### Phase 4: Template Promotion

Promote only stable findings into checked-in config:

- split a broad template into two smaller templates
- add a template-specific anchor policy
- add stronger low-confidence rules
- add explicit outlier handling

## What This Likely Means For The Two Current Cohorts

This is an inference from the current calibration outputs, not a locked decision.

### Industrial

The industrial basket likely contains at least:

- premium industrial compounders
  - likely `PH`, `ETN`
- cyclical heavy equipment
  - likely `CAT`, `DE`
- diversified electrification / automation
  - likely `EMR`

That suggests the current `industrial_manufacturing` template is probably too broad.

### Energy

The energy basket likely contains at least:

- integrated majors
  - likely `XOM`, `CVX`
- upstream E&P
  - likely `COP`, `EOG`
- stressed or event-sensitive operator
  - likely `OXY`

That suggests `energy_asset_heavy` may later split into:

- `integrated_energy`
- `upstream_energy`

And possibly a distinct low-confidence path for stressed operators.

## Recommended Implementation Boundaries

Keep this work offline first.

Do not put template discovery in the live valuation request path.

Recommended future surfaces:

- a research export command
- a research notebook or offline training script
- reviewed outputs written back into checked-in template config

The live engine should only consume:

- approved template files
- approved valuation anchor changes
- approved confidence-rule changes

## What Not To Build Yet

- no end-to-end AutoML loop
- no deep-learning representation model
- no LLM-generated valuation labels
- no direct price-target regressor as the canonical valuation engine
- no online inference dependency in the live service

## Immediate Next Step

The next concrete artifact after this plan should be:

- a research dataset contract and export path for 5 years of PIT rows across
  - `industrial_manufacturing`
  - `energy_asset_heavy`

That is the cleanest first implementation slice because it creates the data foundation without prematurely locking a specific model outcome.

## Sources

- SEC EDGAR API docs: [EDGAR APIs](https://www.sec.gov/edgar/sec-api-documentation)
- SEC EDGAR access guidance: [Accessing EDGAR Data](https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data)
- scikit-learn `MiniBatchKMeans`: [MiniBatchKMeans docs](https://scikit-learn.org/1.5/modules/generated/sklearn.cluster.MiniBatchKMeans.html)
- scikit-learn `Birch`: [Birch docs](https://scikit-learn.org/stable/modules/generated/sklearn.cluster.Birch.html)
- scikit-learn `HDBSCAN`: [HDBSCAN docs](https://scikit-learn.org/stable/modules/generated/sklearn.cluster.HDBSCAN.html)
- scikit-learn `IncrementalPCA`: [IncrementalPCA docs](https://scikit-learn.org/1.5/modules/generated/sklearn.decomposition.IncrementalPCA.html)
- scikit-learn `HistGradientBoostingRegressor`: [HistGradientBoostingRegressor docs](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.HistGradientBoostingRegressor.html)
- scikit-learn `TimeSeriesSplit`: [TimeSeriesSplit docs](https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html)
- scikit-learn `silhouette_score`: [silhouette_score docs](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.silhouette_score.html)
- scikit-learn `adjusted_rand_score`: [adjusted_rand_score docs](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.adjusted_rand_score.html)
- scikit-learn missingness handling: [Imputation user guide](https://scikit-learn.org/stable/modules/impute.html), [MissingIndicator docs](https://scikit-learn.org/stable/modules/generated/sklearn.impute.MissingIndicator.html)
- DuckDB Parquet support: [Reading and Writing Parquet Files](https://duckdb.org/docs/lts/data/parquet/overview.html)
