# Company Valuation Expanded Support Review

Status: decision applied; expanded and stressed boundary tightened

As of: Friday, May 1, 2026

Related:

- [Company Valuation Standards-Based Taxonomy Plan](./2026-04-30_company_valuation_standards_based_taxonomy_plan.md)
- [Curated 50 Checkpoint Freeze](../../outputs/company_valuation/supported_report_pack/curated50_2026-05-01_checkpoint_freeze/summary.md)
- [Stressed Benchmark Prior After 75 Expansion](../../outputs/company_valuation/benchmark_priors/stressed_operator_supported_post_v2_75/summary.md)
- [Stressed Benchmark Prior After Pruning To 68](../../outputs/company_valuation/benchmark_priors/stressed_operator_supported_post_v3_68/summary.md)
- [Retail Benchmark Prior After KMX Demotion](../../outputs/company_valuation/benchmark_priors/retail_consumer_supported_v3_67/summary.md)

## Role Of This Doc

This note records the first targeted review of the `expanded` support tier after the curated supported universe grew from `50` to `75`.

The point is not to re-open broad calibration work.

The point is to make the trust boundary explicit:

- `core` is the trusted valuation universe
- `expanded` is technically supported but more weakly validated

## Review Snapshot

After the `75` expansion:

- supported universe: `75`
- `support_tier` split:
  - `core`: `50`
  - `expanded`: `25`
- low-confidence names:
  - `core`: `1`
  - `expanded`: `2`
- low-quality names:
  - `core`: `9`
  - `expanded`: `9`

This confirms the basic shape we expected:

- the expansion worked mechanically
- quality degraded meaningfully
- the new names are not all equally trustworthy

## Main Review Conclusion

Do **not** expand beyond `75` from the current routing base.

The right interpretation is:

- keep `75` as the current hard cap until the weak additions are reviewed
- keep `core` as the trusted production-quality set
- treat parts of `expanded` as a watchlist, not as equally validated names

## Applied Decision

That review has now been acted on.

The following names were removed from the supported universe:

- `BA`
- `AN`
- `APA`
- `MTDR`
- `HP`
- `PTEN`
- `MUR`

Current validated state after the initial prune:

- supported universe: `68`
- `support_tier` split:
  - `core`: `50`
  - `expanded`: `18`
- expanded low-confidence names: `0`
- expanded low-quality names: `4`
- supported stressed benchmark:
  - `mean_abs_gap_delta`: `0.349`
  - `sign_mismatch_count`: `1`

This is the right outcome.

The point of the `expanded` tier was to discover exactly this kind of weakness without polluting the trusted supported set.

## Follow-On Demotion

After the first benchmark-gated retail calibration pass, `KMX` was also removed from the supported universe.

Current validated state now:

- supported universe: `67`
- `support_tier` split:
  - `core`: `50`
  - `expanded`: `17`
- expanded low-confidence names: `0`
- expanded low-quality names: `3`

Why `KMX` was cut:

- it remained the worst broad-retail benchmark fit after calibration
- it was low-quality from the start
- it distorted the retail cohort enough that keeping it made the retail template look worse than the rest of the supported set justified

This was the correct use of the support boundary: fix the cohort by removing a weak supported name, not by forcing the template to explain a business it does not fit well.

## Highest-Priority Expanded Review Names

These were the names that needed review first before deciding whether to prune the `expanded` tier.

### Likely Demotion Candidates

#### `BA`

- template: `aerospace_defense_prime`
- quality score: `35.37`
- valuation confidence: `0.12`
- valuation gap: `-0.8330`

Reason:

- extremely weak quality
- extremely low confidence
- multiples-only output
- likely too noisy to count as a solid expanded supported name today

#### `AN`

- template: `retail_consumer`
- quality score: `50.35`
- valuation confidence: `0.18`
- valuation gap: `+0.9330`

Reason:

- very low confidence
- positive valuation is large enough to look regime-specific rather than broadly trustworthy
- auto retail likely needs its own benchmark or tighter cohort treatment before this is a comfortable supported add

#### `MTDR`

- effective template: `stressed_operator`
- quality score: `52.94`
- valuation confidence: `0.3880`
- valuation gap: `-0.6583`
- benchmark delta vs analyst prior: `-0.6542`

Reason:

- one of the worst stressed benchmark divergences in the expanded tier
- materially more bearish than the external prior
- should not be trusted as a strong supported stressed name yet

#### `HP`

- effective template: `stressed_operator`
- quality score: `52.41`
- valuation confidence: `0.2667`
- valuation gap: `-0.6314`
- benchmark delta vs analyst prior: `-0.4710`

Reason:

- low confidence
- unprofitable caution
- weak stressed benchmark alignment

#### `PTEN`

- effective template: `stressed_operator`
- quality score: `55.90`
- valuation confidence: `0.3205`
- valuation gap: `-0.4537`
- benchmark delta vs analyst prior: `-0.3300`

Reason:

- no DCF contribution
- still substantially below the stressed benchmark prior
- borderline support quality

### Borderline At Review Time

#### `APA`

- effective template: `stressed_operator`
- quality score: `51.86`
- valuation confidence: `0.3698`
- valuation gap: `+1.0126`
- benchmark delta vs analyst prior: `+1.2221`

Reason:

- this was the single loudest stressed outlier
- the issue was not just low confidence; it was directional disagreement with the benchmark
- it did not deserve to stay supported in the validated set

#### `MUR`

- effective template: `stressed_operator`
- quality score: `58.93`
- valuation confidence: `0.4854`
- valuation gap: `+0.2824`
- benchmark delta vs analyst prior: `+0.4600`

Reason:

- better quality than the weaker stressed additions
- still materially off benchmark
- if any stressed expanded add was going to survive, this was the best candidate
- it still did not justify staying in the validated supported set

## Broader Read On Expanded Names

The expansion did produce legitimate clean additions too.

Examples that look structurally fine as `expanded`:

- software: `NOW`, `PAYC`, `TYL`, `VEEV`
- retail: `TGT`, `ROST`, `CASY`
- energy, non-stressed: `COP`, `EOG`, `XOM`
- industrial: `PH`

So the issue is not “expanded is bad.”

The issue is:

- expanded stressed energy widened too fast
- `BA` is too noisy
- `AN` is too low-confidence for a comfortable supported name

## Final Signoff Tightening

The last signoff pass made three additional decisions:

- remove `ORCL` from the supported universe
- remove the remaining stressed names from the supported universe:
  - `EQT`
  - `HAL`
  - `NOV`
  - `OXY`
  - `PBF`
- keep `defense_platform_systems` supported, but downgrade the whole cohort to `expanded`

Why:

- `ORCL` was still too low-confidence and too far below the franchise benchmark to justify support
- the stressed overlay path is structurally correct, but the supported stressed cohort was still not signoff-grade
- `defense_platform_systems` improved materially after the split, but it is still not a `core`-trust cohort

Current validated state now:

- supported universe: `61`
- `support_tier` split:
  - `core`: `41`
  - `expanded`: `20`
- core low-confidence names: `4`
  - `HD`, `DE`, `AM`, `GTES`
- core low-quality names: `4`
  - `HD`, `TXT`, `DLTR`, `GTES`
- stressed benchmark on the supported universe:
  - `rows_compared=0`
  - the supported universe no longer claims stressed coverage

This is the cleaner end state.

The model still knows how to classify and value stressed names and defense-platform names.

The difference is that the support boundary now reflects current validation truth instead of historical curation inertia.

## Operational Recommendation

Now:

1. keep `61` as the current validated supported universe
2. keep `core=41` as the trusted set
3. treat `expanded=20` as the provisional extension layer
4. do **not** broaden to `100` or `125` from the current routing base

The next legitimate expansion step still requires one of:

- better taxonomy coverage inside currently supported families
- a new supported cohort with its own benchmark/prior pack
- pruning weak expanded names before adding more
