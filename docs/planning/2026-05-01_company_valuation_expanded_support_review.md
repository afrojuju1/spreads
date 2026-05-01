# Company Valuation Expanded Support Review

Status: checkpoint findings

As of: Friday, May 1, 2026

Related:

- [Company Valuation Standards-Based Taxonomy Plan](./2026-04-30_company_valuation_standards_based_taxonomy_plan.md)
- [Curated 50 Checkpoint Freeze](../../outputs/company_valuation/supported_report_pack/curated50_2026-05-01_checkpoint_freeze/summary.md)
- [Stressed Benchmark Prior After 75 Expansion](../../outputs/company_valuation/benchmark_priors/stressed_operator_supported_post_v2_75/summary.md)

## Role Of This Doc

This note records the first targeted review of the `expanded` support tier after the curated supported universe grew from `50` to `75`.

The point is not to re-open broad calibration work.

The point is to make the trust boundary explicit:

- `core` is the trusted valuation universe
- `expanded` is technically supported but more weakly validated

## Current State

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

- keep `75` as the current hard cap
- keep `core` as the trusted production-quality set
- treat parts of `expanded` as a watchlist, not as equally validated names

## Highest-Priority Expanded Review Names

These are the names that should be reviewed first if we later prune the `expanded` tier.

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

### Provisional Keep, But Still Watch

#### `APA`

- effective template: `stressed_operator`
- quality score: `51.86`
- valuation confidence: `0.3698`
- valuation gap: `+1.0126`
- benchmark delta vs analyst prior: `+1.2221`

Reason:

- this is the single loudest stressed outlier
- the issue is not just low confidence; it is directional disagreement with the benchmark
- keep only as a provisional stressed name unless a future stressed-upstream split justifies it

#### `MUR`

- effective template: `stressed_operator`
- quality score: `58.93`
- valuation confidence: `0.4854`
- valuation gap: `+0.2824`
- benchmark delta vs analyst prior: `+0.4600`

Reason:

- better quality than the weaker stressed additions
- still materially off benchmark
- defensible as `expanded`, but not trustworthy enough for `core`

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

## Operational Recommendation

For now:

1. keep `75` as the maximum supported universe
2. keep `core=50` as the trusted set
3. treat `expanded=25` as provisional
4. if pruning becomes necessary, review these names first:
   - `BA`
   - `AN`
   - `MTDR`
   - `HP`
   - `PTEN`
   - `APA`
   - `MUR`

Do **not** broaden to `100` or `125` from the current routing base.

The next legitimate expansion step would require one of:

- better taxonomy coverage inside currently supported families
- a new supported cohort with its own benchmark/prior pack
- pruning weak expanded names before adding more
