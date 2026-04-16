# Alpaca Flow Follow-Through Notes

Last updated: April 16, 2026

Purpose: keep a durable record of the one-off Alpaca option-flow follow-through runs so future work does not depend on terminal memory.

## Method

- Script: [scripts/one_time/alpaca_flow_followthrough_analysis.py](/Users/adeb/Projects/spreads/scripts/one_time/alpaca_flow_followthrough_analysis.py)
- Core event definition:
  - option trades grouped into `5m` windows
  - only scoreable trades with condition codes in `I,J,S,a,b`
  - windows classified as `premium_burst`, `concentrated_burst`, or `repeated_burst`
- Labels:
  - underlying forward returns at `5m`, `15m`, `30m`, `60m`, `to_close`, and `next_open`
- Important implementation notes:
  - `next_open` labeling was initially missing because next-session stock bars were not fetched; this was fixed in the script before the runs below
  - full-chain `SPY/QQQ` pulls are too dense for practical one-off work
  - for `SPY/QQQ`, use `--max-contracts-per-type` to cap the liquid core by open interest
  - Alpaca historical options requests can occasionally fail with SSL EOFs on heavy pages; the script retries automatically

## Run Log

### 1. DIA / IWM, recent 5-session pass

- Label: `real_pass_dia_iwm_5d_b5`
- Command:

```bash
uv run python scripts/one_time/alpaca_flow_followthrough_analysis.py \
  --symbols DIA,IWM \
  --sessions 5 \
  --baseline-sessions 5 \
  --max-dte 7 \
  --batch-size 100 \
  --label real_pass_dia_iwm_5d_b5
```

- Report: [real_pass_dia_iwm_5d_b5/report.md](/Users/adeb/Projects/spreads/outputs/analysis/alpaca_flow_followthrough/real_pass_dia_iwm_5d_b5/report.md)
- Analysis sessions: `2026-04-09` to `2026-04-15`

Summary:

```text
symbol  events  concentrated  repeated  avg_15m   avg_to_close  avg_next_open  15m_hit  close_hit  next_open_hit
DIA        81        77         20       0.0390      0.2404         0.1678        73.08     76.54        65.00
IWM        43         7         10       0.0217      0.2477         0.5030        55.00     72.09        68.29
```

What stood out:

- `DIA` looked cleaner than `IWM`: more events, better `15m` hit rate, and most of the information came from `concentrated_burst` windows.
- `IWM` carried much larger premium but noisier short-horizon behavior.
- The strongest supported bucket was `concentrated_burst + call_dominant + mid`:
  - `33` events
  - avg `15m`: `+0.0289%`
  - avg to close: `+0.1924%`
  - avg next open: `+0.1858%`
- This was still a short-sample hypothesis run, not enough to call an edge.

### 2. SPY / QQQ, bullish regime, liquid-core 0DTE

- Label: `real_pass_spy_qqq_3d_b3_0dte_oi5`
- Command:

```bash
uv run python scripts/one_time/alpaca_flow_followthrough_analysis.py \
  --symbols SPY,QQQ \
  --sessions 3 \
  --baseline-sessions 3 \
  --max-dte 0 \
  --max-contracts-per-type 5 \
  --batch-size 20 \
  --label real_pass_spy_qqq_3d_b3_0dte_oi5
```

- Report: [real_pass_spy_qqq_3d_b3_0dte_oi5/report.md](/Users/adeb/Projects/spreads/outputs/analysis/alpaca_flow_followthrough/real_pass_spy_qqq_3d_b3_0dte_oi5/report.md)
- Analysis sessions: `2026-04-14` to `2026-04-16`

Summary:

```text
symbol  events  concentrated  repeated  call_dom  put_dom  avg_15m   avg_to_close  avg_next_open  15m_hit  close_hit  next_open_hit
QQQ       103       103         95        103        0      0.0521      0.5864         0.9412        75.26     95.15        100.0
SPY        65        65         38         65        0      0.0330      0.3106         0.5252        70.31     89.23        100.0
```

What stood out:

- In this bullish tape, the liquid same-day core was entirely `call_dominant`.
- `QQQ` was stronger than `SPY` across every forward horizon.
- Open and mid-session events were strongest.
- Same-day continuation was positive, but the bigger message was rest-of-day and next-open continuation.
- Some of the largest `SPY` premium windows had weak first `15m` reaction but still closed strongly, which suggests regime continuation mattered more than immediate pop.

### 3. SPY / QQQ, bearish regime, liquid-core 0DTE

- Label: `real_pass_spy_qqq_downtrend_3d_b3_0dte_oi5`
- Command:

```bash
uv run python scripts/one_time/alpaca_flow_followthrough_analysis.py \
  --symbols SPY,QQQ \
  --start-date 2026-03-26 \
  --end-date 2026-03-30 \
  --baseline-sessions 3 \
  --max-dte 0 \
  --max-contracts-per-type 5 \
  --batch-size 20 \
  --label real_pass_spy_qqq_downtrend_3d_b3_0dte_oi5
```

- Report: [real_pass_spy_qqq_downtrend_3d_b3_0dte_oi5/report.md](/Users/adeb/Projects/spreads/outputs/analysis/alpaca_flow_followthrough/real_pass_spy_qqq_downtrend_3d_b3_0dte_oi5/report.md)
- Analysis sessions: `2026-03-26` to `2026-03-30`

Summary:

```text
symbol  events  concentrated  repeated  call_dom  put_dom  avg_15m    avg_to_close  avg_next_open  15m_hit  close_hit  next_open_hit
QQQ        45        45         27          0       45     -0.0522      -0.4656        -0.3174       29.27      2.22         33.33
SPY        52        52         34          1       51     -0.0449      -0.6308         0.3818       30.61      1.92         78.85
```

What stood out:

- In the bearish tape, the liquid same-day core flipped almost entirely to `put_dominant`.
- Same-day continuation also flipped negative:
  - `QQQ` and `SPY` both had negative `15m`, `30m`, `60m`, and `to_close` averages
  - close hit rates collapsed to almost zero
- Overnight behavior was less symmetric than same-day behavior:
  - `QQQ` stayed directionally bearish into next open on average
  - `SPY` mean-reverted overnight despite bearish same-day flow

### 4. SPY / QQQ, bullish regime extension, liquid-core 0DTE

- Label: `real_pass_spy_qqq_uptrend_10d_b5_0dte_oi5_v2`
- Command:

```bash
uv run python scripts/one_time/alpaca_flow_followthrough_analysis.py \
  --symbols SPY,QQQ \
  --start-date 2026-04-01 \
  --end-date 2026-04-15 \
  --baseline-sessions 5 \
  --max-dte 0 \
  --max-contracts-per-type 5 \
  --batch-size 20 \
  --label real_pass_spy_qqq_uptrend_10d_b5_0dte_oi5_v2
```

- Report: [real_pass_spy_qqq_uptrend_10d_b5_0dte_oi5_v2/report.md](/Users/adeb/Projects/spreads/outputs/analysis/alpaca_flow_followthrough/real_pass_spy_qqq_uptrend_10d_b5_0dte_oi5_v2/report.md)
- Analysis sessions: `2026-04-01` to `2026-04-15`

Summary:

```text
symbol  events  concentrated  repeated  call_dom  put_dom  mixed  avg_15m   avg_to_close  avg_next_open  15m_hit  close_hit  next_open_hit
QQQ       212       212        157        174       38       0     0.0596      0.4669         0.8993        67.49     90.57        83.02
SPY       201       201        104        182       12       7     0.0120      0.2133         0.3007        58.97     66.17        59.70
```

What stood out:

- The bullish continuation story held up over a much larger sample.
- `QQQ` stayed materially stronger than `SPY` across all forward horizons.
- The larger sample was less one-sided than the 3-session bullish pass:
  - some `put_dominant` windows showed up even in the bullish regime
  - `SPY` had noticeably more mixed behavior than `QQQ`
- Even so, the dominant direction remained constructive.

### 5. SPY / QQQ, bearish regime extension, liquid-core 0DTE

- Label: `real_pass_spy_qqq_downtrend_10d_b5_0dte_oi5`
- Command:

```bash
uv run python scripts/one_time/alpaca_flow_followthrough_analysis.py \
  --symbols SPY,QQQ \
  --start-date 2026-03-17 \
  --end-date 2026-03-30 \
  --baseline-sessions 5 \
  --max-dte 0 \
  --max-contracts-per-type 5 \
  --batch-size 20 \
  --label real_pass_spy_qqq_downtrend_10d_b5_0dte_oi5
```

- Report: [real_pass_spy_qqq_downtrend_10d_b5_0dte_oi5/report.md](/Users/adeb/Projects/spreads/outputs/analysis/alpaca_flow_followthrough/real_pass_spy_qqq_downtrend_10d_b5_0dte_oi5/report.md)
- Analysis sessions: `2026-03-17` to `2026-03-30`

Summary:

```text
symbol  events  concentrated  repeated  call_dom  put_dom  mixed  avg_15m    avg_to_close  avg_next_open  15m_hit  close_hit  next_open_hit
QQQ       149       149         72        30      108      11    -0.0289      -0.2316        -0.5307       39.29      26.85         33.56
SPY       190       190        110        32      151       7    -0.0131      -0.3439        -0.2890       45.30      20.00         38.42
```

What stood out:

- The bearish continuation story also held up over the larger sample.
- `put_dominant` windows were the majority in both `SPY` and `QQQ`.
- Same-day continuation stayed negative through `15m`, `30m`, `60m`, and `to_close`.
- Unlike the 3-session bearish pass, `SPY` no longer looked overnight bullish on average.
  - the larger sample flipped `SPY` next-open average from positive to negative
  - the bearish overnight story is more symmetric than the short sample first suggested

### 6. SPY / QQQ regime matrix over the two 10-session runs

- Label: `spy_qqq_10d_regime_matrix_v1`
- Command:

```bash
uv run python scripts/one_time/alpaca_flow_regime_matrix_analysis.py \
  --run bull=real_pass_spy_qqq_uptrend_10d_b5_0dte_oi5_v2 \
  --run bear=real_pass_spy_qqq_downtrend_10d_b5_0dte_oi5 \
  --label spy_qqq_10d_regime_matrix_v1 \
  --min-events 5
```

- Report: [spy_qqq_10d_regime_matrix_v1/report.md](/Users/adeb/Projects/spreads/outputs/analysis/alpaca_flow_regime_matrix/spy_qqq_10d_regime_matrix_v1/report.md)
- Combined events: `752`

What stood out:

- Regime-aligned flow worked, but the bigger surprise was the counter-regime behavior:
  - in bullish tape, `put_dominant` windows were not bearish
  - in bearish tape, `call_dominant` windows were not bullish
- Bullish-regime `put_dominant` windows were the strongest continuation buckets in the whole matrix:
  - `QQQ put_dominant open`: `12` events, avg to close `+1.4056%`, avg next open `+2.4467%`
  - `QQQ put_dominant mid`: `25` events, avg to close `+0.7294%`, avg next open `+2.2568%`
  - `SPY put_dominant mid`: `11` events, avg to close `+0.4878%`, avg next open `+2.6736%`
- Bearish-regime `call_dominant` windows looked like relief rallies that failed:
  - `QQQ call_dominant open`: `7` events, avg `15m` `+0.1816%`, avg to close `-0.7087%`, avg next open `-1.2037%`
  - `SPY call_dominant open`: `13` events, avg `15m` `+0.0546%`, avg to close `-0.2879%`, avg next open `-0.9755%`
  - `QQQ call_dominant mid`: `21` events, avg next open `-0.7892%`
- The first `15m` reaction was often the wrong read for the rest of the move:
  - `bear / QQQ / call_dominant / open` had a `71.43%` `15m` hit rate but `0%` hit rate to close and next open
  - `bull / SPY / call_dominant / open` averaged `-0.0102%` over `15m` but `+0.3652%` to close
- Time-of-day mattered:
  - `QQQ` bullish call-flow continuation was strongest in `mid` and `open`
  - bearish `SPY put_dominant close` weakened materially overnight and was roughly flat by next open (`+0.0054%`)
  - bearish `QQQ put_dominant close` still carried negative overnight follow-through (`-0.1674%`)

Best current interpretation from the matrix:

- The flow signal is not just “follow calls” or “follow puts”.
- It behaves more like a stress / positioning read inside the broader tape:
  - downside-heavy flow inside a bullish regime often marked fuel for continuation higher
  - upside-heavy flow inside a bearish regime often marked bounces that failed
- For execution, the holding horizon matters a lot:
  - the opening reaction can say one thing
  - the rest-of-day and next-open move can say the opposite
- using `15m` alone would miss that structure

### 7. Counter-regime flow split by overnight gap and prior-day trend

- Label: `spy_qqq_counter_regime_context_v1`
- Command:

```bash
uv run python scripts/one_time/alpaca_flow_counter_regime_context_analysis.py \
  --source-label spy_qqq_10d_regime_matrix_v1 \
  --label spy_qqq_counter_regime_context_v1 \
  --min-events 3
```

- Report: [spy_qqq_counter_regime_context_v1/report.md](/Users/adeb/Projects/spreads/outputs/analysis/alpaca_flow_counter_regime_context/spy_qqq_counter_regime_context_v1/report.md)
- Definitions used:
  - overnight gap = current session open vs prior session close
  - prior-day trend = prior session close vs prior session open
  - flat thresholds = `0.15%` for gap and `0.20%` for prior-day trend

What stood out:

- The bullish counter-regime effect was not random:
  - every bullish counter-regime window happened on an `against_regime` gap and a `with_regime` prior day
  - in plain English: these were gap-down opens after an up day, inside a bullish regime
- That pattern held across two separate sessions for both symbols:
  - `QQQ`: `38` events across `2` sessions (`2026-04-02`, `2026-04-07`)
  - `SPY`: `12` events across `2` sessions (`2026-04-02`, `2026-04-07`)
- Those pullback-gap buckets were still the best upside continuation buckets:
  - `QQQ open`: `12` events across `2` sessions, avg to close `+1.4056%`, avg next open `+2.4467%`
  - `QQQ mid`: `25` events across `2` sessions, avg to close `+0.7294%`, avg next open `+2.2568%`
  - `SPY mid`: `11` events across `2` sessions, avg to close `+0.4878%`, avg next open `+2.6736%`
- The bearish counter-regime effect also lined up with overnight dislocation, but it was less broad:
  - `QQQ` bearish counter-regime flow was almost entirely one session: `2026-03-23`
  - that session was a gap-up open after a prior down day (`gap +1.4535%`, prior day `-1.5227%`)
  - `SPY` bearish counter-regime flow spread across `2026-03-17`, `2026-03-23`, and `2026-03-25`
- The strongest bearish failure buckets were gap-up squeeze days:
  - `QQQ open`, gap-up after prior down day: `7` events across `1` session, avg to close `-0.7087%`, avg next open `-1.2037%`
  - `SPY open`, gap-up after prior down day: `4` events across `1` session, avg to close `-0.7110%`, avg next open `-1.2387%`
  - `SPY open`, gap-up after prior up day: `9` events across `1` session, avg next open `-0.8585%`
- This makes the counter-regime story more specific:
  - bullish `put_dominant` flow looks most useful on pullback gaps inside an up regime
  - bearish `call_dominant` flow looks most useful on squeeze gaps inside a down regime
- The caution is sample concentration:
  - the bullish setup repeated across two separate sessions
  - the bearish setup is real enough to notice, but a lot of its explanatory power still sits in a few high-stress sessions

Best current interpretation from the context split:

- Counter-regime flow is probably not a standalone flow signal.
- It looks more like an overnight-dislocation signal:
  - gap down into an up regime -> downside-heavy flow often marks stress that clears and reverses higher
  - gap up into a down regime -> upside-heavy flow often marks a squeeze that fails
- That is a better framing than “put flow bullish” or “call flow bearish”.

## Cross-Run Takeaways

- Flow direction in the liquid 0DTE core was highly regime-sensitive:
  - bullish regime: almost entirely `call_dominant`
  - bearish regime: almost entirely `put_dominant`
- Same-day continuation mirrored the direction of dominant flow:
  - bullish call flow -> positive rest-of-day continuation
  - bearish put flow -> negative rest-of-day continuation
- Overnight continuation was asymmetric:
  - bullish call-flow windows continued strongly into next open
  - bearish put-flow windows also continued negative in the larger 10-session sample, though less cleanly than the bullish side
- `QQQ` was the more expressive vehicle:
  - stronger bullish continuation than `SPY`
  - stronger bearish overnight continuation than `SPY`
- `SPY` still looked weaker and noisier than `QQQ`, but the larger sample softened the earlier “SPY mean reverts overnight in bearish regimes” conclusion.
- Excluded-condition prints remain large in every run, especially `g`, `j`, and `f`, so keeping the scoreable-trade allowlist strict is still the correct default.

## Best Current Read

- The liquid `0DTE` core for `SPY/QQQ` behaves more like a regime-confirmation layer than a generic “unusual activity” layer.
- In bullish regimes, call-heavy liquid-core flow is associated with strong same-day and next-open continuation.
- In bearish regimes, put-heavy liquid-core flow is associated with negative same-day continuation and, in the larger sample, negative next-open continuation as well.
- The regime matrix added a more important nuance:
  - counter-regime flow can be more informative than aligned flow
  - bearish-looking flow inside bullish tape often preceded stronger upside continuation
  - bullish-looking flow inside bearish tape often preceded failed bounces and weaker next opens
- The context split sharpened that further:
  - bullish counter-regime flow was specifically a pullback-gap setup
  - bearish counter-regime flow was specifically a squeeze-gap setup
  - overnight dislocation looks like the trigger, not just counter-flow by itself
- `QQQ` is the cleaner instrument for this work:
  - larger directional effect sizes
  - stronger next-open continuation
  - less ambiguity than `SPY`
- The 3-session runs were directionally useful, but the 10-session runs are the ones to trust more.

## Working Hypotheses

- Hypothesis 1: liquid-core `0DTE` flow is more useful as a regime confirmation tool than as a raw anomaly detector.
- Hypothesis 2: `QQQ` is the better vehicle for directional continuation studies.
- Hypothesis 3: bullish overnight continuation is stronger and cleaner than bearish overnight continuation, but both directions appear real in the larger sample.
- Hypothesis 4: counter-regime flow is often a stronger setup than aligned flow because it may represent hedging, stress, or failed countertrend participation rather than genuine directional leadership.
- Hypothesis 5: opening-window flow should be modeled separately because the first `15m` reaction often disagrees with the rest-of-day move.
- Hypothesis 6: counter-regime flow likely needs overnight dislocation context to matter:
  - bullish `put_dominant` continuation seems tied to gap-down pullbacks after prior strength
  - bearish `call_dominant` failure seems tied to gap-up squeezes inside a downtrend

## Next Useful Runs

- Test whether `IWM` behaves more like `QQQ` or more like `SPY` in bearish regimes.
- Separate the next-open effect by gap direction to distinguish overnight continuation from overnight mean reversion.
- Extend the same gap/trend context study to `IWM` and `DIA`.
- Split the pullback-gap and squeeze-gap setups by gap magnitude:
  - modest gap
  - large gap
- Check whether these context buckets survive if grouped by session instead of by `5m` event count.
