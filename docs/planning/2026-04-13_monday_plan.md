# Monday Plan For April 13, 2026

Status: in progress

Related:

- [Ops CLI Visibility Plan](./ops_cli_visibility_plan.md)
- [0DTE System Architecture](./0dte_system_architecture.md)
- [Unusual Activity Scanner Design](./unusual_activity_scanner_design.md)

## Context

Friday, April 10, 2026 closed with three clear signals:

- the real account day was small red and flat by the close, so this is not a capital-preservation emergency
- `explore_10_combined_0dte_auto` degraded again and ended with an empty capture
- post-market analysis rated all three streams `weak`, with `0DTE` clearly worst and `core` promotable selection weaker than the best `monitor` ideas

That means Monday should focus on two things:

1. make bad runtime states obvious and fail closed
2. tighten the strategy surfaces that were visibly weak on Friday

## Monday Goals

- fail closed when `0DTE` capture is empty or materially degraded
- improve operator visibility so we can see runtime weakness versus strategy weakness from the terminal
- tighten `core` promotable selection using Friday's promotable-versus-monitor gap
- keep `weekly` running, but expose best and worst setups so score alone is not treated as enough

## Priority Order

### 1. Ship The First Ops CLI Slice

Implement the first useful operator surface under `spreads`:

- `spreads status`
- `spreads trading`
- `spreads sessions`

Minimum Monday requirement for `spreads sessions`:

- list mode shows session status, capture status, websocket and baseline quote counts, alert counts, and latest post-market verdict when present
- detail mode shows empty-capture flags, post-market recommendations, promotable-versus-monitor comparison, and top/bottom modeled ideas when present

Why this comes first:

- Friday's main problem was not hidden data, it was fragmented visibility
- this gives us a terminal-first way to answer whether a label actually worked after the close
- it also sets up `doctor` cleanly instead of adding one-off checks later

## 2. Add A 0DTE Fail-Closed Safety Rail

Implement a narrow runtime guard for `explore_10_combined_0dte_auto`:

- if quote capture is empty, do not treat the session as usable for promotable selection or alerting
- if the collector finishes with `empty` capture or zero websocket quotes, surface that as an operator-visible failure reason
- keep the failure mode explicit in session status and attention output rather than burying it in logs

Why this comes second:

- Friday's `0DTE` stream did not just perform poorly, it also lacked usable capture
- we should not tune thresholds on top of an invalid session surface

## 3. Raise The 0DTE Quality Floor

After the fail-closed guard is in place, tighten the `0DTE` strategy gate:

- review the minimum setup and quality thresholds used for `monitor` and `promotable`
- bias against setups that still passed Friday even though every modeled `0DTE` idea lost by expiry
- prefer removing marginal setups over preserving idea count

Expected Monday output:

- one concrete threshold change set
- a short before/after explanation tied to Friday's losing ideas

## 4. Tighten Core Promotable Selection

Use Friday's `core` post-market split as the tuning input:

- best `core` ideas were `GLD` monitor names
- worst `core` ideas were `IWM` promotable put credits

Monday task:

- inspect the promotion step from `monitor` to `promotable`
- tighten the criteria so promotable selection is more selective when promotable ideas lag monitor ideas
- prefer simple promotion gates over broad score-model changes

Expected Monday output:

- one promotion rule change
- one verification check that compares promotable versus monitor modeled outcomes on the next closed session

## 5. Keep Weekly Visible, Not Overfit

Do not over-correct `weekly` on Monday.

Friday showed:

- the best provisional weekly idea was good
- the worst provisional weekly idea was also bad
- score alone did not separate them well enough

Monday task:

- make weekly best/worst setups visible in `spreads sessions`
- defer deeper weekly scoring changes until we have another closed session with the new visibility in place

## Verification

Monday is successful if we can answer these questions from the CLI and the closed-session output:

- did `0DTE` capture succeed or fail closed
- did the session produce usable ideas or just technically complete
- did `core` promotable ideas still lag `monitor` ideas
- what were the best and worst modeled setups for each label
- are the same recommendations repeating across days

Minimum verification steps:

- `uv run spreads status`
- `uv run spreads trading`
- `uv run spreads sessions`
- `uv run spreads sessions <session-id>`
- one closed-session review after post-market analysis completes

## Non-Goals For Monday

- broad score-model redesign
- a full `doctor` implementation
- deep `weekly` retuning
- changes that depend on remote-only API access

## Recommendation

Start Monday with the CLI slice and the `0DTE` fail-closed guard.

Those two pieces reduce ambiguity immediately:

- we stop mistaking empty capture for usable runtime
- we get a fast terminal path to the same post-market insights that required manual digging on Friday

Then use the afternoon to tighten `0DTE` thresholds and `core` promotable selection with the smallest rule changes that directly address Friday, April 10, 2026.

## Checkpoint

Updated April 12, 2026 with validation captured at `2026-04-13T03:46Z`.

- Runtime visibility and fail-closed behavior are now shipped. `uv run spreads status` and `uv run spreads trading` both returned `HEALTHY` off-hours, with broker sync shown as `idle` instead of degraded when the latest sync is healthy but the market is closed and there are no open positions.
- The noisy runtime flags were narrowed to current incidents. Historical April 7, 2026 Discord dead-letter rows and April 8-9, 2026 rollout-era skipped/failed jobs no longer degrade the live system-health view.
- `0DTE` replay for April 10, 2026 remained intentionally fail-closed: 4 candidates, 0 promotable, 0 allocated, verdict `weak`. That is the correct outcome for the stored empty-capture session.
- `core` replay for April 10, 2026 produced 6 candidates, 5 promotable, and 2 allocated symbols (`GLD`, `IWM`), but the result still graded `weak`.
- The tuned allocator on April 10, 2026 `core` modeled average close/final PnL at `-11.75` and actual net PnL at `-10.0`, with `50%` actual coverage and `100%` force-close exits. That means the selection path is narrower and explicit, but execution quality is still a limiting factor.
- Recent replay over 5 sessions showed allocator-modeled average PnL `-2.5714` versus `-4.2857` for rank-only, while actual net PnL was `-7.0` for both. Selection improved the modeled slice, but realized execution still did not improve across this small sample.
- Recent replay also showed `0` promotions from legacy monitor names and `0` rejected legacy promotable names across the batch, which means the current tuned path is not yet discovering gains through those legacy comparison surfaces.
