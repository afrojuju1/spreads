# Monday Plan For April 13, 2026

Status: proposed

Related:

- [Ops CLI Visibility Plan](/Users/adeb/Projects/spreads/docs/planning/ops_cli_visibility_plan.md)
- [0DTE System Architecture](/Users/adeb/Projects/spreads/docs/planning/0dte_system_architecture.md)
- [Unusual Activity Scanner Design](/Users/adeb/Projects/spreads/docs/planning/unusual_activity_scanner_design.md)

## Context

Friday, April 10, 2026 closed with three clear signals:

- the real account day was small red and flat by the close, so this is not a capital-preservation emergency
- `explore_10_combined_0dte_auto` degraded again and ended with an empty capture
- post-market analysis rated all three streams `weak`, with `0DTE` clearly worst and `core` board promotion weaker than the best `watchlist` ideas

That means Monday should focus on two things:

1. make bad runtime states obvious and fail closed
2. tighten the strategy surfaces that were visibly weak on Friday

## Monday Goals

- fail closed when `0DTE` capture is empty or materially degraded
- improve operator visibility so we can see runtime weakness versus strategy weakness from the terminal
- tighten `core` board promotion using Friday's board-versus-watchlist gap
- keep `weekly` running, but expose best and worst setups so score alone is not treated as enough

## Priority Order

### 1. Ship The First Ops CLI Slice

Implement the first useful operator surface under `spreads`:

- `spreads status`
- `spreads trading`
- `spreads sessions`

Minimum Monday requirement for `spreads sessions`:

- list mode shows session status, capture status, websocket and baseline quote counts, alert counts, and latest post-market verdict when present
- detail mode shows empty-capture flags, post-market recommendations, board-versus-watchlist comparison, and top/bottom modeled ideas when present

Why this comes first:

- Friday's main problem was not hidden data, it was fragmented visibility
- this gives us a terminal-first way to answer whether a label actually worked after the close
- it also sets up `doctor` cleanly instead of adding one-off checks later

## 2. Add A 0DTE Fail-Closed Safety Rail

Implement a narrow runtime guard for `explore_10_combined_0dte_auto`:

- if quote capture is empty, do not treat the session as usable for board promotion or alerting
- if the collector finishes with `empty` capture or zero websocket quotes, surface that as an operator-visible failure reason
- keep the failure mode explicit in session status and attention output rather than burying it in logs

Why this comes second:

- Friday's `0DTE` stream did not just perform poorly, it also lacked usable capture
- we should not tune thresholds on top of an invalid session surface

## 3. Raise The 0DTE Quality Floor

After the fail-closed guard is in place, tighten the `0DTE` strategy gate:

- review the minimum setup and quality thresholds used for `watchlist` and `board`
- bias against setups that still passed Friday even though every modeled `0DTE` idea lost by expiry
- prefer removing marginal setups over preserving idea count

Expected Monday output:

- one concrete threshold change set
- a short before/after explanation tied to Friday's losing ideas

## 4. Tighten Core Board Promotion

Use Friday's `core` post-market split as the tuning input:

- best `core` ideas were `GLD` watchlist names
- worst `core` ideas were `IWM` board put credits

Monday task:

- inspect the promotion step from `watchlist` to `board`
- tighten the criteria so board selection is more selective when board ideas lag watchlist ideas
- prefer simple promotion gates over broad score-model changes

Expected Monday output:

- one promotion rule change
- one verification check that compares board versus watchlist modeled outcomes on the next closed session

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
- did `core` board ideas still lag `watchlist` ideas
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

Then use the afternoon to tighten `0DTE` thresholds and `core` board promotion with the smallest rule changes that directly address Friday, April 10, 2026.
