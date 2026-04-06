# Features Tracker

Short log of proposed features, why they matter, and the current direction.

## Template

## <number>. <feature name>

- Status:
- Priority:
- Scope:
- Why:
- Direction:
- V1:
- Next:

## 1. Event Context Layer

- Status: done
- Priority: high
- Scope: system-wide
- Why: scheduled events distort option pricing, assignment risk, and scanner rankings; the whole system needs one shared event view
- Direction: build one reusable event subsystem with ingest, normalized store, resolver, and policy
- V1: earnings before expiry, ex-dividend before expiry, macro event annotation for ETF/index products, basic corporate action awareness
- Next: define the event schema, choose initial sources, implement the resolver interface, and apply it to call credit spreads first

### Notes

- Shared event context should expose: `clean`, `penalized`, or `blocked`, plus reasons and nearest-event info
- Single-stock short premium with earnings before expiry should block by default
- Short calls with ex-dividend risk before expiry should block or receive a heavy penalty
- ETFs and index proxies should usually be annotated for macro weeks rather than hard-blocked

## 2. Profile-Aware Ranking Engine

- Status: done
- Priority: high
- Scope: scanner core
- Why: the current ranking still over-rewards premium and near-top-of-band delta instead of surfacing the best trades
- Direction: rank within profile using a weighted quality score instead of primarily sorting by raw return on risk
- V1: delta-target distance, breakeven cushion, fill quality, liquidity, width appropriateness
- Next: tune weights against live scan review and post-run outcomes

## 3. Expected Move And Breakeven Cushion

- Status: done
- Priority: high
- Scope: scanner core
- Why: delta and OTM % alone are not enough to judge whether the short strike has enough room
- Direction: measure short strike and breakeven distance against expected move and use it in filters and ranking
- V1: add breakeven cushion scoring and a basic expected-move filter where data is available
- Next: calibrate minimum cushion rules by profile and symbol type

## 4. Candidate De-Duplication

- Status: proposed
- Priority: medium
- Scope: scanner output
- Why: one short strike can create several nearly identical spreads that clutter the top results
- Direction: collapse near-duplicate candidates and keep the best expression per short leg / expiry group
- V1: keep only the top-ranked spread for each short leg and expiration unless explicitly expanded
- Next: define duplicate rules and output behavior

## 5. Underlying Setup Filter

- Status: proposed
- Priority: medium
- Scope: scanner core
- Why: a structurally valid spread is not enough if the underlying is in a bad regime for bearish or neutral premium selling
- Direction: add a lightweight underlying context layer before final ranking
- V1: trend, momentum, proximity to resistance, and simple regime checks
- Next: define a minimal setup score for `core`

## 6. Run History And Replay

- Status: proposed
- Priority: medium
- Scope: research / evaluation
- Why: we need to measure whether scanner changes actually improve outcomes over time
- Direction: save scan results in a structured history and compare post-scan outcomes over fixed review windows
- V1: persist run metadata and evaluate what happened after 1d, 3d, and expiry
- Next: define storage format and replay metrics
