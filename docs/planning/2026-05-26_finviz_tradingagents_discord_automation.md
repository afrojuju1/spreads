# Finviz To TradingAgents Discord Automation

Status: implemented MVP, validating one-ticker live Finviz flow

As of: Sunday, May 31, 2026

Related:

- [Alpaca Capabilities Statement](../research/alpaca_capabilities_statement.md)
- [Agentic Research Thesis Engine](./2026-05-01_agentic_research_thesis_engine.md)
- [TradingAgents](https://github.com/TauricResearch/TradingAgents)
- [Finviz Screener Help](https://elite.finviz.com/help/screener)
- [Alpaca Most Active Stocks](https://docs.alpaca.markets/us/reference/mostactives-1)
- [Alpaca Top Market Movers](https://docs.alpaca.markets/us/reference/movers-1)

## Intent

Build a research automation that takes ticker candidates from a Finviz scanner, runs the TradingAgents flow for those tickers, and sends Discord alerts from the validated results.

This should be a research and alerting automation first. It should not place trades, create executable orders, or bypass the existing execution architecture. If this later becomes actionable for equities, it should create an equity research signal or opportunity that a separate execution gate can admit or reject.

## Product Boundary

Discovery finds interesting tickers.

TradingAgents explains the likely directional/research signal.

Discord tells the operator what changed.

Execution remains separate.

The first implementation should not:

- place equity or option trades
- create Nautilus execution intents
- modify active option automation config
- treat model output as sufficient for execution
- scrape Finviz pages in a brittle way if a CSV/export path is available

## Recommended Flow

```text
Scheduled Finviz scanner
  |
  v
Normalize and dedupe ticker rows
  |
  v
Rank and cap candidate list
  |
  v
Enqueue one TradingAgents run per ticker
  |
  v
Read TradingAgents run_metadata.json
  |
  v
Apply quality and signal gates
  |
  v
Store result metadata and artifact paths
  |
  v
Plan Discord delivery events
```

## Existing Surfaces To Reuse

Use the repo's existing job, feed, and alert patterns instead of creating a sidecar scheduler.

- Symbol feed pattern: `packages/core/services/symbol_feeds.py`
- Job declaration pattern: `packages/config/jobs/*.yaml`
- Worker job registry: `packages/core/jobs/registry.py`
- Worker task runners: `packages/core/jobs/worker/tasks.py`
- Discord delivery and retry path: `packages/core/services/alert_delivery.py`
- Discord payload rendering: `packages/core/alerts/discord.py`

TradingAgents already has a non-interactive wrapper with a usable artifact contract:

- `/home/ade/Projects/TradingAgents/scripts/benchmark_run.py`
- Emits `run_metadata.json`
- Includes `validated_signal`, `raw_signal`, `quality_status`, `blocked_reason`, report path, timings, model config, and environment snapshots

## NUC Runtime Shape

Run the TradingAgents research lane on the host NUC, not inside Docker. We control the box, and the working TradingAgents environment already lives at `/home/ade/Projects/TradingAgents/.venv`.

Keep Docker responsible for Postgres, Redis, API, scheduler, runtime workers, and discovery workers. Keep `worker_research_replicas: 0` in deploy target config so compose does not start the container research worker.

Host research worker command:

```bash
cd /home/ade/Projects/spreads
set -a
source .env
set +a
PYTHONPATH=/home/ade/Projects/spreads/packages uv run arq core.jobs.worker.ResearchWorkerSettings
```

Expected host env:

```bash
SPREADS_TRADINGAGENTS_DIR=/home/ade/Projects/TradingAgents
SPREADS_TRADINGAGENTS_UV_ENVIRONMENT=/home/ade/Projects/TradingAgents/.venv
OLLAMA_BASE_URL=http://localhost:11434/v1
```

## MVP Implementation Shape

### 1. Finviz Scanner Feed

Add a new feed recipe alongside `stock_prefilter`:

```yaml
symbol_feed_id: finviz_momentum
job_key: symbol_feed:finviz_momentum
enabled: true
schedule:
  type: market_open_plus_minutes
  minutes: 10
allow_off_hours: false
recipe: finviz_screener
recipe_args:
  source: auto
  scanner_url: https://finviz.com/screener.ashx?v=111&f=sh_avgvol_o1000,sh_price_o5,sh_relvol_o1.5,ta_change_u&o=-relativevolume
  top: 10
  min_price: 5
  min_volume: 1000000
  timeout_seconds: 20
singleton_scope: finviz_momentum
```

The feed output should match the existing symbol feed contract:

```json
{
  "status": "completed",
  "feed_id": "finviz_momentum",
  "recipe": "finviz_screener",
  "generated_at": "...",
  "symbols": ["NVDA", "AMD"],
  "entries": [
    {
      "symbol": "NVDA",
      "score": 92.4,
      "reason_codes": ["finviz_screen", "relative_volume", "momentum"],
      "source_tags": ["source:finviz", "screen:momentum"]
    }
  ],
  "summary": {
    "symbol_count": 2,
    "candidate_count": 34
  },
  "degradation": {
    "status": "ok",
    "reason": null
  }
}
```

Prefer a saved Finviz CSV/export URL or local CSV drop over HTML scraping. Finviz has broad screener and export surfaces, but free-tier quote/screener data may be delayed and programmatic export may depend on account tier. Treat Finviz as an idea source, not a trade-timing feed.

### 2. TradingAgents Batch Job

Add a dedicated research job type instead of using the runtime execution queue. The job should:

- consume the latest Finviz feed snapshot
- cap the run list, initially `1` ticker while validating the flow
- dedupe by ticker, scanner id, market date, and signal cooldown
- run `/home/ade/Projects/TradingAgents/scripts/benchmark_run.py`
- use profile `fast` initially
- run with concurrency `1` or `2` on the NUC
- set a hard timeout per ticker
- write one result record per ticker
- write a batch summary result

Suggested job config shape:

```yaml
job_key: tradingagents_scan:finviz_momentum
job_type: tradingagents_scan
enabled: true
schedule:
  type: market_open_plus_minutes
  minutes: 20
payload:
  label: finviz_tradingagents
  feed_id: finviz_momentum
  feed_job_key: symbol_feed:finviz_momentum
  max_feed_age_seconds: 1800
  max_tickers: 1
  profile: fast
  output_root: outputs/tradingagents/finviz_momentum
  timeout_seconds: 1800
  heartbeat_seconds: 30
  allow_quality_warn: false
  actionable_signals: [Buy, Overweight, Sell, Underweight]
singleton_scope: finviz_momentum
```

### 3. Result Gate

Use TradingAgents' validated signal, not the raw model output.

Actionable per-ticker alert:

- `validated_signal` is `Buy`, `Overweight`, `Sell`, or `Underweight`
- `quality_status` is `pass`
- keep `warn` in the batch summary only while validating
- no timeout/error
- not already alerted within cooldown unless the signal changed

Batch-only summary:

- `Hold`
- `Watchlist`
- `quality_status=fail`
- timeout
- no result metadata

### 4. Discord Alerts

Add research alert types instead of reusing spread payloads:

- `research_tradingagents_actionable`
- `research_tradingagents_batch_summary`

Per-ticker alert:

```text
NVDA | TradingAgents OVERWEIGHT | quality pass
Finviz: momentum rank 2 | rel vol 3.1x | price $127.42
Why: scanner tags + top TradingAgents summary line
Report: outputs/tradingagents/finviz_momentum/NVDA_...
Runtime: 184s
```

Batch alert:

```text
Finviz momentum scan complete
12 tickers from Finviz
5 TradingAgents runs completed
2 actionable: NVDA Overweight, AMD Buy
2 hold/watchlist
1 failed quality gate
```

## Alpaca `stock_prefilter` Today

The current `stock_prefilter` is already useful, but it is intentionally narrow. It currently:

- calls Alpaca's most-active stock screener
- calls Alpaca's market movers screener for gainers and losers
- gets stock snapshots for candidate symbols
- optionally filters to optionable underlyings
- optionally excludes leveraged and inverse ETFs by asset name
- applies minimum price and minimum daily volume
- counts recent Alpaca/Benzinga news headlines
- scores candidates with simple weights:
  - most-active rank: 40
  - mover rank: 25
  - absolute move percent: 20
  - log daily volume: 10
  - news count: 5

Current output is a compact symbol feed:

```json
{
  "symbol": "NVDA",
  "price": 127.42,
  "daily_volume": 78234567,
  "move_percent": 3.42,
  "news_count": 2,
  "reason_codes": ["most_actives", "mover_gainer", "news"],
  "source_tags": ["source:alpaca", "screen:most_actives", "screen:mover_gainer"]
}
```

Alpaca documents the most-active endpoint as real-time SIP-based and rankable by `volume` or `trades`, with `top` from 1 to 100. Alpaca's movers endpoint returns gainers and losers based on real-time SIP data, capped at 1 to 50 per side. Alpaca snapshots provide latest trade, latest quote, minute bar, daily bar, and previous daily bar data. Alpaca news is sourced from Benzinga and includes historical news back to 2015.

## Can Alpaca Be As Powerful As Finviz?

Short answer: Alpaca can become more powerful for our automated trading workflow, but it is not a full Finviz replacement out of the box.

Finviz is stronger as a broad human scanner. It has a large catalog of descriptive, fundamental, ownership, technical, analyst, earnings, performance, pattern, and signal filters. It is good for saved screens and human idea discovery.

Alpaca is stronger as a programmable live-market substrate. It gives us authenticated API access to real-time SIP movers, most-actives, snapshots, trades, quotes, bars, news, tradability, optionability, options contracts, options snapshots, OPRA quotes/trades if subscribed, and account/execution context.

That means the practical split is:

- Finviz is better for broad first-pass idea discovery.
- Alpaca is better for machine-native scoring, freshness, repeatability, live validation, and integration with our risk/execution architecture.

Alpaca can exceed Finviz for our use case if we invest in custom features:

- relative volume versus 20-day and 60-day baselines
- gap from previous close and open
- opening-range breakout and VWAP distance
- spread/liquidity filters from latest quotes
- trade-count acceleration
- news freshness and headline clustering
- optionability and option-chain liquidity
- front-expiry IV, Greeks, volume/open-interest, and quote quality
- account-aware filters such as tradable, shortable, marginable, already-held, or blocked by risk
- score transitions and cooldown-aware alerting

Alpaca probably will not match Finviz cheaply in these areas without extra data sources:

- broad fundamental ratios
- analyst ratings and target changes
- institutional ownership and insider transactions
- short float
- prebuilt chart/candlestick/pattern classifications
- sector/industry UI-style screen construction

## Recommended Scanner Strategy

Validate the Finviz flow first.

1. Finviz scanner for broad thematic discovery and externally curated screen logic.
2. TradingAgents for research judgment on the Finviz shortlist.
3. Discord for operator awareness.
4. Later, a separate equity opportunity/intents path if results prove useful.

Alpaca `stock_prefilter` stays useful context and a future backup source, but it should not be part of the first validation loop. The first loop should answer a narrower question: when Finviz surfaces tickers, do TradingAgents results produce alerts that are timely, readable, and worth acting on manually?

Finviz validation metrics:

- Finviz candidate count per scan
- TradingAgents runs attempted, completed, failed, and timed out
- validated signal distribution
- quality pass, warn, and fail counts
- actionable alert count
- duplicate/cooldown suppression count
- manual operator usefulness notes
- follow-through over 1 day, 5 days, and 20 days

## Open Decisions

- Finviz access method: Elite export URL, manual CSV drop, or another compliant export path
- First Finviz screen: momentum, earnings, unusual volume, oversold bounce, or analyst action
- Alert direction: bullish only or bullish plus bearish
- Max tickers per scan: current initial validation value is `1`
- TradingAgents profile: recommended initial value is `fast`
- Schedule: recommended first pass is market-open plus 20 minutes and after-close review
- Storage: reuse job result payloads first, add a dedicated research result table only after the workflow proves useful

## Implementation Sequence

1. Added this design doc and locked the MVP to Finviz validation only.
2. Added `finviz_screener` as a symbol feed recipe.
3. Added `tradingagents_scan` job type and a dedicated research worker lane.
4. Added research alert payload rendering for Discord.
5. Added `finviz_momentum` feed and `tradingagents_scan:finviz_momentum` job config.
6. Validated the live one-ticker host flow against the NUC TradingAgents venv.
7. Next: keep the one-ticker validation loop running and judge alert usefulness before raising the cap.

Latest one-ticker validation:

- selected ticker: `REPL`
- validated signal: `Overweight`
- quality status: `warn`
- completed: `1`
- failed: `0`
- timed out: `0`
- report: `outputs/tradingagents/finviz_momentum/REPL_20260531_233546/complete_report.md`

Follow-up refinement: one-ticker validation now requires `quality_status=pass` for per-ticker actionable alerts, and duplicated TradingAgents quality warnings should be collapsed before operator-facing summaries.

Implemented files:

- `packages/core/services/symbol_feeds.py`
- `packages/config/symbol_feeds/finviz_momentum.yaml`
- `packages/core/services/tradingagents_scan.py`
- `packages/core/jobs/registry.py`
- `packages/core/jobs/worker/tasks.py`
- `packages/core/jobs/worker/__init__.py`
- `packages/core/jobs/worker/lifecycle.py`
- `packages/core/jobs/scheduler.py`
- `packages/config/jobs/tradingagents_scan_finviz_momentum.yaml`
- `packages/core/alerts/discord.py`
- `docker-compose.yml`
- `docker-compose.prod.yml`
