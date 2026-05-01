# Agentic Research Thesis Engine

Status: concept architecture

As of: Friday, May 1, 2026

Related:

- [Agentic Research Thesis Engine Implementation Contract](./2026-05-01_agentic_research_thesis_engine_implementation_contract.md)
- [Agentic Research Thesis Model Selection](./2026-05-01_agentic_research_thesis_model_selection.md)
- [System Architecture](../current_system_state.md)
- [Company Valuation Engine V1 Spec](./2026-04-29_company_valuation_engine_v1_spec.md)
- [UOA V2 System Design](./2026-04-28_uoa_v2_system_design.md)
- [Public Data Sources And Calendars](../research/public_data_sources_and_calendars.md)
- [TradingAgents](https://github.com/TauricResearch/TradingAgents)

## Role Of This Doc

This document defines the architecture boundary for an agentic research process that turns market, filing, news, and valuation inputs into auditable investment thesis cards.

It is not an implementation contract. The agent roster, parallelism, Ollama model strategy, and implementation details live in the companion implementation contract.

## Product Boundary

The research thesis engine is a downstream research layer.

It should not own:

- live opportunity selection
- execution admission
- portfolio accounting
- alert delivery state
- company valuation source-of-truth calculations

It should eventually consume existing artifacts from scanners, UOA, company valuation, market data, filings, news, calendars, and backtests, then produce structured research artifacts that can annotate opportunities or watchlists.

The first implementation should not connect to existing symbol-generation or opportunity-selection sources. It should start as an explicit single-ticker CLI workflow.

The first CLI workflow is not the end state. Keep the contracts compatible with later candidate queues, batch research, cached source refreshes, Postgres-backed history, UI read models, and offline outcome evaluation.

## Core Principle

Discovery finds something interesting.

Research explains why it may be mispriced.

Execution decides whether and how to express it.

Those boundaries should stay separate.

## Target Flow

```text
signals / UOA / movers / filings / news / valuation changes
        |
        v
candidate queue
        |
        v
research dossier
        |
        v
evidence graph
        |
        v
structured thesis draft
        |
        v
skeptic and verification gate
        |
        v
thesis card + source pack + confidence
        |
        v
opportunity annotation / watchlist / offline review
```

The first implementation should use the same research shape, but with the first two stages replaced by a manual CLI ticker input such as `SOFI`.

## First Implementation System Design

The first implementation should be a local CLI workflow with durable run artifacts.

Proposed command shape:

```text
uv run spreads research thesis --ticker SOFI --as-of 2026-05-01
```

System diagram:

```text
+--------------------+
| CLI request         |
| ticker + as_of      |
+---------+----------+
          |
          v
+--------------------+       +----------------------+
| Run orchestrator    +------>| source registry      |
| scope + run id      |       | enabled adapters     |
+---------+----------+       +----------+-----------+
          |                             |
          |                             v
          |                  +----------------------+
          |                  | source adapters      |
          |                  | SEC / IR / news /    |
          |                  | market / calendar    |
          |                  +----------+-----------+
          |                             |
          v                             v
+--------------------+       +----------------------+
| raw artifact store  |<------+ fetched documents    |
| source snapshots    |       | source metadata      |
+---------+----------+       +----------------------+
          |
          v
+--------------------+
| evidence extractor  |
| facts + metrics +   |
| inferences          |
+---------+----------+
          |
          v
+--------------------+       +----------------------+
| evidence graph      +------>| skeptic gate         |
| claims + sources    |       | conflicts + gaps     |
+---------+----------+       +----------+-----------+
          |                             |
          v                             v
+--------------------+       +----------------------+
| thesis compiler     |<------+ review notes         |
| card + scores       |       | downgrades           |
+---------+----------+       +----------------------+
          |
          v
+--------------------+
| output bundle       |
| thesis.md           |
| evidence.json       |
| sources.json        |
| review.md           |
+--------------------+
```

Primary components:

- `CLI`: validates ticker, as-of date, depth, and enabled sources
- `Run orchestrator`: creates a run id, executes stages, and records status
- `Source registry`: declares available adapters and whether each is trusted, optional, or experimental
- `Source adapters`: fetch raw source artifacts without writing thesis logic
- `Raw artifact store`: keeps the exact source snapshots used for the run
- `Evidence extractor`: converts source artifacts into typed claims and metrics
- `Evidence graph`: links claims to sources and records support, conflict, and confidence
- `Thesis compiler`: writes the concise thesis artifact from the evidence graph
- `Skeptic gate`: reviews unsupported claims, stale evidence, conflicts, missing bear cases, and weak invalidation
- `Output renderer`: writes human-readable and machine-readable artifacts

Initial artifact model:

```text
ResearchRun
  run_id
  ticker
  as_of
  started_at
  completed_at
  status
  config_hash

SourceArtifact
  artifact_id
  run_id
  source_type
  source_url
  fetched_at
  available_at
  raw_path
  content_hash

EvidenceItem
  evidence_id
  run_id
  artifact_id
  claim_type
  claim_text
  supports_or_refutes
  confidence
  extraction_method

ThesisArtifact
  run_id
  thesis_quality
  evidence_quality
  catalyst_quality
  market_confirmation
  portfolio_fit
  output_path

ReviewFinding
  run_id
  severity
  finding_type
  claim_ref
  note
```

Storage should start file-backed under a run directory. Postgres can come later when runs need to be queried across tickers or compared over time.

```text
outputs/research_thesis/
  SOFI/
    2026-05-01/
      run_<run_id>/
        run.json
        thesis.md
        evidence.json
        sources.json
        review.md
        raw/
          sec/
          ir/
          news/
          market/
          calendar/
```

## Evidence Contract

Every material claim must be stored separately from prose.

Required fields:

- `claim_text`
- `claim_type`: `fact`, `derived_metric`, `inference`, `forecast`, or `soft_signal`
- `source_type`
- `source_url` or source artifact reference
- `observed_at`
- `available_at`
- `confidence`
- `supports_or_refutes`
- `extraction_method`

Examples:

- `Q1 revenue grew 58% year over year` is a fact.
- `A new merger subsidiary may imply acquisition preparation` is an inference.
- `Expected 3-month return is 22%` is a forecast.

The thesis card may contain prose, but the system of record should be the structured evidence.

## Source Policy

When sources conflict, prefer:

1. SEC filings and official regulatory sources
2. company releases and investor relations materials
3. exchange filings and official event calendars
4. broker or licensed market/news feeds
5. analyst-note summaries and third-party aggregators
6. social, blogs, and unaudited media references

Lower-ranked sources can still be useful, but they should not silently override higher-ranked evidence.

## Thesis Output

Each thesis should produce one compact document with:

- `ticker`
- `as_of`
- `setup`
- `why_now`
- `variant_view`
- `core_evidence`
- `catalysts`
- `base_case`
- `bull_case`
- `bear_case`
- `expected_window`
- `expected_return`
- `invalidation`
- `portfolio_fit`
- `confidence`
- `skeptic_notes`
- `source_pack`

The expected window is required. A thesis without a time horizon should not carry an expected-return claim.

## Scoring Separation

Keep these scores separate:

- `thesis_quality`: strength and coherence of the investment argument
- `evidence_quality`: source reliability, freshness, and claim support
- `catalyst_quality`: specificity, timing, and confirmability
- `market_confirmation`: price, volume, options, and relative-strength confirmation
- `portfolio_fit`: exposure, correlation, liquidity, and sizing fit

A strong thesis with weak portfolio fit should remain a research candidate, not become an execution candidate by default.

## Data Already Available

The repo already has useful foundations:

- Alpaca-backed stock and options market data
- UOA capture, enrichment, and root-level decision summaries
- scanner and opportunity state infrastructure
- backtest and audit surfaces
- company valuation design and storage for SEC filings, ownership, 13F, and market inputs
- calendar and event-context foundations

Over time, the research engine should reuse these instead of creating another source-of-truth path. The first implementation should use only the source adapters needed for the manually supplied ticker.

Company valuation documents should be treated as reference context only for now. The valuation engine is still being refined, so thesis evidence should prefer raw filings, extracted facts, and source-level artifacts over derived valuation summaries.

## Data Gaps

Highest-priority gaps:

- reliable analyst estimates, revisions, and price-target history
- stronger earnings calendar, estimate, surprise, and guidance data
- transcripts and prepared remarks
- broader structured news and entity co-mention coverage
- M&A and strategic-deal context, including precedent transactions and advisor signals
- sector-specific catalyst feeds, especially biotech and pharma events
- point-in-time raw source capture for news, transcripts, and third-party data
- thesis outcome labels for catalyst realization, invalidation, and return path

Known limitation:

- Alpaca is good enough for shortlist-level options enrichment, but not full institutional options-flow reconstruction.

## Data Acquisition Strategy

Default to free and source-level data first.

The first implementation should avoid a large all-in-one market-data subscription until the workflow proves which fields actually matter.

Recommended starting stack:

- SEC EDGAR APIs for filings, company facts, 8-Ks, exhibits, and raw source evidence
- Alpaca for stock and options market data already available in the repo
- company investor-relations pages for press releases, presentations, and event materials
- GDELT for broad news discovery and entity co-mention context
- official macro and regulatory sources for sector-specific context
- EarningsCalls.dev or a similar narrow transcript API if transcript coverage becomes necessary

Optional low-cost paid additions:

- sec-api.io if native SEC search, exhibit extraction, or filing parsing becomes too slow to build directly
- Alpha Vantage if its earnings, transcript, and news endpoints are good enough in validation
- SimFin as a cheap fundamentals comparator, not a source of truth
- Marketaux as a low-cost structured news layer if GDELT quality is too noisy

Defer until proven necessary:

- Benzinga, FMP, Finnhub, or Polygon partner feeds for analyst ratings, price targets, estimate revisions, and richer event feeds
- biotech-specific paid feeds for FDA, PDUFA, clinical-trial, and prescription catalysts
- full options-flow vendors

For sector-specific research, prefer custom official-source adapters before generic vendor fields. For example, a SOFI thesis can use SEC filings, SoFi investor-relations materials, FDIC BankFind, FFIEC call-report data, CFPB complaint data, and Fed/FRED consumer-credit or rate series.

The expected initial budget target should be zero to low tens of dollars per month, with one narrow paid API added only when it closes a validated gap.

## Reference Patterns From TradingAgents

TradingAgents is a useful reference for orchestration and CLI ergonomics, but not the product foundation.

Borrow:

- LangGraph-style explicit workflow stages
- single-ticker CLI entrypoint
- checkpoint and resume for long research runs
- model-profile abstraction for Ollama-backed model selection
- structured output for final artifacts
- persistent run log with later outcome reflection
- debate-style skeptic pass, but only after evidence is collected

Do not borrow directly:

- Buy/Hold/Sell as the primary output
- trader, risk, and portfolio-manager stages for the first implementation
- prose debate as the system of record
- yfinance or Alpha Vantage as trusted research backbone
- prompt-only source handling
- memory reflections that are injected without source-level evidence

The useful pattern is the run mechanics. The core product should remain an evidence-first research engine, not a trading-decision simulator.

## First Implementation Scope

The first implementation should be a read-only research product.

Inputs:

- one manually supplied ticker from the CLI, for example `SOFI`
- source-adapter fetched market data, filings, calendar, and news context
- optional valuation-engine context, clearly marked as untrusted reference material

Outputs:

- thesis card
- evidence table
- source pack
- skeptic notes
- confidence scores

The first implementation should not place trades, change runtime state, or modify active automation config.

## Acceptance Bar

A thesis is usable only if:

- every material claim has a source
- facts and inferences are clearly separated
- the expected window is explicit
- the bear case is present
- invalidation is testable
- stale or conflicting evidence is surfaced
- the skeptic gate can downgrade or reject the thesis

## Deferred Decisions

The following should be designed later:

- vendor selection for estimates, transcripts, and analyst actions
- storage schema and API contract
- UI placement in opportunity or watchlist views
- offline evaluation labels and score calibration
- whether any research score can influence allocation policy

## Recommended Next Step

Start implementation from the companion contract by building the CLI skeleton, file-backed artifact store, Ollama model router, and model eval harness.

The agent layer should be constrained by the evidence contract, not the other way around.
