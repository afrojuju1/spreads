# Agentic Research Thesis Engine Implementation Contract

Status: implementation design draft

As of: Friday, May 1, 2026

Related:

- [Agentic Research Thesis Engine](./2026-05-01_agentic_research_thesis_engine.md)
- [Agentic Research Thesis Model Selection](./2026-05-01_agentic_research_thesis_model_selection.md)
- [System Architecture](../current_system_state.md)
- [Public Data Sources And Calendars](../research/public_data_sources_and_calendars.md)
- [TradingAgents](https://github.com/TauricResearch/TradingAgents)

## Role Of This Doc

This document defines the implementation shape for the research thesis CLI and its agentic layer, starting with a single-ticker workflow and keeping the contracts compatible with later batch and UI use.

It includes the initial sector-specialist roster. Vendor choices, deeper sector playbooks, and UI integration stay deferred.

## Product Shape

The first implementation is a local, read-only CLI workflow, but the design should not be optimized only for a throwaway single-ticker tool. The same contracts should support later batch research, candidate-queue ingestion, Postgres-backed history, and operator UI reads.

```text
uv run spreads research thesis --ticker SOFI --as-of 2026-05-01
```

The system must:

- accept one manually supplied ticker
- fetch and store raw source artifacts
- extract typed evidence
- compile a thesis card
- run a skeptic review
- write durable local artifacts
- expose enough stage boundaries to parallelize slow work
- keep artifacts stable enough for later batch and UI workflows

The first implementation must not:

- connect to existing symbol-generation sources
- create or modify opportunities
- place trades
- change active automation config
- treat company valuation documents as trusted evidence

Target-state expansion should add:

- batch mode over a bounded candidate list
- scheduled source refreshes
- Postgres-backed run and evidence indexes
- reusable source caches
- UI read models over completed runs
- optional opportunity annotation after research quality is proven

## Performance Design

Speed is a product requirement. The architecture should minimize wall-clock time without letting agents fetch uncontrolled data or mutate state.

Guiding rules:

- parallelize I/O-bound source fetches aggressively, subject to source rate limits
- parallelize independent artifact normalization and evidence extraction
- run deterministic extraction before LLM extraction so agents receive smaller inputs
- route sector early using cheap metadata so sector-specific sources can fetch in parallel
- cache raw artifacts by source URL, ticker, as-of, and content hash
- reuse prior run artifacts unless `--refresh` is requested or freshness policy requires a refetch
- keep LLM stages bounded and schema-driven

Expected latency profile:

```text
quick
  target: 30-90 seconds
  sources: cached or minimal SEC + market + news
  LLM: concise thesis + skeptic only

standard
  target: 2-5 minutes
  sources: SEC + IR + market + news + calendar + sector sources
  LLM: artifact summaries + evidence extraction + thesis + skeptic

deep
  target: 5-15 minutes
  sources: broader history, transcripts if available, sector-specific expansions
  LLM: deeper extraction and more complete skeptic review
```

Target execution model:

```text
single ticker
  run one ticker pipeline
  parallelize inside source fetch, normalization, extraction, and skeptic checks

batch mode
  run multiple ticker pipelines with bounded ticker concurrency
  share source caches across tickers
  keep per-ticker output bundles isolated

scheduled refresh
  refresh raw source artifacts independently from thesis compilation
  compile thesis only when source freshness or content hash changes
```

Implementation guidance:

- use bounded concurrency, not unbounded fanout
- use async I/O where adapters support it
- use a small thread pool for blocking clients or parsing-heavy steps
- track elapsed time per stage in `logs.jsonl`
- expose timeout and max-concurrency config per source type
- prefer cached raw artifacts over refetching unless freshness policy requires it

## Proposed Package Shape

```text
packages/core/cli/research.py

packages/core/services/research_thesis/
  __init__.py
  contracts.py
  run_orchestrator.py
  config.py
  ids.py
  artifact_store.py
  source_registry.py
  model_router.py
  evidence_graph.py
  thesis_compiler.py
  skeptic_gate.py
  render.py

packages/core/services/research_thesis/sources/
  __init__.py
  sec.py
  investor_relations.py
  market_data.py
  news.py
  calendar.py
  valuation_context.py

packages/core/services/research_thesis/agents/
  __init__.py
  sector_router.py
  source_summarizer.py
  evidence_extractor.py
  sector_specialists.py
  thesis_drafter.py
  skeptic_reviewer.py
  final_compiler.py
```

Keep source adapters separate from agent code. Source adapters fetch artifacts. Agents transform artifacts into evidence and prose.

## CLI Contract

Initial command:

```text
uv run spreads research thesis --ticker SOFI --as-of 2026-05-01
```

Useful flags:

```text
--ticker SYMBOL
--as-of YYYY-MM-DD
--output-root outputs/research_thesis
--sources sec,ir,market,news,calendar
--depth quick|standard|deep
--no-llm
--refresh
--json
```

Rules:

- `--ticker` is required.
- `--as-of` defaults to now, but must be written into every artifact.
- `--no-llm` should fetch sources and run deterministic extraction only.
- `--refresh` may refetch sources, but must not overwrite old raw artifacts without content hashing.
- `--json` prints the run summary payload, not the full thesis.

LLM settings should be environment-driven for the first implementation. Do not add CLI flags for Ollama backend, base URL, model config, or LLM concurrency yet.

## Runtime Stages

```text
ValidateRequest
  |
  v
CreateResearchRun
  |
  v
BuildSourcePlan
  |
  v
FetchSourceArtifacts
  |
  v
NormalizeArtifacts
  |
  v
ExtractEvidence
  |
  v
BuildEvidenceGraph
  |
  v
DraftThesis
  |
  v
RunSkepticGate
  |
  v
CompileFinalThesis
  |
  v
RenderOutputBundle
```

Each stage should write a small status record so failed runs can be inspected without rerunning everything.

Parallel stage plan:

```text
ValidateRequest
  |
  v
CreateResearchRun
  |
  v
+--------------------+--------------------+
| BuildCommonPlan    | RouteSector        |
+---------+----------+---------+----------+
          |                    |
          v                    v
+--------------------+   +--------------------+
| common fetches      |   | sector fetches     |
| SEC / IR / market   |   | SOFI: FDIC /       |
| news / calendar     |   | FFIEC / CFPB / Fed |
+---------+----------+   +----------+---------+
          |                         |
          +------------+------------+
                       |
                       v
              NormalizeArtifacts
                       |
                       v
              ExtractEvidence
                       |
                       v
              BuildEvidenceGraph
                       |
                       v
              DraftThesis
                       |
                       v
              RunSkepticGate
                       |
                       v
              CompileFinalThesis
                       |
                       v
              RenderOutputBundle
```

Stages that can run in parallel:

- common source request planning and sector routing
- SEC submissions, market data, news, calendar, and IR fetches
- sector-specific official-source fetches after routing
- normalization per artifact
- deterministic extraction per artifact
- LLM source summarization per artifact when enabled
- LLM evidence extraction per artifact or artifact group
- independent skeptic checks after a draft exists

Stages that should stay serialized:

- run creation
- final evidence graph merge
- thesis drafting
- final confidence scoring
- final output rendering

The merged evidence graph is the synchronization point. No thesis drafting should start until required evidence extraction has completed or timed out.

Sector routing can be provisional when only cheap ticker metadata is available. If SEC classification arrives later and contradicts the first route, rerun routing before sector-specific extraction.

Stage parallelization matrix:

```text
Stage                         Parallelism                 Why
----------------------------  --------------------------  ----------------------------
ValidateRequest               serial                      fast, establishes scope
CreateResearchRun             serial                      owns run id and directories
BuildCommonPlan               parallel with RouteSector   independent planning work
RouteSector                   parallel with common plan    cheap metadata path
FetchSourceArtifacts          per source + per request     network-bound
NormalizeArtifacts            per artifact                 independent transforms
ExtractEvidence               per artifact + extractor     independent claim extraction
BuildEvidenceGraph            mostly serial merge          dedupe and conflict resolution
DraftThesis                   serial                      needs merged evidence graph
RunSkepticGate                per check                    independent validation checks
CompileFinalThesis            serial                      applies review decisions
RenderOutputBundle            mostly serial                final write consistency
```

Batch-mode parallelization:

```text
BatchRun
  ticker_1 pipeline
  ticker_2 pipeline
  ticker_3 pipeline

Shared limits
  max_active_tickers
  max_source_requests_per_host
  max_llm_calls
  max_parse_workers
```

Batch guardrails:

- never share mutable per-ticker state
- allow shared read-through source caches
- isolate output bundles by ticker, as-of, and run id
- enforce global rate limits above per-adapter limits
- prioritize cached source refresh before new expensive fetches

## Internal Parallelism

Source fetch parallelism:

```text
FetchSourceArtifacts
  sec_submissions
  sec_company_facts
  sec_recent_filings
  investor_relations_pages
  market_snapshot
  historical_bars
  news_search
  calendar_context
  sector_specific_sources
```

Normalization parallelism:

```text
NormalizeArtifacts
  artifact_1 -> normalized_1
  artifact_2 -> normalized_2
  artifact_3 -> normalized_3
```

Evidence extraction parallelism:

```text
ExtractEvidence
  deterministic_extractors
    financial_metrics
    filing_dates
    price_moves
    event_dates

  llm_extractors
    filing_claims
    press_release_claims
    news_claims
    transcript_claims
    sector_claims
```

Skeptic parallelism:

```text
RunSkepticGate
  unsupported_claim_check
  source_conflict_check
  stale_evidence_check
  missing_bear_case_check
  invalidation_check
  forecast_window_check
  untrusted_source_dependency_check
```

Parallelism guardrails:

- each source adapter owns its own rate-limit policy
- concurrent writes must target unique artifact paths
- only one process writes `run.json` status at a time
- final graph merge must dedupe by content hash and normalized claim key
- LLM extraction should be bounded by max artifacts, max tokens, and timeout
- failure of an optional source should degrade confidence, not fail the run

## Source Adapter Contract

Each source adapter should implement the same conceptual interface.

```text
SourceAdapter
  source_type
  trust_tier
  supports(ticker, as_of, depth)
  build_requests(scope)
  fetch(request)
  normalize(raw_response)
```

Adapter outputs:

```text
SourceArtifact
  artifact_id
  run_id
  ticker
  source_type
  source_name
  source_url
  fetched_at
  observed_at
  available_at
  raw_path
  normalized_path
  content_hash
  trust_tier
  notes
```

Initial adapters:

- `sec`: submissions, 10-K, 10-Q, 8-K, exhibit links, company facts
- `investor_relations`: company releases, presentations, event pages
- `market_data`: price, volume, relative move, optional options context from Alpaca
- `news`: GDELT or low-cost news source, with dedupe and source links
- `calendar`: earnings date, macro calendar, sector calendar where available
- `valuation_context`: optional company valuation snapshot, marked untrusted reference

## Evidence Model

Evidence is the source of truth. Prose is derived.

```text
EvidenceItem
  evidence_id
  run_id
  ticker
  artifact_id
  claim_type
  claim_text
  normalized_value
  observed_at
  available_at
  supports_or_refutes
  source_rank
  extraction_method
  extraction_confidence
  final_confidence
  conflicts
  tags
```

Allowed `claim_type` values:

```text
fact
derived_metric
inference
forecast
soft_signal
```

Allowed `supports_or_refutes` values:

```text
supports
refutes
mixed
neutral
```

Confidence should be computed from:

- source rank
- extraction confidence
- freshness
- conflict count
- whether the claim is fact, inference, or forecast

The LLM may propose confidence, but deterministic code should cap or downgrade it.

## Ollama Model Strategy

The first implementation supports only Ollama as the model backend. Keep the internal model-profile abstraction so a future backend can be added without rewriting agents, but do not build multiple backends now.

Ollama client contract:

```text
OllamaModelClient
  base_url
  supports_structured_output
  supports_json_mode
  invoke(profile, messages, schema, options)
```

Model router contract:

```text
ResearchModelRouter
  route(agent_id, depth, input_size, latency_budget)
  invoke_structured(agent_id, schema, messages)
  invoke_text(agent_id, messages)
```

Supported backend:

```text
ollama
```

Environment variables:

```text
MARKET_INTEL_OLLAMA_BASE_URL
MARKET_INTEL_LLM_MAX_CONCURRENCY
MARKET_INTEL_MODEL_FAST_STRUCTURED
MARKET_INTEL_MODEL_STANDARD_REASONING
MARKET_INTEL_MODEL_DEEP_REASONING
MARKET_INTEL_MODEL_LONG_CONTEXT
MARKET_INTEL_MODEL_EMBEDDING
```

Defaults should be conservative. Missing model env vars should use bundled profile defaults, not fail startup. If `MARKET_INTEL_OLLAMA_BASE_URL` is missing, default to `http://localhost:11434`; remote deployments should set it to the remote-box URL. Temporary `RESEARCH_THESIS_*` aliases may remain only as migration fallback.

Model profiles:

```text
fast_structured
  use: routing, source triage, small extraction jobs
  priority: low latency and reliable JSON

standard_reasoning
  use: evidence extraction, sector assessment, normal thesis drafting
  priority: balanced quality and speed

deep_reasoning
  use: hard filings, skeptic review, final synthesis, deep mode
  priority: reasoning quality over latency

long_context
  use: large filings, transcripts, source bundles
  priority: context window and summarization quality

embedding
  use: dedupe, source clustering, similarity lookup
  priority: cheap local retrieval support
```

Selected initial models for the remote box:

```text
fast_structured
  qwen2.5:3b

standard_reasoning
  qwen2.5:3b

deep_reasoning
  qwen2.5:3b

long_context
  qwen2.5:3b

embedding
  disabled initially; nomic-embed-text once semantic dedupe exists
```

`qwen3.5:27b-q4_K_M` is installed on the box, but it is not a default profile yet. It passed a tiny direct JSON-schema smoke with `think=false`, but OpenClaw timed out on a tiny agent-loop prompt and the market-intel structured SOFI run was too slow for standard mode. Use it only through explicit env override until it passes the eval contract with schema-shape validation.

Deferred candidates:

```text
qwen3:14b
  pull only if qwen3:8b is too weak and GLM is too expensive for standard calls

gpt-oss:20b
  pull only if GLM fails structured/tool-style tasks or skeptic quality

qwen3:30b
  pull only if GLM quality is weak on long-context financial artifacts

deepseek-r1:32b
  pull only if skeptic review needs more explicit reasoning and JSON cleanup is acceptable
```

Model selection rules:

- agents declare `model_profile`, not `model_name`
- depth can upgrade profiles, but agents should not choose models themselves
- if structured output fails, retry once with stricter JSON instructions before downgrading
- if a deep model times out, fall back to `standard_reasoning` and mark the run with a warning
- every LLM call should record backend, model, profile, elapsed time, token estimate, and retry count in `logs.jsonl`

Remote-box Ollama posture:

```text
primary host
  ade-nucbox-k8-plus

recommended remote base URL
  http://ade-nucbox-k8-plus:11434

local laptop role
  CLI client and artifact inspection

remote box role
  model hosting, controlled model downloads, batch research runs
```

Environment example:

```text
MARKET_INTEL_OLLAMA_BASE_URL=http://ade-nucbox-k8-plus:11434
MARKET_INTEL_LLM_MAX_CONCURRENCY=2
MARKET_INTEL_MODEL_FAST_STRUCTURED=qwen2.5:3b
MARKET_INTEL_MODEL_STANDARD_REASONING=qwen2.5:3b
MARKET_INTEL_MODEL_DEEP_REASONING=qwen2.5:3b
MARKET_INTEL_MODEL_LONG_CONTEXT=qwen2.5:3b
MARKET_INTEL_MODEL_EMBEDDING=
```

The embedding profile is optional for the first implementation. Do not call it unless the model router verifies a configured embedding model is available.

Large-model candidates such as `gpt-oss:120b`, `deepseek-r1:70b`, or `qwen3:235b` should not be used on this box. They exceed the first implementation's resource posture while other services are running.

Runtime tuning:

- use `MARKET_INTEL_LLM_MAX_CONCURRENCY=2` for the small default
- keep Ollama at one loaded large model and one parallel large-model request for research workloads
- cap `num_ctx` by stage instead of using advertised maximum context
- use `keep_alive: "0s"` for batch/deep runs unless a queue of calls is already waiting
- use `keep_alive: "2m"` only inside one active run to avoid repeated model loads

Live eval before changing defaults:

- JSON validity for structured outputs
- citation discipline against supplied evidence ids
- extraction accuracy on filings and press releases
- wall-clock latency by depth
- memory pressure on the remote box
- fallback behavior when a model times out

## Model Eval Harness

The model eval harness should be implemented before the full research pipeline depends on LLM output quality.

Command:

```text
uv run spreads research eval-models --suite thesis_v0 --as-of 2026-05-01
```

Rules:

- no provider or model CLI flags
- read Ollama settings from the same market-intel env vars
- run with `MARKET_INTEL_LLM_MAX_CONCURRENCY=2` for small-model defaults; use `1` when a large model is explicitly enabled
- use fixed fixtures and expected outputs
- write inspectable reports under `outputs/research_thesis_eval/`
- do not fetch live market data or mutate runtime state

Proposed package shape:

```text
packages/core/services/research_thesis/eval/
  __init__.py
  contracts.py
  runner.py
  scoring.py
  suites.py
  fixtures/
    thesis_v0/
      tasks.yaml
      sofi_company_profile.json
      sofi_10q_excerpt.txt
      sofi_ir_release.txt
      ardx_press_release.txt
      prompt_injection_news.txt
      expected/
        sector_routes.json
        evidence_items.json
        skeptic_findings.json
```

Output bundle:

```text
outputs/research_thesis_eval/
  2026-05-01/
    <run_id>/
      run.json
      model_calls.jsonl
      scores.json
      report.md
      failures/
```

Task types:

```text
sector_routing
fact_extraction
inference_labeling
skeptic_review
citation_discipline
prompt_injection_resistance
long_context_summary
json_repair
```

Scoring fields:

```text
schema_validity
evidence_accuracy
citation_discipline
unsupported_claims
inference_separation
skeptic_quality
latency_seconds
timeout
memory_pressure
```

Initial promotion bar:

```text
qwen2.5:3b
  schema_validity >= 90%
  sector_routing >= 90%
  small extraction passes accepted fixture checks
  median latency acceptable for quick mode

qwen3.5:27b-q4_K_M or glm-4.7-flash:latest
  schema_validity >= 85%
  citation_discipline >= 4/5
  skeptic_quality >= 4/5
  OpenClaw tiny agent-loop smoke passes
  no severe prompt-injection failures
  memory pressure acceptable with concurrency 1
```

Deferred models may be pulled only after this harness identifies a concrete failure mode in the current selected models.

## Agentic Layer

The system uses agents as constrained transformation nodes, not autonomous decision makers.

```text
SourcePlanner
  input: ResearchRequest + SourceRegistry
  output: SourcePlan

SectorRouter
  input: ticker + cheap issuer metadata + SEC SIC/NAICS when available
  output: primary_specialist + optional_secondary_specialist + rationale

SourceSummarizer
  input: SourceArtifact
  output: ArtifactSummary

EvidenceExtractor
  input: ArtifactSummary + raw refs
  output: EvidenceItem[]

SectorSpecialist
  input: EvidenceGraph + sector sources
  output: SectorAssessment + EvidenceItem[]

ThesisDrafter
  input: EvidenceGraph
  output: ThesisDraft

SkepticReviewer
  input: ThesisDraft + EvidenceGraph
  output: ReviewFinding[]

FinalCompiler
  input: ThesisDraft + ReviewFinding[]
  output: ThesisArtifact
```

Exact common agent roster:

```text
SourcePlanner
SectorRouter
SourceSummarizer
EvidenceExtractor
SectorSpecialist
ThesisDrafter
SkepticReviewer
FinalCompiler
```

Common agent responsibilities:

```text
SourcePlanner
  choose source adapters and depth-specific request plan
  avoid duplicate source requests
  mark optional versus required sources

SectorRouter
  resolve primary and optional secondary sector specialist
  explain routing from issuer metadata, SEC classification, and company description
  run before sector-specific source fetches

SourceSummarizer
  summarize one artifact or bounded artifact group
  preserve source refs and dates
  avoid thesis conclusions

EvidenceExtractor
  emit typed EvidenceItem rows
  separate facts, derived metrics, inferences, forecasts, and soft signals
  attach source refs to every claim

SectorSpecialist
  add sector-specific metrics, risks, catalysts, and invalidation checks
  declare missing sector-source needs for orchestrator follow-up
  avoid generic cross-sector valuation shortcuts

ThesisDrafter
  assemble a compact thesis draft from evidence
  cite evidence ids on material claims
  keep expected return nullable unless supported

SkepticReviewer
  find unsupported claims, conflicts, stale evidence, and missing bear cases
  produce structured ReviewFinding rows
  downgrade or block weak theses

FinalCompiler
  apply skeptic findings
  render final thesis artifact
  preserve source pack and confidence breakdown
```

Agent model profiles:

```text
SourcePlanner
  model_profile: fast_structured

SectorRouter
  model_profile: fast_structured

SourceSummarizer
  model_profile: long_context

EvidenceExtractor
  model_profile: standard_reasoning

SectorSpecialist
  model_profile: standard_reasoning

ThesisDrafter
  model_profile: standard_reasoning

SkepticReviewer
  model_profile: deep_reasoning

FinalCompiler
  model_profile: standard_reasoning
```

Agent rules:

- Every material sentence must cite one or more evidence ids.
- Unsupported claims must be removed or downgraded.
- Inferences must be labeled as inferences.
- Forecasts must include an expected window and invalidation condition.
- The skeptic may reject the thesis.
- Agents must call models through `ResearchModelRouter`, not raw Ollama clients.
- Agents may request follow-up source needs, but fetching stays with source adapters.
- No agent may emit a trading order or portfolio action in the first implementation.

## Sector Specialist Agent Roster

The system should run the common agentic layer for every ticker, then route to one primary sector specialist. A second specialist may run only when the issuer clearly spans two sectors.

```text
SectorRouter
  input: ticker + cheap issuer metadata + SEC SIC/NAICS when available
  output: primary_specialist + optional_secondary_specialist + rationale
```

Initial specialist roster:

```text
FinancialsFintechSpecialist
SoftwareInternetSpecialist
BiotechPharmaSpecialist
HealthcareServicesMedtechSpecialist
ConsumerRetailSpecialist
IndustrialsAeroDefenseSpecialist
EnergyMaterialsUtilitiesSpecialist
RealEstateHousingSpecialist
GeneralOperatingCompanySpecialist
```

Routing rules:

- `FinancialsFintechSpecialist`: banks, lenders, brokers, exchanges, payments, insurance, consumer finance, fintech platforms
- `SoftwareInternetSpecialist`: software, SaaS, internet platforms, marketplaces, digital ads, cybersecurity, cloud infrastructure
- `BiotechPharmaSpecialist`: biotech, specialty pharma, drug developers, commercial pharma, life-science therapeutics
- `HealthcareServicesMedtechSpecialist`: providers, payers, medtech, diagnostics, tools, healthcare IT, distributors
- `ConsumerRetailSpecialist`: retail, restaurants, apparel, autos, travel, leisure, staples, discretionary brands
- `IndustrialsAeroDefenseSpecialist`: aerospace, defense, machinery, electrical equipment, transports, logistics, capital goods
- `EnergyMaterialsUtilitiesSpecialist`: E&P, midstream, oilfield services, refiners, chemicals, metals, mining, regulated utilities
- `RealEstateHousingSpecialist`: REITs, homebuilders, real estate services, mortgage-sensitive housing names
- `GeneralOperatingCompanySpecialist`: fallback for mixed, unclear, or unsupported issuers

Specialist output contract:

```text
SectorAssessment
  specialist_id
  routing_confidence
  key_sector_metrics
  sector_specific_catalysts
  sector_specific_risks
  official_source_priorities
  evidence_items
  open_questions
```

Specialist responsibilities:

- identify the sector-specific metrics that matter for the ticker
- label which claims require sector-specific sources
- add sector-specific bear-case and invalidation checks
- avoid generic valuation claims when sector economics require different metrics
- emit evidence items, not final prose

Specialist source priorities:

```text
FinancialsFintechSpecialist
  SEC filings
  company IR
  FDIC BankFind
  FFIEC call reports
  CFPB complaints
  Fed/FRED rates and consumer credit

SoftwareInternetSpecialist
  SEC filings
  company IR
  transcripts when available
  product/pricing pages
  official customer or platform metrics when disclosed

BiotechPharmaSpecialist
  SEC filings
  company IR
  ClinicalTrials.gov
  FDA approvals, labels, and safety communications
  openFDA
  PubMed when trial context matters

HealthcareServicesMedtechSpecialist
  SEC filings
  company IR
  CMS data where relevant
  FDA device or diagnostics records where relevant
  payer/provider regulatory sources where relevant

ConsumerRetailSpecialist
  SEC filings
  company IR
  Census retail and housing series where relevant
  BLS income, jobs, and CPI series where relevant
  company store, unit, same-store sales, and inventory disclosures

IndustrialsAeroDefenseSpecialist
  SEC filings
  company IR
  USAspending.gov for defense or public-contract exposure
  Census manufacturing data where relevant
  FRED industrial production and rates series

EnergyMaterialsUtilitiesSpecialist
  SEC filings
  company IR
  EIA datasets
  FRED commodity and industrial series where relevant
  public reserve, production, pricing, and rate-case disclosures

RealEstateHousingSpecialist
  SEC filings
  company IR
  Census housing data
  FRED mortgage, housing, and rates series
  property occupancy, rent, backlog, and AFFO disclosures where relevant

GeneralOperatingCompanySpecialist
  SEC filings
  company IR
  market data
  news
  macro context only when directly tied to the thesis
```

For `SOFI`, the router should select `FinancialsFintechSpecialist`.

## Tool Registry

Each tool exposed to an agent needs a small contract.

```text
ToolSpec
  name
  purpose
  input_schema
  output_schema
  allowed_stages
  when_to_use
  when_not_to_use
  side_effects
```

Initial agent tools should be read-only:

- read source artifact
- read normalized artifact
- list evidence items
- find evidence by tag
- read market context
- read prior stage output

No first-implementation agent tool should fetch live data directly. Fetching belongs to source adapters before agent work starts.

## Thesis Artifact Contract

```text
ThesisArtifact
  run_id
  ticker
  as_of
  setup
  why_now
  variant_view
  core_evidence
  catalysts
  base_case
  bull_case
  bear_case
  expected_window
  expected_return
  invalidation
  portfolio_fit
  thesis_quality
  evidence_quality
  catalyst_quality
  market_confirmation
  portfolio_fit_score
  confidence
  skeptic_notes
  source_pack
```

Required rule:

- `expected_return` is nullable unless `expected_window` and supporting evidence exist.

## Output Bundle

File-backed output should be enough for the first implementation.

```text
outputs/research_thesis/
  SOFI/
    2026-05-01/
      run_<run_id>/
        run.json
        thesis.md
        thesis.json
        evidence.json
        sources.json
        review.md
        logs.jsonl
        raw/
          sec/
          ir/
          market/
          news/
          calendar/
```

`thesis.md` is for human review.

`thesis.json`, `evidence.json`, and `sources.json` are the durable machine-readable artifacts.

## Skeptic Gate

The skeptic gate should produce findings, not vague commentary.

```text
ReviewFinding
  finding_id
  run_id
  severity
  finding_type
  claim_ref
  evidence_refs
  note
  required_action
```

Finding types:

```text
unsupported_claim
stale_evidence
source_conflict
missing_bear_case
missing_invalidation
forecast_without_window
overstated_confidence
untrusted_source_dependency
```

Severity:

```text
blocker
major
minor
note
```

Blocker findings should prevent a high-confidence final thesis.

## Run Status

Use explicit status values.

```text
created
fetching_sources
extracting_evidence
drafting_thesis
reviewing
completed
completed_with_warnings
failed
```

Failures should preserve all artifacts already fetched.

## Build Sequence

1. Create contracts and file-backed artifact store.
2. Add CLI skeleton and run directory creation.
3. Add Ollama model router with env-driven profile mapping.
4. Add local/remote Ollama env defaults and LLM call logging.
5. Add model eval harness contracts, fixtures, scoring, and `eval-models` CLI.
6. Historical: run `market-intel eval` against `qwen2.5:3b`; the shipped market-intel CLI was removed on 2026-06-11 and any future eval surface should be rebuilt through `spr-0ct`.
7. Add SEC and market-data adapters.
8. Add investor-relations and news adapters.
9. Add sector router and `GeneralOperatingCompanySpecialist`.
10. Add `FinancialsFintechSpecialist` for the first SOFI-oriented path.
11. Add deterministic evidence extraction for obvious facts and metrics.
12. Add LLM evidence extraction with strict schema.
13. Re-run `thesis_v0` on extraction and citation tasks.
14. Add thesis drafter and final renderer.
15. Add skeptic gate and confidence downgrades.
16. Re-run `thesis_v0` on skeptic and prompt-injection tasks.
17. Add `--no-llm` and cached rerun support.
18. Run one end-to-end SOFI thesis with source artifacts and inspect the output bundle manually.

## Acceptance Criteria

The first implementation is acceptable when:

- one ticker can run end to end from the CLI
- selected sector specialist is recorded in `run.json`
- selected backend, model profile, and model name are recorded for every LLM call
- Ollama is the only supported backend in the first implementation
- agent code depends on model profiles, not raw Ollama model names
- every output bundle includes raw source artifacts
- every material thesis claim maps to evidence ids
- unsupported claims are downgraded or removed
- blocker skeptic findings reduce final confidence
- model eval report exists for the selected defaults
- deferred models were not pulled unless the eval report documents the reason
- rerunning with cached artifacts does not refetch by default
- same ticker and as-of reruns create separate run directories
- no existing opportunity, automation, or execution state is modified

## Deferred

- deeper sector playbooks and scoring weights
- Postgres-backed research tables
- web UI integration
- scanner or UOA candidate ingestion
- paid analyst estimate or price-target adapters
- portfolio allocation use of research scores
