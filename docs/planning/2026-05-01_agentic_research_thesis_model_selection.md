# Agentic Research Thesis Model Selection

Status: desk-research benchmark and initial selection

As of: Friday, May 1, 2026

Related:

- [Agentic Research Thesis Engine](./2026-05-01_agentic_research_thesis_engine.md)
- [Agentic Research Thesis Engine Implementation Contract](./2026-05-01_agentic_research_thesis_engine_implementation_contract.md)
- [Ollama Structured Outputs](https://docs.ollama.com/capabilities/structured-outputs)
- [Ollama Context Length](https://docs.ollama.com/context-length)

## Decision

Use a small reliable default first:

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

Runtime should start with `MARKET_INTEL_LLM_MAX_CONCURRENCY=2` for the small default. If a large model is enabled through env override, drop to `1`.

`qwen3.5:27b-q4_K_M` is installed and available, but it is not selected as a default profile yet. It passed a tiny direct structured-output smoke with `think=false`; it did not pass the OpenClaw primary smoke and was too slow for standard market-intel structured stages.

## Why This Selection

The remote box is already running other services, so the first model plan should avoid multiple large loaded models.

Current installed useful models:

- `qwen2.5:3b`: small reliable structured default
- `qwen3.5:27b-q4_K_M`: installed gated candidate; not a default
- `qwen3:8b`: 8.2B, Q4_K_M, 40K context, tools, thinking, Apache 2.0
- `glm-4.7-flash:latest`: 29.9B MoE, Q4_K_M, 202K context, tools, thinking, MIT
- `ministral-3:8b`: 8.9B, Q4_K_M, 256K context, tools, vision, Apache 2.0
- `devstral:latest`: 23.6B, Q4_K_M, 128K context, tools, Apache 2.0
- `qwen3-coder:30b`: code-focused; not ideal for investment research prose

`nomic-embed-text` is not required for the first thesis run. Treat it as the selected embedding model after semantic dedupe is implemented and the model has been pulled.

The first pass should optimize for reliability per watt, not maximum benchmark score.

## Guesstimate Matrix

Scores are directional: 5 is strongest, 1 is weakest.

```text
Model                  Size       Context   JSON   Research   Long docs   Box fit   Decision
---------------------  ---------  --------  -----  ---------  ---------   -------   -------------------------------
qwen2.5:3b             1.9 GB     modest    4      2          2           5         default all profiles for now
qwen3.5:27b-q4_K_M     17 GB      256K      3      5          5           2         installed, gated; too slow for standard
qwen3:8b               5.2 GB     40K       4      3          3           5         fallback fast_structured candidate
glm-4.7-flash          19 GB      202K      4      5          5           3         reevaluate after qwen3.5 findings
ministral-3:8b         6.0 GB     256K      3      3          4           5         fallback long-context summarizer
devstral:latest        14 GB      128K      3      2          3           3         do not use for research thesis
qwen3-coder:30b        18 GB      unknown   3      2          3           2         code-only fallback, not thesis
qwen3:14b              9.3 GB     40K       4      4          3           4         defer pull
qwen3:30b              19 GB      256K      4      4          5           2         defer; overlaps GLM footprint
gpt-oss:20b            14 GB      128K      4      4          4           3         defer; test if GLM fails
deepseek-r1:32b        20 GB      128K      2      4          4           2         defer; likely too slow/verbose
gpt-oss:120b           65 GB      128K      4      5          5           0         not for this box
qwen3:235b             142 GB     256K      4      5          5           0         not for this box
```

## Profile Use

`qwen2.5:3b` should own the first working defaults:

- analyst/skeptic structured JSON
- quick-mode compact thesis summaries
- schema repair once larger models are reintroduced

`qwen3:8b` remains a candidate for:

- sector routing
- source triage
- small structured extraction
- cheap retry after malformed JSON

`qwen3.5:27b-q4_K_M` should stay gated until it passes schema-shape validation and latency budget:

- optional freeform deep review
- long-context filing review experiments
- OpenClaw primary retry only after tiny agent-loop smoke passes

`glm-4.7-flash:latest` remains a comparison candidate for:

- standard evidence extraction when the artifact is nuanced
- sector-specialist assessment
- thesis drafting
- skeptic review
- long-context filing or transcript summarization

`ministral-3:8b` should stay as a fallback for long-context summarization if GLM is too heavy during live operations.

Do not use `devstral` for investment research. It is a coding-agent model, not the research-thesis default.

## Box Tuning

Start conservative:

```text
MARKET_INTEL_LLM_MAX_CONCURRENCY=2
MARKET_INTEL_MODEL_FAST_STRUCTURED=qwen2.5:3b
MARKET_INTEL_MODEL_STANDARD_REASONING=qwen2.5:3b
MARKET_INTEL_MODEL_DEEP_REASONING=qwen2.5:3b
MARKET_INTEL_MODEL_LONG_CONTEXT=qwen2.5:3b
MARKET_INTEL_MODEL_EMBEDDING=
```

Ollama service posture:

- keep `OLLAMA_NUM_PARALLEL=1`
- keep `OLLAMA_MAX_LOADED_MODELS=1`
- cap large-model LLM concurrency at 1 until live evals prove more is safe
- pass `keep_alive: "0s"` for batch/deep runs unless repeated calls are queued
- use `keep_alive: "2m"` only inside one active run to avoid repeated model loads
- cap `num_ctx` per stage instead of using advertised maximum context

Recommended `num_ctx` caps:

```text
fast_structured
  4096-8192

standard_reasoning
  8192-16384

deep_reasoning
  16384-32768

long_context
  32768 first, then raise only after memory checks
```

Large context should be earned. Ollama notes that larger context increases memory needs, so the engine should chunk filings before asking a local model to hold a full 100K-plus token bundle.

## Download Policy

Already present or now available:

- `qwen2.5:3b`
- `qwen3.5:27b-q4_K_M`
- `qwen3:8b`
- `glm-4.7-flash:latest`
- `ministral-3:8b`
- `devstral:latest`

Small optional pull:

- `nomic-embed-text`: acceptable once embedding-backed dedupe is implemented

Do not pull next unless needed:

- `qwen3:14b`: pull only if the qwen2.5 default is too weak and GLM/qwen3.5 are too expensive or unstable
- `gpt-oss:20b`: pull only if GLM fails structured/tool-style tasks or skeptic quality
- `qwen3:30b`: pull only if GLM quality is weak on long-context financial artifacts
- `deepseek-r1:32b`: pull only if skeptic review needs more explicit reasoning and JSON cleanup is acceptable

Do not pull for this box:

- `gpt-oss:120b`
- `deepseek-r1:70b`
- `qwen3:235b`

## Eval Harness

Historical note: this model-selection checkpoint used the temporary market-intel eval harness before the 2026-06-11 CLI cleanup.

```text
# removed 2026-06-11: uv run spreads market-intel eval --tickers SOFI --as-of 2026-05-01 --sources sec,market --depth standard
```

Any future market-intel product should reintroduce a complete eval surface through bead `spr-0ct`. The harness should test:

- sector routing
- fact extraction
- inference labeling
- skeptic review
- citation discipline
- prompt-injection resistance
- long-context summary
- JSON repair

Outputs:

```text
outputs/market_intel_eval/
  <eval_id>/
    eval.json
    eval.md
    runs/
```

Promotion bar:

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

## Fine-Tuning Plan

Do not fine-tune before the evidence pipeline exists.

The first fine-tune candidate should be a small structured model, likely `qwen2.5:3b` or `qwen3:8b`, not GLM or qwen3.5. Keep fine-tuning focused on extraction and classification, not broad thesis writing.

Fine-tune only narrow transformation tasks:

- filing/news evidence extraction to schema
- claim support/refute classification
- sector routing
- skeptic finding classification

Do not fine-tune:

- final thesis prose
- expected-return forecasts
- portfolio-fit scoring

Those should stay evidence- and rules-driven until the offline evaluation set is real.

Minimum tuning prerequisites:

- 200-plus accepted/rejected evidence-extraction examples
- 100-plus skeptic findings with final human labels
- stable JSON schemas
- a held-out eval set by sector
- baseline prompt-only scores from `qwen3:8b` and `glm-4.7-flash`

## Sources

- Ollama lists `qwen3:8b` at 5.2 GB with 40K context and `qwen3:30b` at 19 GB with 256K context: [Ollama Qwen3](https://registry.ollama.ai/library/qwen3)
- GLM-4.7-Flash is a 30B-A3B MoE model and benchmarks strongly versus Qwen3-30B-A3B and GPT-OSS-20B on several agentic/reasoning tasks: [GLM-4.7-Flash model card](https://huggingface.co/zai-org/GLM-4.7-Flash)
- Ollama lists `gpt-oss:20b` at 14 GB with 128K context and notes native agentic/structured-output capabilities: [Ollama GPT-OSS](https://ollama.com/library/gpt-oss)
- OpenAI says `gpt-oss-20b` targets local/edge use around 16 GB memory and supports structured outputs and adjustable reasoning effort: [Introducing GPT-OSS](https://openai.com/index/introducing-gpt-oss/)
- Ollama supports schema-constrained structured outputs through the `format` field: [Ollama Structured Outputs](https://docs.ollama.com/capabilities/structured-outputs)
- Ollama documents that larger context increases memory needs: [Ollama Context Length](https://docs.ollama.com/context-length)
- `nomic-embed-text` is a small 274 MB embedding model in Ollama: [Ollama Nomic Embed Text](https://ollama.com/library/nomic-embed-text)
- FinGPT uses lightweight finance adaptation patterns, including LoRA variants on 7B-class models: [FinGPT](https://github.com/AI4Finance-Foundation/FinGPT)
