# Market Intel OpenClaw Architecture

Status: historical target architecture; not shipped

As of: Friday, May 1, 2026

2026-06-11 cleanup note: the shipped `spreads market-intel` CLI, `core.services.market_intel` package, and local `plugins/market-intel` shell were deleted because the surface was half-built. Reintroduce this product through bead `spr-0ct` with a complete current design rather than reviving the removed commands.

Related:

- [System Architecture](../current_system_state.md)
- [Agentic Research Thesis Engine](./2026-05-01_agentic_research_thesis_engine.md)
- [Agentic Research Thesis Engine Implementation Contract](./2026-05-01_agentic_research_thesis_engine_implementation_contract.md)
- [Agentic Research Thesis Model Selection](./2026-05-01_agentic_research_thesis_model_selection.md)
- [OpenClaw Tools And Plugins](https://docs.openclaw.ai/tools)
- [OpenClaw Building Plugins](https://docs.openclaw.ai/plugins/building-plugins)
- [OpenClaw Skills](https://docs.openclaw.ai/tools/skills)
- [OpenClaw Creating Skills](https://docs.openclaw.ai/tools/creating-skills)
- [OpenClaw Sub-Agents](https://docs.openclaw.ai/tools/subagents)
- [OpenClaw Agent Runtime](https://docs.openclaw.ai/concepts/agent)
- [OpenClaw Agent Runtimes](https://docs.openclaw.ai/concepts/agent-runtimes)
- [OpenClaw Install](https://docs.openclaw.ai/install)
- [OpenClaw Discord](https://docs.openclaw.ai/channels/discord)
- [OpenClaw Ollama Provider](https://docs.openclaw.ai/providers/ollama)
- [OpenClaw MCP](https://docs.openclaw.ai/cli/mcp)
- [Alpaca MCP Server](https://docs.alpaca.markets/docs/alpaca-mcp-server)
- [Alpaca MCP Server GitHub](https://github.com/alpacahq/alpaca-mcp-server)

## Role Of This Doc

This document defines the target architecture for `market-intel`: an OpenClaw-native agentic market intelligence harness.

It supersedes the CLI-first framing in the earlier research thesis docs. It also supersedes the earlier OpenClaw model-default recommendation; that model-selection doc remains useful as historical baseline and eval-harness context until it is refreshed.

This is not the current shipped architecture. The canonical current system remains [System Architecture](../current_system_state.md).

## Product Boundary

`market-intel` is not just research. It is the autonomous thesis, evidence, and market-context layer.

It should produce:

- source-backed thesis reports
- evidence graphs
- catalyst and risk maps
- skeptic reviews
- confidence and data-gap summaries
- reusable artifacts for future watchlists, alerts, and UI views

It should not own:

- live opportunity selection
- execution admission
- live order submission
- account or portfolio source-of-truth state
- company valuation source-of-truth calculations

The boundary stays:

```text
Discovery finds something interesting.
Market Intel explains why it may be mispriced.
Execution decides whether and how to express it.
```

## Core Decision

Use an OpenClaw plugin as the primary harness surface.

Do not make MCP the core runtime. MCP can be added later as a portability adapter for other clients, but the first-class integration should be an OpenClaw plugin because we want hooks, skills, subagents, background services, tool policy, and gateway-native orchestration.

Lean into OpenClaw rather than wrapping it from the outside:

```text
Use OpenClaw Gateway as the long-lived operator process.
Use Pi as the default runtime.
Use OpenClaw tools, skills, hooks, sessions, subagents, cron, and message delivery directly.
Use the market-intel plugin to bind those surfaces to our thesis engine.
Use the Python engine only for domain truth and durable artifacts.
```

## Target Stack

```text
Operator
  |
  | chat / CLI / scheduled task
  v
+-------------------------------+
| OpenClaw Gateway              |
| channels / sessions / cron    |
| subagents / Ollama provider   |
+---------------+---------------+
                |
                | native plugin tools + hooks
                v
+-------------------------------+
| market-intel plugin           |
| tools / hooks / skills        |
| run policy / agent playbooks  |
+---------------+---------------+
                |
                | typed engine calls
                v
+-------------------------------+
| market_intel engine           |
| contracts / sources / evidence|
| scoring / rendering / evals   |
+-------+---------------+-------+
        |               |
        | source reads  | artifacts
        v               v
+---------------+   +---------------------------+
| Data sources  |   | outputs/market_intel/     |
| SEC / Alpaca  |   | run bundles / reports     |
| MCP / IR /    |   | logs / evidence / traces  |
| news          |   |                           |
+---------------+   +---------------------------+
```

Rename the current scaffold to `market_intel` immediately. Keep short-lived compatibility aliases only where they make the transition easier.

## Runtime Roles

Keep the stack simple:

```text
OpenClaw
  platform: gateway, channels, sessions, tools, skills, cron, subagents

Pi
  default OpenClaw agent loop: prompt, model call, tool use, compaction, response

market-intel plugin
  domain harness: tools, skills, hooks, run policy, agent playbooks

market_intel engine
  truth layer: contracts, artifacts, evidence, scoring, rendering, evals
```

Use Pi first. Reach for ACP, Codex, or OpenCode only when a task needs an external harness.

## OpenClaw-Native Surfaces

Prefer native OpenClaw surfaces before custom orchestration:

```text
Gateway
  long-lived process, channel routing, device pairing, health checks

Discord
  operator commands, run threads, report delivery, lightweight approvals

Terminal / CLI
  setup, smoke runs, diagnostics, manual recovery

Skills
  task playbooks and source-discipline instructions

Plugin tools
  typed market-intel actions exposed to Pi

Hooks
  observation, finalization checks, run logging, install/tool guardrails

Subagents / sessions
  parallel source, filing, market, catalyst, and skeptic work

Cron / background tasks
  later scheduled watchlist refreshes and daily summaries

Gateway health / ready endpoints
  deployment smoke checks

MCP client registry
  Alpaca MCP and later third-party tool servers consumed by Pi
```

Do not rebuild these surfaces in Python unless OpenClaw cannot cover the use case.

## Plugin Shape

Plugin id:

```text
market-intel
```

Bundled skill:

```text
market-intel-thesis
```

Core tool names:

```text
market_intel_start_run
market_intel_collect_context
market_intel_fetch_filings
market_intel_fetch_news
market_intel_fetch_market_snapshot
market_intel_fetch_options_context
market_intel_write_evidence
market_intel_read_artifacts
market_intel_score_thesis
market_intel_finalize_report
market_intel_run_eval
```

Historical plugin shell-out path:

```text
# removed 2026-06-11: uv run spreads market-intel thesis --ticker SOFI --as-of 2026-05-01 --json
```

Later it can call a narrower Python service, HTTP route, or local daemon. Do not create a second source-of-truth path.

Verified terminal harness path:

```text
openclaw gateway call marketIntel.run --json --timeout 120000 --params '{"ticker":"SOFI","asOf":"2026-05-01","sources":"sec,market","noLlm":true}'
```

The `/market-intel` command is registered for OpenClaw native command surfaces. Do not use `openclaw agent --message "/market-intel ..."` as the smoke path until that CLI path is confirmed to bypass the model loop.

Plugin capabilities to use:

```text
registerTool
  expose market-intel actions to the agent

registerHook
  observe and shape run behavior without hiding engine contracts

registerCommand
  slash command such as /market-intel SOFI for native command surfaces

registerGatewayMethod
  deterministic terminal and harness calls such as marketIntel.run

registerHttpRoute
  optional artifact/readiness endpoints for local diagnostics

registerCli
  optional plugin maintenance commands
```

## Plugin And Engine Boundary

```text
Layer                Owns
-------------------  --------------------------------------------------
OpenClaw Gateway     sessions, routing, subagents, scheduling, delivery
Pi runtime           default agent loop and tool execution cycle
market-intel plugin  OpenClaw tools, hooks, skills, orchestration policy
market_intel engine  schemas, sources, evidence, artifacts, scores, reports
```

The plugin may decide what to run next. The engine decides what a valid run, source artifact, evidence item, score, and report look like.

## Skills Plan

Use OpenClaw built-ins first, then local skills. Treat third-party skills as references unless audited.

Enable built-in capabilities:

```text
web_search / web_fetch
  source discovery and current context

browser
  dynamic pages and pages that need interaction

pdf
  filings, investor decks, reports, presentations

tokenjuice
  compact noisy shell/tool output for local Ollama runs
```

Initial local skills:

```text
market-intel-thesis
  main run playbook: plan, fanout, evidence, skeptic, final report

market-intel-sec-market
  v0 narrow mode: SEC filings + market snapshot only

market-intel-source-scout
  official source discovery: SEC, IR, releases, presentations, transcripts

market-intel-evidence-ledger
  claim typing and evidence discipline

market-intel-skeptic
  bear case, unsupported claims, stale data, prompt-injection checks

market-intel-final-report
  concise thesis artifact from accepted evidence
```

Next local skills:

```text
market-intel-filing-diff
market-intel-catalyst-calendar
market-intel-news-triangulator
market-intel-ir-deck-parser
market-intel-transcript-parser
market-intel-options-flow
market-intel-peer-map
market-intel-valuation-context
market-intel-macro-regime
market-intel-regulatory-specialist
market-intel-short-interest
market-intel-insider-ownership
market-intel-run-eval
market-intel-discord-brief
```

Third-party skills to evaluate only after review:

```text
markitdown-skill
  useful for converting PDFs, decks, docs, and HTML to markdown

Exa or local SearXNG-style web search
  useful for discovery; not source of truth

SkillGuard / pincer-style scanner
  useful for auditing skills; audit the scanner too

academic/OpenAlex-style research
  useful later for biotech, scientific, and regulatory-heavy names

Agent Browser / Playwright-style browser skill
  useful only if built-in browser is insufficient
```

Avoid broad third-party finance or trading skills in the main agent. Use them as idea mines, not evidence sources.

## Workspace And Host Shape

OpenClaw has one agent workspace. For this project, make the repo the workspace:

```text
agents.defaults.workspace
  /home/ade/spreads/app

repo checkout
  /home/ade/spreads/app

config/state
  /home/ade/.openclaw

artifact root
  /home/ade/spreads/app/outputs/market_intel
```

The OpenClaw host environment must include the tools needed by plugin shell-outs:

```text
node / openclaw CLI
git
curl
jq
uv
python runtime expected by the repo
repo dependency access required for `uv run spreads ...`
```

Run OpenClaw natively on the box with broad repo and host access under the `ade` user.

Target user:

```text
ade
  owns /home/ade/spreads/app
  owns /home/ade/.openclaw
  can run uv, openclaw, ollama client, git, curl, jq
```

## Hook Responsibilities

```text
Hook                       Responsibility
-------------------------  ------------------------------------------------
before_tool_call           Attach run scope and preferred output root
after_tool_call            Normalize noisy output, attach artifact references
before_prompt_build        Inject current run state and evidence rules
before_agent_finalize      Block unsupported thesis claims from final output
agent_end                  Mark run status, write summary, capture open gaps
message_sending            Prevent accidental trade/order-style language
before_install             Ask for approval on unknown third-party plugin/skill installs
```

The hooks are not a replacement for typed contracts. They are runtime guardrails around the agent.

## Tool Posture

Give the agent broad access on the box so it can do real work without fighting the harness. Keep only the financial blast-radius boundary explicit:

```text
read tools
  source discovery, fetches, artifact reads, market context reads

write tools
  create run, write evidence, write report artifacts under outputs/

eval tools
  run fixed eval suites and read eval reports

off limits
  order placement, broker mutation, live automation config mutation
```

The first plugin path should be generous enough for the agent to work. Use Discord approvals only for truly exceptional actions, such as installing unknown third-party plugins or changing live financial credentials.

Recommended initial OpenClaw tool stance:

```text
allow
  full OpenClaw tool profile where practical
  group:web
  browser
  pdf when available
  exec
  group:fs
  group:sessions
  cron later
  message
  bundle-mcp / configured MCP tools
  market-intel plugin tools

deny
  live broker credentials unless explicitly configured later
```

## Alpaca MCP

Give OpenClaw access to Alpaca's official MCP server.

Use it for:

```text
market data
  stock bars, quotes, trades, snapshots, screeners
  option bars, quotes, trades, snapshots, chains, Greeks, IV
  news and corporate actions

account context
  paper account status, buying power, positions, activities, portfolio history

paper trading experiments
  order lifecycle tests and strategy dry-runs in paper mode
```

Initial config posture:

```text
transport
  stdio

command
  /home/ade/.openclaw/bin/alpaca-mcp-server-paper

env
  wrapper reads APCA_* from /home/ade/spreads/app/.env.deploy.ade-nucbox-k8-plus
  wrapper exports ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_PAPER_TRADE=true
  OpenClaw config does not store raw Alpaca keys
```

Alpaca MCP V2 supports server-side toolset filtering with `ALPACA_TOOLSETS`, but start with full paper access by omitting the filter. Tighten toolsets only if context size, tool confusion, or accidental mutation becomes a real problem.

Live Alpaca keys are a separate decision. `market-intel` may inspect paper account and order state, but it should not become the live execution source of truth without an explicit architecture update.

## Model Strategy

Use the newest practical local model that can run OpenClaw tools reliably on the box. Do not optimize only around already-installed models.

Current observed box posture:

```text
host
  ade-nucbox-k8-plus

memory
  28 GiB total, roughly 21 GiB available during the last read-only check

disk
  enough headroom for more models

installed useful baselines
  qwen2.5:3b
  qwen3:8b
  qwen3.5:27b-q4_K_M
  glm-4.7-flash:latest
  qwen3-coder:30b
```

Model candidates:

```text
qwen3.5:27b
  pulled and evaluated after Ollama upgrade
  direct tiny JSON-schema call works with think=false
  OpenClaw main-agent loop reached the model after timeout tuning, but full tool/schema context still could not return a one-word smoke inside 15 minutes on CPU
  market-intel structured engine run worked only after stricter prompting, but took roughly 6 minutes for two calls
  keep as the dedicated frontier model for the `market-intel` OpenClaw agent while the main/default agent stays recoverable on qwen2.5

qwen3.6:35b
  edge candidate
  24 GB class, newest Qwen agentic/coding family
  likely higher ceiling, but tight on a 28 GiB RAM box with KV cache

glm-4.7-flash:latest
  installed baseline
  strong long-context and agentic/browser benchmark posture

qwen3-coder:30b
  installed code specialist
  use for plugin, repo, and harness implementation work

qwen3:8b
  installed fast helper
  was still too slow for the OpenClaw model-planned tool-call smoke

qwen2.5:3b
  installed fast structured helper
  current OpenClaw main/default model and market-intel engine default because it is the reliable tool/eval path so far

gemma4:26b
  later evaluation candidate after Ollama upgrade
  strong agentic, vision, and 256K-context posture

gpt-oss:20b
  deferred fallback
  useful if Qwen/GLM tool behavior is unstable

nemotron-3-nano:30b
  deferred long-context experiment
  1M context on paper, but 24 GB class and not first for this workflow
```

Initial OpenClaw model order:

```text
current default before new pulls
  OpenClaw main/default: ollama/qwen2.5:3b
  OpenClaw market-intel agent: ollama/qwen3.5:27b-q4_K_M
  market-intel fast structured: qwen2.5:3b
  market-intel standard/deep/long: qwen2.5:3b

known caveat
  model-planned OpenClaw agent tool calls are not reliable yet
  deterministic Gateway RPC is the supported harness path

new-pull result
  evaluated: ollama/qwen3.5:27b
  decision: keep available as the dedicated frontier agent model; do not make it the default `main` model until it can pass a slim-agent smoke
  compare against: ollama/glm-4.7-flash:latest
  specialist: ollama/qwen3-coder:30b
  helper: ollama/qwen3:8b

edge eval
  ollama/qwen3.6:35b only after qwen3.5:27b contract/latency issues are understood
```

Before pulling newer models, upgrade Ollama if needed. Keep the provider native:

```text
baseUrl: "http://host:11434"
api: "ollama"
no /v1
```

Runtime posture:

```text
OLLAMA_NUM_PARALLEL=1
OLLAMA_MAX_LOADED_MODELS=1
OpenClaw primary context: keep small until model-planned tools pass
market-intel fast structured context: 4096
Only raise context after evals show the run needs it
```

OpenClaw timeout tuning observed on the box:

```text
agents.defaults.timeoutSeconds
  1800
  controls the command/task ceiling

models.providers.ollama.timeoutSeconds
  900
  controls slow local Ollama provider calls

qwen3.5:27b-q4_K_M
  direct Ollama tiny JSON call passes
  full OpenClaw main-agent prompt/tool schema timed out after the longer Gateway window
  dedicated market-intel agent exists with minimal tools and qwen3.5, but still needs prompt/tool/context slimming before it can be accepted as reliable
```

Do not treat a short OpenClaw timeout as model failure. First inspect whether the failure is command timeout, provider idle timeout, or overloaded prompt/tool context.

Cloud-only or too-heavy open-weight models are not first-class local candidates for this box:

```text
glm-5:cloud
kimi-k2.5:cloud
minimax-m2.7:cloud
qwen3.5:122b
gpt-oss:120b
```

They can become optional cloud fallbacks later, but the first edge should be local model orchestration plus better tools and artifacts.

## Agent Roster

Start with a small roster. Add sector specialists only when the source plan needs them.

```text
market-intel-chief
  owns run plan, fanout, synthesis, final report

source-scout
  finds official and high-signal source material

filings-agent
  reads SEC, IR, transcripts, presentations

market-structure-agent
  reads price, volume, options, UOA, borrow/liquidity context

catalyst-agent
  maps earnings, regulatory, product, litigation, macro, and event windows

sector-specialist
  applies sector-specific questions and official-source checklists

skeptic
  attacks unsupported claims, stale data, consensus traps, and missing bear cases

synthesis-agent
  converts accepted evidence into concise report artifacts
```

Default OpenClaw subagent concurrency should favor speed on the box:

```text
maxConcurrent: 4
subagents.maxConcurrent: 8
maxSpawnDepth: 2
```

Ollama should be pushed, then tuned from observed memory pressure:

```text
OLLAMA_NUM_PARALLEL=2-4
OLLAMA_MAX_LOADED_MODELS=1-2
market-intel LLM concurrency=2
```

## Runtime Flow

```text
User asks: "run market intel on SOFI"
        |
        v
market-intel-thesis skill activates
        |
        v
market-intel-chief starts run
        |
        v
+--------------------+--------------------+
| source plan         | sector route       |
+---------+----------+---------+----------+
          |                    |
          v                    v
+--------------------+   +--------------------+
| common source tools |   | sector source tools|
| filings / news /    |   | bank / fintech /   |
| market / calendar   |   | regulator context  |
+---------+----------+   +----------+---------+
          |                         |
          +------------+------------+
                       |
                       v
              evidence extraction
                       |
                       v
              draft thesis
                       |
                       v
              skeptic review
                       |
                       v
              final report bundle
```

Stages that can run in parallel:

- common source planning and sector routing
- SEC, IR, news, market, options, and calendar fetches
- sector-specific source fetches after routing
- artifact normalization per source
- evidence extraction per artifact
- source-scout, filings-agent, market-structure-agent, and catalyst-agent work
- skeptic checks after a draft exists

Stages that should stay serialized:

- run creation
- evidence graph merge
- final thesis compilation
- final report write
- eval score finalization

## Discord Operating Model

Use Discord as the first rich operator surface, not just a notification sink.

```text
command channel
  request runs: "run market intel on SOFI"

run thread
  one thread per thesis run when useful

report channel
  final thesis card, confidence, gaps, artifact path

approval buttons
  only for exceptional actions, unknown plugin installs, or credential changes

terminal
  setup, diagnostics, direct smoke runs, recovery
```

Keep Discord permissions lean: bot token is secret, moderation actions disabled, channel permissions least-privilege.

## Failure And Resume

Keep failure behavior inspectable rather than clever.

```text
Source fetch fails
  record missing source + continue if non-critical

Subagent fails
  record failure + let chief decide retry, skip, or narrow task

Model call fails
  retry once with the same model, then downgrade or mark blocked

Skeptic blocks final thesis
  write draft + review.md + blocked status

Run is interrupted
  resume from artifacts already written when practical
```

Every failed or partial run should still leave a useful bundle in `outputs/market_intel/`.

## Artifact Contract

Target output root:

```text
outputs/market_intel/
  SOFI/
    2026-05-01/
      run_<run_id>/
        run.json
        sources.json
        evidence.json
        thesis.json
        thesis.md
        review.md
        agent_trace.jsonl
        hooks.jsonl
        model_calls.jsonl
        raw/
```

Existing output under `outputs/research_thesis/` can remain historical. New runs should use `outputs/market_intel/`.

Every material thesis claim must have a structured evidence record. The final prose is not the source of truth.

## Work Tracking And Memory

Use OpenClaw memory for continuity, but use artifacts as truth.

```text
OpenClaw sessions
  conversational memory and continuity

market-intel run bundle
  durable source of truth for one ticker/as-of run

work ledger
  append-only record of meaningful work and next steps
```

Add a lightweight ledger:

```text
outputs/market_intel/work_ledger.jsonl
```

Ledger row shape:

```json
{
  "ts": "2026-05-01T00:00:00Z",
  "actor": "market-intel-chief",
  "run_id": "run_market_intel_SOFI_...",
  "ticker": "SOFI",
  "action": "fetched_sec_filing",
  "artifact": "raw/sec/...",
  "status": "ok",
  "next": "extract revenue and guidance evidence"
}
```

Run status:

```text
created
collecting
extracting
drafting
reviewing
blocked
completed
refined
```

Refinement should create a revision, not overwrite the prior thesis:

```text
thesis.md
thesis_v2.md
revision_notes.md
```

When asked to refine, the agent should read prior `run.json`, `evidence.json`, `review.md`, and `thesis.md`, append or replace evidence as needed, then write a new revision with clear notes.

## Data Source Policy

Trust order:

```text
1. SEC and official regulatory filings
2. Company IR, releases, and presentations
3. Exchange and official event calendars
4. Broker or licensed market/news feeds
5. Third-party aggregators
6. LLM/web summaries
```

Valuation engine artifacts are allowed as context, not trusted evidence, until that engine is separately validated.

## MCP Position

MCP is not the first runtime boundary.

Use MCP later when we want:

- Codex, Claude Code, or OpenCode to call market-intel tools directly
- a portable read-only tool surface outside OpenClaw
- integration tests that exercise the tool contract without the OpenClaw gateway

Possible later adapter:

```text
market-intel plugin
  |
  +--> OpenClaw native tools and hooks
  |
  +--> optional market-intel MCP server
       read runs / start runs / fetch artifacts / score thesis
```

Do not build MCP first if it delays the OpenClaw plugin, hooks, and subagent loop.

## Safety Posture

The approach can be YOLO without giving the agent direct financial blast radius.

Light constraints:

- run under the `ade` user on the box
- Alpaca MCP starts with paper credentials
- no live trading credentials exposed to OpenClaw unless explicitly approved later
- market-intel artifacts stay under the repo `outputs/`
- OpenClaw may manage its own config, state, sessions, and channel data
- no `/tmp` output roots for run artifacts
- bounded but aggressive subagent and LLM concurrency
- clear prompt-injection rules for external content
- all final claims backed by evidence ids

## Deployment Posture

First target:

```text
host
  ade-nucbox-k8-plus

runtime
  OpenClaw gateway installed natively + native Ollama provider, no /v1 URL

operator surface
  Discord + terminal first

concurrency
  OpenClaw maxConcurrent 4, subagents 8, market-intel LLM concurrency 2

artifacts
  outputs/market_intel/

workspace
  /home/ade/spreads/app

service env
  SPREADS_WORKSPACE=/home/ade/spreads/app
```

Install OpenClaw under the box user that owns the repo checkout and can run `uv run spreads ...`. Point OpenClaw at the box's Ollama service and avoid extra deployment ceremony until the first run works.

Ollama config should use the native API base URL:

```text
baseUrl: "http://host:11434"
```

Deployment checks:

```text
openclaw gateway status
curl http://127.0.0.1:18789/healthz
curl http://127.0.0.1:18789/readyz
# removed 2026-06-11: uv run spreads market-intel thesis --ticker SOFI --as-of 2026-05-01 --json
```

## Build Order

```text
1. Done: rename the current scaffold to market_intel with temporary aliases where useful.
2. Done: install/configure OpenClaw natively on the box with repo workspace, uv, and Ollama access.
3. Done: configure Discord and terminal operation.
4. Done: upgrade Ollama for qwen3.5/qwen3.6/gemma4 support.
5. Evaluate current GLM baseline through OpenClaw before new pulls.
6. Done: pull and evaluate qwen3.5:27b; keep it gated, not default.
7. Done: create local market-intel plugin skeleton.
8. Partial: add market-intel-thesis skill; v0 source skills still need sector/source detail.
9. Done: add Alpaca MCP through OpenClaw's MCP client registry.
10. Done: wrap the repo CLI with initial plugin tool.
11. Done: add SEC and market snapshot source paths.
12. Done: add trace files and v0 finalizer guardrails; rendered thesis claims must map to evidence ids.
13. Done: run SOFI end-to-end through OpenClaw Gateway via `marketIntel.run`.
14. Done: add eval harness as CLI, plugin tool, and Gateway RPC.
15. Done: add LLM analyst and skeptic stages behind the evidence finalizer gate.
16. Done: evaluate qwen3.5:27b as the next OpenClaw primary candidate; it timed out in the OpenClaw loop.
17. Evaluate qwen3.6:35b only if we still want a bigger local model after fixing qwen3.5 contract/latency issues.
```

## Naming Migration

The active code path should now use `market_intel` naming. Historical `research_thesis` outputs and docs can remain as historical context.

Migration rule:

```text
Keep the rename focused:
  outputs/research_thesis       -> outputs/market_intel
  research_thesis module/docs    -> market_intel
  RESEARCH_THESIS_* env vars     -> MARKET_INTEL_*
  old CLI/env names              -> temporary aliases only if cheap
```

## Eval Gate

Before scheduled scans or broader autonomy:

```text
1. Done: run SOFI end-to-end through OpenClaw Gateway.
2. Write the report bundle under outputs/market_intel/.
3. Done: run the fixed model/tool eval suite.
4. Done: write eval output under outputs/market_intel_eval/.
5. Review failures before increasing tool freedom or pushing concurrency past the aggressive default.
```

## Edge Coverage

Cover the obvious failure edges without slowing the first build:

```text
Secrets
  Discord and provider keys live in OpenClaw config/env, never artifacts

Prompt injection
  external content is quoted/source-tagged and cannot override system/run rules

PDFs
  use OpenClaw pdf when available; keep deterministic extraction in the engine as fallback

Disk growth
  watch raw artifacts, session JSONL, cron logs, and media

Model pressure
  allow heavy local fanout, then tune Ollama parallelism from observed memory pressure

Model selection
  qwen2.5:3b remains the main/default model; qwen3.5:27b is installed and assigned to the dedicated market-intel frontier agent; qwen3.6:35b is deferred

Run trace
  keep agent_trace.jsonl, hooks.jsonl, model_calls.jsonl

Human override
  Discord or terminal can stop, inspect, and rerun by run id

Current model-path caveat
  full main-agent prompt/tool context is too heavy for qwen3.5 on CPU today; use the verified Gateway RPC for deterministic harness tests and keep shrinking the dedicated market-intel agent context/tool surface
```

## Decisions

- OpenClaw runs natively on `ade-nucbox-k8-plus` under the right user permissions.
- Discord and terminal are the first operator surfaces.
- Plugin tools shell out through `uv run spreads ...` first.
- Alpaca MCP is available to OpenClaw in paper mode with full toolsets first.
- Alpaca MCP is registered as `alpaca`; the wrapper maps existing spreads `APCA_*` env values to Alpaca MCP's `ALPACA_*` inputs without storing raw keys in OpenClaw config.
- Current OpenClaw main/default model is `ollama/qwen2.5:3b`; model-planned tool calls are not accepted as reliable yet.
- Dedicated OpenClaw `market-intel` agent uses `ollama/qwen3.5:27b-q4_K_M` as the frontier model, with the main agent kept on qwen2.5 for recoverability.
- `qwen3.5:27b-q4_K_M` is installed and available, but the full main-agent context is still too slow on CPU even after raising command and provider timeouts.
- `qwen3.6:35b` is deferred.
- Historical: `market_intel` replaced `research_thesis` in this prototype path, then the market-intel code path was removed from the shipped repo on 2026-06-11.
- First engine-native source providers are SEC and market snapshot; Alpaca MCP is also available as an OpenClaw tool surface.
- The v0 finalizer writes `thesis.json`, `thesis.md`, `review.json`, and `review.md`; a final thesis is only rendered from evidence-backed claims.
- Historical: the eval harness was available as `uv run spreads market-intel eval`, plugin tool `market_intel_eval`, and Gateway RPC `marketIntel.eval`; those shipped surfaces were removed on 2026-06-11.
