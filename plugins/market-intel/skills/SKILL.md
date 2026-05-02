---
name: market-intel
description: Run evidence-backed market intelligence workflows through the spreads engine and OpenClaw tools.
---

# Market Intel

Use this skill when the operator asks for market-intel research, a ticker thesis, catalyst checks, SEC-backed evidence, or Alpaca market context.

## Operating Rules

- Keep the workflow single-ticker first unless the operator asks for a batch.
- Prefer `uv run spreads market-intel thesis --ticker <TICKER> --json`.
- Write durable artifacts under `outputs/market_intel/`.
- Treat Alpaca MCP as paper-mode market/account context unless the operator explicitly changes that policy.
- Use SEC and market snapshot evidence before softer sources.
- Do not treat generated valuation text as ground truth; cite the underlying evidence.
- Keep final claims tied to evidence ids or explicitly mark the gap.

## Default Flow

1. Normalize the ticker and as-of date.
2. Run the `market_intel_run` tool or the equivalent spreads CLI.
3. Use Alpaca MCP for current market context when needed.
4. Inspect the produced run bundle before summarizing.
5. Return a concise operator summary with artifact path, strongest evidence, unresolved gaps, and next action.
