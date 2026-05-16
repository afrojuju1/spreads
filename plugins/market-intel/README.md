# Market Intel OpenClaw Plugin

OpenClaw plugin shell for the spreads market-intel workflow.

Current scope:

- registers `market_intel_run`
- registers `market_intel_eval`
- registers Gateway RPC `marketIntel.run`
- registers Gateway RPC `marketIntel.eval`
- registers `/market-intel` for OpenClaw native command surfaces
- adds lightweight market-intel prompt context
- ships a market-intel skill
- expects Alpaca MCP to be registered as `alpaca` in OpenClaw config

The first runtime target is the NUC OpenClaw Gateway with workspace `/home/ade/spreads/app`.

Verified terminal harness path:

```text
openclaw gateway call marketIntel.run --json --timeout 120000 --params '{"ticker":"SOFI","asOf":"2026-05-01","sources":"sec,market","noLlm":true}'
```

Verified eval harness path:

```text
openclaw gateway call marketIntel.eval --json --timeout 240000 --params '{"tickers":"SOFI","asOf":"2026-05-01","sources":"sec,market","noLlm":false}'
```
