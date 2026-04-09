# Repo Instructions

## Core

- Keep changes minimal and focused unless broader refactors are explicitly requested.
- Do not commit or push unless explicitly asked.
- Prefer `uv run` for Python commands in this repo.
- For Alpaca-related research, scanner design, or alerting work, read the canonical capability statement in [docs/research/alpaca_capabilities_statement.md](/Users/adeb/Projects/spreads/docs/research/alpaca_capabilities_statement.md) first. Re-check Alpaca's official docs/OpenAPI only when the task depends on current product changes, limits, or newly added endpoints.

## Dev Workflow

- This repo is in active development by default.
- Do not run production build commands such as `npm run build` or `next build` unless the user explicitly asks for a production check or release validation.
- Do not run repo-wide Python compile checks such as `python -m compileall` unless the user explicitly asks for them.
- Prefer dev-safe verification during normal work, such as linting, targeted type checks, and narrow runtime checks.

## Web App

- Prefer established utility helpers over bespoke one-off implementations for common collection, object, and string transforms.
- In `apps/web`, prefer `lodash-es` when it covers the job cleanly instead of writing custom utility code from scratch.
- Read and follow the more specific instructions in [apps/web/AGENTS.md](/Users/adeb/Projects/spreads/apps/web/AGENTS.md) when working under `apps/web`.
