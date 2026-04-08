# Repo Instructions

## Core

- Keep changes minimal and focused unless broader refactors are explicitly requested.
- Do not commit or push unless explicitly asked.
- Prefer `uv run` for Python commands in this repo.

## Dev Workflow

- This repo is in active development by default.
- Do not run production build commands such as `npm run build` or `next build` unless the user explicitly asks for a production check or release validation.
- Prefer dev-safe verification during normal work, such as linting, targeted type checks, and narrow runtime or compile checks.

## Web App

- Read and follow the more specific instructions in [apps/web/AGENTS.md](/Users/adeb/Projects/spreads/apps/web/AGENTS.md) when working under `apps/web`.
