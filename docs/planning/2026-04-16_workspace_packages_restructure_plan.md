# Workspace Packages Restructure Plan

Status: completed

As of: Thursday, April 16, 2026

## Goal

Restructure the repo into an explicit workspace layout:

- `packages/core`
- `packages/api`
- `packages/config`
- `packages/web`

This is a packaging and repo-structure cleanup, not a behavioral rewrite.

The goal is to:

- remove unnecessary nesting such as `src/spreads`, `apps/api`, and `apps/web`
- make workspace boundaries explicit
- keep `core` as the canonical backend/runtime package
- make shared config a first-class workspace package instead of hiding it under `packages/config`

## Target Tree

```text
packages/
  core/
    __init__.py
    cli/
    db/
    domain/
    events/
    integrations/
    jobs/
    runtime/
    services/
    storage/
    common.py
    AGENTS.md

  api/
    __init__.py
    app.py
    main.py
    lifespan.py
    errors.py
    routes/
    schemas/
    AGENTS.md

  config/
    automations/
    bots/
    strategies/
    universes/
    system.yaml

  web/
    app/
    package.json
    next.config.*
```

Keep at repo root:

- `alembic/`
- `docs/`
- `docker-compose.yml`
- `scripts/`
- root workspace config such as `pyproject.toml`

## Import Model

After the move:

- `from core.services...` becomes `from core.services...`
- `from api...` becomes `from api...`
- CLI entrypoint moves from `spreads.cli.main:main` to `core.cli.main:main`
- ARQ worker entrypoints move from `spreads.jobs.worker...` to `core.jobs.worker...`

This is the main migration cost.

## Config Root Change

The current options automation config root is:

- `packages/config/...`

Target config root should become:

- `packages/config/automations/...`
- `packages/config/bots/...`
- `packages/config/strategies/...`
- `packages/config/universes/...`

Key current code that assumes the old root:

- `packages/core/services/strategy_configs.py`
- `packages/core/services/automations.py`
- `packages/core/services/bots.py`

These should switch from repo-root-relative `packages/config` resolution to workspace-root-relative `packages/config` resolution.

## Current Path Assumptions To Update

### Packaging

Current `pyproject.toml` assumes:

- wheel package path: `packages/core`
- CLI script target: `spreads.cli.main:main`

This must change to:

- wheel package path: `packages/core`
- CLI script target: `core.cli.main:main`

### Docker Compose

Current `docker-compose.yml` assumes:

- API app import path: `apps.api.main:app`
- worker paths: `spreads.jobs.worker.RuntimeWorkerSettings`
- reload dirs: `src` and `packages/api`
- web build context and bind mount: `packages/web`

These must change to:

- API app import path: `api.main:app`
- worker paths: `core.jobs.worker.RuntimeWorkerSettings` and `DiscoveryWorkerSettings`
- reload dirs: `packages/core` and `packages/api`
- web build context and bind mount: `packages/web`

### Alembic

Current `alembic/env.py` imports from `spreads.storage...`

These must change to `core.storage...`.

### API Thin Adapter

Current `packages/api/main.py` imports:

- `from api.app import app`

That should become `from api.app import app` after the move.

## Migration Approach

Do this in phases. Do not try to combine the filesystem move, import rewrite, and config-root rewrite into one blind mega-change without a plan.

### Phase 1: Prepare The Workspace

1. create `packages/core`, `packages/api`, and `packages/config`
2. move files without changing behavior yet
3. update `pyproject.toml`, Docker, and Alembic import roots
4. switch CLI/worker/api entrypoints to `core.*` and `api.*`

Goal:

- repo boots from the new paths with no behavioral changes

### Phase 2: Update Imports

1. rewrite `spreads.*` imports to `core.*`
2. rewrite `apps.api.*` imports to `api.*`
3. remove any temporary compatibility imports once the repo is green

Goal:

- no code still depends on the old package roots

### Phase 3: Move Config Root

1. move `packages/config/*` into `packages/config/*`
2. update config-root resolution in strategy/bot/automation services
3. reseed jobs and verify bots still load

Goal:

- runtime config loads from the workspace package tree, not the old nested config root

### Phase 4: Cleanup

1. delete old empty directories such as `packages/core` and `packages/api`
2. remove stale docs/examples that refer to old import roots
3. update planning docs and README entrypoints where needed

Goal:

- no ambiguous dual layout remains

## Recommended Rollout Order

1. move `packages/api` to `packages/api` first or together with core
2. move `packages/web` to `packages/web`
3. move `packages/core` to `packages/core`
4. update `pyproject.toml`
5. update Docker Compose commands and reload dirs
6. update Alembic imports
7. update config-root resolution to `packages/config`
8. reseed jobs and restart runtime services

## Validation Checklist

After the restructure:

1. `uv run alembic upgrade head`
2. `uv run spreads jobs seed`
3. `docker compose restart scheduler worker-runtime worker-discovery api web`
4. `uv run spreads status`
5. `uv run spreads trading`
6. `uv run spreads jobs`

Success means:

- CLI still works
- API still boots
- web still boots
- workers still load
- Alembic still imports metadata
- bot config still loads from the new config root

## Recommendation

This restructure makes sense for the repo.

It is a workspace-style cleanup with clear long-term value.

The main cost is import churn and path rewiring, not business-logic risk. That means it should be executed as a packaging migration with strong runtime validation, not mixed into unrelated strategy or automation work.

Implementation status:

- `src/spreads` moved to `packages/core`
- `apps/api` moved to `packages/api`
- `apps/web` moved to `packages/web`
- `config/options_automation/*` moved to `packages/config/*`
- import roots rewired from `spreads.*` to `core.*` and from `apps.api.*` to `api.*`
