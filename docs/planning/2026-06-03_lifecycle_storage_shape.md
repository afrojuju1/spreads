# Lifecycle Storage Shape

Date: 2026-06-03

Bead: `spr-g9s.3`

Status: target schema implemented; 0043 strategy ownership cleanup applied; live writers are not cut over yet.

## Purpose

This bead turns the lifecycle object model and typed state contracts into a durable storage shape. It intentionally uses a breaking-rewrite posture: current Spreads storage is context, not a compatibility target. Downtime is allowed during cutover, and this schema starts as the clean target rather than a dual-write bridge.

## Storage Boundary

The target schema separates lifecycle facts from operator projections.

Fact tables:

- `trade_signals`
- `trade_decisions`
- `trade_execution_intents`
- `trade_admissions`
- `trade_execution_attempts`
- `trade_broker_orders`
- `trade_broker_fills`
- `trade_close_decisions`
- `trade_position_closes`
- `trade_reconciliation_observations`
- `trade_lifecycle_events`

Projection tables:

- `trade_positions`

`trade_positions` is the operator-facing position projection. It should be rebuilt from fills, closes, and broker reconciliation rather than treated as the sole source of historical truth.

## Strategy Ownership

Target lifecycle ownership uses durable trading-strategy vocabulary instead of bot, automation, or strategy-config identifiers. Strategy-owned lifecycle rows carry:

- `trading_strategy_id`
- `trade_structure`
- `routine`
- `config_hash`

`trade_structure` is the target lifecycle name for the old strategy-family concept. The target lifecycle schema does not keep compatibility aliases for `bot_id`, `automation_id`, `strategy_config_id`, or `strategy_family`.

## State Storage

Lifecycle state columns are stored as text values from `core.services.trading_lifecycle`. The schema deliberately avoids database enum types in this first cut so state contracts can settle through Python code while the refactor is active. A later hardening bead can add `CHECK` constraints or Postgres enum types once the runtime is fully cut over.

Raw broker statuses stay in `trade_broker_orders.broker_status`; normalized order state is stored separately in `trade_broker_orders.normalized_order_state`.

## Idempotency And Lineage

The schema makes duplicate prevention and replacement lineage explicit:

- `trade_signals.idempotency_key` keeps feed observations idempotent.
- `trade_decisions.run_key + trade_signal_id` keeps each lifecycle run deterministic.
- `trade_execution_intents.idempotency_key` protects dispatch.
- `trade_execution_attempts.client_order_id` and `trade_broker_orders.broker_order_id` anchor broker-side idempotency.
- `supersedes_*` and `superseded_by_*` fields preserve retry, reprice, revoke, and replacement chains without mutating prior facts into invisibility.

## Cutover Posture

Existing runtime tables such as `execution_intents`, `execution_attempts`, `execution_orders`, `execution_fills`, `portfolio_positions`, `position_closes`, and `risk_decisions` remain historical/read-only context until the later cutover bead decides whether to archive, export, or drop them.

The target lifecycle tables start empty. No automatic backfill is required before the first runtime cutover. If active paper positions exist during the cutover window, they should be closed/canceled intentionally or rebuilt into `trade_positions` from broker state during the position/reconciliation bead.

## Historical Operator CLI

This bead originally exposed a temporary schema inspection command:

```bash
# removed 2026-06-11: uv run spreads lifecycle schema
# removed 2026-06-11: uv run spreads lifecycle schema --json
```

That command was removed in the 2026-06-11 CLI cleanup because it described target schema plumbing rather than an active operator lifecycle workflow. Use direct service/module inspection if this historical schema summary is needed again, or reintroduce it through a development-only surface.

## Runtime Risk

This bead defines the target storage and schema inspection surface only. Live Finviz, admission, attempt, position, and close writers still use the current runtime until later beads wire them into these tables.

## Validation

Validation for this bead is schema and runtime-smoke oriented, not automated-test oriented.

Additional 0043 cleanup validation was run with targeted Python compilation, Alembic `heads`/`upgrade head`/`current`, direct lifecycle summary assertions, direct database column inspection for the target lifecycle tables, and `git diff --check`. The temporary lifecycle schema CLI mentioned here was later removed from the shipped operator surface.

Formatter note: at bead close, `ruff` and `black` were not installed yet. A follow-up tooling setup added both formatters and ran Black against the lifecycle schema files.

Commands run:

```bash
uv run black packages/core/storage/lifecycle_models.py alembic/versions/20260603_0042_target_trade_lifecycle_schema.py
uv run python -m py_compile packages/core/storage/lifecycle_models.py packages/core/services/lifecycle_schema.py packages/core/cli/lifecycle.py packages/core/cli/main.py alembic/env.py alembic/versions/20260603_0042_target_trade_lifecycle_schema.py
# removed 2026-06-11: uv run spreads lifecycle schema --json
# removed 2026-06-11: uv run spreads lifecycle schema
uv run alembic heads
uv run python - <<'PY'
from core.storage.db import Base
from core.storage import lifecycle_models  # noqa: F401

trade_tables = [table.name for table in Base.metadata.sorted_tables if table.name.startswith("trade_")]
print(len(trade_tables))
for name in trade_tables:
    print(name)
PY
git diff --check
```
