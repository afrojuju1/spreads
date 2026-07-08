# Temporal engine rewrite

Tracker: `spr-w3r`

## Target shape

Spreads keeps its current product boundary, strategy catalog, Postgres ledger, ClickHouse market data, Alpaca adapter, and `TradingOpsState` operator model. The execution and close lifecycle move from scheduled dispatch jobs to durable Temporal workflows backed by an engine event log and outbox.

The new ownership model is:

- `core.engine`: command/event vocabulary, deterministic IDs, lifecycle transition facade, outbox publishing.
- `engine_events`: append-only cross-aggregate event log for workflow, broker, position, and projection facts.
- `engine_outbox`: transactional fanout queue for JetStream projections.
- `core.workflows`: Temporal trade and close lifecycle orchestration.
- Existing strategy, admission, execution, broker, and portfolio services become workflow activities where they still own real domain work.

## Rewrite rules

- Delete displaced dispatch paths when a workflow path takes ownership. Do not keep compatibility dispatch loops alive.
- Keep Redis/ARQ only for non-lifecycle background jobs until those are deliberately moved or removed.
- Do not introduce a second trading ledger. Postgres remains the business source of truth.
- Do not move market data into workflow history. Temporal stores orchestration state; ClickHouse stores market data.
- Emit every state transition through `engine_events` and publish projections only from `engine_outbox`.
- Deterministic workflow IDs and broker client order IDs are mandatory before any broker submit activity is enabled.

## Cut order

1. Land the engine event/outbox schema, repository, command/event contracts, runtime configuration, and empty workflow entry points.
2. Move pending execution-intent dispatch into a Temporal workflow starter. This is the first deletion point for the global dispatch loop.
3. Convert broker submit, refresh, cancel, and order/fill sync into idempotent activities. Activities update existing attempt/order/fill tables and append engine events in the same Postgres transaction where possible.
4. Move close lifecycle orchestration into Temporal. Delete close-specific queued dispatch once close workflows own submit and reconciliation.
5. Update `TradingOpsState` to prefer engine events/workflow fields for lifecycle health while preserving existing position and attempt read models.
6. Remove stale scheduler docs, job definitions, and worker registrations for lifecycle dispatch/submit after live validation proves there is no dual-submit path.

## First validation gate

- Alembic can create `engine_events` and `engine_outbox`.
- `EngineEventRepository.append_engine_event()` is idempotent by `idempotency_key` and creates at most one outbox row per event/stream/subject.
- `publish_pending_engine_outbox()` marks messages published only after JetStream accepts them and retries failed publishes from the outbox.
- Workflow IDs come from `core.engine.ids`, not caller-local string formatting.
- Active config exposes Temporal and NATS connection settings without changing current paper runtime behavior.
