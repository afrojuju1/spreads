# API Instructions

- Keep `packages/api` as a thin adapter over backend services.
- Treat [../../docs/current_system_state.md](../../docs/current_system_state.md) as the canonical source of truth for current route and service ownership.
- Do not re-implement business rules, aggregation, or repository queries in route handlers when a service can own them.
- Prefer extending existing service payloads over creating API-only parallel logic.
- Keep endpoint shapes narrow and caller-driven. Expand only when there is a real consumer.
- Keep route ownership explicit:
  - pipeline list/detail routes -> `services/pipelines.py`
  - current runtime/session health surfaces -> `services/live_runtime.py`, `services/live_collector_health/`, and `services/pipelines.py`
  - opportunities routes -> `services/opportunities.py`
  - opportunity and position execution mutations -> `services/execution/`
  - positions read routes -> `services/positions.py`
  - UOA routes -> `services/uoa_state.py`
  - internal ops/health visibility routes -> `services/ops/`
  - account overview routes -> `services/account_state.py`
- For runtime and rollout guidance, also follow [packages/core/AGENTS.md](../../packages/core/AGENTS.md).
