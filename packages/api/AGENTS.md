# API Instructions

- Keep `packages/api` as a thin adapter over backend services.
- Do not re-implement business rules, aggregation, or repository queries in route handlers when a service can own them.
- Prefer extending existing service payloads over creating API-only parallel logic.
- Keep endpoint shapes narrow and caller-driven. Expand only when there is a real consumer.
- Keep route ownership explicit:
  - pipeline runtime routes -> `services/pipelines.py`
  - UOA routes -> `services/uoa_state.py`
  - internal ops/health visibility routes -> `services/ops_visibility.py`
- For runtime and rollout guidance, also follow [packages/core/AGENTS.md](../../packages/core/AGENTS.md).
