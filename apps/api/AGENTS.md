# API Instructions

- Keep `apps/api` as a thin adapter over backend services.
- Do not re-implement business rules, aggregation, or repository queries in route handlers when a service can own them.
- Prefer extending existing service payloads over creating API-only parallel logic.
- Keep endpoint shapes narrow and caller-driven. Expand only when there is a real consumer.
- For runtime and rollout guidance, also follow [src/spreads/AGENTS.md](../../src/spreads/AGENTS.md).
