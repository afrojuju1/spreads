# API Instructions

- Keep `packages/api` as a thin adapter over backend services.
- Treat [../../docs/current_system_state.md](../../docs/current_system_state.md) as the canonical source of truth for current route and service ownership.
- Do not duplicate the domain ownership map in this file. If API work raises ownership questions, update `docs/current_system_state.md` rather than adding a parallel map here.
- Do not re-implement business rules, aggregation, or repository queries in route handlers when a service can own them.
- Prefer extending existing service payloads over creating API-only parallel logic.
- Keep endpoint shapes narrow and caller-driven. Expand only when there is a real consumer.
- Keep route ownership explicit:
  - internal ops/trading/live-doctor routes -> `services/ops/`
  - position execution mutations -> `services/execution/`
  - positions read routes -> `services/positions.py`
  - account overview routes -> `services/account_state.py`
  - control routes -> `services/control_plane.py`
  - execution runtime/manual order routes -> `services/execution/`
- Pipeline, opportunities, discovery-run, and UOA routes are retired active surfaces. Do not re-add compatibility route wrappers around them.
- Active cleanup `spr-zuy` is replacing fragmented internal ops routes with properly named trading/storage ops state routes. During that work, remove old active route concepts instead of preserving compatibility wrappers.
- For runtime and rollout guidance, also follow [packages/core/AGENTS.md](../../packages/core/AGENTS.md).
