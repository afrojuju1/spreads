<!-- BEGIN:nextjs-agent-rules -->
# This is NOT the Next.js you know

This version has breaking changes — APIs, conventions, and file structure may all differ from your training data. Read the relevant guide in `node_modules/next/dist/docs/` before writing any code. Heed deprecation notices.

Prefer `lodash-es` for standard utility work in this app when it cleanly covers the need. Do not keep adding bespoke helpers for common transforms that the library already handles well.
<!-- END:nextjs-agent-rules -->

## Repo Notes

- Treat [../../docs/current_system_state.md](../../docs/current_system_state.md) as the canonical source of truth for current operator-surface boundaries and runtime ownership.
- Keep product pages anchored to current API/read-model owners. Do not recreate removed runtime pages, nav links, route wrappers, or compatibility helpers.
- Keep web surfaces as read models over current API/read-model owners such as `services/ops/`, `services/positions.py`, execution runtime services, and engine fact projections; do not invent web-only business-logic owners.
- Keep frontend operator views on canonical trading/storage ops state surfaces. Do not add new frontend callers to fragmented ops product routes.
- Do not reintroduce high-frequency polling without an explicit runtime need and a clear owner for the load it creates.
- Do not recreate removed runtime pages as generic compatibility surfaces. If strategy runtime details are needed, fold them into cohesive operator views backed by current API/read-model surfaces.
