<!-- BEGIN:nextjs-agent-rules -->
# This is NOT the Next.js you know

This version has breaking changes — APIs, conventions, and file structure may all differ from your training data. Read the relevant guide in `node_modules/next/dist/docs/` before writing any code. Heed deprecation notices.

Prefer `lodash-es` for standard utility work in this app when it cleanly covers the need. Do not keep adding bespoke helpers for common transforms that the library already handles well.
<!-- END:nextjs-agent-rules -->

## Repo Notes

- The generator workbench's `board` and `watchlist` promote actions are legacy manual overrides over the live collector state.
- Do not expand that UI model as if it were the long-term selection architecture; the target direction is one canonical opportunity list with rendered views.
- When touching generator/operator surfaces, preserve existing behavior unless the change is explicitly part of the selection-state migration.
