# Frontend Runtime API Refactor Plan

## Summary

The web app still assumes the older broad API surface. The backend is now intentionally narrow:

- `GET /health`
- `GET /account/overview`
- `GET /control/state`
- `POST /control/mode`
- `GET /sessions`
- `GET /sessions/{session_id}`
- `POST /sessions/{session_id}/executions`
- `POST /sessions/{session_id}/positions/{session_position_id}/close`
- `POST /sessions/{session_id}/executions/{execution_attempt_id}/refresh`
- `GET /internal/uoa/state`
- `GET /internal/uoa/cycles/{cycle_id}`
- `WS /ws/events`

The frontend should be refactored to match that runtime contract instead of preserving legacy generator, alerts, jobs, and other debug views that no longer exist in the main API.

Note:

- the old internal option-stream capture routes were removed when option-stream ownership moved fully into `services/market_recorder.py`

## Current Mismatch

The current web app still calls deleted or intentionally removed surfaces:

- [`apps/web/lib/api.ts`](../../apps/web/lib/api.ts)
  - `getLive()`
  - `getUniverses()`
  - `getGeneratorSymbols()`
  - `getLiveEvents()`
  - `getAlerts()`
  - `getJobs()`
  - `getJobRuns()`
  - `getJobsHealth()`
  - `generateIdeas()`
  - `createGeneratorJob()`
  - `getGeneratorJobs()`
  - `getGeneratorJob()`
  - `createGeneratorCandidateAction()`
  - `buildGeneratorJobWebSocketUrl()`

- [`apps/web/components/layout-nav.tsx`](../../apps/web/components/layout-nav.tsx)
  - still exposes `Generator`, `Alerts`, and `Jobs`

- [`apps/web/components/providers.tsx`](../../apps/web/components/providers.tsx)
  - still invalidates generator, alerts, and jobs queries
  - still renders notices pointing to `/generator`, `/alerts`, and `/jobs`
  - still handles `generator.job.updated` as a first-class product event

- route tree still includes pages that no longer have a canonical backend:
  - [`apps/web/app/generator/page.tsx`](../../apps/web/app/generator/page.tsx)
  - [`apps/web/app/alerts/page.tsx`](../../apps/web/app/alerts/page.tsx)
  - [`apps/web/app/jobs/page.tsx`](../../apps/web/app/jobs/page.tsx)

## Target Product Surface

The runtime web app should become a small session-centric operator console.

Keep:

- `/sessions`
- `/sessions/[sessionId]`
- `/account`

Drop from the main app:

- `/generator`
- `/alerts`
- `/jobs`

If those views are still needed later, they should return as a separate `/ops` or `/labs` surface backed by an explicitly designed backend, not as leftovers from the old API.

## Target Frontend Architecture

### 1. Shrink the API client to canonical runtime calls

[`apps/web/lib/api.ts`](../../apps/web/lib/api.ts) should keep only:

- `getAccountOverview()`
- `getSessions()`
- `getSessionDetail()`
- `createSessionExecution()`
- `closeSessionPosition()`
- `refreshSessionExecution()`
- `buildSessionHref()`
- `parseGlobalRealtimeEvent()`
- `buildGlobalEventsWebSocketUrl()`

Optional internal-only client methods may remain if there is a real page or workflow for them, but the main product app should not depend on them.

Delete the API client methods for removed backend routes instead of leaving them as dead helpers.

### 2. Narrow the route tree

The app route tree should reduce to:

- [`apps/web/app/page.tsx`](../../apps/web/app/page.tsx)
  - keep redirect to `/sessions`
- `apps/web/app/account/page.tsx`
- `apps/web/app/sessions/page.tsx`
- `apps/web/app/sessions/[sessionId]/page.tsx`

Remove:

- `apps/web/app/generator/page.tsx`
- `apps/web/app/alerts/page.tsx`
- `apps/web/app/jobs/page.tsx`

### 3. Simplify navigation

[`apps/web/components/layout-nav.tsx`](../../apps/web/components/layout-nav.tsx) should only show:

- `Sessions`
- `Account`

Do not leave dead nav items that route into pages backed by deleted API contracts.

### 4. Re-scope realtime handling

[`apps/web/components/providers.tsx`](../../apps/web/components/providers.tsx) should be simplified around the runtime app:

- keep global websocket connection to `/ws/events`
- invalidate only:
  - `["account-overview"]`
  - `["sessions"]`
  - `["session", sessionId]`
- keep notices for:
  - `execution.attempt.updated`
  - `live.cycle.updated`
  - `live.collector.degraded`
  - `post_market.analysis.updated`
  - `alert.event.created` / `alert.event.updated` only if they are shown via session detail, not an alerts page
  - `job.run.updated` only if it deep-links to a session context and is treated as operational signal, not a jobs dashboard

Reduce or remove:

- generator-specific query invalidation
- generator-specific notices
- links to `/jobs` and `/alerts`

If a realtime event no longer has a user-facing destination in the app, it should not create a notice that points at a dead route.

### 5. Make session detail the canonical workspace

The session detail page should absorb anything still operationally necessary from removed secondary pages:

- alert history relevant to the session
- execution history and refresh controls
- current cycle opportunities
- control/risk/exposure state
- embedded post-market summary when available

This is already directionally true in the backend. The frontend should follow the same model instead of splitting one session across multiple top-level pages.

## Proposed Migration Sequence

### Phase 1: stop the breakage

- remove deleted backend calls from `lib/api.ts`
- remove dead pages from `app/`
- remove dead nav items
- stop rendering notices that link to dead routes

Result:

- the web app stops depending on missing endpoints
- the runtime UI is reduced to working account/session surfaces

### Phase 2: simplify shared frontend state

- reduce the provider invalidation matrix to account/session keys only
- remove generator/jobs/alerts client schemas and fetch helpers
- remove generator websocket URL builder and parser

Result:

- less stale client code
- fewer unnecessary query keys and notification branches

### Phase 3: tighten the session workspace

- review [`apps/web/components/sessions/session-detail.tsx`](../../apps/web/components/sessions/session-detail.tsx)
- collapse any UI that still assumes separate jobs/alerts/generator destinations
- make session detail the single operational drill-down surface

Result:

- one canonical runtime page model
- less cross-page coordination

## Validation

Do runtime validation only.

- load `/sessions`
- load `/sessions/[sessionId]`
- load `/account`
- confirm websocket connection stays healthy
- confirm execution refresh and session action flows still work in the browser
- confirm nav contains no dead destinations
- confirm deleted pages no longer exist or redirect intentionally

Do not rebuild a fake compatibility layer in the backend just to keep the old web app unchanged.

## Non-Goals

- preserving the current generator workbench
- preserving top-level alerts/jobs dashboards
- recreating deleted backend debug routes
- designing a future `/ops` app in this pass

## Recommendation

Treat the next frontend pass as a deliberate product narrowing, not a compatibility patch. The right web app for the current backend is a focused session-and-account console, not a mixed operator/debug/generator dashboard.
