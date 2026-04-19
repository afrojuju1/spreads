# Spreads Web

Operator dashboard for the `spreads` backend. The app is built with:

- Next.js App Router
- Tailwind CSS
- shadcn/ui primitives
- TanStack Query
- TanStack Table

## What It Shows

- live board and watchlist for a selected collector label
- recent board events
- latest Discord alerts
- seeded job health and recent job runs
- session outcomes and signal-tuning buckets

## Canonical Dev Run

The canonical frontend dev workflow for this repo is the repo-level Docker
Compose stack.

Start or rebuild the `web` service from the repo root:

```bash
docker compose up -d --build web
```

Open [http://localhost:53000](http://localhost:53000).

The web container bind-mounts `packages/web` as source and keeps `node_modules`
in a Docker volume. On startup, the dev entrypoint compares the current
`package-lock.json` to the dependency volume and runs `npm ci` automatically when
they diverge, so dependency changes are picked up without manual `docker compose exec`
recovery commands.

For normal source-only changes, the running dev server hot-reloads. If you change
dependencies, restart or recreate the `web` service:

```bash
docker compose restart web
```

Or:

```bash
docker compose up -d --build web
```

## Secondary Local Run

Running the Next.js app directly from [packages/web](./) is still possible for
isolated frontend work, but it is not the canonical repo workflow:

```bash
npm install
npm run dev
```

That path serves the app on [http://localhost:3000](http://localhost:3000).

## Backend Requirement

The UI talks to the backend API through a Next route handler proxy at `/api/backend/*`.

By default, the proxy targets:

```bash
http://localhost:58080
```

Override it with:

```bash
SPREADS_API_BASE_URL=http://localhost:58080
```

## Validation

```bash
npm run lint
npm run build
```
