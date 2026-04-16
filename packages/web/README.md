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

## Local Run

From [packages/web](./):

```bash
npm install
npm run dev
```

Open [http://localhost:3000](http://localhost:3000).

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
