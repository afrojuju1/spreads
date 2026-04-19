FROM node:22-alpine AS base

WORKDIR /app

FROM base AS dev

COPY package.json package-lock.json ./
COPY scripts/docker-dev-entrypoint.sh /usr/local/bin/web-dev-entrypoint

RUN npm ci
RUN chmod +x /usr/local/bin/web-dev-entrypoint

ENV NODE_ENV=development
ENV NEXT_TELEMETRY_DISABLED=1

CMD ["web-dev-entrypoint"]

FROM base AS deps

COPY package.json package-lock.json ./

RUN npm ci

FROM base AS builder

COPY --from=deps /app/node_modules ./node_modules
COPY . .

ENV NEXT_TELEMETRY_DISABLED=1

RUN npm run build

FROM base AS runner

ENV NODE_ENV=production
ENV PORT=3000
ENV HOSTNAME=0.0.0.0
ENV NEXT_TELEMETRY_DISABLED=1

COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public

EXPOSE 3000

CMD ["node", "server.js"]
