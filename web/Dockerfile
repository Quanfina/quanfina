# Quanfina Next.js 16 — Cloud Run production image (P572, 20 Haz 2026). Standalone output.
# KRITIK: Next.js rewrites() BUILD-TIME degerlendirilir -> API_URL pnpm build sirasinda
# olmali (ARG -> ENV builder stage). Cloud Build --build-arg API_URL=<api Cloud Run URL>.
FROM node:24-slim AS deps
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
# --ignore-scripts: tum build-script'leri KASTEN atla -> pnpm 11 ERR_PNPM_IGNORED_BUILDS olmaz
# (sharp/msw/unrs onay gerektirmez). sharp runtime image-opt: yoksa Next unoptimized fallback
# (crash etmez, deploy sonrasi iyilestirilebilir). --no-frozen: pnpm-workspace ekleme mismatch'i.
RUN pnpm install --no-frozen-lockfile --ignore-scripts

FROM node:24-slim AS builder
WORKDIR /app
RUN corepack enable
COPY --from=deps /app/node_modules ./node_modules
COPY . .
# /api/* proxy hedefi build manifest'e gomulur (rewrites build-time)
ARG API_URL=http://localhost:8000
ENV API_URL=$API_URL
ENV NEXT_TELEMETRY_DISABLED=1
RUN pnpm build

FROM node:24-slim AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
# Next standalone: minimal node_modules + server.js + static + public
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public
EXPOSE 8080
ENV PORT=8080
ENV HOSTNAME=0.0.0.0
CMD ["node", "server.js"]
