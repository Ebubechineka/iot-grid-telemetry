# ─────────────────────────────────────────────────────────────
# Multi-stage build: compile TypeScript, ship only what runs.
# ─────────────────────────────────────────────────────────────
FROM node:20-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev || npm install --omit=dev

FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci || npm install
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

FROM node:20-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production

# Run as a non-root user.
RUN addgroup -S app && adduser -S app -G app

COPY --from=deps  /app/node_modules ./node_modules
COPY --from=build /app/dist         ./dist
COPY public ./public
COPY sql    ./sql
COPY package.json ./

USER app
EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:3000/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

CMD ["node", "dist/server.js"]
