# Builder stage
FROM node:24-trixie-slim AS builder
RUN npm i -g pnpm@10.34.1
WORKDIR /src

COPY package.json pnpm-lock.yaml tsconfig.json ./
COPY types types
COPY src src

# All deps are published on the public npm registry, so no auth is required to
# build from source.
RUN pnpm install --frozen-lockfile

# build:docker skips type checking. We typecheck separately in CI.
RUN pnpm build:docker

# Production stage
FROM node:24-trixie-slim AS production
RUN npm i -g pnpm@10.34.1
WORKDIR /src

COPY package.json pnpm-lock.yaml ./

# Public deps only - no registry auth needed (see the builder stage note above).
RUN pnpm install --frozen-lockfile --prod

COPY --from=builder /src/dist /src/dist

# Drop esbuild + any *-exec binaries we don't need at runtime
RUN find /root/.local/share/pnpm/store -name "*-exec" -type f -delete || true && \
    find /src/node_modules -name "esbuild" -type f -delete || true

ENTRYPOINT ["node", "/src/dist/index.js"]
