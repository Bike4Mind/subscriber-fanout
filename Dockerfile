# Builder stage
FROM node:24-trixie-slim AS builder
RUN npm i -g pnpm@10.34.1
WORKDIR /src

COPY package.json pnpm-lock.yaml tsconfig.json ./
COPY types types
COPY src src

# All deps (including @bike4mind/common) are published on the public npm
# registry, so no auth is required to build from source. NPM_TOKEN stays
# optional: pass it only if you install from a private mirror.
ARG NPM_TOKEN=""
RUN if [ -n "$NPM_TOKEN" ]; then \
      printf '//registry.npmjs.org/:_authToken=%s\nregistry=https://registry.npmjs.org/\nalways-auth=true\n' "$NPM_TOKEN" > ~/.npmrc; \
    fi && \
    pnpm install --frozen-lockfile && \
    rm -f ~/.npmrc

# build:docker skips type checking. We typecheck separately in CI.
RUN pnpm build:docker

# Production stage
FROM node:24-trixie-slim AS production
RUN npm i -g pnpm@10.34.1
WORKDIR /src

COPY package.json pnpm-lock.yaml ./

# NPM_TOKEN optional (public deps) - see the builder stage note above.
ARG NPM_TOKEN=""
RUN if [ -n "$NPM_TOKEN" ]; then \
      printf '//registry.npmjs.org/:_authToken=%s\nregistry=https://registry.npmjs.org/\nalways-auth=true\n' "$NPM_TOKEN" > ~/.npmrc; \
    fi && \
    pnpm install --frozen-lockfile --prod && \
    rm -f ~/.npmrc

COPY --from=builder /src/dist /src/dist

# Drop esbuild + any *-exec binaries we don't need at runtime
RUN find /root/.local/share/pnpm/store -name "*-exec" -type f -delete || true && \
    find /src/node_modules -name "esbuild" -type f -delete || true

ENTRYPOINT ["node", "/src/dist/index.js"]
