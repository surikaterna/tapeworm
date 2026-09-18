# Runtime ENV vars (set at container run time):
#   MONGODB_URI, DATABASE, COLLECTION, RABBITMQ_URI,
#   EXCHANGE, RESUME_COLLECTION, WATCH_MODE, TENANT

# --- build stage ---
FROM node:26.9.0-alpine AS build
RUN npm install --global npm@11.12.1 && test "$(node --version)" = v26.9.0 && test "$(npm --version)" = 11.12.1
WORKDIR /app
COPY package.json package-lock.json turbo.json tsconfig.base.json ./
COPY packages/tapeworm/package.json packages/tapeworm/
COPY packages/tapeworm_dispatcher_mdb_rmq/package.json packages/tapeworm_dispatcher_mdb_rmq/
RUN npm ci --workspace=tapeworm --workspace=tapeworm_dispatcher_mdb_rmq --include-workspace-root
COPY packages/tapeworm/ packages/tapeworm/
COPY packages/tapeworm_dispatcher_mdb_rmq/ packages/tapeworm_dispatcher_mdb_rmq/
RUN npx turbo run build --filter=tapeworm_dispatcher_mdb_rmq

# --- production stage ---
FROM node:26.9.0-alpine
ARG REVISION
ARG VERSION
ARG CORE_VERSION
LABEL org.opencontainers.image.revision=$REVISION
LABEL org.opencontainers.image.version=$VERSION org.tapeworm.core.version=$CORE_VERSION
RUN npm install --global npm@11.12.1 && test "$(node --version)" = v26.9.0 && test "$(npm --version)" = 11.12.1
WORKDIR /app
COPY package.json package-lock.json ./
COPY packages/tapeworm/package.json packages/tapeworm/
COPY packages/tapeworm_dispatcher_mdb_rmq/package.json packages/tapeworm_dispatcher_mdb_rmq/
RUN npm ci --omit=dev --workspace=tapeworm --workspace=tapeworm_dispatcher_mdb_rmq --include-workspace-root=false
COPY --from=build /app/packages/tapeworm/dist/ packages/tapeworm/dist/
COPY --from=build /app/packages/tapeworm_dispatcher_mdb_rmq/dist/ packages/tapeworm_dispatcher_mdb_rmq/dist/
RUN addgroup -S app && adduser -S app -G app
RUN apk add --no-cache tini
USER app
STOPSIGNAL SIGTERM
ENTRYPOINT ["/sbin/tini", "--", "node", "packages/tapeworm_dispatcher_mdb_rmq/dist/bin/cli.js"]
