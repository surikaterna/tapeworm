# Runtime ENV vars (set at container run time):
#   MONGODB_URI, DATABASE, COLLECTION, RABBITMQ_URI,
#   EXCHANGE, RESUME_COLLECTION, WATCH_MODE, TENANT

# --- build stage ---
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json turbo.json tsconfig.base.json ./
COPY packages/tapeworm/package.json packages/tapeworm/
COPY packages/tapeworm_dispatcher_mdb_rmq/package.json packages/tapeworm_dispatcher_mdb_rmq/
RUN npm ci
COPY packages/tapeworm/ packages/tapeworm/
COPY packages/tapeworm_dispatcher_mdb_rmq/ packages/tapeworm_dispatcher_mdb_rmq/
RUN npx turbo run build --filter=tapeworm_dispatcher_mdb_rmq

# --- production stage ---
FROM node:22-alpine
WORKDIR /app
COPY package.json package-lock.json ./
COPY packages/tapeworm/package.json packages/tapeworm/
COPY packages/tapeworm_dispatcher_mdb_rmq/package.json packages/tapeworm_dispatcher_mdb_rmq/
RUN npm ci --omit=dev
COPY --from=build /app/packages/tapeworm/dist/ packages/tapeworm/dist/
COPY --from=build /app/packages/tapeworm_dispatcher_mdb_rmq/dist/ packages/tapeworm_dispatcher_mdb_rmq/dist/
RUN addgroup -S app && adduser -S app -G app
RUN apk add --no-cache tini
USER app
ENTRYPOINT ["/sbin/tini", "--", "node", "packages/tapeworm_dispatcher_mdb_rmq/dist/bin/cli.js"]
