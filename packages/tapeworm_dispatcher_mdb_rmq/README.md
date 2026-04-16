# tapeworm_dispatcher_mdb_rmq

MongoDB-to-RabbitMQ commit dispatcher for [tapeworm](../tapeworm) event stores.

Watches a tapeworm commits collection for new inserts and publishes each commit to a RabbitMQ fanout exchange with **at-least-once delivery** semantics.

## Features

- **Two watch modes**: Change stream (majority-safe) or direct oplog tailing (lowest latency)
- **Two-level resume**: MongoDB resume token (primary) + UUID v7 `.token` cursor (fallback)
- **Publisher confirms**: Every message is acknowledged by the RabbitMQ broker before checkpointing
- **Pluggable resume storage**: Built-in MongoDB store or bring your own `IResumeTokenStore`
- **Library + CLI**: Use as a library in your process or run as a standalone daemon
- **Multi-tenant**: Optional `tenant` header on every AMQP message

## CLI Usage

### Basic — change stream mode (default)

```bash
npx tapeworm-dispatcher \
  --mongodb-uri mongodb://localhost:27017 \
  --database mydb \
  --collection tw_master_commits \
  --rabbitmq-uri amqp://localhost \
  --exchange tw.commits.exchange
```

### Direct oplog mode (lowest latency)

```bash
npx tapeworm-dispatcher \
  --mongodb-uri mongodb://localhost:27017 \
  --database mydb \
  --collection tw_master_commits \
  --rabbitmq-uri amqp://localhost \
  --exchange tw.commits.exchange \
  --watch-mode oplog
```

### Multi-tenant with custom resume collection

```bash
npx tapeworm-dispatcher \
  --mongodb-uri mongodb://rs0.example.com:27017,rs1.example.com:27017/?replicaSet=rs0 \
  --database lx3_acme \
  --collection tw_master_commits \
  --rabbitmq-uri amqp://user:pass@rabbit.example.com:5672 \
  --exchange acme.commits.exchange \
  --watch-mode changeStream \
  --resume-collection tw_dispatcher_state \
  --tenant acme
```

### All CLI flags

| Flag                  | Required | Default               | Description                                   |
| --------------------- | -------- | --------------------- | --------------------------------------------- |
| `--mongodb-uri`       | Yes      | —                     | MongoDB connection URI                        |
| `--database`          | Yes      | —                     | Database name                                 |
| `--collection`        | Yes      | —                     | Commits collection (e.g. `tw_master_commits`) |
| `--rabbitmq-uri`      | Yes      | —                     | AMQP connection URI                           |
| `--exchange`          | Yes      | —                     | Fanout exchange name (asserted on startup)    |
| `--watch-mode`        | No       | `changeStream`        | `changeStream` or `oplog`                     |
| `--resume-collection` | No       | `tw_dispatcher_state` | MongoDB collection for resume tokens          |
| `--tenant`            | No       | —                     | Tenant ID added to AMQP message headers       |

## Library Usage

```typescript
import { MongoClient } from "mongodb";
import { Dispatcher, MongoResumeTokenStore } from "tapeworm_dispatcher_mdb_rmq";

const client = new MongoClient("mongodb://localhost:27017");
await client.connect();
const db = client.db("mydb");

const dispatcher = new Dispatcher({
  mongodb: {
    db,
    collection: "tw_master_commits",
  },
  rabbitmq: {
    uri: "amqp://localhost",
    exchange: "tw.commits.exchange",
  },
  resumeTokenStore: new MongoResumeTokenStore(db, "tw_dispatcher_state"),
  watchMode: "oplog", // or "changeStream" (default)
  tenant: "acme", // optional
});

dispatcher.on("started", () => console.log("Watching for commits..."));
dispatcher.on("dispatched", (commit) => console.log(`Published: ${commit.id}`));
dispatcher.on("fallback", () =>
  console.warn("Oplog token expired, replaying from .token cursor"),
);
dispatcher.on("error", (err) => console.error("Error:", err));

// Graceful shutdown
process.on("SIGTERM", async () => {
  await dispatcher.stop();
  await client.close();
});

// Blocks until stop() is called
await dispatcher.start();
```

### Custom resume token store

```typescript
import type {
  IResumeTokenStore,
  ResumeState,
} from "tapeworm_dispatcher_mdb_rmq";

class RedisResumeTokenStore implements IResumeTokenStore {
  async load(): Promise<ResumeState | null> {
    // Load from Redis
  }
  async save(state: ResumeState): Promise<void> {
    // Save to Redis
  }
}
```

## Watch Modes

### `changeStream` (default)

Uses [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/). The change stream only fires after a write is **majority-committed** (replicated to a majority of replica set members). This is the safe default — no risk of delivering commits that later get rolled back.

### `oplog`

Tails `local.oplog.rs` directly with a tailable-await cursor. Sees inserts **immediately** on the primary, before replication. This gives the lowest possible dispatch latency.

**⚠️ Trade-off:** If the primary loses an election before the write replicates, the write may be rolled back. The dispatcher would have already published a message for a commit that no longer exists. Consumers must handle this case (e.g. idempotent projections, or accepting rare phantom events).

## Resume Strategy

The dispatcher uses a two-level resume strategy to survive restarts:

| Level    | Token                                          | Stored as                                  | When used                                    |
| -------- | ---------------------------------------------- | ------------------------------------------ | -------------------------------------------- |
| Primary  | Change stream resume token / oplog `Timestamp` | `ResumeState.changeStreamToken`            | Normal restart within oplog retention window |
| Fallback | UUID v7 `.token` field on commits              | `ResumeState.lastCommitToken` (hex string) | When primary token is expired or invalid     |

**Resume flow on startup:**

1. Load `ResumeState` from the token store
2. Try resuming from the primary token
3. If the primary token is expired → emit `"fallback"`, query commits by `.token > lastCommitToken`, replay missed commits
4. Switch to live tailing (change stream or oplog)

**Checkpoint flow per commit:**

1. Publish to RabbitMQ → wait for broker ack (publisher confirms)
2. **Only after confirmed delivery:** save the resume state

This ensures at-least-once delivery. If the process crashes between publish and checkpoint, the commit will be re-published on restart. Consumers should use `messageId` (= `commit.id`) for deduplication.

## AMQP Message Format

**Exchange:** Fanout (durable), asserted on startup.

**Message body** (JSON):

```json
{
  "id": "a1b2c3d4-...",
  "partitionId": "master",
  "streamId": "order-123",
  "commitSequence": 5,
  "events": [{ "id": "...", "type": "order.created", "payload": {} }],
  "token": "<BSON UUID v7>",
  "isDispatched": false,
  "createDateTime": "2026-04-16T..."
}
```

**AMQP properties:**
| Property | Value |
|----------|-------|
| `contentType` | `application/json` |
| `deliveryMode` | `2` (persistent) |
| `messageId` | `commit.id` (for deduplication) |
| `timestamp` | Unix epoch seconds |

**AMQP headers:**
| Header | Value |
|--------|-------|
| `collection` | Commits collection name (e.g. `tw_master_commits`) |
| `partitionId` | Tapeworm partition ID (e.g. `master`) |
| `streamId` | Aggregate/stream ID |
| `tenant` | Tenant ID (only if configured) |

## Architecture

```
                  ┌─────────────────────────────────┐
                  │         MongoDB Replica Set      │
                  │                                  │
                  │  tw_master_commits   oplog.rs    │
                  │        │                │        │
                  └────────┼────────────────┼────────┘
                           │                │
               changeStream│          oplog │
                    mode   │          mode  │
                           ▼                ▼
                  ┌────────────────────────────────┐
                  │     tapeworm-dispatcher         │
                  │                                 │
                  │  ChangeStreamWatcher  ──┐       │
                  │           OR            ├─►     │
                  │  OplogWatcher        ──┘       │
                  │         │                       │
                  │         ▼                       │
                  │  CommitPublisher                │
                  │  (publisher confirms)           │
                  │         │                       │
                  │         ▼                       │
                  │  IResumeTokenStore              │
                  │  (checkpoint after publish)     │
                  └────────┬───────────────────────┘
                           │
                           ▼
                  ┌─────────────────────┐
                  │  RabbitMQ Fanout    │
                  │  Exchange (durable) │
                  └────────┬────────────┘
                           │
                    ┌──────┼──────┐
                    ▼      ▼      ▼
                 Queue   Queue   Queue
                (consumer bindings)
```

## Exports

| Export                  | Type      | Description                            |
| ----------------------- | --------- | -------------------------------------- |
| `Dispatcher`            | Class     | Main orchestrator                      |
| `ChangeStreamWatcher`   | Class     | Change stream watcher                  |
| `OplogWatcher`          | Class     | Direct oplog watcher                   |
| `CommitPublisher`       | Class     | RabbitMQ fanout publisher              |
| `MongoResumeTokenStore` | Class     | MongoDB resume token store             |
| `ICommitWatcher`        | Interface | Watcher contract                       |
| `IResumeTokenStore`     | Interface | Resume store contract                  |
| `DispatcherConfig`      | Type      | Config for `Dispatcher`                |
| `DispatcherEvents`      | Type      | Event map for typed listeners          |
| `MongoConfig`           | Type      | MongoDB config                         |
| `RabbitConfig`          | Type      | RabbitMQ config                        |
| `ResumeState`           | Type      | Resume token state                     |
| `WatchMode`             | Type      | `"changeStream" \| "oplog"`            |
| `CommitHandler`         | Type      | Callback signature for commit handlers |

## Requirements

- Node.js >= 18
- MongoDB replica set (required for both change streams and oplog tailing)
- RabbitMQ broker
