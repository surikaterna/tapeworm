# tapeworm_dispatcher_mdb_rmq

MongoDB-to-RabbitMQ CDC relay for Tapeworm commits. Change streams are the default;
direct oplog tailing is an explicit, weaker-durability option.

## Delivery contract

For each admitted commit the relay performs, in order:

1. Publish the complete commit with persistent delivery mode to a durable **headers** exchange.
2. Require a publisher confirm **and no mandatory return**.
3. Durably save the acknowledged CDC position.
4. Emit `dispatched`.

Failed publication is never skipped, including a repeatedly failing/poison commit.
Checkpoint failure is a delivery failure too. Retrying after a confirm/checkpoint
crash can duplicate messages. `messageId` remains `commit.id`; consumers must
deduplicate and make effects idempotent or reconcilable. There is **no exactly-once
external-effect guarantee**. Broker acceptance is not consumer/business completion.

`mandatory: true` proves routing to **at least one queue**, not to every required
subscriber, and not that the destination queues are durable. Before launching,
operators must provision and verify all required durable queues, headers bindings,
replication/quorum policies, persistence and retention. A matching disposable queue
can satisfy mandatory routing while a required business queue is missing.

Every attempt has a separate AMQP `correlationId`, even concurrent attempts for the
same commit. Return precedes confirm on Rabbit's channel protocol. Nack, return,
timeout, connection/channel closure and stop reject outstanding publication.
Timeout means **unknown outcome**, not proof of non-delivery. Channel closure and
connection closure use a shared reconnect loop. Internal outstanding attempts are
bounded (`maxPending`, default 256); channel backpressure rejects new attempts
until drain. The watcher retries from the last acknowledged checkpoint, not from
a cached startup position. There is no unbounded publication waiting queue.

## Recovery and its accepted limitation

Normal recovery uses the opaque MongoDB resume token, or the acknowledged BSON
oplog Timestamp in explicit oplog mode. UUID is an identity/range selector, **not**
an insertion-order completeness frontier.

Only cursor history expiry (Mongo code 286 / `ChangeStreamHistoryLost`, or a proven
oplog retention gap) activates fallback. Code 280 is **not** treated generically
as expiry. Authentication, connection, handler, validation, publish and checkpoint
errors never masquerade as history expiry. Legacy UUID-only checkpoints have a
separate, observable migration path, not an invented expiry event.

Fallback:

1. Capture a server `operationTime` using an explicit Mongo session, primary
   majority read. This works on an empty collection. No client-clock timestamps or
   synthetic resume tokens are used.
2. Require an existing complete, non-sparse, non-partial `{ token: 1 }` index.
   Capture the highest UUID **after** the live boundary to establish a finite range.
3. Persist `recovery.phase = scan`, its original lower UUID, upper UUID and live
   boundary before publishing replay records. Use majority, index-hinted ascending
   `(lower, upper]` traversal with bounded driver batches (default 256).
4. Save the scan UUID only after confirmed publication. The previous primary
   position is not overwritten by a fake replay token. After a scan retry, restart
   **inclusively** at the saved UUID; nonunique index ties may duplicate but are not
   skipped. One session's scan has finite upper bounds, not a continuously growing tail.
5. Persist cutover even for an empty scan. Start the live cursor at the original
   server boundary, inclusively. Inserts during the scan, including lower UUIDs,
   are delivered by that live cursor. History/live overlap can duplicate records.

There is no in-memory live backlog or universal recovery journal. The oplog **must
retain the original live boundary throughout replay and restart/cutover**. If that
boundary expires during recovery the relay fails explicitly with `fatal`; it does
not silently reset to now and loop. Increase retention/capacity and reconcile the
affected interval under an operator-approved procedure before changing the checkpoint.

**Accepted historical omission (redemeine-gqxm):** A allocates UUID 100; B allocates
110, inserts, publishes and checkpoints; relay crashes; A inserts milliseconds
later; outage lasts beyond resume history retention. Fallback `token > 110` can
omit A if it predates the new live boundary. Valid primary resume would deliver A.
Neither clock skew nor a writer paused for the whole outage is necessary. This
is an explicitly accepted, unmeasured residual risk at hundred-million-commit
scale, not a claim of lossless fallback or a measured rare event.

There is no default full-history scan, all-stream enumeration, per-stream ledger,
or universal journal. An indexed range can still contain many records and take a
long time; bounded memory does not mean constant recovery work. Optional lookback
is not implemented in this slice. Counts of unknown omitted records cannot be
inferred from a successful fallback.

A fresh relay with **no checkpoint** starts from a newly captured server boundary,
not from the beginning of history. Back up checkpoints; deleting one is an explicit
change to that starting point, not a routine repair.

## Single-owner launch gate

Run exactly **one externally fenced active owner per source feed/checkpoint**.
Repeated starts of one instance are guarded; this is **not** a distributed lease
or proof of global ownership. Do not run two replicas against a key. Prevent old
owners from resuming before launching replacements (or supply external fencing).
Mongo majority checkpoint writes alone do not establish ownership.

Use a unique checkpoint key and `feedId` identifying the source cluster and Rabbit
destination/vhost without credentials. The checkpoint identity also includes
database, collection, watch mode, exchange and tenant. A changed identity is rejected.
Default identity cannot distinguish identically named resources on different
clusters/vhosts; explicit IDs and keys are required for those deployments.

## Library usage

```typescript
import { MongoClient } from "mongodb";
import { Dispatcher, MongoResumeTokenStore, checkpointFeed } from "tapeworm_dispatcher_mdb_rmq";

const client = await new MongoClient("mongodb://localhost/?replicaSet=rs0").connect();
const config = {
  mongodb: { db: client.db("events"), collection: "tw_master_commits", batchSize: 256 },
  rabbitmq: { uri: "amqp://localhost", exchange: "commits", confirmTimeoutMs: 30000, maxPending: 256 },
  feedId: "cluster-a/to-rabbit-a-vhost-events",
  tenant: "acme",
};
const dispatcher = new Dispatcher({
  ...config,
  resumeTokenStore: new MongoResumeTokenStore(config.mongodb.db, "cdc_state", {
    checkpointKey: "acme-master-commits", feedId: checkpointFeed(config),
  }),
});
dispatcher.on("error", (error) => console.error("retry", error.message));
dispatcher.on("fatal", (error) => console.error("operator action", error.message));
dispatcher.on("recovery", (event) => console.warn("recovery exposure", event));
process.once("SIGTERM", () => { void dispatcher.stop(); });
try { await dispatcher.start(); }
finally { await dispatcher.stop(); await client.close(); }
```

`start()` is long-running and rejects on exhaustion/fatal failure after cleanup.
`mongodb.maxRetries` defaults to 50 failed attempts per run, with interruptible
`retryDelayMs` (default 1000ms). A poison record is not bypassed to serve later
records. Repair the record/routing/dependency under an explicit operational
procedure, then restart with the same checkpoint.

### Stores, compatibility and migration

**Breaking upgrade — major changeset (redemeine-gqxm).** Preserved constructors
and additive API signatures do not make the new recovery behavior backward
compatible. Existing unscoped checkpoints require explicit adoption, three-field
stores require the new durable fields, and legacy watcher callbacks must migrate
to `startWithProgress` when recovery is needed. Follow the migration procedure
below before upgrading; the fail-closed safety guards are intentional. This
classification does not authorize publication or promotion of a release.

`MongoResumeTokenStore(db, collection)` remains available with the legacy key
`dispatcher_resume`. The optional third argument adds `checkpointKey`, `feedId`
(the complete `checkpointFeed(config)` value) and `adoptLegacyCheckpoint`.
Using only two arguments is unsafe for shared/multi-feed state collections.

Existing `IResumeTokenStore.load()` / `save(state)` signatures remain. Version 1
adds `feed`, discriminated `primary`, and `recovery` to `ResumeState`; the original
`changeStreamToken`, `lastCommitToken`, `updatedAt` fields remain. Custom stores
must durably roundtrip **all fields and BSON values**, not just the old three
fields. Use BSON/EJSON-aware serialization for Timestamp and opaque resume token
values. Transition saves are read back and checked; three-field stores fail
clearly. A resolved `save()` must mean durable storage, not queued background I/O.

For an unidentifiable legacy checkpoint, stop/fence the old owner, back up its
document, independently verify its source/destination and watch mode, upgrade the
store, then explicitly set `DispatcherConfig.adoptLegacyCheckpoint = true` for
the first run. If the Mongo store also asserts a feed, set its adoption option for
that run. A known mismatched feed is never adopted. Remove adoption flags afterward.
To change keys, copy the verified checkpoint to the chosen key under operator
control before starting; an empty new key means fresh-from-now, **not migration**.
Synthetic `_replayFallback` tokens in old stored documents require explicit repair.

Direct `ChangeStreamWatcher.start(state, handler)` and `OplogWatcher.start(...)`
retain their commit/Document-compatible callback (now safely typed as
`Record<string, unknown>`). It cannot describe durable replay/cutover, so legacy
callbacks fail clearly when replay is needed. Migrate to
`startWithProgress(state, handler)` for recovery. `ProgressHandler` receives an
optional commit and discriminated `DurableProgress` (`live`, `replay`, `transition`).
The handler must persist the supplied state on every callback, including transitions;
only a resolved handler advances the watcher's retry position. Runtime commit
validation checks the Tapeworm envelope and event id/type/version, leaving domain
payloads as `unknown` for application schema validation.

### Events and operations

`DispatcherEvents` types `started`, `stopped`, `dispatched`, `resumed`, `error`,
`fatal`, `fallback` and `recovery`. `fallback` retains its no-argument API;
`recovery` supplies phase, reason and state. Recovery state exposes start time,
last acknowledged position, chosen range, scan cursor and live boundary. Export
these to monitoring, compute recovery age/outage exposure, count scan/live
publications and duplicate attempts, and alert on fallback/exhaustion. Do not
label this as a measured unknown-miss count. The CLI logs recovery events.

## CLI

```bash
tapeworm-dispatcher --mongodb-uri 'mongodb://localhost/?replicaSet=rs0' \
  --database events --collection tw_master_commits \
  --rabbitmq-uri amqp://localhost --exchange commits \
  --resume-collection cdc_state --checkpoint-key acme-master-commits \
  --feed-id cluster-a/to-rabbit-a-vhost-events --tenant acme
```

Required flags: `mongodb-uri`, `database`, `collection`, `rabbitmq-uri`, `exchange`.
Optional flags: `resume-collection` (default `tw_dispatcher_state`), `watch-mode`
(`changeStream` default or `oplog`), `tenant`, `checkpoint-key`, `feed-id`,
`adopt-legacy-checkpoint` (`true`/`false`, default false). All flags have uppercase
underscore environment equivalents, e.g. `CHECKPOINT_KEY`; CLI takes precedence.
The CLI warns when legacy key/feed defaults are used. Mongo ownership covers all
initialization, including invalid configuration after connection.

### Signal shutdown and the ten-second grace budget

The first SIGINT/SIGTERM, normal completion, or failure starts **one absolute
10,000ms shutdown budget**. There is no timer while healthy; repeated signals do
not extend the budget or invoke stop/close again. Dispatcher stop begins without
waiting for `start()` to settle. Successful stop allows the in-flight operation to
drain within that budget before closing Mongo; rejected stop immediately attempts
Mongo close. Graceful completion requires both the active operation and cleanup
to settle, not merely a resolved `stop()`.

At the deadline, even a pending stop cannot prevent the Mongo close attempt and
removal of owned signal listeners. The CLI then logs the timeout and **exits 124**
as a last resort for still-pending work/driver handles. It does not force an early
exit while the drain can still complete. Normal signal drain exits 0; ordinary
initialization/operation errors retain voluntary exit 1. The lifecycle helper
never calls `process.exit`: the CLI host alone owns the deadline exit policy.
Its optional `shutdownTimeoutMs`, `onDeadline`, and `onLateError` hooks allow
deterministic lifecycle tests without adding CLI flags. Late promise failures
remain observed, and timeout errors preserve known primary/cleanup failures.

Confirmation followed by a blocked/unsaved checkpoint can cause stable-identity
duplicates on restart. **Process exit or a client-close attempt does not prove a
Mongo write was aborted**: an already-issued checkpoint may complete after a lock
or outage is resolved. Fence the old owner, let outstanding server operations
settle, inspect durable state, then restart from that checkpoint. Never advance
or delete it merely to bypass a shutdown fault. This policy preserves the existing
at-least-once crash contract; it adds no exactly-once guarantee.

## Explicit oplog mode

Oplog mode tails server BSON timestamps, checks the retained floor on resume and
shares the indexed recovery/checkpoint engine. It observes **direct inserts only**;
transactional `applyOps` is unsupported. It can publish writes that later roll back
after primary failure. It is not majority-safe and is not appropriate when those
limitations are unacceptable. Change streams remain the recommended default.

## AMQP format

Body: JSON of the entire validated `ICommit` including `events[]` and its domain
fields. Properties: `contentType=application/json`, `deliveryMode=2`,
`messageId=commit.id`, per-attempt `correlationId`, Unix `timestamp`, `mandatory=true`.
Headers: `collection`, `partitionId`, `streamId`, optional `tenant`.

## Qualification commands

Use project Node 26 and npm 11.12.1 (not a downgrade of TS 6/Vitest 5):

```bash
npm ci
npm run build
npm run test --workspace=tapeworm_dispatcher_mdb_rmq
npm run check --workspace=tapeworm_dispatcher_mdb_rmq
npm run test:consumer --workspace=tapeworm_dispatcher_mdb_rmq
```

Real services on isolated localhost ports (Docker required):

```bash
docker run -d --name gqxm-mongo -p 127.0.0.1:27187:27017 mongo:8 --replSet rs0 --bind_ip_all
docker exec gqxm-mongo mongosh --quiet --eval 'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})'
docker run -d --name gqxm-rabbit -p 127.0.0.1:56787:5672 rabbitmq:4
docker exec gqxm-rabbit rabbitmq-diagnostics -q ping
docker run -d --name gqxm-expiry-mongo -p 127.0.0.1:27188:27017 mongo:8 --replSet rs0 --bind_ip_all --oplogSize 1 --syncdelay 1
docker exec gqxm-expiry-mongo mongosh --quiet --eval 'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})'
npm run test:integration --workspace=tapeworm_dispatcher_mdb_rmq
docker rm -f gqxm-mongo gqxm-rabbit gqxm-expiry-mongo
```

Wait for Mongo primary and Rabbit readiness. `TEST_MONGODB_URI` and
`TEST_RABBITMQ_URI` override normal test endpoints; `TEST_EXPIRY_MONGODB_URI` must
point at a dedicated small-oplog test replica set. Its startup storage checkpoint
interval (`syncdelay`) must be one second for bounded rollover.
Missing services **fail**, not skip.
Use dedicated test services, never shared or production endpoints: the shutdown
test temporarily fsync-locks the entire normal Mongo server, not just its test database.
Tests create/drop isolated databases, queues and exchanges. Integration includes
real empty-collection operationTime capture, finite IXSCAN explain, nonunique UUID
ties, history/live overlap, accepted lower-UUID omission, Mongo history-expiry
classification, scoped BSON checkpoints, Rabbit mandatory returns/channel reconnect,
and actual child-process SIGKILL between broker confirmation and checkpoint in
both watch modes. A separate real-Mongo subprocess regression verifies that an
invalid database name exits voluntarily with a nonzero status, without watchdog
termination. The shutdown regression fsync-locks an isolated Mongo server, proves
a confirmed commit's checkpoint update is `waitingForLock`, and requires actual
CLI exit 124 near ten seconds **before unlocking**. It then lets outstanding writes
settle with a majority visibility barrier and proves restart delivery/identity;
it does not assume the timed-out write was aborted. Expiry tests capture a real
resume token/server boundary, write
bounded noise to roll a dedicated 1MB oplog past that position, then verify actual
server history-expiry and recovery. This is not a multi-day outage or scale benchmark.

`test:consumer` packs and installs built declarations outside the workspace under
`/tmp/opencode`, then compiles positive and negative type fixtures without aliases.
`check` uses strict TS (including tests/CLI, no unchecked indexing, no skipped
library checks) and type-aware unsafe-operation lint. Unit tests use narrow typed
ports, not casts of whole Mongo Db or AMQP channels.

These are correctness regressions, **not production certification**, 100M-record
capacity measurements, replica election/rollback qualification, broker disk-loss
testing, or proof that an operator's required queue topology is correct. Qualify
retention headroom, failover, queue durability and peak recovery throughput for
the deployment before enabling delivery of business-critical intents.
