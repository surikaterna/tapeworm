# tapeworm_dispatcher_mdb_rmq

MongoDB-to-RabbitMQ CDC relay for Tapeworm commits. Change streams are the default;
direct oplog tailing is an explicit, weaker-durability option.

For business-critical distribution, configure durable quarantine through the SDK:
**enabled quarantine continues by default after durable capture of eligible poison**.
Use `mode:"pause"` only when stopping is intentional. Absent/disabled quarantine
remains fail-closed; the SDK cannot invent a store or a source-retention policy.
The CLI does not provision or enable quarantine automatically. See the
[production quarantine setup](#durable-poison-quarantine-sdk-redemeine-1i0g).

## Delivery contract

With quarantine absent or disabled, for each admitted commit the
relay performs, in order:

1. Publish the complete commit with persistent delivery mode to a durable **headers** exchange.
2. Require a publisher confirm **and no mandatory return**.
3. Durably save the acknowledged CDC position.
4. Emit `dispatched`.

With quarantine disabled, failed publication is never skipped, including a
repeatedly failing/poison commit. Configured default-continue and explicit-pause semantics are described in
the [quarantine runbook](#durable-poison-quarantine-sdk-redemeine-1i0g).
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
4. Save the scan UUID only after confirmed publication or a permitted durable
   quarantine outcome. The previous primary
   position is not overwritten by a fake replay token. After a scan retry, restart
   **inclusively** at the saved UUID; nonunique index ties may duplicate but are not
   skipped. One session's scan has finite upper bounds, not a continuously growing tail.
5. Persist cutover even for an empty scan. Start the live cursor at the original
   server boundary, inclusively. Inserts during the scan, including lower UUIDs,
   are delivered by that live cursor. History/live overlap can duplicate records.

There is no in-memory live backlog or universal recovery journal; opt-in quarantine
stores only rejected-record references and audit. The oplog **must retain
the original live boundary throughout replay and restart/cutover**. If that boundary
expires during recovery the relay fails explicitly with `fatal`; it does not
silently reset to now and loop. Increase retention/capacity and reconcile the
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

## Library usage without quarantine

This compatibility example is fail-closed. For the availability-oriented production
configuration, use the [enabled-quarantine example](#durable-poison-quarantine-sdk-redemeine-1i0g)
below; it supplies the durable store and source-retention assertion explicitly.

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
`retryDelayMs` (default 1000ms). With quarantine disabled, a poison record is not
bypassed to serve later records, and no quarantine journal is created. Repair the
record/routing/dependency under an explicit operational procedure, then restart
with the same checkpoint. For configured durable capture and default continue-mode
ordering gaps, follow the [quarantine runbook](#durable-poison-quarantine-sdk-redemeine-1i0g).

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

The CLI does **not** configure a quarantine store or source retention; its delivery
therefore remains fail-closed. Configure the SDK as shown in the quarantine runbook
to continue after durably captured eligible poison.

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

## Durable poison quarantine (SDK, redemeine-1i0g)

**Explicit composition (redemeine-kiwb):** the core dispatcher owns publication and
checkpointing, and accepts an optional `DeliveryFailureHandler`. Core delivery,
recovery, and configuration types do not import the quarantine feature, even
transitively. `QuarantineFailureHandler` implements that port and owns the typed
`quarantined` event; applications construct it separately and pass it as
`failureHandler`. The conventional package root exports both independent surfaces;
it is not used by core modules to conceal a feature dependency.

The port is a trusted extension, not a sandbox or proof of database durability.
`handle(error, { commit, feed, collection })` runs only after publication fails.
It returns `{kind:"unhandled"}` to preserve the original error, or awaits durable
acceptance before returning `{kind:"durablyHandled", onCheckpointed?}`. It cannot
supply checkpoint state/store or claim publication. Core validates receipt shape,
then saves its own checkpoint; it never routes checkpoint or notification failures
back into the handler. Successful publication never invokes the handler or allocates
its failure context/notification. There is no generic handled event.

**Availability policy (redemeine-ihn0): enabled quarantine defaults to continue**
when `mode` is omitted. Configure a durable `store` and assert source retention;
no secondary acknowledgement flag is required. Explicit `mode:"pause"` opts into
stopping, for example for debugging or compliance workflows. Without quarantine
configuration, or with `enabled:false`, delivery remains fail-closed and no
quarantine index initialization or lookup is performed. There is no automatic
store provisioning, retention assertion, or CLI enablement.

The optional transport-only `publication` policy supports only `maxMessageBytes`
(positive safe integer, actual encoded UTF-8 JSON bytes; equality is allowed).
There is **no default broker-size guess** and no business-schema gate. Only this
configured **prepublication** size rejection qualifies for quarantine. Without
quarantine it fails closed. One deterministic rejection suffices; retrying it
fifty times is not required. Broker/network/auth, mandatory returns, nacks,
timeouts, backpressure, invalid source identity and arbitrary serialization or
TypeError failures are **not poison**: they still retry/stop without checkpoint
advancement. Readiness, capture or checkpoint failures likewise cannot authorize
skipping a rejected record. Source applications and consumers own business-schema
validation; domain fields remain application-owned `unknown` values.

If `maxMessageBytes` is omitted, no encoded-size rejection threshold is applied,
and this transport-only quarantine cannot classify new oversized messages, even
when quarantine is enabled. Choose a threshold at or below the destination
broker's accepted message-size limit, accounting for its size-accounting policy.
There is no automatic discovery of the broker limit.

**Cold path (redemeine-rq8p, draft PR #44):** healthy publication performs no
quarantine reads/writes, source-index readiness checks, reference fingerprinting
or BSON canonicalization. The live/history input boundary validates the transport
envelope once; the typed publisher trusts that `ICommit` and does not decode or
reconstruct its events again. Direct publisher callers must provide validated
data. Each ordinary publication attempt JSON-stringifies and builds its UTF-8
Buffer once, then compares the actual byte length. This avoids per-record database
round trips and full-payload copies/hashes unrelated to healthy delivery; it is
not a messages/second or 100M-capacity guarantee. Configuration shape checks run
at construction, not per message. Unknown/removed publication options fail clearly.

The reference store writes majority+journal acknowledgements and reads primary/
majority. It stores no second payload: a BSON-derived SHA-256 fingerprint covers
the validated commit, including domain fields and BSON types; only Mongo's
top-level `_id` is excluded. Canonical field ordering avoids `_id` insertion/order
differences. Capture is unique by feed/source collection/commit id; redrive uses
the original source and Rabbit `messageId`. No payload substitution is supported.
On the first eligible rejection, lazy readiness checks the source index, then
initializes the quarantine store before fingerprinting and idempotent capture.
Successful readiness is reused; a failed attempt resets readiness for retry.
These prerequisites apply to **poison capture/redrive, not healthy startup**:
unavailable quarantine metadata/storage cannot block otherwise healthy delivery.
The source must **already have a unique, nonpartial, nonsparse, simple-collation
`{id:1}` index**; the SDK checks but never builds that potentially expensive
production index. Quarantine methods still await their own safe initialization.
Source fetches are
id-indexed, and inspection uses scoped keyset indexes without reading payloads.

Quarantine identity is **binary and case/accent-sensitive**, regardless of the
collection default collation. Non-simple collection defaults are supported: all
quarantine indexes, reads, captures, claims, completion update pipelines and list
queries explicitly use `{locale:"simple"}`. Source reads likewise explicitly use
simple collation with the required simple unique id index, so `Commit-A` and
`commit-a` (or `Cafe` and `Café`) remain distinct indexed point lookups. Collection
defaults and source indexes are never rewritten or created by the SDK.

Before quarantine data access, initialization inspects existing indexes. It rejects
non-simple unique indexes (except the ObjectId `_id` index) and non-simple indexes
using the SDK names `quarantine_identity`, `quarantine_scope_id`, or
`quarantine_status_id`, with an **operator index migration** error. Merely adding
a binary index does not remove an old case/accent-insensitive unique constraint.
The SDK does not drop or alter user indexes. If rejected, stop/fence relay and
operator processes, back up the quarantine data/audit, inspect index definitions
and previously conflated identities, and have the schema owner reconcile records
and explicitly replace the incompatible indexes with simple-collation equivalents
before restarting. Unrelated non-simple nonunique indexes may remain. Do not
change index definitions while owners are active; validation is cached for the
store lifetime. This repair cannot prove or restore historical isolation for
records previously handled with incompatible collation.

`sourceRetention: "immutable-until-resolved"` is an operator assertion, not a
retention service. Keep every unresolved source record immutable and retained for
the entire incident/redrive period (including uncertain claims); configure source
TTL/retention accordingly. The SDK does **not pin records**. Retain resolved source
and quarantine audit records for your required audit period. There is no automatic
TTL or garbage collection; monitor both collection growth and oldest unresolved
age. Each record permits at most 100 explicit manual attempts, preserving every
attempt rather than truncating audit. Actor (128 UTF-8 bytes), reason (1024 bytes),
static diagnostic codes and reference metadata (4 KiB) bound document growth well
below Mongo's 16 MiB limit; duplicate capture observations use a saturating counter,
not an ever-growing history. At the attempt limit, stop and escalate for explicit
operator remediation; never delete history to bypass the bound.

Production SDK setup and inspection (assumes `db` is a connected Mongo `Db`, destination
topology provisioned, and the source id index provisioned by your schema owner):
the example's `1_000_000`-byte threshold is illustrative, not a default or a broker
recommendation; replace it with the destination-appropriate threshold described above.

```ts
import {
  Dispatcher, CommitPublisher, MongoResumeTokenStore, checkpointFeed,
  MongoQuarantineStore, MongoQuarantineSourceReader, QuarantineService, QuarantineFailureHandler,
} from "tapeworm_dispatcher_mdb_rmq";
import type { DispatcherConfig, PublicationPolicy } from "tapeworm_dispatcher_mdb_rmq";

const destination = { uri: "amqp://localhost", exchange: "commits" };
const config = {
  mongodb: { db, collection: "tw_master_commits" }, rabbitmq: destination,
  feedId: "cluster-a/to-rabbit-a-vhost-events", watchMode: "changeStream" as const,
};
const feed = checkpointFeed(config);
const store = new MongoQuarantineStore(db, "cdc_quarantine", {
  feed, sourceCollection: config.mongodb.collection, leaseMs: 60_000,
});
const publication: PublicationPolicy = {
  maxMessageBytes: 1_000_000,
};
const adapter = new QuarantineFailureHandler({ ...config,
  quarantine: { enabled: true, store, sourceRetention: "immutable-until-resolved" }, // default continue, no flag
});
adapter.on("quarantined", (event) => { console.log(event.id, event.checkpointAdvanced, event.resolution); });
const relayConfig: DispatcherConfig = {
  ...config, publication,
  resumeTokenStore: new MongoResumeTokenStore(db, "cdc_state", { checkpointKey: "relay-a", feedId: feed }),
  failureHandler: adapter,
};
const relay = new Dispatcher(relayConfig);

// Trusted operator process: bind this caller-owned publisher to the SAME destination/tenant.
const publisher = new CommitPublisher(config.rabbitmq);
const service = new QuarantineService({ ...config, publication, store, publisher,
  source: new MongoQuarantineSourceReader(db, config.mongodb.collection, feed),
  sourceRetention: "immutable-until-resolved",
});
try {
  const page = await service.list({ status: "quarantined", limit: 25 }); // maximum 100
  if (page.after) console.log(await service.list({ status: "quarantined", after: page.after }));
  const selected = page.records[0];
  if (selected) console.log(await service.redrive(selected.id, {
    actor: "operator@example.org", reason: "Transport limit raised; incident reviewed",
  }));
} finally {
  await service.close(); // stops new attempts and drains active calls
  await publisher.close(); // explicit caller-owned resource cleanup
}
```

**Continue is the default for configured, enabled quarantine:**
`{ enabled:true, store, sourceRetention:"immutable-until-resolved" }`.
Explicit `mode:"continue"` is equivalent and needs no acknowledgement flag.
Core checkpoints only after durable capture; the adapter then emits `quarantined`,
not a dispatcher `dispatched` success. Following healthy records proceed while the adapter stays enabled.
**This creates per-stream ordering gaps. Later redrive cannot restore original
order.** Use trusted consumers that tolerate gaps and enforce stable-identity
idempotency or reconciliation; operational availability is not exactly-once delivery.

To opt into stopping, set
`{ enabled:true, store, sourceRetention:"immutable-until-resolved", mode:"pause" }`.
Explicit pause durably captures, emits `quarantined` with `checkpointAdvanced:false`, then
halts immediately with `QuarantinePaused`: no subsequent record and no checkpoint
advancement. Fix the size limit/cause, then restart a **new** relay instance or
redrive explicitly. **Ordinary source replay always re-evaluates publication**:
if now acceptable it publishes/confirms/checkpoints even when an old receipt is
quarantined, claimed or published. It does not read or update that receipt. Normal
replay and operator redrive can therefore duplicate the same stable message id;
consumer idempotency is required, and manual-only redelivery is not promised.

Only while the record is **currently size-rejected** does cold idempotent capture
consult its prior receipt. An existing published status and operator audit remain
intact: that resolved exception permits checkpoint advancement even in pause mode,
emitting `quarantined` with `resolution:"published"`, not `dispatched`. Unresolved
capture follows pause/continue as configured. Changed source fingerprints fail
closed rather than silently aliasing an earlier identity. Capture precedes
checkpoint; checkpoint failure retries capture without resetting status/audit.

**Unreleased API rework:** redemeine-rq8p removes the former draft business-validator
option/decision type and schema-rejection code, without a legacy decoder shim.
This is not a released-data migration. If an operator deployed an earlier draft
and retained its quarantine data, stop and plan that migration before upgrading.
The original split's byte-equivalence target is superseded for this B-only rework;
A/C changes remain unchanged. PR #44 still requires independent audit/qualification.

Redemeine-kiwb additionally replaces the unpublished B draft's
`new Dispatcher({...base, quarantine})` and `dispatcher.on("quarantined", ...)`
with the explicit adapter composition above. `QuarantineConfig` retains its precise
store/retention/mode requirements. Construct adapter and dispatcher from the same
base feed/collection configuration; the adapter snapshots its binding and rejects
eligible size failures from a different scope before database I/O or fingerprinting.
The dispatcher now rejects unsupported **own** top-level options, including keys
whose value is `undefined`, before constructing dependencies. This is an intentional
runtime guard change for JavaScript callers passing extraneous, never-supported
fields; pass only dispatcher options, not an entire application configuration object.
Published 0.2 non-quarantine options are otherwise unchanged. These are changes to
the pending minor feature, not a separate A major or C patch release migration.

**Previous local draft migration:** the unpublished pause-by-default policy is
superseded by redemeine-ihn0. Add explicit `mode:"pause"` to retain that behavior.
`acceptOrderingGaps:true` is deprecated, optional compatibility syntax and does not
select the mode. Existing true-valued callers remain valid with either mode;
omit the field in new code. A supplied false or other nontrue value is rejected,
even when quarantine is disabled; use `mode:"pause"`, not a false flag, to stop.

Operational EventEmitter notifications are not transactional audit. A persistent
quarantine document is a historical rejection/operator-attempt receipt, **not a
delivery ledger, global pending truth or proof that the source was never delivered**.
Normal successful replay does not synchronize operator status: a list entry may
remain quarantined/claimed after source delivery.
Adapter event observers must be synchronous and must not throw. EventEmitter's
void listener signatures cannot forbid async listeners; they are not awaited or
given a new framework-wide rejection policy. At the new receipt boundary,
`onCheckpointed: () => undefined` intentionally rejects async/boolean returns in
TypeScript. Core invokes it only after checkpoint persistence (and transition
roundtrip validation). A synchronous throw becomes terminal `DeliveryHalted` with
the original cause; an untyped non-undefined return, including a Promise/thenable,
is observed for rejection without awaiting it and terminally halts as well. No
current-message retry runs from stale in-memory progress: restart a new dispatcher
to load the advanced checkpoint. A throwing pause observer likewise halts terminally,
without advancing. Notifications are not exactly-once or transactional business effects.

`redrive(id,{actor,reason})` performs one explicit attempt, without background or
automatic retry. Outcomes are `published`, `already-published`, `busy`, `missing`,
`rejected` (safe static code, including attempt limit), or `outcome-unknown`.
Claims and audit entries are atomic, use Mongo server time, and expire after the
configured lease (default 60s, maximum 1h). Expired takeover marks the old attempt
unknown before creating a new one; stale/expired owners cannot record completion.
Broker confirmation precedes persisted success. Lost confirms or completion-write
failure remain uncertain and may require a later explicit retry with the same
message id. Redrive **never writes the primary CDC checkpoint**.
The optional operator service verifies source immutability and preflights encoding
before calling the supplied publisher, which may encode again. That cold operator
cost is deliberate; the once-only encoding claim concerns normal publication,
not redrive. There is no automatic sweep or new publisher pipeline.

This trusted in-process SDK is not an authorization service: actor/reason are audit
metadata, not authentication. The caller owns RBAC, operator authorization and
source/destination binding. A Mongo lease cannot fence an actual Rabbit side
effect from an old operator process: externally fence old operators and require
downstream idempotency. Do not steal active claims. All existing single-feed-owner,
oplog limitations and accepted lower-UUID expiry-fallback risks still apply; this
feature does not provide exactly-once effects, restore ordering, or certify 100M
capacity. No new CLI/subcommands or automatic CRUD-saga authority are introduced.

## Qualification commands

The shared local/Jenkins gate is **`./ci/qualify.sh`** from the repository root.
See [the operator contract](../../ci/README.md) for the Docker-capable Linux agent,
Node 26.9.0/npm 11.12.1, pinned private services, image smoke, cleanup and main-only
publication policy. No host Node installation or manually published service ports
are needed for this command. Production container stop grace must be **30 seconds**
to leave margin beyond the unchanged ten-second CLI deadline.

For individual developer checks, use Node 26.9.0 and npm 11.12.1:

```bash
npm ci
npm run build
npm run test --workspace=tapeworm_dispatcher_mdb_rmq
npm run check --workspace=tapeworm_dispatcher_mdb_rmq
npm run test:consumer --workspace=tapeworm_dispatcher_mdb_rmq
```

For individual integration debugging, wait for Mongo primary and Rabbit readiness.
The shared qualifier provisions/verifies them automatically. `TEST_MONGODB_URI` and
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
a unique OS temporary directory, then compiles positive and negative type fixtures
without aliases and removes only its own scratch directory, even on failure.
`check` uses strict TS (including tests/CLI, no unchecked indexing, no skipped
library checks) and type-aware unsafe-operation lint. Unit tests use narrow typed
ports, not casts of whole Mongo Db or AMQP channels.

These are correctness regressions, **not production certification**, 100M-record
capacity measurements, replica election/rollback qualification, broker disk-loss
testing, or proof that an operator's required queue topology is correct. Qualify
retention headroom, failover, queue durability and peak recovery throughput for
the deployment before enabling delivery of business-critical intents.
