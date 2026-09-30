# tapeworm_dispatcher_mdb_rmq

## 1.0.0

### Major Changes

- 24d1304: Address redemeine-gqxm: never checkpoint past failed publication, correlate mandatory
  Rabbit returns per attempt, and retry from acknowledged CDC progress. Add versioned
  recovery progress and scoped checkpoint options while retaining legacy constructors
  and Document-compatible watcher callbacks. Legacy callbacks/stores that cannot
  persist recovery now fail explicitly; unidentifiable checkpoints require operator
  adoption. Indexed UUID fallback preserves a captured server live boundary and finite
  scan range, with the documented accepted historical lower-UUID omission after resume
  history expires. This is not an exactly-once or lossless-fallback guarantee.

  This is a breaking migration despite retaining or adding API signatures. Before
  upgrading, fence the old owner, back up and verify its checkpoint, and explicitly
  adopt unscoped legacy state. Upgrade three-field custom stores to durably roundtrip
  all versioned recovery fields and BSON values. Direct watcher users needing replay
  must migrate from legacy `start` callbacks to `startWithProgress`, persisting every
  transition. These fail-closed guards intentionally reject unsafe legacy behavior;
  there is no pre-1.0 compatibility exception. A major changeset records the required
  consumer action only: publishing or promoting a release requires separate approval.

  For redemeine-lkz5, source and tests are grouped by concern. The supported
  `tapeworm_dispatcher_mdb_rmq` package-root API is unchanged by this layout change;
  unsupported internal deep module paths relocate without compatibility shims.

### Minor Changes

- 40c163a: Add configured reference-backed durable poison quarantine and explicit operator redrive (redemeine-1i0g,
  availability policy redemeine-ihn0). Enabled quarantine defaults to continue after durable capture;
  mode:pause explicitly opts into stopping. No secondary ordering-gap acknowledgement is required;
  acceptOrderingGaps:true remains deprecated compatibility syntax. Absent/disabled quarantine stays fail-closed.
  Reworked as transport-only cold-path quarantine (redemeine-rq8p, draft PR #44): only configured local
  encoded message-size rejection qualifies; infrastructure and arbitrary serialization failures do not.
  There is no business-schema hook or default broker-size guess. Removed draft policy options reject at construction.
  Healthy publication does not access quarantine storage/index metadata or fingerprint the source;
  the validated transport envelope is encoded once without redundant decoding. Adapter source-index and store
  readiness are lazy, retryable prerequisites for cold capture of rejected records, not healthy startup.
  Normal source replay re-evaluates publishability and may deliver a previously quarantined/claimed/published
  record with the same message id, without updating its historical operator receipt. Consumer idempotency
  is required. Only a currently size-rejected record reuses a prior published resolution to advance without publication.
  Continue creates ordering gaps that later redrive cannot repair; source retention and a durable store are prerequisites.
  Quarantine identity and indexed source lookups use explicit binary collation, including on
  collections with linguistic defaults. Incompatible legacy quarantine indexes require operator migration.
  The accompanying CDC migration changeset remains major and determines the combined release bump.

  Decouple core delivery from optional quarantine (redemeine-kiwb). Compose a
  QuarantineFailureHandler explicitly as Dispatcher failureHandler and subscribe to
  quarantined on the adapter, not the dispatcher. This intentionally replaces the
  unreleased B draft's quarantine constructor option/event; published non-quarantine
  0.2 options otherwise remain unchanged. Unsupported own dispatcher options now
  reject early, including undefined-valued extras previously silently ignored.
  The small trusted DeliveryFailureHandler port returns unhandled or durablyHandled;
  core alone owns checkpoint persistence and validates receipts before saving.
  Optional synchronous onCheckpointed notifications run only after persistence.
  Throws or untyped non-undefined returns terminally halt via DeliveryHalted rather
  than retrying from stale in-memory progress; restart reloads the saved checkpoint.
  This does not promise exactly-once notifications or certify custom-handler durability.

  Clarify operator claim/completion orchestration with private named atomic steps
  (redemeine-nqxf), preserving MongoDB commands, server-time leases and stored audit layout.

  For redemeine-lkz5, group generic delivery failures and RabbitMQ encoding with their
  source concerns, and mirror quarantine tests beside their local support fixtures.
  Package-root exports are unchanged; private deep module paths relocate as noted
  in the accompanying major CDC migration changeset, without compatibility shims.

### Patch Changes

- 87794f8: Address redemeine-4tud.2 (parent redemeine-4tud; source redemeine-gqxm): isolate
  the CLI operational repair from the core CDC migration.

  Also ensure CLI initialization and teardown always attempt Mongo closure and signal
  listener removal, including initialization errors and rejected dispatcher shutdown.
  Signal shutdown now has one ten-second grace budget independent of pending work.
  Only after the deadline and cleanup attempts may the CLI host exit 124; library
  lifecycle code reports a typed timeout and observes late failures without exiting.
  An already-issued Mongo checkpoint may still complete after process exit; restart
  must use inspected durable state and retain stable-identity duplicate handling.

- 5581d54: Align the production container with Node 26.9.0 and npm 11.12.1, isolate workspace dependencies, and qualify the shipped image and its shutdown behavior before publication.

  Clean only validated workspace-generated output before qualification and require exact npm/runtime-image file inventories and byte hashes, preventing obsolete modules from surviving reused-workspace builds.

- ceea5df: Upgrade slf dependencies.

## 0.2.0

### Minor Changes

- 9e6d52f: Add MongoDB-to-RabbitMQ commit dispatcher with oplog-based change stream tailing.

  MongoDB store: adds UUID v7 .token field to persisted commits for dispatcher resume fallback.
  Dispatcher: new package providing at-least-once delivery of tapeworm commits to a RabbitMQ fanout exchange, with two-level resume strategy (change stream token + UUID v7 .token cursor).

### Patch Changes

- Upgrade packages to fix vulnerabilities.
- Updated dependencies
- Updated dependencies [855c605]
  - tapeworm@0.6.0
