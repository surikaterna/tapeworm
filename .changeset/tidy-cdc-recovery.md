---
"tapeworm_dispatcher_mdb_rmq": major
---

Address redemeine-gqxm: never checkpoint past failed publication, correlate mandatory
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
