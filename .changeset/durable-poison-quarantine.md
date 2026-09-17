---
"tapeworm_dispatcher_mdb_rmq": minor
---

Add configured reference-backed durable poison quarantine and explicit operator redrive (redemeine-1i0g,
availability policy redemeine-ihn0). Enabled quarantine defaults to continue after durable capture;
mode:pause explicitly opts into stopping. No secondary ordering-gap acknowledgement is required;
acceptOrderingGaps:true remains deprecated compatibility syntax. Absent/disabled quarantine stays fail-closed.
Reworked as transport-only cold-path quarantine (redemeine-rq8p, draft PR #44): only configured local
encoded message-size rejection qualifies; infrastructure and arbitrary serialization failures do not.
There is no business-schema hook or default broker-size guess. Removed draft policy options reject at construction.
Healthy publication does not access quarantine storage/index metadata or fingerprint the source;
the validated transport envelope is encoded once without redundant decoding. Dispatcher source-index and store
readiness are lazy, retryable prerequisites for cold capture of rejected records, not healthy startup.
Normal source replay re-evaluates publishability and may deliver a previously quarantined/claimed/published
record with the same message id, without updating its historical operator receipt. Consumer idempotency
is required. Only a currently size-rejected record reuses a prior published resolution to advance without publication.
Continue creates ordering gaps that later redrive cannot repair; source retention and a durable store are prerequisites.
Quarantine identity and indexed source lookups use explicit binary collation, including on
collections with linguistic defaults. Incompatible legacy quarantine indexes require operator migration.
The accompanying CDC migration changeset remains major and determines the combined release bump.
