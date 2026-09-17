---
"tapeworm_dispatcher_mdb_rmq": minor
---

Add configured reference-backed durable poison quarantine and explicit operator redrive (redemeine-1i0g,
availability policy redemeine-ihn0). Enabled quarantine defaults to continue after durable capture;
mode:pause explicitly opts into stopping. No secondary ordering-gap acknowledgement is required;
acceptOrderingGaps:true remains deprecated compatibility syntax. Absent/disabled quarantine stays fail-closed.
Only deliberate unsupported-schema rejection and configured local
encoded message-size rejection qualify; infrastructure and arbitrary serialization failures do not.
Continue creates ordering gaps that later redrive cannot repair; source retention and a durable store are prerequisites.
Quarantine identity and indexed source lookups use explicit binary collation, including on
collections with linguistic defaults. Incompatible legacy quarantine indexes require operator migration.
The accompanying CDC migration changeset remains major and determines the combined release bump.
