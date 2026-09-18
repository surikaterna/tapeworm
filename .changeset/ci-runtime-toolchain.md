---
"tapeworm_dispatcher_mdb_rmq": patch
---

Align the production container with Node 26.9.0 and npm 11.12.1, isolate workspace dependencies, and qualify the shipped image and its shutdown behavior before publication.

Clean only validated workspace-generated output before qualification and require exact npm/runtime-image file inventories and byte hashes, preventing obsolete modules from surviving reused-workspace builds.
