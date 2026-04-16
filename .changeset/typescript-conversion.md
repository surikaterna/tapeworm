---
tapeworm: minor
tapeworm_persistence_store_hybrid: minor
tapeworm_persistence_store_indexeddb: minor
tapeworm_persistence_store_mongodb: minor
tapeworm_persistence_store_remote: minor
---

Convert monorepo to TypeScript with full type definitions

- Convert all packages from JavaScript to TypeScript with complete type annotations
- Add shared type system (IBaseEvent, ICommit, ISnapshot, IPersistencePartition)
- Migrate all test suites to vitest
- Add CJS backward-compatibility shims to preserve existing require() API
- Fix _partitions initialization bug and indexeddb loadSnapshot error handler
