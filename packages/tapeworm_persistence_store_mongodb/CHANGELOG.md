# tapeworm_persistence_store_mongodb

## 3.1.0

### Minor Changes

- 855c605: Convert monorepo to TypeScript with full type definitions

  - Convert all packages from JavaScript to TypeScript with complete type annotations
  - Add shared type system (IBaseEvent, ICommit, ISnapshot, IPersistencePartition)
  - Migrate all test suites to vitest
  - Add CJS backward-compatibility shims to preserve existing require() API
  - Fix \_partitions initialization bug and indexeddb loadSnapshot error handler

### Patch Changes

- Upgrade packages to fix vulnerabilities.
- 9e6d52f: Add MongoDB-to-RabbitMQ commit dispatcher with oplog-based change stream tailing.

  MongoDB store: adds UUID v7 .token field to persisted commits for dispatcher resume fallback.
  Dispatcher: new package providing at-least-once delivery of tapeworm commits to a RabbitMQ fanout exchange, with two-level resume strategy (change stream token + UUID v7 .token cursor).

- Updated dependencies
- Updated dependencies [855c605]
  - tapeworm@0.6.0
