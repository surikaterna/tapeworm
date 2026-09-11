# tapeworm

## 0.6.0

### Minor Changes

- 855c605: Convert monorepo to TypeScript with full type definitions

  - Convert all packages from JavaScript to TypeScript with complete type annotations
  - Add shared type system (IBaseEvent, ICommit, ISnapshot, IPersistencePartition)
  - Migrate all test suites to vitest
  - Add CJS backward-compatibility shims to preserve existing require() API
  - Fix \_partitions initialization bug and indexeddb loadSnapshot error handler

### Patch Changes

- Upgrade packages to fix vulnerabilities.
