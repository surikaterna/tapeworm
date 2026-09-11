# tapeworm_dispatcher_mdb_rmq

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
