---
"tapeworm_persistence_store_mongodb": patch
"tapeworm_dispatcher_mdb_rmq": minor
---

Add MongoDB-to-RabbitMQ commit dispatcher with oplog-based change stream tailing.

MongoDB store: adds UUID v7 .token field to persisted commits for dispatcher resume fallback.
Dispatcher: new package providing at-least-once delivery of tapeworm commits to a RabbitMQ fanout exchange, with two-level resume strategy (change stream token + UUID v7 .token cursor).
