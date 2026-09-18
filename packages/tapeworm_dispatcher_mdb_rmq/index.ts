export { Dispatcher } from "./src/dispatcher";
export { ChangeStreamWatcher } from "./src/ingestion/watcher";
export { OplogWatcher } from "./src/ingestion/oplog-watcher";
export { CommitPublisher } from "./src/rabbitmq/publisher";
export { MongoResumeTokenStore } from "./src/checkpoints/mongodb-store";
export { checkpointFeed } from "./src/checkpoints/feed";
export type { PublisherPort } from "./src/delivery/delivery";

export type { ICommitWatcher, CommitHandler, DurableCommitWatcher, ProgressHandler } from "./src/ingestion/watcher";
export type { MongoResumeStoreOptions } from "./src/checkpoints/mongodb-store";
export type { IResumeTokenStore } from "./src/checkpoints/types";
export type {
  DispatcherConfig,
  DispatcherEvents,
  MongoConfig,
  RabbitConfig,
  ResumeState,
  WatchMode,
  PrimaryPosition,
  RecoveryPosition,
  DurableProgress,
  RecoveryEvent,
} from "./src/types";
