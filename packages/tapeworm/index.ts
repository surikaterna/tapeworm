import EventStore from "./lib/event_store";

export {
  Commit,
  ConcurrencyError,
  DuplicateCommitError,
  Event,
  EventStorePartition,
  EventStream,
} from "./lib/event_store";
// Re-export types for TypeScript consumers
export type {
  DispatchHandler,
  IBaseEvent,
  ICommit,
  IPersistencePartition,
  IPersistenceProvider,
  ISnapshot,
  NodeCallback,
} from "./lib/types";
export default EventStore;
