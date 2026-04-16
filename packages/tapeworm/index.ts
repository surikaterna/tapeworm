import EventStore from "./src/event_store";

export {
  Commit,
  ConcurrencyError,
  DuplicateCommitError,
  Event,
  EventStorePartition,
  EventStream,
} from "./src/event_store";
// Re-export types for TypeScript consumers
export type {
  DispatchHandler,
  IBaseEvent,
  ICommit,
  IPersistencePartition,
  IPersistenceProvider,
  ISnapshot,
  NodeCallback,
} from "./src/types";
export default EventStore;
