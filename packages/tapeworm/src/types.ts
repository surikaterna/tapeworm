import type Bluebird from "bluebird";

/**
 * Node-style error-first callback.
 */
export type NodeCallback<T> = (err: Error | null, result?: T) => void;

/**
 * Minimum event contract that tapeworm requires.
 * Tapeworm reads `type` for delete detection and manages `version` internally.
 * All other fields (payload, aggregateId, etc.) pass through untouched.
 */
export interface IBaseEvent {
  id: string;
  type: string;
  version?: number;
  [key: string]: unknown;
}

/**
 * A commit groups one or more events for atomic persistence.
 */
export interface ICommit<TEvent extends IBaseEvent = IBaseEvent> {
  id: string;
  partitionId: string;
  streamId: string;
  commitSequence: number;
  events: TEvent[];
  isDispatched?: boolean;
  [key: string]: unknown;
}

/**
 * Snapshot of aggregate state at a point in time.
 */
export interface ISnapshot {
  id: string;
  version: number;
  snapshot?: Record<string, unknown>;
  storedDateTime?: string;
}

/**
 * The persistence partition interface that all store implementations must satisfy.
 * Methods marked optional are only implemented by some stores.
 */
export interface IPersistencePartition<TEvent extends IBaseEvent = IBaseEvent> {
  append(
    commit: ICommit<TEvent>,
    callback?: NodeCallback<ICommit<TEvent>>,
  ): Bluebird<ICommit<TEvent>>;
  queryAll(
    callback?: NodeCallback<ICommit<TEvent>[]>,
  ): Bluebird<ICommit<TEvent>[]>;
  queryStream(
    streamId: string,
    fromEventSequence?: number | NodeCallback<ICommit<TEvent>[]>,
    callback?: NodeCallback<ICommit<TEvent>[]>,
  ): Bluebird<ICommit<TEvent>[]>;
  getUndispatched(
    callback?: NodeCallback<ICommit<TEvent>[]>,
  ): Bluebird<ICommit<TEvent>[]>;
  markAsDispatched(
    commit: ICommit<TEvent>,
    callback?: NodeCallback<ICommit<TEvent>>,
  ): Bluebird<ICommit<TEvent>>;

  // Optional methods — implemented by some stores
  storeSnapshot?(
    streamId: string,
    snapshot: Record<string, unknown> | undefined,
    version: number,
    callback?: NodeCallback<ISnapshot>,
  ): Bluebird<ISnapshot>;
  loadSnapshot?(
    streamId: string,
    callback?: NodeCallback<ISnapshot | undefined>,
  ): Bluebird<ISnapshot | undefined>;
  removeSnapshot?(
    streamId: string,
    callback?: NodeCallback<void>,
  ): Bluebird<void>;
  removeSnapshots?(
    streamIds: string[],
    callback?: NodeCallback<void>,
  ): Bluebird<void>;
  getLatestCommit?(
    streamId: string,
    callback?: NodeCallback<ICommit<TEvent> | undefined>,
  ): Bluebird<ICommit<TEvent> | undefined>;
  truncateStreamFrom?(
    streamId: string,
    commitSequence: number,
    remove?: boolean | NodeCallback<void>,
    callback?: NodeCallback<void>,
  ): Bluebird<void>;
  applyCommitHeader?(
    commitId: string,
    header: Record<string, unknown>,
    callback?: NodeCallback<ICommit<TEvent>>,
  ): Bluebird<ICommit<TEvent>>;
  queryStreamWithSnapshot?(
    streamId: string,
    callback?: NodeCallback<unknown>,
  ): Bluebird<unknown>;
  querySnapshotsByMaxDateTime?(
    dateTime: string,
    callback?: NodeCallback<ISnapshot[]>,
  ): Bluebird<ISnapshot[]>;
}

/**
 * Top-level persistence provider — manages partitions.
 */
export interface IPersistenceProvider<TEvent extends IBaseEvent = IBaseEvent> {
  openPartition(
    partitionId?: string,
    callback?: NodeCallback<IPersistencePartition<TEvent>>,
  ): Bluebird<IPersistencePartition<TEvent>>;
}

/**
 * Dispatch handler for publishing committed events.
 */
export type DispatchHandler<TEvent extends IBaseEvent = IBaseEvent> = (
  commit: ICommit<TEvent>,
  done?: () => void,
) => void;
