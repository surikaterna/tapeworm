import Promise from "bluebird";
import Event from "./event";
import Partition from "./event_store_partition";
import EventStream from "./event_stream";
import Commit from "./persistence/commit";
import ConcurrencyError from "./persistence/concurrency_error";
import DuplicateCommitError from "./persistence/duplicate_commit_error";
import InMemoryPersistenceStore from "./persistence/inmemory/inmemory_persistence";
import type { DispatchHandler, IPersistencePartition, IPersistenceProvider, NodeCallback } from "./types";

type PartitionInstance = InstanceType<typeof Partition>;

var _PENDING: Record<string, never> = {};

interface IEventStore {
  _store: IPersistenceProvider;
  _dispatchService?: DispatchHandler;
  _partitions: Record<string, PartitionInstance | typeof _PENDING>;
  openPartition(partitionId?: string, callback?: NodeCallback<PartitionInstance>): Promise<PartitionInstance>;
}

var EventStore = function (this: IEventStore, persistenceStore?: IPersistenceProvider, dispatchService?: DispatchHandler) {
  this._store = persistenceStore || new InMemoryPersistenceStore();
  this._dispatchService = dispatchService;
  this._partitions = {};
} as unknown as {
  new (persistenceStore?: IPersistenceProvider, dispatchService?: DispatchHandler): IEventStore;
  Event: typeof Event;
  EventStream: typeof EventStream;
  EventStorePartition: typeof Partition;
  Commit: typeof Commit;
  ConcurrencyError: typeof ConcurrencyError;
  DuplicateCommitError: typeof DuplicateCommitError;
};

EventStore.prototype.openPartition = function (this: IEventStore, partitionId?: string, callback?: NodeCallback<PartitionInstance>) {
  var partition: PartitionInstance | typeof _PENDING | null = null;
  var pid = partitionId || "master";
  partition = this._partitions[pid];
  var self = this;
  if (!partition) {
    this._partitions[pid] = _PENDING;
    return this._store
      .openPartition(pid)
      .then(function (persistencePartition: IPersistencePartition) {
        var p = new Partition(pid, persistencePartition, self._dispatchService);
        return (self._partitions[pid] = p);
      })
      .nodeify(callback);
  } else {
    //_PENDING is set when the partition is loading, wait 5ms at a time to see if it has been loaded
    if (this._partitions[pid] === _PENDING) {
      var promise = new Promise<PartitionInstance>(function (resolve) {
        var resolver = function () {
          var partition = self._partitions[pid];
          if (partition === _PENDING) {
            setTimeout(resolver, 5);
          } else {
            resolve(partition as PartitionInstance);
          }
        };
        resolver();
      });
      return promise.nodeify(callback);
    } else {
      return Promise.resolve(this._partitions[pid] as PartitionInstance).nodeify(callback);
    }
  }
};

EventStore.Event = Event;
EventStore.EventStream = EventStream;
EventStore.EventStorePartition = Partition;
EventStore.Commit = Commit;
EventStore.ConcurrencyError = ConcurrencyError;
EventStore.DuplicateCommitError = DuplicateCommitError;

export default EventStore;

// Named exports for destructured imports: import EventStore, { Event, Commit } from 'tapeworm'
export { Event, EventStream, Commit, ConcurrencyError, DuplicateCommitError };
export { Partition as EventStorePartition };
