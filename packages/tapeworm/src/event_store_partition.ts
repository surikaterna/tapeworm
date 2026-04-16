import Promise from "bluebird";
import _ from "lodash";
import { v4 as uuid } from "uuid";
import EventStream from "./event_stream";
import type {
  DispatchHandler,
  IBaseEvent,
  ICommit,
  IPersistencePartition,
  ISnapshot,
  NodeCallback,
} from "./types";

type EventStreamInstance = InstanceType<typeof EventStream>;

interface IEventStorePartition {
  _partitionId: string;
  _persistencePartition: IPersistencePartition;
  _dispatchService?: DispatchHandler;
  openStream(
    streamId: string,
    writeOnly?: boolean | NodeCallback<EventStreamInstance>,
    callback?: NodeCallback<EventStreamInstance>,
  ): Promise<EventStreamInstance>;
  append(
    commits: ICommit | ICommit[],
    callback?: NodeCallback<unknown>,
  ): Promise<ICommit[]>;
  delete(
    streamId: string,
    deleteEvent?: Record<string, unknown>,
  ): Promise<void>;
  _queryStream(
    streamId: string,
    callback?: NodeCallback<ICommit[]>,
  ): Promise<ICommit[]>;
  _queryAll(callback?: NodeCallback<ICommit[]>): Promise<ICommit[]>;
  _queryStreamWithSnapshotFallback(
    streamId: string,
    callback?: NodeCallback<unknown>,
  ): Promise<unknown>;
  _truncateStreamFrom(
    streamId: string,
    commitSequence: number,
    callback?: NodeCallback<void>,
  ): Promise<void>;
  _applyCommitHeader(
    commitId: string,
    header: Record<string, unknown>,
    callback?: NodeCallback<ICommit>,
  ): Promise<ICommit>;
  // Dynamically delegated methods — may or may not exist at runtime
  storeSnapshot?: IPersistencePartition["storeSnapshot"];
  loadSnapshot?: IPersistencePartition["loadSnapshot"];
  queryStream?: IPersistencePartition["queryStream"];
  queryStreamWithSnapshot?: IPersistencePartition["queryStreamWithSnapshot"];
  removeSnapshot?: IPersistencePartition["removeSnapshot"];
  getLatestCommit?: IPersistencePartition["getLatestCommit"];
  querySnapshotsByMaxDateTime?: IPersistencePartition["querySnapshotsByMaxDateTime"];
  [key: string]: unknown;
}

var EventStorePartition = function (
  this: IEventStorePartition,
  partitionId: string,
  persistencePartition: IPersistencePartition,
  dispatchService?: DispatchHandler,
) {
  this._partitionId = partitionId;
  this._persistencePartition = persistencePartition;
  this._dispatchService = dispatchService;

  var self = this;
  _.forEach(
    [
      "storeSnapshot",
      "loadSnapshot",
      "queryStream",
      "queryStreamWithSnapshot",
      "removeSnapshot",
      "getLatestCommit",
      "querySnapshotsByMaxDateTime",
    ] as const,
    function (what: string) {
      // biome-ignore lint/complexity/noBannedTypes: dynamic method delegation requires generic function type
      var pp = self._persistencePartition as unknown as Record<
        string,
        Function
      >;
      if (pp[what]) {
        self[what] = function () {
          return pp[what].apply(self._persistencePartition, arguments);
        };
      } else {
        if (what === "queryStreamWithSnapshot") {
          // fallback function
          self[what] = function () {
            return self._queryStreamWithSnapshotFallback.apply(
              self._persistencePartition,
              arguments as unknown as [string, NodeCallback<unknown>?],
            );
          };
        }
      }
    },
  );
} as unknown as new (
  partitionId: string,
  persistencePartition: IPersistencePartition,
  dispatchService?: DispatchHandler,
) => IEventStorePartition;

EventStorePartition.prototype.openStream = function (
  this: IEventStorePartition,
  streamId: string,
  writeOnly?: boolean | NodeCallback<EventStreamInstance>,
  callback?: NodeCallback<EventStreamInstance>,
) {
  if (typeof writeOnly === "function") {
    callback = writeOnly;
    writeOnly = false;
  }
  var stream = new EventStream(
    this as unknown as ConstructorParameters<typeof EventStream>[0],
    streamId,
    writeOnly as boolean,
  );
  return stream._prepareStream(callback);
};

EventStorePartition.prototype.append = function (
  this: IEventStorePartition,
  commits: ICommit | ICommit[],
  callback?: NodeCallback<unknown>,
) {
  //pre hooks
  var self = this;

  if (!_.isArray(commits)) {
    commits = [commits];
  }
  return Promise.each(commits, function (commit: ICommit) {
    return self._persistencePartition
      .append(commit, callback as NodeCallback<ICommit>)
      .then(function (r: ICommit) {
        var done = function () {
          self._persistencePartition.markAsDispatched(commit);
        };
        if (self._dispatchService) {
          self._dispatchService(commit, done);
        }
        return r;
      });
  });
  //post hooks
};

/**
 * Delete function in tapeworm.
 * Triggers a $stream.deleted.event, truncates the stream, and removes snapshots.
 */
EventStorePartition.prototype.delete = function (
  this: IEventStorePartition,
  streamId: string,
  deleteEvent?: Record<string, unknown>,
) {
  var self = this;
  var event = Object.assign({}, deleteEvent, {
    id: uuid(),
    type: "$stream.deleted.event",
    payload: Object.assign(
      {},
      { dateOfDeletion: new Date() },
      (deleteEvent && (deleteEvent.payload as Record<string, unknown>)) || {},
    ),
  });
  return this._truncateStreamFrom(streamId, -1)
    .then(function () {
      return self.openStream(streamId, false);
    })
    .then(function (stream: EventStreamInstance) {
      stream.append(event as IBaseEvent);
      return stream.commit(uuid());
    })
    .then(function () {
      // support legacy/unsupported approach
      if (self._persistencePartition.removeSnapshot) {
        return self._persistencePartition.removeSnapshot(streamId);
      } else if (self._persistencePartition.removeSnapshots) {
        return self._persistencePartition.removeSnapshots([streamId]);
      }
    });
};

/*** UNDOCUMENTED API ***/

EventStorePartition.prototype._queryStream = function (
  this: IEventStorePartition,
  streamId: string,
  callback?: NodeCallback<ICommit[]>,
) {
  return this._persistencePartition.queryStream(streamId, callback);
};

EventStorePartition.prototype._queryAll = function (
  this: IEventStorePartition,
  callback?: NodeCallback<ICommit[]>,
) {
  return this._persistencePartition.queryAll(callback);
};

EventStorePartition.prototype._queryStreamWithSnapshotFallback = function (
  this: IPersistencePartition,
  streamId: string,
  callback?: NodeCallback<unknown>,
) {
  var self = this;
  return new Promise(function (
    resolve: (value: unknown) => void,
    reject: (reason: Error) => void,
  ) {
    self.loadSnapshot?.(
      streamId,
      function (err: Error | null, snapshot?: ISnapshot) {
        if (err) {
          reject(err);
        } else {
          var snapshotVersion = (snapshot && snapshot.version) || -1;
          self.queryStream(
            streamId,
            snapshotVersion,
            function (_err: Error | null, res?: ICommit[]) {
              resolve({ snapshot: snapshot, commits: res });
            },
          );
        }
      },
    );
  }).nodeify(callback);
};

/*** NEEDED FOR SYNCING ***/

EventStorePartition.prototype._truncateStreamFrom = function (
  this: IEventStorePartition,
  streamId: string,
  commitSequence: number,
  callback?: NodeCallback<void>,
) {
  return this._persistencePartition.truncateStreamFrom?.(
    streamId,
    commitSequence,
    callback,
  );
};

EventStorePartition.prototype._applyCommitHeader = function (
  this: IEventStorePartition,
  commitId: string,
  header: Record<string, unknown>,
  callback?: NodeCallback<ICommit>,
) {
  return this._persistencePartition.applyCommitHeader?.(
    commitId,
    header,
    callback,
  );
};

export default EventStorePartition;
