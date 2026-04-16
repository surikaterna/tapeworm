import Promise from "bluebird";
import type { Collection, Db } from "mongodb";
import { ConcurrencyError, DuplicateCommitError } from "tapeworm";
import type { IBaseEvent, ICommit, ISnapshot, NodeCallback } from "tapeworm";
import { isNil, get, isFunction } from "lodash";
import { v7 as uuidv7 } from "uuid";

var CONCURRENCY_EXCEPTION_CODE = 11000;

interface IMdbPartition {
  _mongodb: Db;
  _partitionId: string;
  _collection: Collection | null;
  _commits: Collection<ICommit>;
  _snapshots: Collection<ISnapshot & { _id: string }>;
  _opened: Promise<IMdbPartition>;
  open(): Promise<IMdbPartition>;
  storeSnapshot(
    streamId: string,
    snapshot: Record<string, unknown> | undefined,
    version: number,
    callback?: NodeCallback<ISnapshot>,
  ): Promise<ISnapshot>;
  loadSnapshot(
    streamId: string,
    callback?: NodeCallback<ISnapshot | undefined>,
  ): Promise<ISnapshot | undefined>;
  removeSnapshot(
    streamId: string,
    callback?: NodeCallback<ISnapshot>,
  ): Promise<ISnapshot>;
  append(commit: ICommit, callback?: NodeCallback<ICommit>): Promise<ICommit>;
  markAsDispatched(
    commit: ICommit,
    callback?: NodeCallback<ICommit>,
  ): Promise<ICommit>;
  getUndispatched(callback?: NodeCallback<ICommit[]>): Promise<ICommit[]>;
  queryAll(callback?: NodeCallback<ICommit[]>): Promise<ICommit[]>;
  getLatestCommit(
    streamId: string,
    callback?: NodeCallback<ICommit | undefined>,
  ): Promise<ICommit | undefined>;
  queryStream(
    streamId: string,
    fromEventSequence?: number | NodeCallback<ICommit[]>,
    callback?: NodeCallback<ICommit[]>,
  ): Promise<ICommit[]>;
  queryStreamFallback(
    streamId: string,
    fromEventSequence?: number | NodeCallback<ICommit[]>,
    callback?: NodeCallback<ICommit[]>,
  ): Promise<ICommit[]>;
  truncateStreamFrom(
    streamId: string,
    commitSequence: number,
    remove?: boolean | NodeCallback<void>,
    callback?: NodeCallback<void>,
  ): Promise<void>;
}

var MdbPartition = function (
  this: IMdbPartition,
  mongodb: Db,
  partitionId: string,
) {
  this._mongodb = mongodb;
  this._partitionId = partitionId;
  this._collection = null;
} as unknown as new (mongodb: Db, partitionId: string) => IMdbPartition;

MdbPartition.prototype.open = function (
  this: IMdbPartition,
): Promise<IMdbPartition> {
  var self = this;
  this._commits = this._mongodb.collection(
    "tw_" + this._partitionId + "_commits",
  );
  this._snapshots = this._mongodb.collection(
    "tw_" + this._partitionId + "_snapshots",
  );
  return new Promise<IMdbPartition>(function (resolve, reject) {
    self._snapshots
      .createIndex({ id: 1 }, { unique: true })
      .then(() => self._commits.createIndex({ id: 1 }, { unique: true }))
      .then(() =>
        self._commits.createIndex(
          { streamId: 1, commitSequence: 1 },
          { unique: true },
        ),
      )
      .then(() =>
        self._commits.createIndex({ createDateTime: 1 }, { unique: false }),
      )
      .then(() => self._commits.createIndex({ token: 1 }, { unique: false }))
      .then(() => resolve(self))
      .catch(reject);
  });
};

MdbPartition.prototype.storeSnapshot = function (
  this: IMdbPartition,
  streamId: string,
  snapshot: Record<string, unknown> | undefined,
  version: number,
  callback?: NodeCallback<ISnapshot>,
): Promise<ISnapshot> {
  var self = this;
  return new Promise<ISnapshot>(function (resolve, reject) {
    var toStore = {
      _id: streamId,
      id: streamId,
      version: version,
      snapshot: snapshot,
    };
    self._snapshots
      .updateOne(
        { _id: streamId } as Record<string, unknown>,
        { $set: toStore },
        { upsert: true },
      )
      .then(function () {
        resolve(toStore);
      })
      .catch(function (err: Error) {
        reject(err);
      });
  }).nodeify(callback);
};

MdbPartition.prototype.loadSnapshot = function (
  this: IMdbPartition,
  streamId: string,
  callback?: NodeCallback<ISnapshot | undefined>,
): Promise<ISnapshot | undefined> {
  var self = this;
  return new Promise<ISnapshot | undefined>(function (resolve, reject) {
    self._snapshots
      .find({ id: streamId } as Record<string, unknown>)
      .toArray()
      .then(function (res) {
        resolve(res[0] as ISnapshot | undefined);
      })
      .catch(function (err: Error) {
        reject(err);
      });
  }).nodeify(callback);
};

MdbPartition.prototype.removeSnapshot = function (
  this: IMdbPartition,
  streamId: string,
  callback?: NodeCallback<ISnapshot>,
): Promise<ISnapshot> {
  var self = this;
  return new Promise<ISnapshot>(function (resolve, reject) {
    var toStore = { _id: streamId, id: streamId, version: -1 };
    var filter = { _id: streamId } as Record<string, unknown>;
    self._snapshots
      .replaceOne(filter, toStore as ISnapshot & { _id: string })
      .then(function () {
        resolve(toStore as ISnapshot);
      })
      .catch(function (err: Error) {
        reject(err);
      });
  }).nodeify(callback);
};

MdbPartition.prototype.append = function (
  this: IMdbPartition,
  commit: ICommit,
  callback?: NodeCallback<ICommit>,
): Promise<ICommit> {
  var self = this;
  commit.token = uuidv7();
  commit.isDispatched = false;
  commit.createDateTime = new Date();
  return new Promise<ICommit>(function (resolve, reject) {
    self._commits
      .insertOne(commit as ICommit & Record<string, unknown>)
      .then(function () {
        resolve(commit);
      })
      .catch(function (err: Error & { code?: number }) {
        if (err.code === CONCURRENCY_EXCEPTION_CODE) {
          if (err.message.indexOf("commitSequence") > 0) {
            reject(
              new ConcurrencyError(
                "Concurrency error on stream " + commit.streamId,
              ),
            );
          } else {
            reject(
              new DuplicateCommitError(
                "Duplicate commit on stream " + commit.streamId,
              ),
            );
          }
        } else {
          reject(err);
        }
      });
  }).nodeify(callback);
};

MdbPartition.prototype.markAsDispatched = function (
  this: IMdbPartition,
  _commit: ICommit,
  _callback?: NodeCallback<ICommit>,
): Promise<ICommit> {
  throw Error("not implemented");
};

MdbPartition.prototype.getUndispatched = function (
  this: IMdbPartition,
  _callback?: NodeCallback<ICommit[]>,
): Promise<ICommit[]> {
  throw Error("not implemented");
};

MdbPartition.prototype.queryAll = function (
  this: IMdbPartition,
  callback?: NodeCallback<ICommit[]>,
): Promise<ICommit[]> {
  var self = this;
  return new Promise<ICommit[]>(function (resolve, reject) {
    //TODO: sort in insert order...
    self._commits
      .find({})
      .sort({ createDateTime: 1 })
      .toArray()
      .then(function (commits) {
        resolve(commits as ICommit[]);
      })
      .catch(function (err: Error) {
        reject(err);
      });
  }).nodeify(callback);
};

MdbPartition.prototype.getLatestCommit = function (
  this: IMdbPartition,
  streamId: string,
  callback?: NodeCallback<ICommit | undefined>,
): Promise<ICommit | undefined> {
  if (!streamId) {
    throw new Error("missing streamId");
  }
  var self = this;
  return new Promise<ICommit | undefined>(function (resolve, reject) {
    self._commits
      .find({ streamId: streamId } as Record<string, unknown>)
      .limit(1)
      .sort({ commitSequence: -1 })
      .toArray()
      .then(function (commits) {
        // assume this will probably not happen
        if (commits.length === 0) {
          resolve(undefined);
          return;
        }
        var commit = commits[0];
        resolve(commit as ICommit);
      })
      .catch(function (err: Error) {
        reject(err);
      });
  }).nodeify(callback);
};

MdbPartition.prototype.queryStream = function (
  this: IMdbPartition,
  streamId: string,
  fromEventSequence?: number | NodeCallback<ICommit[]>,
  callback?: NodeCallback<ICommit[]>,
): Promise<ICommit[]> {
  var self = this;
  if (typeof fromEventSequence === "function") {
    callback = fromEventSequence;
    fromEventSequence = 0;
  }
  // if from event sequence is 0 / undefined - use fallback..
  if (isNil(fromEventSequence) || fromEventSequence === 0) {
    return self.queryStreamFallback(streamId, fromEventSequence, callback);
  }
  var seq = fromEventSequence as number;
  return new Promise<ICommit[]>(function (resolve, reject) {
    // use limit 1 to get last commit - if we are unable to find the correct event, use fallback...
    self._commits
      .find({ streamId: streamId } as Record<string, unknown>)
      .limit(1)
      .sort({ commitSequence: -1 })
      .toArray()
      .then(function (commits) {
        // assume this will probably not happen
        if (commits.length === 0) {
          resolve([]);
          return;
        }
        var events = get(commits, "[0].events", []) as IBaseEvent[];
        // check if our requested event version is out of reach for the last commit - need to use fallback then..
        if (events[0].version! > seq) {
          // problem... use fallback.
          return self.queryStreamFallback(streamId, seq).then(function (
            res: ICommit[],
          ) {
            resolve(res);
          });
        }
        // assume this will happen most of the time.
        if (events[events.length - 1].version! < seq) {
          return resolve([]);
        }

        // find every event that has version equal or larger than fromEventSequence
        var foundEvents: IBaseEvent[] = [];
        for (var i = 0; i < events.length; i++) {
          if (events[i].version! >= seq) {
            foundEvents.push(events[i]);
          }
        }

        if (foundEvents.length === 0) {
          resolve([]);
          return;
        }

        var commit = commits[0] as ICommit;
        commit.events = foundEvents;
        resolve([commit]);
      })
      .catch(function (err: Error) {
        reject(err);
      });
  }).nodeify(callback);
};

MdbPartition.prototype.queryStreamFallback = function (
  this: IMdbPartition,
  streamId: string,
  fromEventSequence?: number | NodeCallback<ICommit[]>,
  callback?: NodeCallback<ICommit[]>,
): Promise<ICommit[]> {
  if (typeof fromEventSequence === "function") {
    callback = fromEventSequence;
    fromEventSequence = 0;
  }
  var seq = (fromEventSequence as number) || 0;
  var self = this;
  return new Promise<ICommit[]>(function (resolve, reject) {
    //TODO: sort in insert order...
    self._commits
      .find({ streamId: streamId } as Record<string, unknown>)
      .sort({ commitSequence: 1 })
      .toArray()
      .then(function (commits) {
        var result = commits as ICommit[];
        if (seq > 0) {
          var startCommitId = 0;
          var foundEvents = 0;
          for (var i = 0; i < result.length; i++) {
            foundEvents += result[i].events.length;
            startCommitId++;
            if (foundEvents >= seq) {
              break;
            }
          }
          var tooMany = foundEvents - seq;

          result = result.slice(startCommitId - (tooMany > 0 ? 1 : 0));
          if (tooMany > 0) {
            result[0].events = result[0].events.slice(
              result[0].events.length - tooMany,
            );
          }
        }
        resolve(result);
      })
      .catch(function (err: Error) {
        reject(err);
      });
  }).nodeify(callback);
};

MdbPartition.prototype.truncateStreamFrom = function (
  this: IMdbPartition,
  streamId: string,
  commitSequence: number,
  remove?: boolean | NodeCallback<void>,
  callback?: NodeCallback<void>,
): Promise<void> {
  var self = this;
  if (isFunction(remove)) {
    callback = remove;
    remove = false;
  }
  return new Promise<void>(function (resolve, reject) {
    self._commits
      .deleteMany({
        streamId,
        commitSequence: { $gte: commitSequence },
      } as Record<string, unknown>)
      .then(function () {
        resolve();
      })
      .catch(function (err: Error) {
        reject(err);
      });
  }).nodeify(callback);
};

export default MdbPartition;
