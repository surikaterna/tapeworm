import _ from "lodash";
import Promise from "bluebird";
import { ConcurrencyError, DuplicateCommitError } from "tapeworm";
import type { ICommit, ISnapshot, NodeCallback } from "tapeworm";
import { LoggerFactory } from "slf";

var LOG = LoggerFactory.getLogger(
  "tapeworm-persistence-store-indexdb:idb-partition",
);
const VERSION = 4;

interface IIdbPartition {
  _idb: IDBFactory;
  _partitionId: string;
  _dbname: string;
  _db: IDBDatabase;
  _promisify<T>(value: T, cb?: NodeCallback<T>): Promise<T>;
  open(): Promise<IIdbPartition>;
  truncateStreamFrom(
    sid: string,
    seq: number,
    rm?: boolean | NodeCallback<void>,
    cb?: NodeCallback<void>,
  ): Promise<unknown>;
  applyCommitHeader(
    cid: string,
    hdr: Record<string, unknown>,
    cb?: NodeCallback<ICommit>,
  ): Promise<ICommit>;
  loadSnapshot(
    sid: string,
    cb?: NodeCallback<ISnapshot | null>,
  ): Promise<ISnapshot | null>;
  _storeSnapshots(
    snaps: ISnapshot[],
    cb?: NodeCallback<ISnapshot[]>,
  ): Promise<ISnapshot[]>;
  storeSnapshot(
    sid: string,
    snap: Record<string, unknown> | undefined,
    ver: number,
    cb?: NodeCallback<ISnapshot>,
  ): Promise<ISnapshot>;
  removeSnapshots(
    ids: string[] | string,
    cb?: NodeCallback<unknown>,
  ): Promise<unknown>;
  append(commit: ICommit, cb?: NodeCallback<ICommit>): Promise<ICommit>;
  markAsDispatched(commit: ICommit, cb?: NodeCallback<ICommit>): never;
  getUndispatched(cb?: NodeCallback<ICommit[]>): never;
  queryAll(cb?: NodeCallback<ICommit[]>): Promise<ICommit[]>;
  queryStream(
    sid: string,
    fromSeq?: number | NodeCallback<ICommit[]>,
    cb?: NodeCallback<ICommit[]>,
  ): Promise<ICommit[]>;
  querySnapshotsOlderThanMaxDate(
    dt: string,
    cb?: NodeCallback<ISnapshot[]>,
  ): Promise<ISnapshot[]>;
}

var IdbPartition = function (
  this: IIdbPartition,
  idb: IDBFactory,
  partitionId: string,
  dbname: string,
) {
  this._idb = idb;
  this._partitionId = partitionId;
  this._dbname = dbname;
} as unknown as new (
  idb: IDBFactory,
  partitionId: string,
  dbname: string,
) => IIdbPartition;

IdbPartition.prototype._promisify = function <T>(
  this: IIdbPartition,
  value: T,
  callback?: NodeCallback<T>,
): Promise<T> {
  return Promise.resolve(value).nodeify(callback);
};

IdbPartition.prototype.open = function (
  this: IIdbPartition,
): Promise<IIdbPartition> {
  var self = this;
  var request: IDBOpenDBRequest = this._idb.open(
    "tw_" + this._dbname + "_" + this._partitionId,
    VERSION,
  );
  return new Promise(function (resolve, reject) {
    request.onsuccess = function () {
      var db = request.result;
      self._db = db;
      var stores = db.objectStoreNames;
      if (!stores.contains("commits"))
        LOG.error(
          "Object store %s is missing in tapeworm idb_partition. Version %s",
          "commits",
          db.version,
        );
      if (!stores.contains("truncated"))
        LOG.error(
          "Object store %s is missing in tapeworm idb_partition. Version %s",
          "truncated",
          db.version,
        );
      if (!stores.contains("snapshots"))
        LOG.error(
          "Object store %s is missing in tapeworm idb_partition. Version %s",
          "snapshots",
          db.version,
        );
      resolve(self);
    };
    request.onupgradeneeded = function () {
      var db = request.result;
      var stores = db.objectStoreNames;
      if (!stores.contains("commits")) {
        try {
          var cs = db.createObjectStore("commits", {
            keyPath: "id",
            autoIncrement: false,
          });
          cs.createIndex("streamId", "streamId", { unique: false });
          cs.createIndex("streamIdCommitSequence", "streamIdCommitSequence", {
            unique: true,
          });
        } catch (e) {
          LOG.error("Failed to create objectStore %s. %j", "commits", e);
        }
      }
      if (!stores.contains("truncated")) {
        try {
          var ts = db.createObjectStore("truncated", {
            keyPath: "id",
            autoIncrement: false,
          });
          ts.createIndex("streamId", "streamId", { unique: false });
        } catch (e) {
          LOG.error("Failed to create objectStore %s. %j", "truncated", e);
        }
      }
      if (!stores.contains("snapshots")) {
        try {
          db.createObjectStore("snapshots", {
            keyPath: "id",
            autoIncrement: false,
          });
        } catch (e) {
          LOG.error("Failed to create objectStore %s. %j", "snapshots", e);
        }
      }
    };
    request.onerror = function (event: Event) {
      LOG.error("Failed to open tapeworm idb %j", event);
      reject(new Error(String(event)));
    };
    request.onblocked = function (event: Event) {
      LOG.error("Failed to open tapeworm idb %j", event);
      reject(new Error(String(event)));
    };
  });
};

IdbPartition.prototype.truncateStreamFrom = function (
  this: IIdbPartition,
  streamId: string,
  commitSequence: number,
  remove?: boolean | NodeCallback<void>,
  callback?: NodeCallback<void>,
): Promise<unknown> {
  var self = this;
  if (_.isFunction(remove)) {
    callback = remove;
    remove = false;
  }
  return this.queryStream(streamId)
    .then(function (commits: ICommit[]) {
      return new Promise(function (resolve, reject) {
        var txn = self._db.transaction(["commits"], "readwrite");
        var txn2 = self._db.transaction(["truncated"], "readwrite");
        txn.oncomplete = function () {
          resolve(self);
        };
        txn.onerror = function (event: Event) {
          reject(new Error(String(event)));
        };
        var commitStore = txn.objectStore("commits");
        var truncStore = txn2.objectStore("truncated");
        _.forEach(commits, function (commit: ICommit) {
          if (commit.commitSequence >= commitSequence) {
            commitStore.delete(commit.id);
            if (!remove) {
              truncStore.add(commit);
            }
          }
        });
        resolve(self);
      });
    })
    .nodeify(callback);
};

IdbPartition.prototype.applyCommitHeader = function (
  this: IIdbPartition,
  commitId: string,
  header: Record<string, unknown>,
  callback?: NodeCallback<ICommit>,
): Promise<ICommit> {
  var self = this;
  return new Promise<ICommit>(function (resolve, reject) {
    var txn = self._db.transaction(["commits"], "readwrite");
    var commitStore = txn.objectStore("commits");
    var req = commitStore.get(commitId);
    req.onsuccess = function () {
      var c = req.result as ICommit | undefined;
      if (c) {
        _.assign(c, header);
        var save = commitStore.put(c);
        save.onsuccess = function () {
          resolve(c);
        };
        save.onerror = function (error: Event) {
          reject(error);
        };
      } else {
        reject(new Error("Unable to find commit: " + commitId));
      }
    };
    req.onerror = function (error: Event) {
      console.log(commitId);
      reject(error);
    };
  });
};

IdbPartition.prototype.loadSnapshot = function (
  this: IIdbPartition,
  streamId: string,
  callback?: NodeCallback<ISnapshot | null>,
): Promise<ISnapshot | null> {
  var self = this;
  return new Promise<ISnapshot | null>(function (resolve, reject) {
    var txn = self._db.transaction(["snapshots"], "readonly");
    var request = txn.objectStore("snapshots").get(streamId);
    request.onsuccess = function () {
      resolve(
        request.result !== undefined ? (request.result as ISnapshot) : null,
      );
    };
    request.onerror = function () {
      reject(new Error("Unable to load snapshot for id: " + streamId));
    };
  }).nodeify(callback);
};

IdbPartition.prototype._storeSnapshots = function (
  this: IIdbPartition,
  snapshots: ISnapshot[],
  callback?: NodeCallback<ISnapshot[]>,
): Promise<ISnapshot[]> {
  var self = this;
  return new Promise<ISnapshot[]>(function (resolve, reject) {
    var txn = self._db.transaction(["snapshots"], "readwrite");
    var docs = txn.objectStore("snapshots");
    txn.oncomplete = function () {
      process.nextTick(function () {
        resolve(snapshots);
      });
    };
    txn.onerror = function (event: Event) {
      reject(new Error(String(event)));
    };
    var idx = 0,
      total = snapshots.length;
    function addNext() {
      var request = docs.put(snapshots[idx++]);
      request.onsuccess = function () {
        if (idx < total) {
          addNext();
        }
      };
      request.onerror = function (event: Event) {
        reject(new Error(String(event)));
      };
    }
    addNext();
  }).nodeify(callback);
};

IdbPartition.prototype.storeSnapshot = function (
  this: IIdbPartition,
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
      storedDateTime: new Date().toISOString(),
    } as ISnapshot & { _id: string };
    var txn = self._db.transaction(["snapshots"], "readwrite");
    txn.oncomplete = function () {};
    txn.onerror = function () {
      reject(new Error("Failed to save snapshot"));
    };
    var request = txn.objectStore("snapshots").put(toStore);
    request.onsuccess = function () {
      process.nextTick(function () {
        resolve(toStore);
      });
    };
    request.onerror = function () {
      reject(new Error("Failed to store snapshot"));
    };
  }).nodeify(callback);
};

IdbPartition.prototype.removeSnapshots = function (
  this: IIdbPartition,
  ids: string[] | string,
  callback?: NodeCallback<unknown>,
): Promise<unknown> {
  var self = this;
  var idsToDelete: string[] = _.isArray(ids) ? ids : [ids];
  return new Promise(function (resolve, reject) {
    var txn = self._db.transaction(["snapshots"], "readwrite");
    txn.oncomplete = function () {
      resolve(self);
    };
    txn.onerror = function (event: Event) {
      reject(new Error(String(event)));
    };
    var store = txn.objectStore("snapshots");
    _.forEach(idsToDelete, function (id: string) {
      store.delete(id);
    });
  }).nodeify(callback);
};

IdbPartition.prototype.append = function (
  this: IIdbPartition,
  commit: ICommit,
  _callback?: NodeCallback<ICommit>,
): Promise<ICommit> {
  var self = this;
  commit.isDispatched = false;
  commit.streamIdCommitSequence = commit.streamId + commit.commitSequence;
  commit.appendDateTime = new Date().toISOString();
  return new Promise<ICommit>(function (resolve, reject) {
    var txn = self._db.transaction(["commits"], "readwrite");
    var commits = txn.objectStore("commits");
    txn.oncomplete = function () {};
    txn.onerror = function (event: Event) {
      reject(new DuplicateCommitError(String(event)));
    };
    var request = commits.add(commit);
    request.onsuccess = function () {
      process.nextTick(function () {
        resolve(commit);
      });
    };
    request.onerror = function (event: Event) {
      var req = commits.get(commit.id);
      req.onsuccess = function () {
        reject(new DuplicateCommitError(String(event)));
      };
      req.onerror = function () {
        reject(new ConcurrencyError(String(event)));
      };
    };
  });
};

IdbPartition.prototype.markAsDispatched = function (_commit: ICommit): never {
  throw Error("not implemented");
};
IdbPartition.prototype.getUndispatched = function (): never {
  throw Error("not implemented");
};

IdbPartition.prototype.queryAll = function (
  this: IIdbPartition,
  callback?: NodeCallback<ICommit[]>,
): Promise<ICommit[]> {
  var commits: ICommit[] = [];
  var store = this._db
    .transaction(["commits"], "readonly")
    .objectStore("commits");
  return new Promise<ICommit[]>(function (resolve, reject) {
    var cursor = store.openCursor();
    cursor.onerror = function (event: Event) {
      reject(new Error(String(event)));
    };
    cursor.onsuccess = function () {
      var c = cursor.result;
      if (c) {
        commits.push(c.value as ICommit);
        c.continue();
      } else {
        resolve(_.sortBy(commits, "commitSequence"));
      }
    };
  }).nodeify(callback);
};

IdbPartition.prototype.queryStream = function (
  this: IIdbPartition,
  streamId: string,
  fromEventSequence?: number | NodeCallback<ICommit[]>,
  callback?: NodeCallback<ICommit[]>,
): Promise<ICommit[]> {
  var self = this;
  var commits: ICommit[] = [];
  return new Promise<ICommit[]>(function (resolve, reject) {
    var txn = self._db.transaction(["commits"], "readonly");
    var cursor = txn
      .objectStore("commits")
      .index("streamId")
      .openCursor(IDBKeyRange.only(streamId));
    cursor.onerror = function (event: Event) {
      reject(new Error(String(event)));
    };
    cursor.onsuccess = function () {
      var c = cursor.result;
      if (c) {
        commits.push(c.value as ICommit);
        c.continue();
      } else {
        var result = _.sortBy(commits, "commitSequence");
        if ((fromEventSequence as number) > 0) {
          var startCommitId = 0,
            foundEvents = 0;
          for (var i = 0; i < result.length; i++) {
            foundEvents += result[i].events.length;
            startCommitId++;
            if (foundEvents >= (fromEventSequence as number)) {
              break;
            }
          }
          var tooMany = foundEvents - (fromEventSequence as number);
          result = result.slice(startCommitId - (tooMany > 0 ? 1 : 0));
          if (tooMany > 0) {
            result[0].events = result[0].events.slice(
              result[0].events.length - tooMany,
            );
          }
        }
        resolve(result);
      }
    };
  }).nodeify(callback);
};

IdbPartition.prototype.querySnapshotsOlderThanMaxDate = function (
  this: IIdbPartition,
  dateTime: string,
  callback?: NodeCallback<ISnapshot[]>,
): Promise<ISnapshot[]> {
  var maxTimeEpoch = new Date(dateTime).getTime();
  var store = this._db
    .transaction(["snapshots"], "readonly")
    .objectStore("snapshots");
  return new Promise<ISnapshot[]>(function (resolve, reject) {
    var snapshots: ISnapshot[] = [];
    var cursor = store.openCursor();
    cursor.onerror = function (event: Event) {
      reject(new Error(String(event)));
    };
    cursor.onsuccess = function () {
      var snapshotResult = cursor.result;
      if (!snapshotResult) {
        resolve(snapshots);
        return;
      }
      var snapshot = snapshotResult.value as ISnapshot;
      if (snapshot.storedDateTime) {
        var storedDateEpoch = new Date(snapshot.storedDateTime).getTime();
        if (storedDateEpoch < maxTimeEpoch) {
          snapshots.push(snapshot);
        }
      }
      snapshotResult.continue();
    };
  }).nodeify(callback);
};

export default IdbPartition;
