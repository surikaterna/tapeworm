import _ from "lodash";
import Promise from "bluebird";
import type { ICommit, ISnapshot, NodeCallback } from "tapeworm";

type ClientInstance = InstanceType<typeof import("./rrtw_client").default>;

interface IRemotePartition {
  _client: ClientInstance;
  _partitionId: string;
  _promisify<T>(value: T, callback?: NodeCallback<T>): Promise<T>;
  open(): Promise<IRemotePartition>;
  loadSnapshot(
    streamId: string,
    includeSubsequentCommits?: boolean | NodeCallback<unknown>,
    callback?: NodeCallback<unknown>,
  ): Promise<unknown>;
  queryStreamWithSnapshot(
    streamId: string,
    callback?: NodeCallback<unknown>,
  ): Promise<unknown>;
  queryStream(
    streamId: string,
    fromEventSequence?: number | NodeCallback<ICommit[]>,
    callback?: NodeCallback<ICommit[]>,
  ): Promise<ICommit[]>;
  truncateStreamFrom(
    streamId: string,
    commitSequence: number,
    remove?: unknown,
    callback?: NodeCallback<unknown>,
  ): Promise<unknown>;
  applyCommitHeader(
    commitId: string,
    header: Record<string, unknown>,
    callback?: NodeCallback<unknown>,
  ): Promise<unknown>;
  _storeSnapshots(
    snapshots: unknown,
    callback?: NodeCallback<unknown>,
  ): Promise<unknown>;
  storeSnapshot(
    streamId: string,
    snapshot: Record<string, unknown> | undefined,
    version: number,
    callback?: NodeCallback<ISnapshot>,
  ): Promise<unknown>;
  removeSnapshots(
    ids: string[],
    callback?: NodeCallback<unknown>,
  ): Promise<unknown>;
  append(commit: ICommit, callback?: NodeCallback<unknown>): Promise<unknown>;
  markAsDispatched(
    commit: ICommit,
    callback?: NodeCallback<unknown>,
  ): Promise<unknown>;
  getUndispatched(callback?: NodeCallback<unknown>): Promise<unknown>;
  queryAll(callback?: NodeCallback<ICommit[]>): never;
}

var RemotePartition = function (
  this: IRemotePartition,
  client: ClientInstance,
  partitionId: string,
) {
  this._client = client;
  this._partitionId = partitionId;
} as unknown as new (
  client: ClientInstance,
  partitionId: string,
) => IRemotePartition;

RemotePartition.prototype._promisify = function <T>(
  this: IRemotePartition,
  value: T,
  callback?: NodeCallback<T>,
): Promise<T> {
  return Promise.resolve(value).nodeify(callback);
};

RemotePartition.prototype.open = function (
  this: IRemotePartition,
): Promise<IRemotePartition> {
  var self = this;
  return new Promise((resolve: (value: IRemotePartition) => void) => {
    resolve(self);
  });
};

RemotePartition.prototype.loadSnapshot = function (
  this: IRemotePartition,
  streamId: string,
  includeSubsequentCommits?: boolean | NodeCallback<unknown>,
  callback?: NodeCallback<unknown>,
): Promise<unknown> {
  var self = this;
  if (_.isUndefined(includeSubsequentCommits)) {
    includeSubsequentCommits = false;
  } else if (_.isFunction(includeSubsequentCommits)) {
    callback = includeSubsequentCommits as NodeCallback<unknown>;
    includeSubsequentCommits = false;
  }
  var payload = {
    loadSnapshot: {
      streamId: streamId,
      includeSubsequentCommits: includeSubsequentCommits,
    },
  };
  return new Promise(function (
    resolve: (value: unknown) => void,
    reject: (reason: Error) => void,
  ) {
    var cb = function (err: Error | null, res: unknown) {
      if (callback) {
        callback(err, res);
      }
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    };
    self._client.request(payload, cb);
  });
};

RemotePartition.prototype.queryStreamWithSnapshot = function (
  this: IRemotePartition,
  streamId: string,
  callback?: NodeCallback<unknown>,
): Promise<unknown> {
  var self = this;
  var payload = {
    queryStreamWithSnapshot: {
      streamId: streamId,
    },
  };
  return new Promise(function (
    resolve: (value: unknown) => void,
    reject: (reason: Error) => void,
  ) {
    var cb = function (err: Error | null, res: unknown) {
      if (callback) {
        callback(err, res);
      }
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    };
    self._client.request(payload, cb);
  });
};

RemotePartition.prototype.queryStream = function (
  this: IRemotePartition,
  streamId: string,
  fromEventSequence?: number | NodeCallback<ICommit[]>,
  callback?: NodeCallback<ICommit[]>,
): Promise<ICommit[]> {
  var self = this;
  if (_.isUndefined(fromEventSequence)) {
    fromEventSequence = -1;
  } else if (_.isFunction(fromEventSequence)) {
    callback = fromEventSequence as NodeCallback<ICommit[]>;
    fromEventSequence = -1;
  }
  var payload = {
    queryCommits: {
      streamId: streamId,
      fromSequence: fromEventSequence,
    },
  };
  return new Promise(function (
    resolve: (value: ICommit[]) => void,
    reject: (reason: Error) => void,
  ) {
    var cb = function (err: Error | null, res: unknown) {
      if (callback) {
        callback(
          err,
          _.get(res as Record<string, unknown>, "commits") as ICommit[],
        );
      }
      if (err) {
        reject(err);
      } else {
        resolve(_.get(res as Record<string, unknown>, "commits") as ICommit[]);
      }
    };
    self._client.request(payload, cb);
  });
};

RemotePartition.prototype.truncateStreamFrom = function (
  this: IRemotePartition,
  _streamId: string,
  _commitSequence: number,
  _remove?: unknown,
  callback?: NodeCallback<unknown>,
): Promise<unknown> {
  // no-op
  return this._promisify(null, callback as NodeCallback<null>);
};

RemotePartition.prototype.applyCommitHeader = function (
  this: IRemotePartition,
  _commitId: string,
  _header: Record<string, unknown>,
  callback?: NodeCallback<unknown>,
): Promise<unknown> {
  // no-op
  return this._promisify(null, callback as NodeCallback<null>);
};

RemotePartition.prototype._storeSnapshots = function (
  this: IRemotePartition,
  _snapshots: unknown,
  callback?: NodeCallback<unknown>,
): Promise<unknown> {
  // no-op
  return this._promisify(null, callback as NodeCallback<null>);
};

RemotePartition.prototype.storeSnapshot = function (
  this: IRemotePartition,
  _streamId: string,
  _snapshot: Record<string, unknown> | undefined,
  _version: number,
  callback?: NodeCallback<ISnapshot>,
): Promise<unknown> {
  // no-op
  return this._promisify(null, callback as unknown as NodeCallback<null>);
};

RemotePartition.prototype.removeSnapshots = function (
  this: IRemotePartition,
  _ids: string[],
  callback?: NodeCallback<unknown>,
): Promise<unknown> {
  // no-op
  return this._promisify(null, callback as NodeCallback<null>);
};

RemotePartition.prototype.append = function (
  this: IRemotePartition,
  _commit: ICommit,
  callback?: NodeCallback<unknown>,
): Promise<unknown> {
  // no-op
  return this._promisify(null, callback as NodeCallback<null>);
};

RemotePartition.prototype.markAsDispatched = function (
  this: IRemotePartition,
  _commit: ICommit,
  callback?: NodeCallback<unknown>,
): Promise<unknown> {
  // no-op
  return this._promisify(null, callback as NodeCallback<null>);
};

RemotePartition.prototype.getUndispatched = function (
  this: IRemotePartition,
  callback?: NodeCallback<unknown>,
): Promise<unknown> {
  // no-op
  return this._promisify(null, callback as NodeCallback<null>);
};

RemotePartition.prototype.queryAll = function (
  this: IRemotePartition,
  _callback?: NodeCallback<ICommit[]>,
): never {
  throw new Error("queryAll Not supported for remote partition");
};

export default RemotePartition;
