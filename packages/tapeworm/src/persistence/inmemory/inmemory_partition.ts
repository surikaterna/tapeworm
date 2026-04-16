import Promise from "bluebird";
import _ from "lodash";
import type { ICommit, ISnapshot, NodeCallback } from "../../types";
import ConcurrencyError from "../concurrency_error";
import DuplicateCommitError from "../duplicate_commit_error";

interface IInMemoryPartition {
  _commits: ICommit[];
  _streamIndex: Record<string, ICommit[]>;
  _commitIds: string[];
  _commitConcurrencyCheck: string[];
  _snapshots: Record<string, ISnapshot>;
  _promisify<T>(value: T, callback?: NodeCallback<T>): Promise<T>;
  truncateStreamFrom(streamId: string, commitSequence: number, remove?: boolean | NodeCallback<void>, callback?: NodeCallback<void>): Promise<void>;
  applyCommitHeader(commitId: string, header: Record<string, unknown>, callback?: NodeCallback<ICommit>): Promise<ICommit>;
  append(commit: ICommit, callback?: NodeCallback<ICommit>): Promise<ICommit>;
  storeSnapshot(streamId: string, snapshot: Record<string, unknown> | undefined, version: number, callback?: NodeCallback<ISnapshot>): Promise<ISnapshot>;
  loadSnapshot(streamId: string, callback?: NodeCallback<ISnapshot | undefined>): Promise<ISnapshot | undefined>;
  markAsDispatched(commit: ICommit, callback?: NodeCallback<ICommit>): Promise<ICommit>;
  getUndispatched(callback?: NodeCallback<ICommit[]>): Promise<ICommit[]>;
  queryAll(callback?: NodeCallback<ICommit[]>): Promise<ICommit[]>;
  getLatestCommit(streamId: string, callback?: NodeCallback<ICommit | undefined>): Promise<ICommit | undefined>;
  queryStream(streamId: string, fromEventSequence?: number | NodeCallback<ICommit[]>, callback?: NodeCallback<ICommit[]>): Promise<ICommit[]>;
}

var InMemoryPartition = function (this: IInMemoryPartition) {
  this._commits = [];
  this._streamIndex = {};
  this._commitIds = [];
  this._commitConcurrencyCheck = [];
  this._snapshots = {};
} as unknown as new () => IInMemoryPartition;

function getConcurrencyKey(commit: ICommit): string {
  return commit.streamId + "-" + commit.commitSequence;
}

InMemoryPartition.prototype._promisify = function <T>(this: IInMemoryPartition, value: T, callback?: NodeCallback<T>): Promise<T> {
  return Promise.resolve(value).nodeify(callback);
};

InMemoryPartition.prototype.truncateStreamFrom = function (
  this: IInMemoryPartition,
  streamId: string,
  commitSequence: number,
  remove?: boolean | NodeCallback<void>,
  callback?: NodeCallback<void>
) {
  if (typeof remove === "function") {
    callback = remove;
  }
  var self = this;
  var commits = Array.from(this._streamIndex[streamId]);
  for (var i = 0; i < commits.length; i++) {
    var commit = commits[i];

    if (commit.commitSequence >= commitSequence) {
      //remove from commitId
      self._commitIds = _.without(self._commitIds, commit.id);
      self._commitConcurrencyCheck = _.without(self._commitConcurrencyCheck, getConcurrencyKey(commit));
      self._commits = _.without(self._commits, commit);
    }
  }
  commits = commits.slice(0, Math.max(commitSequence, 0));
  this._streamIndex[streamId] = commits;
  return this._promisify(undefined as unknown as undefined, callback);
};

InMemoryPartition.prototype.applyCommitHeader = function (
  this: IInMemoryPartition,
  commitId: string,
  header: Record<string, unknown>,
  callback?: NodeCallback<ICommit>
) {
  var commit = _.find(this._commits, { id: commitId });
  if (commit) {
    _.assign(commit, header);
  } else {
    throw new Error("Trying to apply header to missing commit: " + commitId);
  }
  return this._promisify(commit, callback);
};

InMemoryPartition.prototype.append = function (this: IInMemoryPartition, commit: ICommit, callback?: NodeCallback<ICommit>) {
  commit.isDispatched = false;
  //check for duplicates
  if (_.includes(this._commitIds, commit.id)) {
    throw new DuplicateCommitError("Duplicate commit of " + commit.id);
  }
  var concurrencyKey = getConcurrencyKey(commit);
  if (_.includes(this._commitConcurrencyCheck, concurrencyKey)) {
    throw new ConcurrencyError("Concurrency error on stream " + commit.streamId);
  }
  //check concurrency
  this._commits.push(commit);
  this._commitIds.push(commit.id);
  this._commitConcurrencyCheck.push(concurrencyKey);
  var index = this._streamIndex[commit.streamId];
  if (!index) {
    index = this._streamIndex[commit.streamId] = [];
  }
  index.push(commit);
  return this._promisify(commit, callback);
};

InMemoryPartition.prototype.storeSnapshot = function (
  this: IInMemoryPartition,
  streamId: string,
  snapshot: Record<string, unknown> | undefined,
  version: number,
  callback?: NodeCallback<ISnapshot>
) {
  return this._promisify(
    (this._snapshots[streamId] = {
      id: streamId,
      version: version,
      snapshot: snapshot,
    }),
    callback
  );
};

// Loads the latest snapshot
InMemoryPartition.prototype.loadSnapshot = function (this: IInMemoryPartition, streamId: string, callback?: NodeCallback<ISnapshot | undefined>) {
  return this._promisify(this._snapshots[streamId], callback);
};

InMemoryPartition.prototype.markAsDispatched = function (this: IInMemoryPartition, commit: ICommit, callback?: NodeCallback<ICommit>) {
  commit.isDispatched = true;
  return this._promisify(commit, callback);
};

InMemoryPartition.prototype.getUndispatched = function (this: IInMemoryPartition, callback?: NodeCallback<ICommit[]>) {
  var commits = this._commits;
  var undispatched: ICommit[] = [];
  for (var i = 0; i < commits.length; i++) {
    if (!commits[i].isDispatched) {
      undispatched.push(commits[i]);
    }
  }
  return this._promisify(undispatched, callback);
};

InMemoryPartition.prototype.queryAll = function (this: IInMemoryPartition, callback?: NodeCallback<ICommit[]>) {
  return this._promisify(this._commits.slice(), callback);
};

InMemoryPartition.prototype.getLatestCommit = function (this: IInMemoryPartition, streamId: string, callback?: NodeCallback<ICommit | undefined>) {
  var result = this._streamIndex[streamId];
  if (result) {
    return this._promisify(result.slice().pop(), callback);
  }
  return this._promisify(undefined as ICommit | undefined, callback);
};

InMemoryPartition.prototype.queryStream = function (
  this: IInMemoryPartition,
  streamId: string,
  fromEventSequence?: number | NodeCallback<ICommit[]>,
  callback?: NodeCallback<ICommit[]>
) {
  if (_.isFunction(fromEventSequence)) {
    callback = fromEventSequence as NodeCallback<ICommit[]>;
    fromEventSequence = 0;
  }
  var result = this._streamIndex[streamId];

  if (result) {
    var sliced = result.slice();
    if ((fromEventSequence as number) > 0) {
      var startCommitId = 0;
      var foundEvents = 0;
      for (var i = 0; i < sliced.length; i++) {
        foundEvents += sliced[0].events.length;
        startCommitId++;
        if (foundEvents >= (fromEventSequence as number)) {
          break;
        }
      }
      var tooMany = foundEvents - (fromEventSequence as number);

      sliced = sliced.slice(startCommitId - (tooMany > 0 ? 1 : 0));
      if (tooMany > 0) {
        sliced[0] = _.clone(sliced[0]); // avoid modifying reference
        sliced[0].events = sliced[0].events.slice(sliced[0].events.length - tooMany);
      }
    }
    return this._promisify(sliced, callback);
  }

  return this._promisify(result as ICommit[], callback);
};

export default InMemoryPartition;
