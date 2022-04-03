import Promise from 'bluebird';
import { assign, clone, find, includes, isFunction, without } from 'lodash';
import { Commit, ConcurrencyError, DuplicateCommitError, FromEventSequence, Partition, Snapshot } from '..';
import { Callback } from '../../types';

export class InMemoryPartition<T extends object> implements Partition<T> {
  _commits: Array<Commit>;
  _streamIndex: Record<string, Array<Commit>>;
  _commitIds: Array<string>;
  _commitConcurrencyCheck: Array<string>;
  _snapshots: Record<string, Snapshot<T>>;

  constructor() {
    this._commits = [];
    this._streamIndex = {};
    this._commitIds = [];
    this._commitConcurrencyCheck = [];
    this._snapshots = {};
  }

  _promisify<Value>(value: Value, callback?: Callback<Value>) {
    return Promise.resolve(value).nodeify(callback);
  }

  truncateStreamFrom(streamId: string, commitSequence: number, callback?: Callback<any>) {
    let commits = Array.from(this._streamIndex[streamId]);
    for (let i = 0; i < commits.length; i++) {
      const commit = commits[i];

      if (commit.commitSequence >= commitSequence) {
        //remove from commitId
        this._commitIds = without(this._commitIds, commit.id);
        this._commitConcurrencyCheck = without(this._commitConcurrencyCheck, getConcurrencyKey(commit));
        this._commits = without(this._commits, commit);
      }
    }
    commits = commits.slice(0, Math.max(commitSequence, 0));
    this._streamIndex[streamId] = commits;
    return this._promisify(this);
  }

  applyCommitHeader<Header extends object>(commitId: string, header: Header, callback?: Callback<Commit>) {
    const commit = find(this._commits, { id: commitId });
    if (commit) {
      assign(commit, header);
    } else {
      throw new Error('Trying to apply header to missing commit: ' + commitId);
    }
    return this._promisify(commit, callback);
  }

  append(commit: Commit, callback?: Callback<Commit>) {
    commit.isDispatched = false;
    // Check for duplicates
    if (includes(this._commitIds, commit.id)) {
      throw new DuplicateCommitError('Duplicate commit of ' + commit.id);
    }
    const concurrencyKey = getConcurrencyKey(commit);
    if (includes(this._commitConcurrencyCheck, concurrencyKey)) {
      throw new ConcurrencyError('Concurrency error on stream ' + commit.streamId);
    }
    // Check concurrency
    this._commits.push(commit);
    this._commitIds.push(commit.id);
    this._commitConcurrencyCheck.push(concurrencyKey);
    let index = this._streamIndex[commit.streamId];
    if (!index) {
      index = this._streamIndex[commit.streamId] = [];
    }
    index.push(commit);
    return this._promisify(commit, callback);
  }

  storeSnapshot(streamId: string, snapshot: T, version: number, callback?: Callback<Snapshot<T>>) {
    return this._promisify(
      (this._snapshots[streamId] = {
        id: streamId,
        version,
        snapshot
      }),
      callback
    );
  }

  // Loads the latest snapshot
  loadSnapshot(streamId: string, callback?: Callback<Snapshot<T>>) {
    return this._promisify(this._snapshots[streamId], callback);
  }

  markAsDispatched(commit: Commit, callback?: Callback<Commit>) {
    commit.isDispatched = true;
    return this._promisify(commit, callback);
  }

  queryAll(callback?: Callback<Array<Commit>>) {
    return this._promisify(this._commits.slice(), callback);
  }

  getLatestCommit(streamId: string, callback?: Callback<Commit | undefined>) {
    const commits = this._streamIndex[streamId];
    const commit = commits?.slice().pop();
    return this._promisify(commit, callback);
  }

  queryStream(streamId: string, fromEventSequence: FromEventSequence<Array<Commit>>, callback?: Callback<Array<Commit>>) {
    if (isFunction(fromEventSequence)) {
      callback = fromEventSequence;
      fromEventSequence = 0;
    }
    let result = this._streamIndex[streamId];

    if (result) {
      result = result.slice();
      if (fromEventSequence > 0) {
        let startCommitId = 0;
        let foundEvents = 0;
        for (let i = 0; i < result.length; i++) {
          foundEvents += result[0].events.length;
          startCommitId++;
          if (foundEvents >= fromEventSequence) {
            break;
          }
        }
        const tooMany = foundEvents - fromEventSequence;

        result = result.slice(startCommitId - (tooMany > 0 ? 1 : 0));
        if (tooMany > 0) {
          result[0] = clone(result[0]); // avoid modifying reference
          result[0].events = result[0].events.slice(result[0].events.length - tooMany);
        }
      }
    }

    return this._promisify(result, callback);
  }
}

function getConcurrencyKey(commit: Commit) {
  return commit.streamId + '-' + commit.commitSequence;
}
