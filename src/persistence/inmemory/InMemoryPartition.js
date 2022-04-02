import Promise from 'bluebird';
import {assign, clone, contains, find, isFunction, without} from 'lodash';
import {ConcurrencyError} from '../ConcurrencyError';
import {DuplicateCommitError} from '../DuplicateCommitError';

class InMemoryPartition {
  constructor() {
    this._commits = [];
    this._streamIndex = {};
    this._commitIds = [];
    this._commitConcurrencyCheck = [];
    this._snapshots = {};
  }

  _promisify(value, callback) {
    return Promise.resolve(value).nodeify(callback);
  }

  truncateStreamFrom(streamId, commitSequence, callback) {
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

  applyCommitHeader(commitId, header, callback) {
    const commit = find(this._commits, {id: commitId});
    if (commit) {
      assign(commit, header);
    } else {
      throw new Error('Trying to apply header to missing commit: ' + commitId);
    }
    return this._promisify(commit, callback);
  }

  append(commit, callback) {
    commit.isDispatched = false;
    // Check for duplicates
    if (contains(this._commitIds, commit.id)) {
      throw new DuplicateCommitError('Duplicate commit of ' + commit.id);
    }
    const concurrencyKey = getConcurrencyKey(commit);
    if (contains(this._commitConcurrencyCheck, concurrencyKey)) {
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

  storeSnapshot(streamId, snapshot, version, callback) {
    return this._promisify((this._snapshots[streamId] = {
      id: streamId,
      version: version,
      snapshot: snapshot
    }), callback);
  }

  // Loads the latest snapshot
  loadSnapshot(streamId, callback) {
    return this._promisify(this._snapshots[streamId], callback);
  }

  markAsDispatched(commit, callback) {
    commit.isDispatched = true;
    return this._promisify(commit, callback);
  }

  getUndispatched(callback) {
    const commits = this.queryAll;
    const undispatched = [];
    for (let i = 0; i < commits.length; i++) {
      if (!commits[i].isDispatched) {
        undispatched.push(commits[i]);
      }
    }
    return this._promisify(undispatched, callback);
  }

  queryAll(callback) {
    return this._promisify(this._commits.slice(), callback);
  }

  getLatestCommit(streamId, callback) {
    let result = this._streamIndex[streamId];
    if (result) {
      result = result.slice().pop();
    }
    return this._promisify(result, callback);
  }

  queryStream(streamId, fromEventSequence, callback) {
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

function getConcurrencyKey(commit) {
  return commit.streamId + '-' + commit.commitSequence;
}

export default InMemoryPartition;
