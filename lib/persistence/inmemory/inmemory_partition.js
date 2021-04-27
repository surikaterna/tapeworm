var _ = require('lodash');
var Promise = require('bluebird');
var ConcurrencyError = require('../concurrency_error');
var DuplicateCommitError = require('../duplicate_commit_error');

var InMemoryPartition = function () {
  this._commits = [];
  this._streamIndex = {};
  this._commitIds = [];
  this._commitConcurrencyCheck = [];
  this._snapshots = {};
}


function getConcurrencyKey(commit) {
  return commit.streamId + '-' + commit.commitSequence;
}
InMemoryPartition.prototype._promisify = function (value, callback) {
  return Promise.resolve(value).nodeify(callback);
};

InMemoryPartition.prototype.truncateStreamFrom = function (streamId, commitSequence, callback) {
  var self = this;
  var commits = Array.from(this._streamIndex[streamId]);
  for (var i = 0; i < commits.length; i++) {
    var commit = commits[i];

        if (commit.commitSequence >= commitSequence) {
      //remove from commitId
      self._commitIds = _.without(self._commitIds, commit.id);
      self._commitConcurrencyCheck = _.without(self._commitConcurrencyCheck, getConcurrencyKey(commit));
      self._commits = _.without(self._commits, commit);
      // i--;
    }
  }
  commits = commits.slice(0, Math.max(commitSequence,0));
  this._streamIndex[streamId] = commits;
  return this._promisify(this);
}

// InMemoryPartition.prototype.delete = function (streamId, headers, callback) {

// }

InMemoryPartition.prototype.applyCommitHeader = function (commitId, header, callback) {
  var self = this;
  var commit = _.find(this._commits, { id: commitId });
  if (commit) {
    _.assign(commit, header);
  } else {
    throw new Error("Trying to apply header to missing commit: " + commitId);
  }
  return this._promisify(commit, callback);
}


InMemoryPartition.prototype.append = function (commit, callback) {
  commit.isDispatched = false;
  //check for duplicates
  if (_.contains(this._commitIds, commit.id)) {
    throw new DuplicateCommitError("Duplicate commit of " + commit.id);
  }
  var concurrencyKey = getConcurrencyKey(commit);
  if (_.contains(this._commitConcurrencyCheck, concurrencyKey)) {
    throw new ConcurrencyError('Concurrency error on stream ' + commit.streamId);
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

InMemoryPartition.prototype.storeSnapshot = function (streamId, snapshot, version, callback) {
  return this._promisify(this._snapshots[streamId] = { id: streamId, version: version, snapshot: snapshot }, callback);
};

// Loads the latest snapshot
InMemoryPartition.prototype.loadSnapshot = function (streamId, callback) {
  return this._promisify(this._snapshots[streamId], callback);
};

InMemoryPartition.prototype.markAsDispatched = function (commit, callback) {
  commit.isDispatched = true;
  return this._promisify(commit, callback);
};

InMemoryPartition.prototype.getUndispatched = function (callback) {
  var commits = this.queryAll;
  var undispatched = [];
  for (var i = 0; i < commits.length; i++) {
    if (!commits[i].isDispatched) {
      undispatched.push(commit);
    }
  }
  return this._promisify(undispatched, callback);
};

InMemoryPartition.prototype.queryAll = function (callback) {
  return this._promisify(this._commits.slice(), callback);
};

InMemoryPartition.prototype.getLatestCommit = function (streamId, callback) {
  var result = this._streamIndex[streamId];
  if (result) {
    result = result.slice().pop();
  }
  return this._promisify(result, callback);
};

InMemoryPartition.prototype.queryStream = function (streamId, fromEventSequence, callback) {
  if (_.isFunction(fromEventSequence)) {
    callback = fromEventSequence;
    fromEventSequence = 0;
  }
  var result = this._streamIndex[streamId];

  if (result) {
    result = result.slice();
    if (fromEventSequence > 0) {
      var startCommitId = 0;
      var foundEvents = 0;
      for (var i = 0; i < result.length; i++) {
        foundEvents += result[0].events.length;
        startCommitId++;
        if (foundEvents >= fromEventSequence) {
          break;
        }
      }
      var tooMany = foundEvents - fromEventSequence;

      result = result.slice(startCommitId - (tooMany > 0 ? 1 : 0));
      if (tooMany > 0) {
        result[0] = _.clone(result[0]); // avoid modifying reference
        result[0].events = result[0].events.slice(result[0].events.length - tooMany);
      }
    }
  }

  return this._promisify(result, callback);
};

module.exports = InMemoryPartition;
