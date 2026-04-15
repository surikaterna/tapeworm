var _ = require('lodash');
var Promise = require('bluebird');

var RemotePartition = function (client, partitionId) {
  this._client = client;
  this._partitionId = partitionId;
};

RemotePartition.prototype._promisify = function (value, callback) {
  return Promise.resolve(value).nodeify(callback);
};

RemotePartition.prototype.open = function () {
  var self = this;
  return new Promise((resolve) => {
    resolve(self);
  });
};

RemotePartition.prototype.loadSnapshot = function (streamId, includeSubsequentCommits, callback) {
  var self = this;
  if (_.isUndefined(includeSubsequentCommits)) {
    includeSubsequentCommits = false;
  } else if (_.isFunction(includeSubsequentCommits)) {
    callback = includeSubsequentCommits;
    includeSubsequentCommits = false;
  }
  var payload = {
    loadSnapshot: {
      streamId: streamId,
      includeSubsequentCommits: includeSubsequentCommits
    }
  };
  return new Promise(function (resolve, reject) {
    var cb = function (err, res) {
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
  })
};

RemotePartition.prototype.queryStreamWithSnapshot = function (streamId, callback) {
  var self = this;
  var payload = {
    queryStreamWithSnapshot: {
      streamId: streamId,
    }
  };
  return new Promise(function (resolve, reject) {
    var cb = function (err, res) {
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
  })
};

RemotePartition.prototype.queryStream = function (streamId, fromEventSequence, callback) {
  var self = this;
  if (_.isUndefined(fromEventSequence)) {
    fromEventSequence = -1;
  } else if (_.isFunction(fromEventSequence)) {
    callback = fromEventSequence;
    fromEventSequence = -1;
  }
  var payload = {
    queryCommits: {
      streamId: streamId,
      fromSequence: fromEventSequence
    }
  };
  return new Promise(function (resolve, reject) {
    var cb = function (err, res) {
      if (callback) {
        callback(err, _.get(res, 'commits'));
      }
      if (err) {
        reject(err);
      } else {
        resolve(_.get(res, 'commits'));
      }
    };
    self._client.request(payload, cb);
  })
};

RemotePartition.prototype.truncateStreamFrom = function (streamId, commitSequence, remove, callback) {
  // no-op
  return this._promisify(null, callback);
};

RemotePartition.prototype.applyCommitHeader = function (commitId, header, callback) {
  // no-op
  return this._promisify(null, callback);
};

RemotePartition.prototype._storeSnapshots = function (snapshots, callback) {
  // no-op
  return this._promisify(null, callback);
};

RemotePartition.prototype.storeSnapshot = function (streamId, snapshot, version, callback) {
  // no-op
  return this._promisify(null, callback);
};

RemotePartition.prototype.removeSnapshots = function (ids, callback) {
  // no-op
  return this._promisify(null, callback);
};

RemotePartition.prototype.append = function (commit, callback) {
  // no-op
  return this._promisify(null, callback);
};

RemotePartition.prototype.markAsDispatched = function (commit, callback) {
  // no-op
  return this._promisify(null, callback);
};

RemotePartition.prototype.getUndispatched = function (callback) {
  // no-op
  return this._promisify(null, callback);
};

RemotePartition.prototype.queryAll = function (callback) {
  throw new Error('queryAll Not supported for remote partition');
};

module.exports = RemotePartition;
