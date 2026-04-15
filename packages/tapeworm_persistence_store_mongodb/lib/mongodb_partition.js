var Promise = require('bluebird');
var ConcurrencyError = require('tapeworm').ConcurrencyError;
var DuplicateCommitError = require('tapeworm').DuplicateCommitError;
var { isNil, get, isFunction } = require('lodash');

var CONCURRENCY_EXCEPTION_CODE = 11000;

var MdbPartition = function (mongodb, partitionId) {
  this._mongodb = mongodb;
  this._partitionId = partitionId;
  this._collection = null;
}

MdbPartition.prototype.open = function () {
  const self = this;
  this._commits = this._mongodb.collection('tw_' + this._partitionId + "_commits");
  this._snapshots = this._mongodb.collection('tw_' + this._partitionId + "_snapshots");
  return new Promise(function (resolve, reject) {
    self._snapshots.createIndex({ id: 1 }, { unique: true })
      .then(() => self._commits.createIndex({ id: 1 }, { unique: true }))
      .then(() => self._commits.createIndex({ streamId: 1, commitSequence: 1 }, { unique: true }))
      .then(() => self._commits.createIndex({ createDateTime: 1 }, { unique: false }))
      .then(() => resolve(self))
      .catch(reject);
  });
};

MdbPartition.prototype.storeSnapshot = function (streamId, snapshot, version, callback) {
  var self = this;
  return new Promise(function (resolve, reject) {
    var toStore = { _id: streamId, id: streamId, version: version, snapshot: snapshot };
    self._snapshots.updateOne({ _id: streamId }, { $set: toStore }, { upsert: true })
      .then(function () {
        resolve(toStore);
      })
      .catch(function (err) {
        reject(err);
      })
  }).nodeify(callback);
};

MdbPartition.prototype.loadSnapshot = function (streamId, callback) {
  var self = this;
  return new Promise(function (resolve, reject) {
    self._snapshots.find({ id: streamId }).toArray()
      .then(function (res) {
        resolve(res[0]);
      })
      .catch(function (err) {
        reject(err);
      })
  }).nodeify(callback);
}

MdbPartition.prototype.removeSnapshot = function (streamId, callback) {
  var self = this;
  return new Promise(function (resolve, reject) {
    var toStore = { _id: streamId, id: streamId, version: -1 };
    var filter = { _id: streamId };
    self._snapshots.replaceOne(filter, toStore)
      .then(function () {
        resolve(toStore);
      })
      .catch(function (err) {
        reject(err);
      })
  }).nodeify(callback);
};

MdbPartition.prototype.append = function (commit, callback) {
  var self = this;
  commit.isDispatched = false;
  commit.createDateTime = new Date();
  return new Promise(function (resolve, reject) {
    self._commits.insertOne(commit)
      .then(function () {
        resolve(commit);
      })
      .catch(function (err) {
        if (err.code === CONCURRENCY_EXCEPTION_CODE) {
          //console.log(JSON.stringify(err));
          if (err.message.indexOf('commitSequence') > 0) {
            reject(new ConcurrencyError('Concurrency error on stream ' + commit.streamId));
          } else {
            reject(new DuplicateCommitError('Duplicate commit on stream ' + commit.streamId));
          }
        } else {
          reject(err);
        }
      })
  }).nodeify(callback);
}

MdbPartition.prototype.markAsDispatched = function (commit, callback) {
  throw Error('not implemented');
};

MdbPartition.prototype.getUndispatched = function (callback) {
  throw Error('not implemented');
};

MdbPartition.prototype.queryAll = function (callback) {
  //return this._promisify(this._commits.slice(), callback);
  var self = this;
  return new Promise(function (resolve, reject) {
    //TODO: sort in insert order...
    self._commits.find({}).sort({ createDateTime: 1 }).toArray()
      .then(function (commits) {
        resolve(commits);
      })
      .catch(function (err) {
        reject(err);
      })
  }).nodeify(callback);
};

MdbPartition.prototype.getLatestCommit = function (streamId, callback) {
  if (!streamId) {
    throw new Error('missing streamId');
  }
  var self = this;
  return new Promise(function (resolve, reject) {
    self._commits.find({ streamId: streamId }).limit(1).sort({ commitSequence: -1 }).toArray()
      .then(function (commits) {
        // assume this will probably not happen
        if (commits.length === 0) {
          resolve(undefined);
          return;
        }
        var commit = commits[0];
        resolve(commit);
      })
      .catch(function (err) {
        reject(err);
      })
  }).nodeify(callback);
};

MdbPartition.prototype.queryStream = function (streamId, fromEventSequence, callback) {
  var self = this;
  if (typeof fromEventSequence === 'function') {
    callback = fromEventSequence;
    fromEventSequence = 0;
  }
  // if from event sequence is 0 / undefined - use fallback..
  if (isNil(fromEventSequence) || fromEventSequence === 0) {
    return self.queryStreamFallback(streamId, fromEventSequence, callback);
  }
  return new Promise(function (resolve, reject) {
    // use limit 1 to get last commit - if we are unable to find the correct event, use fallback...
    self._commits.find({ streamId: streamId }).limit(1).sort({ commitSequence: -1 }).toArray()
      .then(function (commits) {
        // assume this will probably not happen
        if (commits.length === 0) {
          resolve([]);
          return;
        }
        var events = get(commits, '[0].events', []);
        // check if our requested event version is out of reach for the last commit - need to use fallback then..
        if (events[0].version > fromEventSequence) {
          // problem... use fallback.
          return self.queryStreamFallback(streamId, fromEventSequence).then(function (res) {
            resolve(res);
          })
        }
        // assume this will happen most of the time.
        if (events[events.length - 1].version < fromEventSequence) {
          return resolve([]);
        }

        // find every event that has version equal or larger than fromEventSequence
        var foundEvents = [];
        for (var i = 0; i < events.length; i++) {
          if (events[i].version >= fromEventSequence) {
            foundEvents.push(events[i]);
          }
        }

        if (foundEvents.length === 0) {
          resolve([]);
          return;
        }

        var commit = commits[0];
        commit.events = foundEvents;
        resolve([commit]);
      })
      .catch(function (err) {
        reject(err);
      })
  }).nodeify(callback);
}

MdbPartition.prototype.queryStreamFallback = function (streamId, fromEventSequence, callback) {
  if (typeof fromEventSequence === 'function') {
    callback = fromEventSequence;
    fromEventSequence = 0;
  }
  var self = this;
  return new Promise(function (resolve, reject) {
    //TODO: sort in insert order...
    self._commits.find({ streamId: streamId }).sort({ commitSequence: 1 }).toArray()
      .then(function (commits) {
        var result = commits
        if (fromEventSequence > 0) {
          var startCommitId = 0;
          var foundEvents = 0;
          for (var i = 0; i < result.length; i++) {
            foundEvents += result[i].events.length;
            startCommitId++;
            if (foundEvents >= fromEventSequence) {
              break;
            }
          }
          var tooMany = foundEvents - fromEventSequence;

          result = result.slice(startCommitId - (tooMany > 0 ? 1 : 0));
          if (tooMany > 0) {
            result[0].events = result[0].events.slice(result[0].events.length - tooMany);
          }
        }
        resolve(result);
      })
      .catch(function (err) {
        reject(err);
      })
  }).nodeify(callback);
};

MdbPartition.prototype.truncateStreamFrom = function (streamId, commitSequence, remove, callback) {
  var self = this;
  if (isFunction(remove)) {
    callback = remove;
    remove = false;
  }
  return new Promise(function (resolve, reject) {
    self._commits.deleteMany({ streamId, commitSequence: { $gte: commitSequence } })
      .then(function () {
        resolve();
      })
      .catch(function (err) {
        reject(err);
      })
  }).nodeify(callback);
};

module.exports = MdbPartition;
