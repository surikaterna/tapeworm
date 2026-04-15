var _ = require('lodash');
var Promise = require('bluebird');
var ConcurrencyError = require('tapeworm').ConcurrencyError;
var DuplicateCommitError = require('tapeworm').DuplicateCommitError;
var slf = require('slf');
var LOG = slf.LoggerFactory.getLogger('tapeworm-persistence-store-indexdb:idb-partition');

const VERSION = 4;

var IdbPartition = function (idb, partitionId, dbname) {
  this._idb = idb;
  this._partitionId = partitionId;
  this._dbname = dbname;
}

IdbPartition.prototype._promisify = function (value, callback) {
  return Promise.resolve(value).nodeify(callback);
};

IdbPartition.prototype.open = function () {
  var self = this;
  var request = this._idb.open('tw_' + this._dbname + '_' + this._partitionId, VERSION);
  return new Promise(function (resolve, reject) {
    request.onsuccess = function (event) {
      const db = event.target.result;
      self._db = db;

      // Check and log if missing
      const objectStores = db.objectStoreNames;
      if (!objectStores.contains('commits')) {
        LOG.error('Object store %s is missing in tapeworm idb_partition. Version %s', 'commits', db.version);
      }
      if (!objectStores.contains('truncated')) {
        LOG.error('Object store %s is missing in tapeworm idb_partition. Version %s', 'truncated', db.version);
      }
      if (!objectStores.contains('snapshots')) {
        LOG.error('Object store %s is missing in tapeworm idb_partition. Version %s', 'snapshots', db.version);
      }

      resolve(self);
    }
    request.onupgradeneeded = function (event) {
      const db = event.target.result;
      const objectStores = db.objectStoreNames;
      
      if (!objectStores.contains('commits')) {
        try {
          var persistedCommits = db.createObjectStore('commits', { keyPath: 'id', unique: true });
          persistedCommits.createIndex('streamId', 'streamId', { unique: false });
          persistedCommits.createIndex('streamIdCommitSequence', 'streamIdCommitSequence', { unique: true });
        } catch (e) {
          LOG.error('Failed to create objectStore %s. %j', 'commits', e);
        }
      }

      if (!objectStores.contains('truncated')) {
        try {
          var truncatedCommits = db.createObjectStore('truncated', { keyPath: 'id', unique: true });
          truncatedCommits.createIndex('streamId', 'streamId', { unique: false });
        } catch (e) {
          LOG.error('Failed to create objectStore %s. %j', 'truncated', e);
        }
      }

      if (!objectStores.contains('snapshots')) {
        try {
          db.createObjectStore('snapshots', { keyPath: 'id', unique: true });
        } catch (e) {
          LOG.error('Failed to create objectStore %s. %j', 'snapshots', e);
        }
      }
    }
    request.onerror = function (event) {
      LOG.error('Failed to open tapeworm idb %j', event);
      reject(new Error(event));
    }
    request.onblocked = function (event) {
      LOG.error('Failed to open tapeworm idb %j', event);
      reject(new Error(event));
    }
  });
}

IdbPartition.prototype.truncateStreamFrom = function (streamId, commitSequence, remove, callback) {
  var self = this;
  if (_.isFunction(remove)) {
    callback = remove;
    remove = false;
  }
  return this.queryStream(streamId).then(function (commits) {
    return new Promise(function (resolve, reject) {
      var txn = self._db.transaction(["commits"], "readwrite");
      var txn2 = self._db.transaction(["truncated"], "readwrite");
      txn.oncomplete = txn.onsuccess = function () {
        resolve(self);
      }

      txn.onerror = function (event) {
        reject(new Error(event));
      }

      var commitStore = txn.objectStore("commits");
      var truncStore = txn2.objectStore("truncated");
      _.forEach(commits, function (commit) {
        if (commit.commitSequence >= commitSequence) {
          commitStore.delete(commit.id);
          if (!remove) {
            truncStore.add(commit);
          }
        }
      });
      resolve(self);
    });
  }).nodeify(callback);
};

IdbPartition.prototype.applyCommitHeader = function (commitId, header, callback) {
  var self = this;
  return new Promise(function (resolve, reject) {
    var txn = self._db.transaction(["commits"], "readwrite");
    var commitStore = txn.objectStore("commits");
    var req = commitStore.get(commitId);
    req.onsuccess = function (event) {
      var c = event.target.result;
      if (c) {
        _.assign(c, header);
        var save = commitStore.put(c);
        save.onsuccess = function () {
          resolve(c);
        }
        save.onerror = function (error) {
          reject(error);
        }
      } else {
        reject(new Error("Unable to find commit: " + commitId));
      }
    };
    req.onerror = function (error) {
      console.log(commitId);
      reject(error);
    }
  });
}

IdbPartition.prototype.loadSnapshot = function (streamId, callback) {
  var self = this;
  return new Promise(function (resolve, reject) {
    var txn = self._db.transaction(['snapshots'], 'readonly');
    var snapshotStore = txn.objectStore('snapshots');
    var request = snapshotStore.get(streamId);

    request.onsuccess = function (event) {
      if (request.result !== undefined) {
        resolve(request.result);
      } else {
        resolve(null)
      }
    }
    request.onerror = function (event) {
      reject(new Error('Unable to load snapshot for id: ' + key));
    }
  }).nodeify(callback)
}

// bulk insert snapshots
IdbPartition.prototype._storeSnapshots = function (snapshots, callback) {
  var self = this;

  return new Promise(function (resolve, reject) {
    var txn = self._db.transaction(['snapshots'], 'readwrite');
    var docs = txn.objectStore('snapshots');

    txn.oncomplete = txn.onsuccess = function () {
      process.nextTick(function () {
        resolve(snapshots);
      });
    }

    txn.onerror = function (event) {
      reject(new Error(event));
    }
    var currentIndex = 0;
    var numberOfDocs = snapshots.length;

    function addNext() {
      var document = snapshots[currentIndex++];
      var request = docs.put(document);
      request.onsuccess = function () {
        if (currentIndex < numberOfDocs) {
          addNext();
        }
      };
      request.onerror = function (event) {
        reject(new Error(event));
      };
    }

    addNext();
  }).nodeify(callback);
};

IdbPartition.prototype.storeSnapshot = function (streamId, snapshot, version, callback) {
  var self = this;
  return new Promise(function (resolve, reject) {
    var toStore = { _id: streamId, id: streamId, version: version, snapshot: snapshot, storedDateTime: new Date().toISOString() };

    var txn = self._db.transaction(['snapshots'], 'readwrite');
    var snapshots = txn.objectStore('snapshots');

    txn.oncomplete = txn.onsuccess = function () {
    }

    txn.onerror = function (event) {
      reject(new Error('Failed to save snapshot'));
    };

    var request = snapshots.put(toStore);
    request.onsuccess = function () {
      process.nextTick(function () {
        resolve(toStore)
      });
    };
    request.onerror = function (event) {
      reject(new Error('Failed to store snapshot'));
    };
  }).nodeify(callback)
};

IdbPartition.prototype.removeSnapshots = function (ids, callback) {
  var self = this;
  var idsToDelete = [];
  if (_.isArray(ids)) {
    idsToDelete = ids;
  } else {
    idsToDelete.push(ids);
  }
  return new Promise(function (resolve, reject) {
    var txn = self._db.transaction(["snapshots"], "readwrite");
    txn.oncomplete = txn.onsuccess = function () {
      resolve(self);
    }
    txn.onerror = function (event) {
      reject(new Error(event));
    }
    var snapshotStore = txn.objectStore("snapshots");
    _.forEach(idsToDelete, function (id) {
      snapshotStore.delete(id);
    });
  }).nodeify(callback);
};

IdbPartition.prototype.append = function (commit, callback) {
  var self = this;
  commit.isDispatched = false;
  commit.streamIdCommitSequence = commit.streamId + commit.commitSequence;
  commit.appendDateTime = new Date().toISOString();

  //console.log(commit.streamIdCommitSequence);
  //check for duplicates
  /*	if(_.contains(this._commitIds, commit.id)) {
      throw new DuplicateCommitError("Duplicate commit of " + commit.id);
    }
    var concurrencyKey = commit.streamId+'-'+commit.commitSequence;
    if(_.contains(this._commitConcurrencyCheck, concurrencyKey)) {
      throw new ConcurrencyError('Concurrency error on stream ' + commit.streamId);
    }
  */
  //check concurrency
  return new Promise(function (resolve, reject) {

    var txn = self._db.transaction(['commits'], 'readwrite');
    var commits = txn.objectStore('commits');

    txn.oncomplete = txn.onsuccess = function () {
    }

    txn.onerror = function (event) {
      //console.log(event);
      reject(new DuplicateCommitError(event));
    }

    var request = commits.add(commit);
    request.onsuccess = function () {
      process.nextTick(function () {
        resolve(commit)
      });
    };
    request.onerror = function (event) {
      var req = commits.get(commit.id)
      req.onsuccess = function () {
        reject(new DuplicateCommitError(event));
      }
      req.onerror = function () {
        reject(new ConcurrencyError(event));
      }

    };
  });

  /*	this._commits.push(commit);
    this._commitIds.push(commit.id);
    this._commitConcurrencyCheck.push(concurrencyKey);
    var index = this._streamIndex[commit.streamId];
    if(!index) {
      index = this._streamIndex[commit.streamId] = [];
    }
    index.push(commit);
  */
  //return this._promisify(commit, callback);
};

IdbPartition.prototype.markAsDispatched = function (commit, callback) {
  throw Error('not implemented');
};

IdbPartition.prototype.getUndispatched = function (callback) {
  throw Error('not implemented');
};

IdbPartition.prototype.queryAll = function (callback) {
  //return this._promisify(this._commits.slice(), callback);
  var self = this;
  var commits = [];
  var commitsStore = this._db.transaction(['commits'], 'readonly').objectStore('commits');
  return new Promise(function (resolve, reject) {
    var cursor = commitsStore.openCursor();
    cursor.onerror = function (event) {
      reject(new Error(event));
    }
    cursor.onsuccess = function (event) {
      var c = event.target.result;
      if (c) {
        commits.push(c.value);
        c.continue();
      } else {
        //get indexeddb to sort
        resolve(_.sortBy(commits, 'commitSequence'));
      }
    }
  }).nodeify(callback);
};

IdbPartition.prototype.queryStream = function (streamId, fromEventSequence, callback) {
  var self = this;
  var commits = [];
  return new Promise(function (resolve, reject) {
    var txn = self._db.transaction(['commits'], 'readonly');
    var commitsStore = txn.objectStore('commits');
    var cursor = commitsStore.index('streamId').openCursor(IDBKeyRange.only(streamId));
    cursor.onerror = function (event) {
      reject(new Error(event));
    }
    cursor.onsuccess = function (event) {
      var c = event.target.result;
      if (c) {
        commits.push(c.value);
        c.continue();
      } else {
        //get indexeddb to sort?
        var result = _.sortBy(commits, 'commitSequence');
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
      }
    }
  }).nodeify(callback);
};

IdbPartition.prototype.querySnapshotsOlderThanMaxDate = function (dateTime, callback) {
  var maxTimeEpoch = new Date(dateTime).getTime();
  var snapshotStore = this._db
    .transaction(["snapshots"], "readonly")
    .objectStore("snapshots");
  return new Promise(function (resolve, reject) {
    var snapshots = [];
    var cursor = snapshotStore.openCursor();
    cursor.onerror = function (event) {
      reject(new Error(event));
    };
    cursor.onsuccess = function (event) {
      var snapshotResult = event.target.result;

      if (!snapshotResult) {
        resolve(snapshots);
        return;
      }

      var snapshot = snapshotResult.value;

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

module.exports = IdbPartition;
