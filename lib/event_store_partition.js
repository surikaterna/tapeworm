var _ = require("lodash");
var Promise = require("bluebird");

var EventStream = require("./event_stream");

var EventStorePartition = function (
  partitionId,
  persistencePartition,
  dispatchService
) {
  this._partitionId = partitionId;
  this._persistencePartition = persistencePartition;
  this._dispatchService = dispatchService;

  var self = this;
  _.forEach(
    [
      "storeSnapshot",
      "loadSnapshot",
      "queryStream",
      "queryStreamWithSnapshot",
      "removeSnapshot",
      "getLatestCommit",
    ],
    function (what) {
      if (self._persistencePartition[what]) {
        self[what] = function () {
          return self._persistencePartition[what].apply(
            self._persistencePartition,
            arguments
          );
        };
      } else {
        if (what === "queryStreamWithSnapshot") {
          // fallback function
          self[what] = function () {
            return self._queryStreamWithSnapshotFallback.apply(
              self._persistencePartition,
              arguments
            );
          };
        }
      }
    }
  );
};

EventStorePartition.prototype.openStream = function (
  streamId,
  writeOnly,
  callback
) {
  if (typeof writeOnly === "function") {
    callback = writeOnly;
    writeOnly = false;
  }
  var stream = new EventStream(this, streamId, writeOnly);
  return stream._prepareStream(callback);
};

EventStorePartition.prototype.append = function (commits, callback) {
  //pre hooks
  var self = this;

  if (!_.isArray(commits)) {
    commits = [commits];
  }
  return Promise.each(commits, function (commit) {
    return self._persistencePartition
      .append(commit, callback)
      .then(function (r) {
        var done = function () {
          self._persistencePartition.markAsDispatched(commit);
        };
        if (self._dispatchService) {
          self._dispatchService(commit, done);
        }
        return r;
      });
  });
  //post hooks
};
/**
 * Delete function in tapeworm,
 * Delete should trigger a $stream.deleted.event,  including aggregateType of first event and publish it.
 * 2. Implement in projector service to listen to this event and then do a delete of all related projections.,
 * 3. Delete function should store the following new commit (deleted event) under the stream:
 *   streamId,
 *   aggregateType,
 *   date of deletion,
 *   date of creation,
 *   principal
 * 4. Then remove snapshot for this stream and all commits for this stream.
 * @param {*} streamId
 * @param {*} headers
 */
EventStorePartition.prototype.delete = function (streamId, headers) {
  var self = this;
  
  return this._truncateStreamFrom(streamId, -1)
    .then(function (result) {
      return self.openStream(streamId, false);
    })
    .then(function (stream) {
      stream.append({
        type: "$stream.deleted.event",
        payload: Object.assign({}, headers, { dateOfDeletion: new Date() }),
      });
      return stream.commit();
    });
};

/*** UNDOCUMENTED API ***/

EventStorePartition.prototype._queryStream = function (streamId, callback) {
  return this._persistencePartition.queryStream(streamId, callback);
};

EventStorePartition.prototype._queryAll = function (callback) {
  return this._persistencePartition.queryAll(callback);
};

EventStorePartition.prototype._queryStreamWithSnapshotFallback = function (
  streamId,
  callback
) {
  var self = this;
  return new Promise(function (resolve, reject) {
    self.loadSnapshot(streamId, function (err, snapshot) {
      if (err) {
        reject(err);
      } else {
        var snapshotVersion = (snapshot && snapshot.version) || -1;
        self.queryStream(streamId, snapshotVersion, function (err, res) {
          resolve({ snapshot: snapshot, commits: res });
        });
      }
    });
  }).nodeify(callback);
};

/*** NEEDED FOR SYNCING ***/

EventStorePartition.prototype._truncateStreamFrom = function (
  streamId,
  commitSequence,
  callback
) {
  return this._persistencePartition.truncateStreamFrom(
    streamId,
    commitSequence,
    callback
  );
};

EventStorePartition.prototype._applyCommitHeader = function (
  commit,
  header,
  callback
) {
  return this._persistencePartition.applyCommitHeader(commit, header, callback);
};

//EventStorePartition.prototype.loadEvents = function(streamId, callback)

/*EventStorePartition.prototype.append = function(streamId, expectedVersion, events) {
	var commit = new Commit(uuid(), this._partitionId, streamId, expectedVersion, events);
	return this._persistencePartition.append(commit).then(function()
	{
		return this._dispatchService.dispatch(commit);
	}).then(function() {
		return this._persistencePartition.markAsDispatched(commit);
	});
};
*/

module.exports = EventStorePartition;
