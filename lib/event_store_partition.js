var _ = require('lodash');
var Promise = require('bluebird');

var EventStream = require('./event_stream');

var EventStorePartition = function (partitionId, persistencePartition, dispatchService) {
  this._partitionId = partitionId;
  this._persistencePartition = persistencePartition;
  this._dispatchService = dispatchService;
}

EventStorePartition.prototype.openStream = function (streamId, callback) {
  var stream = new EventStream(this, streamId);
  return stream._prepareStream(callback);
};

EventStorePartition.prototype.append = function (commits, callback) {
  //pre hooks
  var self = this;

  if (!_.isArray(commits)) {
    commits = [commits];
  }
  return Promise.each(commits, function (commit) {
    return self._persistencePartition.append(commit, callback).then(function (r) {
      var done = function () {
        self._persistencePartition.markAsDispatched(commit);
      }
      if (self._dispatchService) {
        self._dispatchService(commit, done);
      }
      return r;
    });
  });
  //post hooks
};

/*** UNDOCUMENTED API ***/

EventStorePartition.prototype.storeSnapshot = function (streamId, snapshot, version, callback) {
  return this._persistencePartition.storeSnapshot(streamId, snapshot, version, callback);
};
EventStorePartition.prototype.loadSnapshot = function (streamId, callback) {
  return this._persistencePartition.loadSnapshot(streamId, callback);
};

EventStorePartition.prototype._queryStream = function (streamId, fromEventSequence, callback) {
  return this._persistencePartition.queryStream(streamId, fromEventSequence, callback);
};

EventStorePartition.prototype._queryAll = function (callback) {
  return this._persistencePartition.queryAll(callback);
};

/*** NEEDED FOR SYNCING ***/

EventStorePartition.prototype._truncateStreamFrom = function (streamId, commitSequence, callback) {
  return this._persistencePartition.truncateStreamFrom(streamId, commitSequence, callback);
}

EventStorePartition.prototype._applyCommitHeader = function (commit, header, callback) {
  return this._persistencePartition.applyCommitHeader(commit, header, callback);
}

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