var Promise = require('bluebird');
var Commit = require('./persistence/commit');
var _ = require('lodash');

// writeOnly - will never read entire stream from
var EventStream = function (eventPartition, streamId, writeOnly) {
  if (streamId === undefined) {
    throw new Error('StreamId must be defined!');
  }
  this._partition = eventPartition;
  this._streamId = streamId;
  this._writeOnly = writeOnly;
  this._uncommittedEvents = [];
  this._committedEvents = [];
  this._version = 0;
};

EventStream.prototype._prepareStream = function (callback) {
  this._committedEvents = [];
  this._commitSequence = -1;
  var self = this;
  if (self._writeOnly === true && self._partition.getLatestCommit) {
    // if stream should only be opened for appending commits, only really care about getting the correct commitSequence (from last commit)
    return self._partition
      .getLatestCommit(self._streamId)
      .then(function (commit) {
        if (commit) {
          self._commitSequence = commit.commitSequence;
          var lastEvent = commit.events[commit.events.length - 1];
          if (lastEvent && lastEvent.type === '$stream.deleted.event') {
            throw new Error('Stream is deleted');
          }
          self._version = lastEvent.version + 1;
        }
        // if no commit is found, assume its a new stream, and keep default versions
      })
      .then(function () {
        return self;
      });
  } else {
    return self._partition
      ._queryStream(self._streamId, callback)
      .then(function (commits) {
        self._version = 0;

        if (!commits || commits.length === 0) {
          commits = [];
          self._version = -1;
        } else {
          if (commits[0] && commits[0].events && commits[0].events[0] && commits[0].events[0].type === '$stream.deleted.event') {
            throw new Error('Stream is deleted');
          }
        }
        var version = 0;
        for (var i = 0; i < commits.length; i++) {
          self._commitSequence++;
          for (var j = 0; j < commits[i].events.length; j++) {
            self._version++;
            commits[i].events[j].version = version++;
            self._committedEvents.push(commits[i].events[j]);
          }
        }
      })
      .then(function () {
        return self;
      });
  }
};

EventStream.prototype.getVersion = function () {
  return this._version;
};

EventStream.prototype.append = function (event) {
  this._uncommittedEvents.push(event);
};

EventStream.prototype.hasChanges = function () {
  return this._uncommittedEvents.length > 0;
};

EventStream.prototype.commit = function (commitId, callback) {
  var self = this;
  if (!this.hasChanges()) {
    //nothing to commit
    return Promise.resolve().nodeify(callback);
  } else {
    var commit = this._buildCommit(commitId, this._uncommittedEvents);
    return this._partition.append(commit, callback).then(function (commit) {
      //rebuild local state
      var events = self._uncommittedEvents;
      self._version = events[events.length - 1].version + 1;
      if (self._writeOnly === true) {
        self._commitSequence++;
        self._clearChanges();
      } else {
        for (var i = 0; i < events.length; i++) {
          self._committedEvents.push(events[i]);
        }
        self._clearChanges();
        self._commitSequence++;
        return self;
      }
    });
  }
};

EventStream.prototype._clearChanges = function () {
  this._uncommittedEvents = [];
};

EventStream.prototype.revertChanges = function () {
  //trunc the uncomitted events log
  var arr = this._uncommittedEvents;
  this._uncommittedEvents = [];
  delete arr;
};

EventStream.prototype._buildCommit = function (commitId, events) {
  var commitSequence = this._commitSequence;
  var commit = new Commit(commitId, this._partition._partitionId, this._streamId, ++commitSequence, events);
  var version = this._version == -1 ? 0 : this._version;

  _.forEach(events, function (evt) {
    evt.version = version++;
  });
  return commit;
};

EventStream.prototype.getCommittedEvents = function () {
  if (this._writeOnly) {
    throw new Error('Cannot access committed events when using writeOnly mode...');
  }
  return this._committedEvents.slice();
};

EventStream.prototype.getUncommittedEvents = function () {
  return this._uncommittedEvents.slice();
};

module.exports = EventStream;
