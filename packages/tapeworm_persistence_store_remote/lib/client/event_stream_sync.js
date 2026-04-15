var _ = require('lodash');
var Promise = require('bluebird');

var util = require('./util');

var JobQueue = require('@surikat/job-queue');
var LOG = require('slf').Logger.getLogger('tapeworm:event-stream-sync');

var EventCommitsSynchronizer = function (partition, commits, streamId) {
  this._partition = partition;
  this._commits = commits;
  this._streamId = streamId;
  this._hasConflict = false;
}

EventCommitsSynchronizer.prototype.sync = function () {
  var self = this;
  var q = this._q = new JobQueue(this.processCommit.bind(this));
  q.pause();
  _.forEach(this._commits, function (commit) {
    q.add(commit);
  });
  return new Promise(function (resolve, reject) {
    q.once('empty', function () {
      resolve(self._hasConflict);
    });
    q.once('error', function (e) {
      LOG.debug('error', e);
      reject(e);
    })
    self._partition._queryStream(self._streamId).then(function (localCommits) {
      //return self._processCommitsLocal(commits, localCommits, streamId);
      self._skipSeq = util.getLastAuthorizedCommitSequence(localCommits);
      self._lastLocalSeq = util.getLastCommitSequence(localCommits);
      self._seenSequence = self._lastLocalSeq;
      self._localCommits = localCommits;
      q.resume();
    });
  });
}

EventCommitsSynchronizer.prototype.processCommit = function (commit, done) {
  //do we need to process this commit
  if (commit.commitSequence > this._skipSeq) {
    this._processCommit(commit, done);
  } else {
    done();
  }
}

EventCommitsSynchronizer.prototype._processCommit = function (commit, done) {
  var matchingLocalCommit = _.find(this._localCommits, { commitSequence: commit.commitSequence });
  //no such commit locally. just appen
  if (!matchingLocalCommit) {
    this._applyNewCommit(commit, done);
  } else {
    //check if matching commits
    return this._applyMergeCommit(commit, matchingLocalCommit, done);
  }
  return true;
};

EventCommitsSynchronizer.prototype._applyNewCommit = function (commit, done) {
  var newSeq = this._seenSequence + 1;
  if (newSeq === commit.commitSequence) {
    this._seenSequence++;
    commit.authorative = true;
    this._partition.append(commit).then(function () {
      done();
    });
  } else {
    this._hasConflict = false;
    LOG.debug(this._localCommits);
    LOG.debug(this._commits);
    done(new Error("Missing commits, gap in commit sequence local: " + newSeq + " server: " + commit.commitSequence));
  }
};

EventCommitsSynchronizer.prototype._applyMergeCommit = function (commit, localCommit, done) {
  var self = this;
  //similiar enough, just marking this commit was approved by server
  if (util.commitsMatch(commit, localCommit)) {
    this._partition._applyCommitHeader(localCommit.id, { authorative: true }).then(function () {
      done();
    });
  } else {
    //streams out of sync... :'(
    //self.emit('conflict', self._streamId);
    LOG.debug("Merge conflict, truncing", self._streamId);
    self._partition._truncateStreamFrom(self._streamId, commit.commitSequence).then(function () {
      var i = _.findIndex(self._commits, { id: commit.id });
      self._hasConflict = true;
      //do not process more commits as we are resetting the stream and appending all remaining commits here...
      self._q.clear();
      var commitsToAdd = _.drop(self._commits, i);
      return self._partition.append(commitsToAdd).then(function () {
        done();
      });
    });
  }
}

var EventStreamSynchronizer = function (tapewormPartition) {

  this._partition = tapewormPartition;
}

/**
 * commits arrived form server
 */
EventStreamSynchronizer.prototype._processCommits = function (commits, streamId) {
  return new EventCommitsSynchronizer(this._partition, commits, streamId).sync();
}

EventStreamSynchronizer.prototype._processCommitsLocal = function (commits, localCommits, streamId) {
  var skipSeq = util.getLastAuthorizedCommitSequence(localCommits);
  var lastLocalSeq = util.getLastCommitSequence(localCommits);
  var seenSequence = lastLocalSeq;
  for (var i = 0; i < commits.length; i++) {
    var commit = commits[i];

    //do we need to process this commit
    if (commit.commitSequence > skipSeq) {
      var result = this._processCommit(commit, commits, localCommits, streamId, seenSequence);
      if (result === false) {
        return;
      }
    }
  }
};

module.exports = EventStreamSynchronizer;