import _ from "lodash";
import Promise from "bluebird";
import {
  getLastAuthorizedCommitSequence,
  getLastCommitSequence,
  commitsMatch,
} from "./util";
import JobQueue from "@surikat/job-queue";
import { Logger } from "slf";
import type { ICommit } from "tapeworm";

var LOG = Logger.getLogger("tapeworm:event-stream-sync");

type CommitWithAuthorative = ICommit & { authorative?: boolean };

/**
 * Minimal interface for the partition methods used by the synchronizer.
 * Avoids importing the full EventStorePartition type.
 */
export interface ISyncPartition {
  _queryStream(streamId: string): Promise<ICommit[]>;
  _applyCommitHeader(
    commitId: string,
    header: Record<string, unknown>,
  ): Promise<ICommit>;
  _truncateStreamFrom(streamId: string, commitSequence: number): Promise<void>;
  append(commits: ICommit | ICommit[]): Promise<unknown>;
}

interface IEventCommitsSynchronizer {
  _partition: ISyncPartition;
  _commits: CommitWithAuthorative[];
  _streamId: string;
  _hasConflict: boolean;
  _q: JobQueue<CommitWithAuthorative>;
  _skipSeq: number;
  _lastLocalSeq: number;
  _seenSequence: number;
  _localCommits: ICommit[];
  sync(): Promise<boolean>;
  processCommit(
    commit: CommitWithAuthorative,
    done: (err?: Error) => void,
  ): void;
  _processCommit(
    commit: CommitWithAuthorative,
    done: (err?: Error) => void,
  ): boolean | undefined;
  _applyNewCommit(
    commit: CommitWithAuthorative,
    done: (err?: Error) => void,
  ): void;
  _applyMergeCommit(
    commit: CommitWithAuthorative,
    localCommit: ICommit,
    done: (err?: Error) => void,
  ): void;
}

var EventCommitsSynchronizer = function (
  this: IEventCommitsSynchronizer,
  partition: ISyncPartition,
  commits: CommitWithAuthorative[],
  streamId: string,
) {
  this._partition = partition;
  this._commits = commits;
  this._streamId = streamId;
  this._hasConflict = false;
} as unknown as new (
  partition: ISyncPartition,
  commits: CommitWithAuthorative[],
  streamId: string,
) => IEventCommitsSynchronizer;

EventCommitsSynchronizer.prototype.sync = function (
  this: IEventCommitsSynchronizer,
): Promise<boolean> {
  var self = this;
  var q = (this._q = new JobQueue<CommitWithAuthorative>(
    this.processCommit.bind(this),
  ));
  q.pause();
  _.forEach(this._commits, function (commit: CommitWithAuthorative) {
    q.add(commit);
  });
  return new Promise(function (
    resolve: (value: boolean) => void,
    reject: (reason: Error) => void,
  ) {
    q.once("empty", function () {
      resolve(self._hasConflict);
    });
    q.once("error", function (e: Error) {
      LOG.debug("error", e);
      reject(e);
    });
    self._partition._queryStream(self._streamId).then(function (
      localCommits: ICommit[],
    ) {
      //return self._processCommitsLocal(commits, localCommits, streamId);
      self._skipSeq = getLastAuthorizedCommitSequence(localCommits);
      self._lastLocalSeq = getLastCommitSequence(localCommits);
      self._seenSequence = self._lastLocalSeq;
      self._localCommits = localCommits;
      q.resume();
    });
  });
};

EventCommitsSynchronizer.prototype.processCommit = function (
  this: IEventCommitsSynchronizer,
  commit: CommitWithAuthorative,
  done: (err?: Error) => void,
) {
  //do we need to process this commit
  if (commit.commitSequence > this._skipSeq) {
    this._processCommit(commit, done);
  } else {
    done();
  }
};

EventCommitsSynchronizer.prototype._processCommit = function (
  this: IEventCommitsSynchronizer,
  commit: CommitWithAuthorative,
  done: (err?: Error) => void,
): boolean | undefined {
  var matchingLocalCommit = _.find(this._localCommits, {
    commitSequence: commit.commitSequence,
  });
  //no such commit locally. just appen
  if (!matchingLocalCommit) {
    this._applyNewCommit(commit, done);
  } else {
    //check if matching commits
    return this._applyMergeCommit(
      commit,
      matchingLocalCommit,
      done,
    ) as unknown as boolean;
  }
  return true;
};

EventCommitsSynchronizer.prototype._applyNewCommit = function (
  this: IEventCommitsSynchronizer,
  commit: CommitWithAuthorative,
  done: (err?: Error) => void,
) {
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
    done(
      new Error(
        "Missing commits, gap in commit sequence local: " +
          newSeq +
          " server: " +
          commit.commitSequence,
      ),
    );
  }
};

EventCommitsSynchronizer.prototype._applyMergeCommit = function (
  this: IEventCommitsSynchronizer,
  commit: CommitWithAuthorative,
  localCommit: ICommit,
  done: (err?: Error) => void,
) {
  var self = this;
  //similiar enough, just marking this commit was approved by server
  if (commitsMatch(commit, localCommit)) {
    this._partition
      ._applyCommitHeader(localCommit.id, { authorative: true })
      .then(function () {
        done();
      });
  } else {
    //streams out of sync... :'(
    //self.emit('conflict', self._streamId);
    LOG.debug("Merge conflict, truncing", self._streamId);
    self._partition
      ._truncateStreamFrom(self._streamId, commit.commitSequence)
      .then(function () {
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
};

export interface IEventStreamSynchronizer {
  _partition: ISyncPartition;
  _processCommits(
    commits: CommitWithAuthorative[],
    streamId: string,
  ): Promise<boolean>;
  _processCommitsLocal(
    commits: CommitWithAuthorative[],
    localCommits: ICommit[],
    streamId: string,
  ): void;
}

var EventStreamSynchronizer = function (
  this: IEventStreamSynchronizer,
  tapewormPartition: ISyncPartition,
) {
  this._partition = tapewormPartition;
} as unknown as new (
  tapewormPartition?: ISyncPartition,
) => IEventStreamSynchronizer;

/**
 * commits arrived from server
 */
EventStreamSynchronizer.prototype._processCommits = function (
  this: IEventStreamSynchronizer,
  commits: CommitWithAuthorative[],
  streamId: string,
): Promise<boolean> {
  return new EventCommitsSynchronizer(
    this._partition,
    commits,
    streamId,
  ).sync();
};

/* Legacy method — not used in production or tests */
EventStreamSynchronizer.prototype._processCommitsLocal = function (
  this: IEventStreamSynchronizer,
  commits: CommitWithAuthorative[],
  localCommits: ICommit[],
  _streamId: string,
) {
  var skipSeq = getLastAuthorizedCommitSequence(localCommits);
  var lastLocalSeq = getLastCommitSequence(localCommits);
  var seenSequence = lastLocalSeq;
  void seenSequence;
  for (var i = 0; i < commits.length; i++) {
    var commit = commits[i];

    //do we need to process this commit
    if (commit.commitSequence > skipSeq) {
      // Original code called this._processCommit with different arity — dead code path
      var result = (
        this as unknown as Record<string, (...args: unknown[]) => unknown>
      )._processCommit(commit, commits, localCommits, _streamId, seenSequence);
      if (result === false) {
        return;
      }
    }
  }
};

export default EventStreamSynchronizer;
