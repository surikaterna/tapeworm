import { drop, find, findIndex, forEach } from "lodash";
import {
  getLastAuthorizedCommitSequence,
  getLastCommitSequence,
  commitsMatch,
} from "./util";
import JobQueue from "@surikat/job-queue";
import { Logger } from "slf";
import type { ICommit } from "tapeworm";

const LOG = Logger.getLogger("tapeworm:event-stream-sync");

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

class EventCommitsSynchronizer {
  declare _partition: ISyncPartition;
  declare _commits: CommitWithAuthorative[];
  declare _streamId: string;
  declare _hasConflict: boolean;
  declare _q: JobQueue<CommitWithAuthorative>;
  declare _skipSeq: number;
  declare _lastLocalSeq: number;
  declare _seenSequence: number;
  declare _localCommits: ICommit[];

  constructor(
    partition: ISyncPartition,
    commits: CommitWithAuthorative[],
    streamId: string,
  ) {
    this._partition = partition;
    this._commits = commits;
    this._streamId = streamId;
    this._hasConflict = false;
  }

  async sync(): Promise<boolean> {
    const q = (this._q = new JobQueue<CommitWithAuthorative>(
      this.processCommit.bind(this),
    ));
    q.pause();
    forEach(this._commits, (commit: CommitWithAuthorative) => {
      q.add(commit);
    });
    const localCommits = await this._partition._queryStream(this._streamId);
    this._skipSeq = getLastAuthorizedCommitSequence(localCommits);
    this._lastLocalSeq = getLastCommitSequence(localCommits);
    this._seenSequence = this._lastLocalSeq;
    this._localCommits = localCommits;

    return new Promise<boolean>((
      resolve: (value: boolean) => void,
      reject: (reason: Error) => void,
    ) => {
      q.once("empty", () => {
        resolve(this._hasConflict);
      });
      q.once("error", (e: Error) => {
        LOG.debug("error", e);
        reject(e);
      });
      q.resume();
    });
  }

  async processCommit(
    commit: CommitWithAuthorative,
    done: (err?: Error) => void,
  ): Promise<void> {
    //do we need to process this commit
    try {
      if (commit.commitSequence > this._skipSeq) {
        await this._processCommit(commit);
      }
    } catch (err) {
      done(err as Error);
      return;
    }
    done();
  }

  async _processCommit(commit: CommitWithAuthorative): Promise<void> {
    const matchingLocalCommit = find(this._localCommits, { commitSequence: commit.commitSequence });
    //no such commit locally. just appen
    if (!matchingLocalCommit) {
      await this._applyNewCommit(commit);
    } else {
      //check if matching commits
      await this._applyMergeCommit(commit, matchingLocalCommit);
    }
  }

  async _applyNewCommit(commit: CommitWithAuthorative): Promise<void> {
    const newSeq = this._seenSequence + 1;
    if (newSeq === commit.commitSequence) {
      this._seenSequence++;
      commit.authorative = true;
      await this._partition.append(commit);
    } else {
      this._hasConflict = false;
      LOG.debug(this._localCommits);
      LOG.debug(this._commits);
      throw new Error(
        "Missing commits, gap in commit sequence local: " +
          newSeq +
          " server: " +
          commit.commitSequence,
      );
    }
  }

  async _applyMergeCommit(
    commit: CommitWithAuthorative,
    localCommit: ICommit,
  ): Promise<void> {
    //similiar enough, just marking this commit was approved by server
    if (commitsMatch(commit, localCommit)) {
      await this._partition._applyCommitHeader(localCommit.id, { authorative: true });
    } else {
      //streams out of sync... :'(
      //this.emit('conflict', this._streamId);
      LOG.debug("Merge conflict, truncing", this._streamId);
      await this._partition._truncateStreamFrom(this._streamId, commit.commitSequence);
      const i = findIndex(this._commits, { id: commit.id });
      this._hasConflict = true;
      //do not process more commits as we are resetting the stream and appending all remaining commits here...
      this._q.clear();
      const commitsToAdd = drop(this._commits, i);
      await this._partition.append(commitsToAdd);
    }
  }
}

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

class EventStreamSynchronizer implements IEventStreamSynchronizer {
  declare _partition: ISyncPartition;

  constructor(tapewormPartition?: ISyncPartition) {
    this._partition = tapewormPartition!;
  }

  /**
   * commits arrived from server
   */
  _processCommits(
    commits: CommitWithAuthorative[],
    streamId: string,
  ): Promise<boolean> {
    return new EventCommitsSynchronizer(
      this._partition,
      commits,
      streamId,
    ).sync();
  }

  /* Legacy method — not used in production or tests */
  _processCommitsLocal(
    commits: CommitWithAuthorative[],
    localCommits: ICommit[],
    _streamId: string,
  ): void {
    const skipSeq = getLastAuthorizedCommitSequence(localCommits);
    const lastLocalSeq = getLastCommitSequence(localCommits);
    const seenSequence = lastLocalSeq;
    void seenSequence;
    for (let i = 0; i < commits.length; i++) {
      const commit = commits[i];

      //do we need to process this commit
      if (commit.commitSequence > skipSeq) {
        // Original code called this._processCommit with different arity — dead code path
        const result = (
          this as unknown as Record<string, (...args: unknown[]) => unknown>
        )._processCommit(
          commit,
          commits,
          localCommits,
          _streamId,
          seenSequence,
        );
        if (result === false) {
          return;
        }
      }
    }
  }
}

export default EventStreamSynchronizer;
