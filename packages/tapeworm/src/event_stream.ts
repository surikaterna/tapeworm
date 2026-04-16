import Promise from "bluebird";
import _ from "lodash";
import Commit from "./persistence/commit";
import type { IBaseEvent, ICommit, NodeCallback } from "./types";

/** Subset of EventStorePartition that EventStream depends on. */
interface IEventPartition {
  _partitionId: string;
  getLatestCommit?: (streamId: string) => Promise<ICommit | undefined>;
  _queryStream: (streamId: string, callback?: NodeCallback<ICommit[]>) => Promise<ICommit[]>;
  append: (commit: ICommit, callback?: NodeCallback<unknown>) => Promise<unknown>;
}

interface IEventStream {
  _partition: IEventPartition;
  _streamId: string;
  _writeOnly: boolean;
  _uncommittedEvents: IBaseEvent[];
  _committedEvents: IBaseEvent[];
  _version: number;
  _isDeleted: boolean;
  _commitSequence: number;
  _prepareStream(callback?: NodeCallback<IEventStream>): Promise<IEventStream>;
  getVersion(): number;
  append(event: IBaseEvent): void;
  hasChanges(): boolean;
  commit(commitId: string, callback?: NodeCallback<IEventStream>): Promise<IEventStream | undefined>;
  _clearChanges(): void;
  revertChanges(): void;
  _buildCommit(commitId: string, events: IBaseEvent[]): ICommit;
  getCommittedEvents(): IBaseEvent[];
  getUncommittedEvents(): IBaseEvent[];
}

// writeOnly - will never read entire stream from
var EventStream = function (this: IEventStream, eventPartition: IEventPartition, streamId: string, writeOnly?: boolean) {
  if (streamId === undefined) {
    throw new Error("StreamId must be defined!");
  }
  this._partition = eventPartition;
  this._streamId = streamId;
  this._writeOnly = writeOnly || false;
  this._uncommittedEvents = [];
  this._committedEvents = [];
  this._version = 0;
  this._isDeleted = false;
} as unknown as new (
  eventPartition: IEventPartition,
  streamId: string,
  writeOnly?: boolean
) => IEventStream;

EventStream.prototype._prepareStream = function (this: IEventStream, callback?: NodeCallback<IEventStream>) {
  this._committedEvents = [];
  this._commitSequence = -1;
  var self = this;
  if (self._writeOnly === true && self._partition.getLatestCommit) {
    // if stream should only be opened for appending commits, only really care about getting the correct commitSequence (from last commit)
    return self._partition
      .getLatestCommit(self._streamId)
      .then(function (commit: ICommit | undefined) {
        if (commit) {
          self._commitSequence = commit.commitSequence;
          var lastEvent = commit.events[commit.events.length - 1];
          if (lastEvent && lastEvent.type === "$stream.deleted.event") {
            self._isDeleted = true;
          }
          self._version = (lastEvent.version as number) + 1;
        }
        // if no commit is found, assume its a new stream, and keep default versions
      })
      .then(function () {
        return self;
      });
  } else {
    return self._partition
      ._queryStream(self._streamId, callback as unknown as NodeCallback<ICommit[]>)
      .then(function (commits: ICommit[]) {
        self._version = 0;

        if (!commits || commits.length === 0) {
          commits = [];
          self._version = -1;
        } else {
          if (commits[0] && commits[0].events && commits[0].events[0] && commits[0].events[0].type === "$stream.deleted.event") {
            throw new Error("Stream is deleted");
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

EventStream.prototype.getVersion = function (this: IEventStream): number {
  return this._version;
};

EventStream.prototype.append = function (this: IEventStream, event: IBaseEvent): void {
  this._uncommittedEvents.push(event);
};

EventStream.prototype.hasChanges = function (this: IEventStream): boolean {
  return this._uncommittedEvents.length > 0;
};

EventStream.prototype.commit = function (this: IEventStream, commitId: string, callback?: NodeCallback<IEventStream>) {
  var self = this;

  if (this._isDeleted) {
    throw new Error(
      "Stream is deleted, unable to commit: " +
        this._uncommittedEvents.map(function (event: IBaseEvent) {
          return event.type;
        })
    );
  }

  if (!this.hasChanges()) {
    //nothing to commit
    return Promise.resolve().nodeify(callback);
  } else {
    var commit = this._buildCommit(commitId, this._uncommittedEvents);
    return this._partition.append(commit, callback as NodeCallback<unknown>).then(function () {
      //rebuild local state
      var events = self._uncommittedEvents;
      self._version = (events[events.length - 1].version as number) + 1;
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

EventStream.prototype._clearChanges = function (this: IEventStream): void {
  this._uncommittedEvents = [];
};

EventStream.prototype.revertChanges = function (this: IEventStream): void {
  //trunc the uncomitted events log
  this._uncommittedEvents = [];
};

EventStream.prototype._buildCommit = function (this: IEventStream, commitId: string, events: IBaseEvent[]): ICommit {
  var commitSequence = this._commitSequence;
  var commit = new Commit(commitId, this._partition._partitionId, this._streamId, ++commitSequence, events);
  var version = this._version == -1 ? 0 : this._version;

  _.forEach(events, function (evt: IBaseEvent) {
    evt.version = version++;
  });
  return commit;
};

EventStream.prototype.getCommittedEvents = function (this: IEventStream): IBaseEvent[] {
  if (this._writeOnly) {
    throw new Error("Cannot access committed events when using writeOnly mode...");
  }
  return this._committedEvents.slice();
};

EventStream.prototype.getUncommittedEvents = function (this: IEventStream): IBaseEvent[] {
  return this._uncommittedEvents.slice();
};

export default EventStream;
