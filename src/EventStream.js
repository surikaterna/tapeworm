import Promise from 'bluebird';
import {forEach} from 'lodash';
import Commit from './persistence/commit';

// writeOnly - will never read entire stream from
class EventStream {
  constructor(eventPartition, streamId, writeOnly) {
    if (streamId === undefined) {
      throw new Error('StreamId must be defined!');
    }
    this._partition = eventPartition;
    this._streamId = streamId;
    this._writeOnly = writeOnly;
    this._uncommittedEvents = [];
    this._committedEvents = [];
    this._version = 0;
    this._isDeleted = false;
  }

  _prepareStream(callback) {
    this._committedEvents = [];
    this._commitSequence = -1;
    if (this._writeOnly === true && this._partition.getLatestCommit) {
      // if stream should only be opened for appending commits, only really care about getting the correct commitSequence (from last commit)
      return this._partition
        .getLatestCommit(this._streamId)
        .then((commit) => {
          if (commit) {
            this._commitSequence = commit.commitSequence;
            const lastEvent = commit.events[commit.events.length - 1];
            if (lastEvent && lastEvent.type === '$stream.deleted.event') {
              this._isDeleted = true;
            }
            this._version = lastEvent.version + 1;
          }
          // if no commit is found, assume it's a new stream, and keep default versions
        })
        .then(() => this);
    } else {
      return this._partition
        ._queryStream(this._streamId, callback)
        .then((commits) => {
          this._version = 0;

          if (!commits || commits.length === 0) {
            commits = [];
            this._version = -1;
          } else {
            if (commits[0] && commits[0].events && commits[0].events[0] && commits[0].events[0].type === '$stream.deleted.event') {
              throw new Error('Stream is deleted');
            }
          }
          let version = 0;
          for (let i = 0; i < commits.length; i++) {
            this._commitSequence++;
            for (let j = 0; j < commits[i].events.length; j++) {
              this._version++;
              commits[i].events[j].version = version++;
              this._committedEvents.push(commits[i].events[j]);
            }
          }
        })
        .then(() => this);
    }
  }

  getVersion() {
    return this._version;
  }

  append(event) {
    this._uncommittedEvents.push(event);
  }

  hasChanges() {
    return this._uncommittedEvents.length > 0;
  }

  commit(commitId, callback) {
    if (this._isDeleted) {
      throw new Error('Stream is deleted, unable to commit: ' + this._uncommittedEvents.map((event) => event.type));
    }

    if (!this.hasChanges()) {
      //nothing to commit
      return Promise.resolve().nodeify(callback);
    } else {
      const commit = this._buildCommit(commitId, this._uncommittedEvents);
      return this._partition.append(commit, callback).then((commit) => {
        //rebuild local state
        const events = this._uncommittedEvents;
        this._version = events[events.length - 1].version + 1;
        if (this._writeOnly === true) {
          this._commitSequence++;
          this._clearChanges();
        } else {
          for (let i = 0; i < events.length; i++) {
            this._committedEvents.push(events[i]);
          }
          this._clearChanges();
          this._commitSequence++;
          return this;
        }
      });
    }
  }

  _clearChanges() {
    this._uncommittedEvents = [];
  }

  revertChanges() {
    this._uncommittedEvents = [];
  }

  _buildCommit(commitId, events) {
    let commitSequence = this._commitSequence;
    const commit = new Commit(commitId, this._partition._partitionId, this._streamId, ++commitSequence, events);
    let version = this._version == -1 ? 0 : this._version;

    forEach(events, (evt) => {
      evt.version = version++;
    });
    return commit;
  }

  getCommittedEvents() {
    if (this._writeOnly) {
      throw new Error('Cannot access committed events when using writeOnly mode...');
    }
    return this._committedEvents.slice();
  }

  getUncommittedEvents() {
    return this._uncommittedEvents.slice();
  }
}

export default EventStream;
