import {forEach, isArray} from 'lodash';
import Promise from 'bluebird';
import {v4 as uuid} from 'uuid';
import EventStream from './event_stream';

class EventStorePartition {
  constructor(partitionId, persistencePartition, dispatchService) {
    this._partitionId = partitionId;
    this._persistencePartition = persistencePartition;
    this._dispatchService = dispatchService;

    forEach(
      ['storeSnapshot', 'loadSnapshot', 'queryStream', 'queryStreamWithSnapshot', 'removeSnapshot', 'getLatestCommit', 'querySnapshotsByMaxDateTime'],
      (what) => {
        if (this._persistencePartition[what]) {
          this[what] = () => {
            return this._persistencePartition[what].apply(this._persistencePartition, arguments);
          };
        } else {
          if (what === 'queryStreamWithSnapshot') {
            // fallback function
            this[what] = () => {
              return this._queryStreamWithSnapshotFallback.apply(this._persistencePartition, arguments);
            };
          }
        }
      }
    );
  }

  openStream(streamId, writeOnly, callback) {
    if (typeof writeOnly === 'function') {
      callback = writeOnly;
      writeOnly = false;
    }
    const stream = new EventStream(this, streamId, writeOnly);
    return stream._prepareStream(callback);
  }

  append(commits, callback) {
    //pre hooks
    if (!isArray(commits)) {
      commits = [commits];
    }
    return Promise.each(commits, (commit) => {
      return this._persistencePartition.append(commit, callback).then((r) => {
        const done = () => {
          this._persistencePartition.markAsDispatched(commit);
        };
        if (this._dispatchService) {
          this._dispatchService(commit, done);
        }
        return r;
      });
    });
    //post hooks
  }

  /**
   * Delete function in tapeworm,
   * Delete should trigger a $stream.deleted.event, including aggregateType of first event and publish it.
   * 2. Implement in projector service to listen to this event and then do a delete of all related projections.,
   * 3. Delete function should store the following new commit (deleted event) under the stream:
   *   streamId,
   *   aggregateType,
   *   date of deletion,
   *   date of creation,
   *   principal
   * 4. Then remove snapshot for this stream and all commits for this stream.
   * @param {*} streamId
   * @param {*} deleteEvent optional delete event
   */
  delete(streamId, deleteEvent) {
    const event = Object.assign({}, deleteEvent, {
      type: '$stream.deleted.event',
      payload: Object.assign({}, {dateOfDeletion: new Date()}, (deleteEvent && deleteEvent.payload) || {})
    });

    return this._truncateStreamFrom(streamId, -1)
      .then((result) => {
        return this.openStream(streamId, false);
      })
      .then((stream) => {
        stream.append(event);
        return stream.commit(uuid());
      })
      .then(() => {
        // support legacy/unsupported approach
        if (this._persistencePartition.removeSnapshot) {
          return this._persistencePartition.removeSnapshot(streamId);
        } else if (this._persistencePartition.removeSnapshots) {
          return this._persistencePartition.removeSnapshots([streamId]);
        }
      });
  }


  _queryStream(streamId, callback) {
    return this._persistencePartition.queryStream(streamId, callback);
  }

  _queryAll(callback) {
    return this._persistencePartition.queryAll(callback);
  }

  _queryStreamWithSnapshotFallback(streamId, callback) {
    return new Promise((resolve, reject) => {
      this.loadSnapshot(streamId, (err, snapshot) => {
        if (err) {
          reject(err);
        } else {
          const snapshotVersion = (snapshot && snapshot.version) || -1;
          this.queryStream(streamId, snapshotVersion, (err, res) => {
            resolve({snapshot: snapshot, commits: res});
          });
        }
      });
    }).nodeify(callback);
  }

  /*** NEEDED FOR SYNCING ***/

  _truncateStreamFrom(streamId, commitSequence, callback) {
    return this._persistencePartition.truncateStreamFrom(streamId, commitSequence, callback);
  }

  _applyCommitHeader(commit, header, callback) {
    return this._persistencePartition.applyCommitHeader(commit, header, callback);
  }
}

export default EventStorePartition;
