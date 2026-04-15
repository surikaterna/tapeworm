import BluebirdPromise from 'bluebird';
// @ts-ignore
import { Commit } from 'tapeworm';
import { LoggingOptions, Partition, queryStreamCallback, truncateStreamFromCallback } from './HybridPersistence';
import { LoggerFactory } from 'slf';
import { Snapshot } from './utils';

const log = LoggerFactory.getLogger('tapeworm-persistence-hybrid:hybrid-partition');

const DEFAULT_LOAD_SNAPSHOT_MAX_TIME = 10;

class HybridPartition implements Partition {
  private loggingOptions: LoggingOptions;

  /**
   * @param localPartition
   * @param remotePartition
   * @param autoCleanLocalPartition If set to true the partition will automatically clean out data older than dataLifeTime minutes dataLifeTime.
   * @param snapshotLifeTime Number of minutes before data will be considered old.
   * @param loggingOptions Options for logging
   */
  constructor(
    private localPartition: Partition,
    private remotePartition: Partition,
    autoCleanLocalPartition: boolean,
    private snapshotLifeTime: number,
    loggingOptions: LoggingOptions
  ) {
    this.loggingOptions = loggingOptions;
    if (!this.loggingOptions.loadSnapshotMaxTime) {
      this.loggingOptions.loadSnapshotMaxTime = DEFAULT_LOAD_SNAPSHOT_MAX_TIME;
    }

    if (autoCleanLocalPartition) {
      if (!localPartition.querySnapshotsOlderThanMaxDate) {
        throw new Error('Unable to start auto clean of local partition since it does not support it.');
      }

      log.info('Starting auto clean of local partition');
      setInterval(() => {
        this.cleanPartition();
      }, 1000 * 60 * 30); //Clean every 30 minutes
    }
  }

  private cleanPartition() {
    log.info('Auto cleaning partition');
    const snapshotTTLDate = new Date();
    snapshotTTLDate.setMinutes(snapshotTTLDate.getMinutes() - this.snapshotLifeTime);
    this.localPartition.querySnapshotsOlderThanMaxDate(snapshotTTLDate.toISOString()).then((snapshots) => {
      log.info('cleaning %s snapshots', snapshots.length);
      const snapshotIdsToRemove = snapshots.map((snapshot) => snapshot.id);
      this.localPartition.removeSnapshots(snapshotIdsToRemove);
      snapshotIdsToRemove.forEach((snapshotId) => this.localPartition.truncateStreamFrom(snapshotId, 0, true));
    });
  }

  open() {
    return BluebirdPromise.resolve(this);
  }

  append(commit: Commit<Record<string, any>>) {
    return this.localPartition.append(commit);
  }

  storeSnapshot(streamId: string, snapshot: Snapshot, version: number) {
    return this.localPartition.storeSnapshot(streamId, snapshot, version);
  }

  private checkIsSnapshotToOld(snapshot?: Snapshot) {
    if (!snapshot) {
      return true;
    }

    if (snapshot.storedDateTime) {
      const storedDateEpoch = new Date(snapshot.storedDateTime).getTime();
      const nowEpoch = new Date().getTime();

      const elapsedTimeInMilliSeconds = nowEpoch - storedDateEpoch;
      const snapshotLifetimeMilliSeconds = this.snapshotLifeTime * 60 * 1000;

      return elapsedTimeInMilliSeconds > snapshotLifetimeMilliSeconds;
    }

    return false;
  }

  loadSnapshot(streamId: string, callback?: (err: Error, snapshot: Snapshot) => void) {
    const startTime = Date.now();
    return new BluebirdPromise<Snapshot>((resolve) => {
      this.localPartition.loadSnapshot(streamId).then((localSnapshot) => {
        const isSnapshotToOld = this.checkIsSnapshotToOld(localSnapshot);
        if (localSnapshot && !isSnapshotToOld) {
          log.info('loadSnapshot: Using local snapshot');
          callback?.(null, localSnapshot);
          resolve(localSnapshot);
          return;
        }

        this.remotePartition
          .loadSnapshot(streamId)
          .then((remoteSnapshot) => {
            log.info('loadSnapshot: Using remote snapshot');
            if (remoteSnapshot) {
              this.localPartition.storeSnapshot(remoteSnapshot.id, remoteSnapshot.snapshot, remoteSnapshot.version);
              this.localPartition.truncateStreamFrom(remoteSnapshot.id, 0, true);
            }
            callback?.(null, remoteSnapshot);
            resolve(remoteSnapshot);
          })
          .catch((error) => {
            log.warn('Failed to load snapshot with stream id %s from remote partition', streamId);
          });
      });
    }).finally(() => {
      const endTime = Date.now();
      const queryTime = (endTime - startTime) / 1000;
      if (this.loggingOptions.loggingEnabled && queryTime > this.loggingOptions.loadSnapshotMaxTime) {
        log.warn('Loading snapshot with id %s, took %s seconds. Allowed max time is %s seconds.', streamId, queryTime, this.loggingOptions.loadSnapshotMaxTime);
      }
    });
  }

  queryStream(streamId: string, fromEventSequence?: number | queryStreamCallback, callback?: queryStreamCallback) {
    log.debug('queryStream, streamId %s ,fromEventSequence %s', streamId, fromEventSequence);
    return new BluebirdPromise<Commit<Record<string, any>>[]>((resolve) => {
      this.localPartition.loadSnapshot(streamId).then((localSnapshot) => {
        let currentPartition = this.localPartition;
        const isSnapshotToOld = this.checkIsSnapshotToOld(localSnapshot);

        if (!localSnapshot || isSnapshotToOld) {
          currentPartition = this.remotePartition;
        }

        currentPartition.queryStream(streamId, fromEventSequence).then((queryStream) => {
          callback?.(null, queryStream);
          resolve(queryStream);
          return;
        });
      });
    });
  }

  removeSnapshot(streamId: string) {
    return this.localPartition.removeSnapshot(streamId);
  }

  truncateStreamFrom(
    streamId: string,
    commit: Commit<Record<string, any>>,
    remove: boolean | truncateStreamFromCallback,
    callback?: truncateStreamFromCallback
  ) {
    return this.localPartition.truncateStreamFrom(streamId, commit, remove, callback);
  }

  applyCommitHeader(streamId: string, commit: Commit<Record<string, any>>, remove: any, callback: () => void) {
    return this.localPartition.applyCommitHeader(streamId, commit, remove, callback);
  }
}

export default HybridPartition;
