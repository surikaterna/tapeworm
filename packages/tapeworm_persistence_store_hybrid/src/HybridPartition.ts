import BluebirdPromise from 'bluebird';
import { LoggerFactory } from 'slf';
import type { ICommit, ISnapshot } from 'tapeworm';
import type { loadSnapshotCallback, LoggingOptions, Partition, queryStreamCallback, SnapshotResult, truncateStreamFromCallback } from './HybridPersistence';

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
      setInterval(
        () => {
          this.cleanPartition();
        },
        1000 * 60 * 30
      ); //Clean every 30 minutes
    }
  }

  private cleanPartition() {
    log.info('Auto cleaning partition');
    const snapshotTTLDate = new Date();
    snapshotTTLDate.setMinutes(snapshotTTLDate.getMinutes() - this.snapshotLifeTime);
    this.localPartition.querySnapshotsOlderThanMaxDate!(snapshotTTLDate.toISOString()).then((snapshots) => {
      log.info('cleaning %s snapshots', snapshots.length);
      const snapshotIdsToRemove = snapshots.map((snapshot) => snapshot.id);
      this.localPartition.removeSnapshots!(snapshotIdsToRemove);
      snapshotIdsToRemove.forEach((snapshotId) => this.localPartition.truncateStreamFrom(snapshotId, 0, true));
    });
  }

  open() {
    return BluebirdPromise.resolve(this);
  }

  append(commit: ICommit) {
    return this.localPartition.append(commit);
  }

  storeSnapshot(streamId: string, snapshot: ISnapshot['snapshot'], version: number) {
    return this.localPartition.storeSnapshot(streamId, snapshot, version);
  }

  private checkIsSnapshotToOld(snapshot?: ISnapshot | null) {
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

  loadSnapshot(streamId: string, callback?: loadSnapshotCallback): BluebirdPromise<SnapshotResult> {
    const startTime = Date.now();
    const loadPartitionSnapshot = (partition: Partition) =>
      new Promise<SnapshotResult>((resolve, reject) => {
        try {
          partition.loadSnapshot(streamId).then(resolve, reject);
        } catch (error) {
          reject(error);
        }
      });

    const snapshotPromise = new BluebirdPromise<SnapshotResult>((resolve, reject) => {
      loadPartitionSnapshot(this.localPartition)
        .then((localSnapshot) => {
          const isSnapshotToOld = this.checkIsSnapshotToOld(localSnapshot);

          if (localSnapshot && !isSnapshotToOld) {
            log.info('loadSnapshot: Using local snapshot');
            return localSnapshot;
          }

          return loadPartitionSnapshot(this.remotePartition)
            .then((remoteSnapshot) => {
              log.info('loadSnapshot: Using remote snapshot');

              if (remoteSnapshot) {
                this.localPartition.storeSnapshot(remoteSnapshot.id, remoteSnapshot.snapshot, remoteSnapshot.version);
                this.localPartition.truncateStreamFrom(remoteSnapshot.id, 0, true);
              }
              return remoteSnapshot;
            })
            .catch((error) => {
              log.warn('Failed to load snapshot with stream id %s from remote partition', streamId);
              throw error;
            });
        })
        .then(resolve, reject)
        .finally(() => {
          const endTime = Date.now();
          const queryTime = (endTime - startTime) / 1000;
          const isMaxTimeExceeded = this.loggingOptions.loadSnapshotMaxTime && queryTime > this.loggingOptions.loadSnapshotMaxTime;

          if (this.loggingOptions.loggingEnabled && isMaxTimeExceeded) {
            log.warn(
              'Loading snapshot with id %s, took %s seconds. Allowed max time is %s seconds.',
              streamId,
              queryTime,
              this.loggingOptions.loadSnapshotMaxTime
            );
          }
        });
    });

    if (callback) {
      snapshotPromise.then(
        (snapshot) => callback(null, snapshot),
        (error) => callback(error)
      );
    }

    return snapshotPromise;
  }

  queryStream(streamId: string, fromEventSequence?: number | queryStreamCallback, callback?: queryStreamCallback) {
    log.debug('queryStream, streamId %s ,fromEventSequence %s', streamId, fromEventSequence);
    return new BluebirdPromise<ICommit[]>((resolve) => {
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

  truncateStreamFrom(streamId: string, commitSequence: number, remove: boolean | truncateStreamFromCallback, callback?: truncateStreamFromCallback) {
    return this.localPartition.truncateStreamFrom(streamId, commitSequence, remove, callback);
  }

  applyCommitHeader(streamId: string, commit: ICommit, remove: any, callback: () => void) {
    return this.localPartition.applyCommitHeader(streamId, commit, remove, callback);
  }
}

export default HybridPartition;
