import BluebirdPromise from 'bluebird';
import HybridPartition from './HybridPartition';
import { Commit, Snapshot } from './utils';

export type truncateStreamFromCallback = (error: Error, result: Record<string, any>) => void;
type TruncateStreamFrom = (
  streamId: string,
  commitSequence: number,
  remove: boolean | truncateStreamFromCallback,
  callback?: truncateStreamFromCallback
) => BluebirdPromise<void>;

export type queryStreamCallback = (err: Error, steam: Commit<Record<string, any>>[]) => void;
type QueryStream = (
  streamId: string,
  fromEventSequence?: number | queryStreamCallback,
  callback?: queryStreamCallback
) => BluebirdPromise<Commit<Record<string, any>>[]>;

export type Partition = {
  open: () => BluebirdPromise<Partition>;
  storeSnapshot: (streamId: string, snapshot: Snapshot['snapshot'], version: number) => BluebirdPromise<void>;
  loadSnapshot: (streamId: string, callback?: (err: Error, snapshot: Snapshot | undefined) => void) => BluebirdPromise<Snapshot | undefined>;
  queryStream: QueryStream;
  append: (commit: Commit<Record<string, any>>) => BluebirdPromise<Commit<Record<string, any>>[]>;
  removeSnapshot: (streamId: string) => BluebirdPromise<void>;
  removeSnapshots?: (streamIds: string[]) => BluebirdPromise<void>;
  truncateStreamFrom: TruncateStreamFrom;
  applyCommitHeader: (streamId: string, commit: Commit<Record<string, any>>, remove: any, callback: () => void) => BluebirdPromise<void>;
  querySnapshotsOlderThanMaxDate?: (dateTime: string) => BluebirdPromise<Snapshot[]>;
};

export interface LoggingOptions {
  loggingEnabled: boolean;
  loadSnapshotMaxTime?: number;
}

export type Persistence = {
  openPartition: (partitionId?: string) => BluebirdPromise<Partition>;
};

const DEFAULT_PARTITION = 'master';

class HybridPersistence {
  private partitions: Record<string, Partition> = {};

  constructor(
    private localPersistence: Persistence,
    private remotePersistence: Persistence,
    private autoCleanLocalPartition: boolean,
    private snapshotLifeTime: number,
    private loggingOptions: LoggingOptions
  ) {}

  openPartition(partitionId: string = DEFAULT_PARTITION) {
    return new BluebirdPromise(async (resolve) => {
      const partition = this.getPartition(partitionId);
      if (partition) {
        resolve(partition);
      } else {
        const localPartition = await this.localPersistence.openPartition(partitionId);
        const remotePartition = await this.remotePersistence.openPartition(partitionId);
        const newPartition = new HybridPartition(localPartition, remotePartition, this.autoCleanLocalPartition, this.snapshotLifeTime, this.loggingOptions);
        this.setPartition(partitionId, newPartition);
        newPartition.open().then(() => {
          resolve(newPartition);
        });
      }
    });
  }

  private getPartition(partitionId: string = DEFAULT_PARTITION) {
    return this.partitions[partitionId];
  }

  private setPartition(partitionId?: string, partition?: Partition) {
    this.partitions[partitionId || DEFAULT_PARTITION] = partition;
    return partition;
  }
}

export default HybridPersistence;
