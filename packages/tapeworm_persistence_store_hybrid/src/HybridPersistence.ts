import BluebirdPromise from 'bluebird';
import HybridPartition from './HybridPartition';
import type { ICommit, ISnapshot } from 'tapeworm';

export type truncateStreamFromCallback = (error: Error | null, result: Record<string, any>) => void;
type TruncateStreamFrom = (
  streamId: string,
  commitSequence: number,
  remove: boolean | truncateStreamFromCallback,
  callback?: truncateStreamFromCallback
) => BluebirdPromise<void>;

export type queryStreamCallback = (err: Error | null, steam: ICommit[]) => void;
type QueryStream = (streamId: string, fromEventSequence?: number | queryStreamCallback, callback?: queryStreamCallback) => BluebirdPromise<ICommit[]>;

export type Partition = {
  open: () => BluebirdPromise<Partition>;
  storeSnapshot: (streamId: string, snapshot: ISnapshot['snapshot'], version: number) => BluebirdPromise<void>;
  loadSnapshot: (streamId: string, callback?: (err: Error | null, snapshot: ISnapshot | undefined) => void) => BluebirdPromise<ISnapshot | undefined>;
  queryStream: QueryStream;
  append: (commit: ICommit) => BluebirdPromise<ICommit[]>;
  removeSnapshot: (streamId: string) => BluebirdPromise<void>;
  removeSnapshots?: (streamIds: string[]) => BluebirdPromise<void>;
  truncateStreamFrom: TruncateStreamFrom;
  applyCommitHeader: (streamId: string, commit: ICommit, remove: any, callback: () => void) => BluebirdPromise<void>;
  querySnapshotsOlderThanMaxDate?: (dateTime: string) => BluebirdPromise<ISnapshot[]>;
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

  private setPartition(partitionId: string, partition: Partition) {
    this.partitions[partitionId] = partition;
    return partition;
  }
}

export default HybridPersistence;
