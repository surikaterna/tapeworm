import { Callback } from '../types';
import { Commit } from './Commit';

export interface Partition<T extends object> {
  storeSnapshot(streamId: string, snapshot: T, version: number, callback?: Callback<Snapshot<T>>): Promise<Snapshot<T>>;

  loadSnapshot(streamId: string, callback?: Callback<Snapshot<T>>): Promise<Snapshot<T>>;

  queryStream(streamId: string, fromEventSequence: FromEventSequence<Array<Commit>>, callback?: Callback<Array<Commit>>): Promise<Array<Commit>>;

  queryStreamWithSnapshot?(streamId: string, callback?: Callback<Snapshot<T>>): Promise<SnapshotStream<Snapshot<T>>>;

  removeSnapshot?(streamId: string): Promise<void>;

  removeSnapshots?(streamIds: Array<string>): Promise<void>;

  getLatestCommit(streamId: string, callback?: Callback<Commit | undefined>): Promise<Commit | undefined>;

  querySnapshotsByMaxDateTime?(dateTime: number): Promise<Snapshot<T>>;

  _partitionId?: string;
}

export type Stream = any;

export interface StorePartition<T extends object> extends Partition<T> {
  append(commit: Commit, callback?: Callback<Commit>): Promise<Commit>
  commit(commitId: string, callback?: Callback<void>): Promise<void>;
  openStream(streamId: string, writeOnly: boolean, callback?: Callback<Stream>): Promise<Stream>;
  _queryStream: Partition<T>['queryStream'];
}

export type FromEventSequence<T> = number | Callback<T>;

export interface SnapshotStream<T extends object> {
  snapshot: Snapshot<T>;
  commits: Array<Commit>;
}

export type Snapshot<T extends object> = {
  id: string;
  version: number;
  snapshot: T;
};

export const DEFAULT_PARTITION_ID = 'master';
