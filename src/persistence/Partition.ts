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
