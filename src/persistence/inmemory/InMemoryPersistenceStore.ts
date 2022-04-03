import Promise from 'bluebird';
import { Callback } from '../../types';
import {InMemoryPartition as Partition} from './InMemoryPartition';

export class InMemoryPersistenceStore<T extends object> {
  _partitions: Record<string, Partition<T>>;

  constructor() {
    this._partitions = {};
  }

  _promisify<Value>(value: Value, callback?: Callback<Value>) {
    return Promise.resolve(value).nodeify(callback);
  }

  openPartition(partitionId: string, callback?: Callback<Partition<T>>) {
    return this._promisify(this._getPartition(partitionId), callback);
  }

  _getPartition(partitionId?: string) {
    const currentPartitionId = partitionId || 'master';
    let partition = this._partitions[currentPartitionId];
    if (partition == null) {
      partition = this._partitions[currentPartitionId] = new Partition();
    }
    return partition;
  }
}
