import Promise from 'bluebird';
import {InMemoryPartition as Partition} from './InMemoryPartition';

export class InMemoryPersistenceStore {
  constructor() {
    this._partitions = [];
  }

  _promisify(value, callback) {
    return Promise.resolve(value).nodeify(callback);
  }

  openPartition(partitionId, callback) {
    return this._promisify(this._getPartition(partitionId), callback);
  }

  _getPartition(partitionId) {
    const currentPartitionId = partitionId || 'master';
    let partition = this._partitions[currentPartitionId];
    if (partition == null) {
      partition = this._partitions[currentPartitionId] = new Partition();
    }
    return partition;
  }
}
