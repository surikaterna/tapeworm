import Promise from "bluebird";
import type { NodeCallback } from "../../types";
import Partition from "./inmemory_partition";

type InMemoryPartitionInstance = InstanceType<typeof Partition>;

interface IInMemoryPersistence {
  _partitions: Record<string, InMemoryPartitionInstance>;
  _promisify<T>(value: T, callback?: NodeCallback<T>): Promise<T>;
  openPartition(partitionId?: string, callback?: NodeCallback<InMemoryPartitionInstance>): Promise<InMemoryPartitionInstance>;
  _getPartition(partitionId?: string): InMemoryPartitionInstance;
}

var InMemoryPersistence = function InMemoryPersistance(this: IInMemoryPersistence) {
  this._partitions = {};
} as unknown as new () => IInMemoryPersistence;

InMemoryPersistence.prototype._promisify = function <T>(this: IInMemoryPersistence, value: T, callback?: NodeCallback<T>): Promise<T> {
  return Promise.resolve(value).nodeify(callback);
};

InMemoryPersistence.prototype.openPartition = function (this: IInMemoryPersistence, partitionId?: string, callback?: NodeCallback<InMemoryPartitionInstance>) {
  return this._promisify(this._getPartition(partitionId), callback);
};

InMemoryPersistence.prototype._getPartition = function (this: IInMemoryPersistence, partitionId?: string): InMemoryPartitionInstance {
  var currentPartitionId = partitionId || "master";
  var partition = this._partitions[currentPartitionId];
  if (partition == null) {
    partition = this._partitions[currentPartitionId] = new Partition();
  }
  return partition;
};

export default InMemoryPersistence;
