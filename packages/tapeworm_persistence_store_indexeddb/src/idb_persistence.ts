import Promise from "bluebird";
import type { NodeCallback } from "tapeworm";
import Partition from "./idb_partition";

type IdbPartitionInstance = InstanceType<typeof Partition>;

interface IIdbPersistence {
  _partitions: Record<string, IdbPartitionInstance>;
  _idb: IDBFactory;
  _name: string | undefined;
  _promisify<T>(value: T, callback?: NodeCallback<T>): Promise<T>;
  openPartition(
    partitionId?: string,
    callback?: NodeCallback<IdbPartitionInstance>,
  ): Promise<IdbPartitionInstance>;
  _getPartition(partitionId?: string): IdbPartitionInstance | undefined;
  _setPartition(
    partitionId: string | undefined,
    partition: IdbPartitionInstance,
  ): IdbPartitionInstance;
}

var IdbPersistence = function (
  this: IIdbPersistence,
  idb: IDBFactory,
  dbname?: string,
) {
  this._partitions = {};
  this._idb = idb;
  this._name = dbname;
} as unknown as new (idb: IDBFactory, dbname?: string) => IIdbPersistence;

IdbPersistence.prototype._promisify = function <T>(
  this: IIdbPersistence,
  value: T,
  callback?: NodeCallback<T>,
): Promise<T> {
  return Promise.resolve(value).nodeify(callback);
};

IdbPersistence.prototype.openPartition = function (
  this: IIdbPersistence,
  partitionId?: string,
  callback?: NodeCallback<IdbPartitionInstance>,
): Promise<IdbPartitionInstance> {
  var currentPartitionId = partitionId || "master";
  var partition = this._getPartition(currentPartitionId);
  if (partition == null) {
    partition = new Partition(this._idb, currentPartitionId, this._name || "");
    this._setPartition(currentPartitionId, partition);
    return partition.open();
  } else {
    return this._promisify(partition, callback);
  }
};

IdbPersistence.prototype._getPartition = function (
  this: IIdbPersistence,
  partitionId?: string,
): IdbPartitionInstance | undefined {
  partitionId = partitionId || "master";
  var partition = this._partitions[partitionId];
  return partition;
};

IdbPersistence.prototype._setPartition = function (
  this: IIdbPersistence,
  partitionId: string | undefined,
  partition: IdbPartitionInstance,
): IdbPartitionInstance {
  partitionId = partitionId || "master";
  this._partitions[partitionId] = partition;
  return partition;
};

export default IdbPersistence;
