import Promise from "bluebird";
import type { Db } from "mongodb";
import type { NodeCallback } from "tapeworm";
import Partition from "./mongodb_partition";

type MdbPartitionInstance = InstanceType<typeof Partition>;

interface IMdbPersistence {
  _partitions: Record<string, MdbPartitionInstance>;
  _mongodb: Db;
  _promisify<T>(value: T, callback?: NodeCallback<T>): Promise<T>;
  openPartition(
    partitionId?: string,
    callback?: NodeCallback<MdbPartitionInstance>,
  ): Promise<MdbPartitionInstance>;
  _getPartition(partitionId?: string): MdbPartitionInstance | undefined;
  _setPartition(
    partitionId: string,
    partition: MdbPartitionInstance,
  ): MdbPartitionInstance;
}

var MdbPersistence = function (this: IMdbPersistence, mongodb: Db) {
  this._partitions = {};
  this._mongodb = mongodb;
} as unknown as new (mongodb: Db) => IMdbPersistence;

MdbPersistence.prototype._promisify = function <T>(
  this: IMdbPersistence,
  value: T,
  callback?: NodeCallback<T>,
): Promise<T> {
  return Promise.resolve(value).nodeify(callback);
};

MdbPersistence.prototype.openPartition = function (
  this: IMdbPersistence,
  partitionId?: string,
  callback?: NodeCallback<MdbPartitionInstance>,
): Promise<MdbPartitionInstance> {
  partitionId = partitionId || "master";
  var partition = this._getPartition(partitionId);
  if (partition == null) {
    partition = new Partition(this._mongodb, partitionId);
    this._setPartition(partitionId, partition);
    return partition.open();
  } else {
    return this._promisify(partition, callback);
  }
};

MdbPersistence.prototype._getPartition = function (
  this: IMdbPersistence,
  partitionId?: string,
): MdbPartitionInstance | undefined {
  partitionId = partitionId || "master";
  var partition = this._partitions[partitionId];
  return partition;
};

MdbPersistence.prototype._setPartition = function (
  this: IMdbPersistence,
  partitionId: string,
  partition: MdbPartitionInstance,
): MdbPartitionInstance {
  partitionId = partitionId || "master";
  this._partitions[partitionId] = partition;
  return partition;
};

export default MdbPersistence;
