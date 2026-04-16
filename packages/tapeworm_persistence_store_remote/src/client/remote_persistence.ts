import Promise from "bluebird";
import Partition from "./remote_partition";
import type { NodeCallback } from "tapeworm";

type ClientInstance = InstanceType<typeof import("./rrtw_client").default>;
type RemotePartitionInstance = InstanceType<typeof Partition>;

export interface IRemotePersistence {
  _partitions: Record<string, RemotePartitionInstance>;
  _client: ClientInstance;
  _name: string | undefined;
  _promisify<T>(value: T, callback?: NodeCallback<T>): Promise<T>;
  openPartition(
    partitionId?: string,
    callback?: NodeCallback<RemotePartitionInstance>,
  ): Promise<RemotePartitionInstance>;
  _getPartition(partitionId?: string): RemotePartitionInstance | undefined;
  _setPartition(
    partitionId: string,
    partition: RemotePartitionInstance,
  ): RemotePartitionInstance;
}

var RemotePersistence = function (
  this: IRemotePersistence,
  client: ClientInstance,
  dbname?: string,
) {
  this._partitions = {};
  this._client = client;
  this._name = dbname;
} as unknown as new (
  client: ClientInstance,
  dbname?: string,
) => IRemotePersistence;

RemotePersistence.prototype._promisify = function <T>(
  this: IRemotePersistence,
  value: T,
  callback?: NodeCallback<T>,
): Promise<T> {
  return Promise.resolve(value).nodeify(callback);
};

RemotePersistence.prototype.openPartition = function (
  this: IRemotePersistence,
  partitionId?: string,
  callback?: NodeCallback<RemotePartitionInstance>,
): Promise<RemotePartitionInstance> {
  var partition = this._getPartition(partitionId);
  if (partition == null) {
    partition = new Partition(this._client, partitionId || "master");
    this._setPartition(partitionId || "master", partition);
    return partition.open() as unknown as Promise<RemotePartitionInstance>;
  } else {
    return this._promisify(partition, callback);
  }
};

RemotePersistence.prototype._getPartition = function (
  this: IRemotePersistence,
  partitionId?: string,
): RemotePartitionInstance | undefined {
  partitionId = partitionId || "master";
  return this._partitions[partitionId];
};

RemotePersistence.prototype._setPartition = function (
  this: IRemotePersistence,
  partitionId: string,
  partition: RemotePartitionInstance,
): RemotePartitionInstance {
  partitionId = partitionId || "master";
  this._partitions[partitionId] = partition;
  return partition;
};

export default RemotePersistence;
