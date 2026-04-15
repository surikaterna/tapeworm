var Promise = require('bluebird');
var Partition = require('./remote_partition');

var RemotePersistence = function (client, dbname) {
  this._partitions = [];
  this._client = client;
  this._name = dbname;
}

RemotePersistence.prototype._promisify = function (value, callback) {
  return Promise.resolve(value).nodeify(callback);
};

RemotePersistence.prototype.openPartition = function (partitionId, callback) {
  var partition = this._getPartition(partitionId, callback);
  if (partition == null) {
    partition = new Partition(this._client, partitionId, this._name);
    this._setPartition(partitionId, partition);
    return partition.open();
  } else {
    return this._promisify(partition, callback);
  }
}

RemotePersistence.prototype._getPartition = function (partitionId) {
  partitionId = partitionId || 'master';
  return this._partitions[partitionId];
};

RemotePersistence.prototype._setPartition = function (partitionId, partition) {
  partitionId = partitionId || 'master';
  this._partitions[partitionId] = partition;
  return partition;
};
module.exports = RemotePersistence;