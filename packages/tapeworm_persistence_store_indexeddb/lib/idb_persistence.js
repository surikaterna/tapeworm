var Promise = require('bluebird');
var Partition = require('./idb_partition');

var IdbPersistence = function(idb, dbname)
{
	this._partitions = [];
	this._idb = idb;
	this._name = dbname;
}

IdbPersistence.prototype._promisify = function(value, callback) {
	return Promise.resolve(value).nodeify(callback);
};

IdbPersistence.prototype.openPartition = function(partitionId, callback) {
	var partition = this._getPartition(partitionId, callback);
	if(partition == null) {
		partition =  new Partition(this._idb, partitionId, this._name);
		this._setPartition(partitionId, partition);
		return partition.open();	
	} else {
		return this._promisify(partition, callback);	
	}
}

IdbPersistence.prototype._getPartition = function(partitionId) {
	partitionId = partitionId || 'master';
	var partition = this._partitions[partitionId];
	return partition;
};

IdbPersistence.prototype._setPartition = function(partitionId, partition) {
	partitionId = partitionId || 'master';
	this._partitions[partitionId] = partition;
	return partition;
};
module.exports = IdbPersistence;