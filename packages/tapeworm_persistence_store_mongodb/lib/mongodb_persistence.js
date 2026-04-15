var Promise = require('bluebird');
var Partition = require('./mongodb_partition');

var MdbPersistence = function(mongodb) {
	this._partitions = [];
	this._mongodb = mongodb;
}

MdbPersistence.prototype._promisify = function(value, callback) {
	return Promise.resolve(value).nodeify(callback);
};

MdbPersistence.prototype.openPartition = function(partitionId, callback) {
	partitionId = partitionId || 'master';
	var partition = this._getPartition(partitionId, callback);
	if(partition == null) {
		partition =  new Partition(this._mongodb, partitionId);
		this._setPartition(partitionId, partition);
		return partition.open();	
	} else {
		return this._promisify(partition, callback);	
	}
};


MdbPersistence.prototype._getPartition = function(partitionId) {
	partitionId = partitionId || 'master';
	var partition = this._partitions[partitionId];
	return partition;
};

MdbPersistence.prototype._setPartition = function(partitionId, partition) {
	partitionId = partitionId || 'master';
	this._partitions[partitionId] = partition;
	return partition;
};

module.exports = MdbPersistence;
