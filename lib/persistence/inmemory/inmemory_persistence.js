var Promise = require('bluebird');
var Partition = require('./inmemory_partition');

var InMemoryPersistence = function()
{
	this._partitions = [];
}

InMemoryPersistence.prototype._promisify = function(value, callback) {
	return Promise.resolve(value).nodeify(callback);
};

InMemoryPersistence.prototype.openPartition = function(partitionId, callback) {
	return this._promisify(this._getPartition(partitionId), callback);
}

InMemoryPersistence.prototype._getPartition = function(partitionId) {
	partitionId = partitionId || 'master';
	var partition = this._partitions[partitionId];
	if(partition == null) {
		partition = this._partitions[partitionId] = new Partition();	
	} 
	return partition;
};


module.exports = InMemoryPersistence;