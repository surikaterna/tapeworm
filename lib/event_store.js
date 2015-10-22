var Promise = require('bluebird');

var Partition = require('./event_store_partition');
var InMemoryPersistenceStore = require('./persistence/inmemory/inmemory_persistence');
var _PENDING = {};

var EventStore = function(persistenceStore, dispatchService) {
	this._store = persistenceStore || new InMemoryPersistenceStore();
	this._dispatchService = dispatchService;
	this._partitions = {};
}

EventStore.prototype.openPartition = function(partitionId, callback) {
	var partition = null;
	partitionId = partitionId || 'master';
	partition = this._partitions[partitionId];
	var self = this;
	if(!partition) {
		this._partitions[partitionId] = _PENDING;
		return this._store.openPartition(partitionId).then(function(persistencePartition) {
			partition = new Partition(partitionId, persistencePartition, self._dispatchService);
			return self._partitions[partitionId] = partition;
		}).nodeify(callback);
	} else {
		//_PENDING is set when the partition is loading, wait 5ms at a time to see if it has been loaded
		if(this._partitions[partitionId] === _PENDING) {
			var promise = new Promise(function(resolve, reject) {
				var resolver = function() {
					var partition = self._partitions[partitionId];
					if(partition === _PENDING) {
						setTimeout(resolver, 5);
					} else {
						resolve(partition);
					}
				}
				resolver();
			});
			return promise.nodeify(callback);
		} else {
			return Promise.resolve(this._partitions[partitionId]).nodeify(callback);
		}
	}
};

var resolve


module.exports = EventStore;
module.exports.Event = require('./event');
module.exports.EventStream = require('./event_stream');
module.exports.Commit = require('./persistence/commit');
module.exports.ConcurrencyError = require('./persistence/concurrency_error');
module.exports.DuplicateCommitError = require('./persistence/duplicate_commit_error');