var Promise = require('bluebird');
var EventStream = require('./event_stream');

var EventStorePartition = function(partitionId, persistencePartition, dispatchService) {
	this._partitionId = partitionId;
	this._persistencePartition = persistencePartition;
	this._dispatchService = dispatchService;
}

EventStorePartition.prototype.openStream = function(streamId, callback) {
	var stream = new EventStream(this, streamId);
	return stream._prepareStream(callback);
};

EventStorePartition.prototype.append = function(commit, callback) {
	//pre hooks
	return this._persistencePartition.append(commit, callback);
	//post hooks
};

EventStorePartition.prototype._queryStream = function(streamId, callback) {
	return this._persistencePartition.queryStream(streamId, callback);
};


//EventStorePartition.prototype.loadEvents = function(streamId, callback)


/*EventStorePartition.prototype.append = function(streamId, expectedVersion, events) {
	var commit = new Commit(uuid(), this._partitionId, streamId, expectedVersion, events);
	return this._persistencePartition.append(commit).then(function()
	{
		return this._dispatchService.dispatch(commit);
	}).then(function() {
		return this._persistencePartition.markAsDispatched(commit);
	});
};
*/

module.exports = EventStorePartition;