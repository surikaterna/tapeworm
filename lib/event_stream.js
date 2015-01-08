var Promise = require('bluebird');
var Commit = require('./persistence/commit');


var EventStream = function(eventPartition, streamId) {
	this._partition = eventPartition;
	this._streamId = streamId;
	
	this._committedEvents = [];
	this._uncommittedEvents = [];

	this._commitSequence = -1;

	this._prepareStream();
}

EventStream.prototype._prepareStream = function() {
	//this._partition.loadEvents()
};

EventStream.prototype.append = function(event) {
	this._uncommittedEvents.push(event);
};

EventStream.prototype.commit = function(commitId, callback) {
	var self = this;
	var events = this._uncommittedEvents;
	if(events.length == 0) {
		//nothing to commit
		Promise.resolve().nodeify(callback);
	} else {
		this._uncommittedEvents = [];
		var commit = this._buildCommit(commitId, events);
		return this._partition.append(commit, callback).then(function(commit) {
			return self._prepareStream();
		});
	}
}

EventStream.prototype.revertChanges = function() {
	//trunc the uncomitted events log
	var arr = this._uncommittedEvents;
	this._uncommittedEvents = [];
	delete arr;
};


EventStream.prototype._buildCommit = function(commitId, events) {
	var commit = new Commit(commitId, this._streamId, this._commitSequence, events);
	return commit;
};

EventStream.prototype.getCommittedEvents = function() {
	return this._committedEvents.splice();
};

EventStream.prototype.getUncommittedEvents = function() {
	return this._uncommittedEvents.splice();
};


module.exports = EventStream;