var Promise = require('bluebird');
var Commit = require('./persistence/commit');

var EventStream = function(eventPartition, streamId) {
	if(streamId === undefined) {
		throw new Error('StreamId must be defined!');
	}
	this._partition = eventPartition;
	this._streamId = streamId;
	
	this._uncommittedEvents = [];
	this._version = 0;
//	this._prepareStream();
}

EventStream.prototype._prepareStream = function(callback) {
	this._committedEvents = [];
	this._commitSequence = -1;
	var self = this;

	return this._partition._queryStream(this._streamId, callback).then(function(commits) {
		if(!commits) {
			commits = [];
			this._version = -1;
		}

		for(var i=0; i<commits.length; i++) {
			self._commitSequence++;
			for(var j=0;j<commits[i].events.length;j++) {
				self._committedEvents.push(commits[i].events[j]);
			}
		}
	}).then(function(){
		return self;  
	});
};

EventStream.prototype.getVersion = function() {
	return this._version;
};

EventStream.prototype.append = function(event) {
	this._uncommittedEvents.push(event);
};

EventStream.prototype.hasChanges = function() {
	return this._uncommittedEvents.length > 0;
};

EventStream.prototype.commit = function(commitId, callback) {
	var self = this;
	if(!this.hasChanges()) {
		//nothing to commit
		Promise.resolve().nodeify(callback);
	} else {
		var commit = this._buildCommit(commitId, this._uncommittedEvents);
		return this._partition.append(commit, callback).then(function(commit) {
			self._clearChanges();
			return self._prepareStream(callback);
		});
	}
}

EventStream.prototype._clearChanges = function() {
	this._uncommittedEvents = [];
};

EventStream.prototype.revertChanges = function() {
	//trunc the uncomitted events log
	var arr = this._uncommittedEvents;
	this._uncommittedEvents = [];
	delete arr;
};


EventStream.prototype._buildCommit = function(commitId, events) {
	var commit = new Commit(commitId, this._partition._partitionId, this._streamId, this._commitSequence++, events);
	return commit;
};

EventStream.prototype.getCommittedEvents = function() {
	return this._committedEvents.slice();
};

EventStream.prototype.getUncommittedEvents = function() {
	return this._uncommittedEvents.slice();
};


module.exports = EventStream;