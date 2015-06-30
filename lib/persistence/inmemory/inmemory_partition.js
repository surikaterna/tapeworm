var _ = require('lodash');
var Promise = require('bluebird');
var ConcurrencyError = require('../concurrency_error');
var DuplicateCommitError = require('../duplicate_commit_error');

var InMemoryPartition = function() {
	this._commits = [];
	this._streamIndex = {};
	this._commitIds = [];
	this._commitConcurrencyCheck = [];
}


function getConcurrencyKey(commit) {
	return commit.streamId+'-'+commit.commitSequence;
}
InMemoryPartition.prototype._promisify = function(value, callback) {
	return Promise.resolve(value).nodeify(callback);
};

InMemoryPartition.prototype.truncateStreamFrom = function(streamId, commitSequence, callback) {
	var self = this;
	var commits = this._streamIndex[streamId];
	for(var i=0;i<commits.length; i++) {
		var commit = commits[i];
		
		if(commit.commitSequence >= commitSequence) {
			//remove from commitId
			self._commitIds = _.without(self._commitIds, commit.id);
			self._commitConcurrencyCheck = _.without(self._commitConcurrencyCheck, getConcurrencyKey(commit));
			self._commits = _.without(self._commits, commit);
		}
	}
	commits = commits.slice(0, commitSequence);
	this._streamIndex[streamId] = commits;
	return this._promisify(this);
}

InMemoryPartition.prototype.applyCommitHeader = function(commitId, header, callback) {
	var self = this;
	var commit = _.find(this._commits, {id:commitId});
	if(commit) {
		_.assign(commit, header);
	} else {
		throw new Error("Trying to apply header to missing commit: " + commitId);
	}
	return this._promisify(commit, callback);
}


InMemoryPartition.prototype.append = function(commit, callback) {
	commit.isDispatched = false;
	//check for duplicates
	if(_.contains(this._commitIds, commit.id)) {
		throw new DuplicateCommitError("Duplicate commit of " + commit.id);
	}
	var concurrencyKey = getConcurrencyKey(commit);
	if(_.contains(this._commitConcurrencyCheck, concurrencyKey)) {
		throw new ConcurrencyError('Concurrency error on stream ' + commit.streamId);
	}
	//check concurrency
	this._commits.push(commit);
	this._commitIds.push(commit.id);
	this._commitConcurrencyCheck.push(concurrencyKey);
	var index = this._streamIndex[commit.streamId];
	if(!index) {
		index = this._streamIndex[commit.streamId] = [];
	}
	index.push(commit);
	return this._promisify(commit, callback);
};

InMemoryPartition.prototype.markAsDispatched = function(commit, callback) {
	commit.isDispatched = true;	
	return this._promisify(commit, callback);
};

InMemoryPartition.prototype.getUndispatched = function(callback) {
	var commits = this.queryAll;
	var undispatched = [];
	for(var i=0;i<commits.length; i++) {
		if(!commits[i].isDispatched) {
			undispatched.push(commit);
		}
	}
	return this._promisify(undispatched, callback);
};

InMemoryPartition.prototype.queryAll = function(callback) {
 	return this._promisify(this._commits.slice(), callback);
};

InMemoryPartition.prototype.queryStream = function(streamId, callback) {
	var result = this._streamIndex[streamId];
	
	if(result) {
		result = result.slice();
	}
		
	return this._promisify(result, callback);
};

module.exports = InMemoryPartition;