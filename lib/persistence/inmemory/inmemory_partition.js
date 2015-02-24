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

InMemoryPartition.prototype._promisify = function(value, callback) {
	return Promise.resolve(value).nodeify(callback);
};

InMemoryPartition.prototype.append = function(commit, callback) {
	commit.isDispatched = false;
	//check for duplicates
	if(_.contains(this._commitIds, commit.id)) {
		throw new DuplicateCommitError("Duplicate commit of " + commit.id);
	}
	var concurrencyKey = commit.streamId+'-'+commit.commitSequence;
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