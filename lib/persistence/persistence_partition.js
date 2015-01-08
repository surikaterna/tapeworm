var PersistencePartition = function() {

}

PersistencePartition.prototype.append = function(commit, callback) {
	//add commit
};

PersistencePartition.prototype.getUndispatched = function(callback) {
	//return events which are not dispatched
};

PersistencePartition.prototype.markAsDispatched = function(commit, callback) {
	//return events which are not dispatched
};

PersistencePartition.prototype.queryAll = function() {
	//return Query
};

PersistencePartition.prototype.queryStream = function(streamId) {
	//return Query;
}
