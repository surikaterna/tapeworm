var Commit = function(id, partitionId, streamId, commitSequence, events)
{
	this.id = id;
	this.partitionId = partitionId;
	this.streamId = streamId;
	this.commitSequence = commitSequence;
	this.events = events;
	//this.eventTypes = [];
	//this.metadata = {};
}

module.exports = Commit;