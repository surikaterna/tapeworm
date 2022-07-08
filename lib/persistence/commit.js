var Commit = function (id, partitionId, streamId, commitSequence, events, correlationId) {
  this.id = id;
  this.partitionId = partitionId;
  this.streamId = streamId;
  this.commitSequence = commitSequence;
  this.events = events;
	//this.eventTypes = [];
	//this.metadata = {};
  this.correlationId = correlationId;
};

module.exports = Commit;
