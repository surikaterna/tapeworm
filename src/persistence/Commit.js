class Commit {
  constructor(id, partitionId, streamId, commitSequence, events) {
    this.id = id;
    this.partitionId = partitionId;
    this.streamId = streamId;
    this.commitSequence = commitSequence;
    this.events = events;
  }
}

export default Commit;