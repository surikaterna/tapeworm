import type { IBaseEvent, ICommit } from "../types";

var Commit = function <TEvent extends IBaseEvent = IBaseEvent>(
  this: ICommit<TEvent>,
  id: string,
  partitionId: string,
  streamId: string,
  commitSequence: number,
  events: TEvent[]
) {
  this.id = id;
  this.partitionId = partitionId;
  this.streamId = streamId;
  this.commitSequence = commitSequence;
  this.events = events;
} as unknown as new <TEvent extends IBaseEvent = IBaseEvent>(
  id: string,
  partitionId: string,
  streamId: string,
  commitSequence: number,
  events: TEvent[]
) => ICommit<TEvent>;

export default Commit;
