import {Event} from '../Event'

export interface Commit<Data extends object = Record<string, unknown>, Metadata extends object = Record<string, unknown>> {
  id: string;
  partitionId: string;
  streamId: string;
  commitSequence: number;
  events: Array<Event<Data, Metadata>>;
}

export class Commit<Data extends object = Record<string, unknown>, Metadata extends object = Record<string, unknown>> {
  constructor(id: string, partitionId: string, streamId: string, commitSequence: number, events: Array<Event<Data, Metadata>>) {
    this.id = id;
    this.partitionId = partitionId;
    this.streamId = streamId;
    this.commitSequence = commitSequence;
    this.events = events;
  }
}
