interface Event<Payload = Record<string, any>> {
  type: string;
  aggregateId: string;
  correlationId: string;
  payload: Payload;
  id?: string;
  dateTime?: string;
  headers?: Record<string, any>;
  version?: number;
}

export interface Commit<Payload> {
  id: string;
  partitionId: string;
  streamId: string;
  commitSequence: number;
  events: Event<Payload>[];
}

export interface Snapshot {
  id: string;
  version: number;
  storedDateTime?: string;
  snapshot?: Record<string, any>;
}
