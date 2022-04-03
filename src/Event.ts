export interface Event<Data extends object = Record<string, unknown>, Metadata extends object = Record<string, unknown>> {
  id: string;
  type: string;
  data: Data;
  timestamp: Date;
  metadata: Metadata;
  revision: string | null;
  version: number;
}

export class Event<Data extends object = Record<string, unknown>, Metadata extends object = Record<string, unknown>> {
  constructor(id: string, type: string, data: Data, metadata?: Metadata) {
    this.id = id;
    this.type = type;
    this.data = data;
    this.timestamp = new Date();
    this.metadata = metadata ?? {} as Metadata;
    // Filled by EventStore on append
    this.revision = null;
    this.version = 0;
  }
}
