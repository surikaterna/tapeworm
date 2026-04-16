import type { IBaseEvent } from "./types";

interface IEvent extends IBaseEvent {
  payload: Record<string, unknown>;
  timestamp: Date;
  metadata: Record<string, unknown>;
  revision: number | null;
}

var Event = function (this: IEvent, id: string, type: string, payload: Record<string, unknown>, metadata?: Record<string, unknown>) {
  this.id = id;
  this.type = type;
  this.payload = payload;
  this.timestamp = new Date();
  this.metadata = metadata || {};
  this.revision = null; //filled by eventstore on append
} as unknown as new (
  id: string,
  type: string,
  payload: Record<string, unknown>,
  metadata?: Record<string, unknown>
) => IEvent;

export default Event;
