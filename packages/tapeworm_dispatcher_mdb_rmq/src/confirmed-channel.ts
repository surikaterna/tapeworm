import { randomUUID } from "node:crypto";
import { record } from "./validation";

export interface PublishProperties {
  contentType?: string;
  deliveryMode?: number;
  messageId?: string;
  timestamp?: number;
  headers?: Record<string, string>;
  mandatory?: boolean;
  correlationId?: string;
}

export interface ConfirmPort {
  publish(exchange: string, key: string, content: Buffer, options: PublishProperties,
    callback: (error: unknown) => void): boolean;
  on(event: string, listener: (...args: unknown[]) => void): unknown;
  removeListener(event: string, listener: (...args: unknown[]) => void): unknown;
}

interface Attempt { finish(error?: Error): void }

/** Rabbit sends basic.return before the corresponding publisher confirm. */
export class ConfirmedChannel {
  private readonly pending = new Map<string, Attempt>();
  private writable = true;
  private stopped = false;
  private readonly returned = (value: unknown) => {
    try {
      const properties = record(record(value).properties);
      if (typeof properties.correlationId !== "string") throw new Error("Uncorrelated return");
      this.pending.get(properties.correlationId)?.finish(new Error("Mandatory publication unroutable"));
    } catch { this.failed(new Error("Invalid Rabbit return correlation")); }
  };
  private readonly drained = () => { this.writable = true; };
  private readonly closed = () => { this.failed(new Error("Rabbit channel closed")); };
  private readonly errored = () => { this.failed(new Error("Rabbit channel error")); };

  constructor(private readonly channel: ConfirmPort, private readonly timeoutMs: number,
    private readonly capacity: number, private readonly invalidate: () => void) {
    channel.on("return", this.returned);
    channel.on("drain", this.drained);
    channel.on("close", this.closed);
    channel.on("error", this.errored);
  }

  private assertCapacity(): void {
    if (this.stopped) throw new Error("Publisher channel stopped");
    if (!this.writable || this.pending.size >= this.capacity) {
      throw new Error("Publisher backpressure capacity reached");
    }
  }

  publish(exchange: string, body: Buffer, options: PublishProperties): Promise<void> {
    try { this.assertCapacity(); } catch (error: unknown) { return Promise.reject(error instanceof Error ? error : new Error("Capacity unavailable")); }
    const correlationId = randomUUID();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.failed(new Error("Publish confirm timeout; outcome unknown")); }, this.timeoutMs);
      const finish = (error?: Error) => {
        if (!this.pending.delete(correlationId)) return;
        clearTimeout(timer);
        if (error) reject(error); else resolve();
      };
      this.pending.set(correlationId, { finish });
      try {
        this.writable = this.channel.publish(exchange, "", body,
          { ...options, mandatory: true, correlationId }, (error: unknown) => {
            finish(error ? new Error("Rabbit negatively acknowledged publication", { cause: error }) : undefined);
          });
      } catch (error: unknown) {
        finish(new Error("Rabbit publication failed", { cause: error }));
      }
    });
  }

  private failed(error: Error): void { this.close(error); this.invalidate(); }

  close(error = new Error("Publisher stopped")): void {
    if (this.stopped) return;
    this.stopped = true;
    for (const attempt of this.pending.values()) attempt.finish(error);
    this.channel.removeListener("return", this.returned);
    this.channel.removeListener("drain", this.drained);
    this.channel.removeListener("close", this.closed);
    this.channel.removeListener("error", this.errored);
  }
}
