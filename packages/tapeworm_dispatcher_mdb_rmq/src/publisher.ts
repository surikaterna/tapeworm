import type { ICommit } from "tapeworm";
import type { RabbitConfig } from "./types";
import { ConfirmedChannel } from "./confirmed-channel";
import { openRabbit, type RabbitConnection } from "./rabbit-connection";
import { encodePublication, validatePublicationPolicy, type PublicationPolicy } from "./publication-policy";

/** Persistent mandatory publication. Failure is retried from the durable CDC position. */
export class CommitPublisher {
  private resource?: RabbitConnection;
  private disposing: Promise<void> = Promise.resolve();
  private confirmed?: ConfirmedChannel;
  private connecting?: Promise<void>;
  private stopped = false;
  private generation = 0;
  private cancelDelay?: () => void;
  private readonly quietError = () => {};

  constructor(private readonly config: RabbitConfig, private readonly tenant?: string,
    private readonly publication?: PublicationPolicy) {
    validatePublicationPolicy(publication);
    for (const value of [config.maxPending ?? 256, config.confirmTimeoutMs ?? 30000]) {
      if (!Number.isSafeInteger(value) || value < 1) throw new Error("Publisher limits must be positive integers");
    }
  }

  async connect(): Promise<void> {
    if (this.stopped) throw new Error("Publisher stopped");
    if (this.confirmed) return;
    if (!this.connecting) {
      this.connecting = this.establish().finally(() => { this.connecting = undefined; });
    }
    await this.connecting;
  }

  private async establish(): Promise<void> {
    const generation = ++this.generation;
    let resource: RabbitConnection | undefined;
    try {
      await this.disposing;
      resource = await openRabbit(this.config, () => { this.disconnected(generation); });
      if (this.stopped || generation !== this.generation) throw new Error("Connection superseded");
      this.resource = resource;
      this.confirmed = new ConfirmedChannel(resource.channel, this.config.confirmTimeoutMs ?? 30000,
        this.config.maxPending ?? 256, () => { this.disconnected(generation); });
    } catch {
      await resource?.dispose();
      // Do not expose driver errors containing a credential-bearing URI.
      throw new Error("Rabbit connection/setup failed");
    }
  }

  private disconnected(generation: number): void {
    if (generation !== this.generation) return;
    ++this.generation;
    this.confirmed?.close(new Error("Rabbit disconnected; publication outcome may be unknown"));
    this.confirmed = undefined;
    const resource = this.resource;
    this.resource = undefined;
    this.disposing = resource?.dispose() ?? this.disposing;
    if (!this.stopped) void this.reconnect();
  }

  private async reconnect(): Promise<void> {
    // Both channel and connection close share this single reconnect promise.
    if (this.connecting) {
      await this.connecting.catch(this.quietError);
      if (this.stopped || this.confirmed) return;
    }
    if (this.connecting) return;
    this.connecting = this.retryConnection().finally(() => { this.connecting = undefined; });
    await this.connecting;
  }

  private async retryConnection(): Promise<void> {
    let delay = 100;
    while (!this.isStopped()) {
      await this.pause(delay);
      if (this.isStopped()) return;
      try { await this.establish(); return; }
      catch { delay = Math.min(delay * 2, 30000); }
    }
  }

  private isStopped(): boolean { return this.stopped; }

  /** Requires a validated ICommit; application event schemas belong to the source/consumer. */
  async publish(commit: ICommit, collectionName: string): Promise<void> {
    if (this.stopped) throw new Error("Publisher stopped");
    const confirmed = this.confirmed;
    if (!confirmed) throw new Error("Rabbit unavailable; retry from checkpoint");
    confirmed.assertCapacity();
    const body = encodePublication(commit, this.publication);
    const headers: Record<string, string> = { collection: collectionName,
      partitionId: commit.partitionId, streamId: commit.streamId };
    if (this.tenant) headers.tenant = this.tenant;
    await confirmed.publish(this.config.exchange, body, {
      contentType: "application/json", deliveryMode: 2, messageId: commit.id,
      timestamp: Math.floor(Date.now() / 1000), headers,
    });
  }

  private pause(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const finish = () => { clearTimeout(timer); this.cancelDelay = undefined; resolve(); };
      const timer = setTimeout(finish, ms);
      this.cancelDelay = finish;
    });
  }

  async close(): Promise<void> {
    this.stopped = true;
    ++this.generation;
    this.cancelDelay?.();
    this.confirmed?.close();
    this.confirmed = undefined;
    await this.resource?.dispose();
    await this.disposing;
    await this.connecting?.catch(this.quietError);
    this.resource = undefined;
  }
}
