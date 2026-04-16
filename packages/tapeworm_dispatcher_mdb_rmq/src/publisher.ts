import { connect, type ChannelModel, type ConfirmChannel } from "amqplib";
import type { ICommit } from "tapeworm";
import { LoggerFactory } from "slf";
import type { RabbitConfig } from "./types";

const LOG = LoggerFactory.getLogger("tapeworm-dispatcher:publisher");

/**
 * Publishes commits to a RabbitMQ fanout exchange with publisher confirms.
 * Each publish() call waits for broker acknowledgement before resolving.
 * Includes connection resilience with heartbeat, reconnect, and publish retry.
 */
export class CommitPublisher {
  private readonly _config: RabbitConfig;
  private readonly _tenant: string | undefined;
  private _connection: ChannelModel | null = null;
  private _channel: ConfirmChannel | null = null;
  private _connected = false;
  private _reconnecting = false;
  private _stopped = false;

  constructor(config: RabbitConfig, tenant?: string) {
    this._config = config;
    this._tenant = tenant;
  }

  /**
   * Connect to RabbitMQ, create a confirm channel, and assert the fanout exchange.
   */
  async connect(): Promise<void> {
    LOG.info("connecting to %s", this._config.uri);
    await this._connectInternal();
    LOG.info("connected");
  }

  /**
   * Publish a commit with retry (max 3 attempts).
   * Waits for connection if currently reconnecting.
   */
  async publish(commit: ICommit, collectionName: string): Promise<void> {
    const maxAttempts = 3;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        await this._waitForConnection();
        await this._publishOnce(commit, collectionName);
        return;
      } catch (err: any) {
        LOG.warn(
          "publish failed attempt=%d commitId=%s: %s",
          attempt,
          commit.id,
          err.message,
        );
        if (attempt === maxAttempts) throw err;
        await this._sleep(500 * attempt);
      }
    }
  }

  /** Close the channel and connection gracefully. */
  async close(): Promise<void> {
    LOG.info("closing");
    this._stopped = true;
    this._connected = false;

    try {
      if (this._channel) {
        await this._channel.close();
        this._channel = null;
      }
    } catch (err: any) {
      LOG.error("error closing channel: %s", err.message);
    }

    try {
      if (this._connection) {
        await this._connection.close();
        this._connection = null;
      }
    } catch (err: any) {
      LOG.error("error closing connection: %s", err.message);
    }

    LOG.info("closed");
  }

  /** Shared connection logic for initial connect and reconnect. */
  private async _connectInternal(): Promise<void> {
    this._connection = await connect(this._config.uri, { heartbeat: 30 });

    this._connection.on("error", (err) => {
      LOG.error("connection error: %s", err.message);
    });
    this._connection.on("close", () => {
      LOG.warn("connection closed");
      this._onDisconnect();
    });

    this._channel = await this._connection.createConfirmChannel();

    this._channel.on("error", (err) => {
      LOG.error("channel error: %s", err.message);
    });
    this._channel.on("close", () => {
      LOG.warn("channel closed");
    });

    await this._channel.assertExchange(this._config.exchange, "fanout", {
      durable: true,
    });

    this._connected = true;
  }

  /** Handle unexpected disconnect — trigger reconnect if not stopped. */
  private _onDisconnect(): void {
    this._connected = false;
    this._channel = null;
    this._connection = null;
    if (!this._stopped) {
      void this._reconnect();
    }
  }

  /** Reconnect with exponential backoff (1s → 30s cap). */
  private async _reconnect(): Promise<void> {
    if (this._reconnecting || this._stopped) return;
    this._reconnecting = true;
    let delay = 1000;
    const maxDelay = 30000;

    while (!this._stopped) {
      LOG.info("reconnecting in %dms", delay);
      await this._sleep(delay);
      if (this._stopped) break;
      try {
        await this._connectInternal();
        LOG.info("reconnected");
        this._reconnecting = false;
        return;
      } catch (err: any) {
        LOG.error("reconnect failed: %s", err.message);
        delay = Math.min(delay * 2, maxDelay);
      }
    }
    this._reconnecting = false;
  }

  /** Publish a single commit to the fanout exchange with publisher confirms. */
  private async _publishOnce(
    commit: ICommit,
    collectionName: string,
  ): Promise<void> {
    if (!this._channel) {
      throw new Error("Must call connect() before publish()");
    }

    LOG.debug("publishing commitId=%s to %s", commit.id, this._config.exchange);

    const body = Buffer.from(JSON.stringify(commit));
    const headers: Record<string, string> = {
      collection: collectionName,
      partitionId: commit.partitionId,
      streamId: commit.streamId,
    };

    if (this._tenant) {
      headers.tenant = this._tenant;
    }

    const publishPromise = new Promise<void>((resolve, reject) => {
      this._channel!.publish(
        this._config.exchange,
        "", // fanout ignores routing key
        body,
        {
          contentType: "application/json",
          deliveryMode: 2, // persistent
          messageId: commit.id, // for consumer deduplication
          timestamp: Math.floor(Date.now() / 1000),
          headers,
        },
        (err) => {
          if (err) reject(err);
          else resolve();
        },
      );
    });

    const timeoutPromise = new Promise<never>((_, reject) => {
      const timer = setTimeout(() => {
        reject(new Error("publish confirm timeout (30s)"));
      }, 30000);
      timer.unref();
    });

    await Promise.race([publishPromise, timeoutPromise]);
  }

  /** Wait for an active connection, with timeout. */
  private _waitForConnection(timeoutMs = 30000): Promise<void> {
    if (this._connected && this._channel) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error("connection timeout")),
        timeoutMs,
      );
      const check = setInterval(() => {
        if (this._connected && this._channel) {
          clearTimeout(timer);
          clearInterval(check);
          resolve();
        }
        if (this._stopped) {
          clearTimeout(timer);
          clearInterval(check);
          reject(new Error("publisher stopped"));
        }
      }, 100);
    });
  }

  private _sleep(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }
}
