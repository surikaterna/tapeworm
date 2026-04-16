import { connect, type ChannelModel, type ConfirmChannel } from "amqplib";
import type { ICommit } from "tapeworm";
import type { RabbitConfig } from "./types";

/**
 * Publishes commits to a RabbitMQ fanout exchange with publisher confirms.
 * Each publish() call waits for broker acknowledgement before resolving.
 */
export class CommitPublisher {
  private readonly _config: RabbitConfig;
  private readonly _tenant: string | undefined;
  private _connection: ChannelModel | null = null;
  private _channel: ConfirmChannel | null = null;

  constructor(config: RabbitConfig, tenant?: string) {
    this._config = config;
    this._tenant = tenant;
  }

  /**
   * Connect to RabbitMQ, create a confirm channel, and assert the fanout exchange.
   */
  async connect(): Promise<void> {
    this._connection = await connect(this._config.uri);
    this._channel = await this._connection.createConfirmChannel();

    await this._channel.assertExchange(this._config.exchange, "fanout", {
      durable: true,
    });
  }

  /**
   * Publish a commit to the fanout exchange with publisher confirms.
   * Resolves after the broker acknowledges receipt.
   * Rejects if the broker nacks or an error occurs.
   */
  async publish(commit: ICommit, collectionName: string): Promise<void> {
    if (!this._channel) {
      throw new Error("Must call connect() before publish()");
    }

    const body = Buffer.from(JSON.stringify(commit));
    const headers: Record<string, string> = {
      collection: collectionName,
      partitionId: commit.partitionId,
      streamId: commit.streamId,
    };

    if (this._tenant) {
      headers.tenant = this._tenant;
    }

    return new Promise<void>((resolve, reject) => {
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
  }

  /** Close the channel and connection gracefully. */
  async close(): Promise<void> {
    if (this._channel) {
      await this._channel.close();
      this._channel = null;
    }
    if (this._connection) {
      await this._connection.close();
      this._connection = null;
    }
  }
}
