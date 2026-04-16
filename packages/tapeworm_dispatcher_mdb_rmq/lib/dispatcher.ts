import { EventEmitter } from "events";
import type { Document } from "mongodb";
import type { ICommit } from "tapeworm";
import type { DispatcherConfig, DispatcherEvents, ResumeState } from "./types";
import { CommitWatcher } from "./watcher";
import { CommitPublisher } from "./publisher";

/**
 * Dispatcher: tails a MongoDB commits collection via change stream
 * and publishes each new commit to a RabbitMQ fanout exchange.
 *
 * Provides at-least-once delivery with two-level resume:
 *   1. MongoDB change stream resume token (primary)
 *   2. UUID v7 .token field on commits (fallback when oplog expires)
 */
export class Dispatcher extends EventEmitter {
  private readonly _config: DispatcherConfig;
  private readonly _watcher: CommitWatcher;
  private readonly _publisher: CommitPublisher;

  constructor(config: DispatcherConfig) {
    super();
    this._config = config;
    this._watcher = new CommitWatcher(config.mongodb);
    this._publisher = new CommitPublisher(config.rabbitmq, config.tenant);

    // Forward watcher fallback events
    this._watcher.on("fallback", () => this.emit("fallback"));
  }

  /**
   * Start the dispatcher:
   * 1. Connect to MongoDB and RabbitMQ
   * 2. Load resume state
   * 3. Begin tailing the change stream
   *
   * Note: start() is long-running — blocks until stop() is called or stream ends.
   */
  async start(): Promise<void> {
    await this._watcher.connect();
    await this._publisher.connect();

    const resumeState = await this._config.resumeTokenStore.load();
    if (resumeState) {
      this.emit("resumed", resumeState);
    }

    this.emit("started");

    await this._watcher.start(resumeState, this._handleCommit.bind(this));
  }

  /**
   * Handle a single commit from the watcher:
   * 1. Publish to RabbitMQ (with confirms — waits for broker ack)
   * 2. Save resume token (only after confirmed publish)
   */
  private async _handleCommit(
    commit: ICommit,
    resumeToken: Document,
  ): Promise<void> {
    await this._publisher.publish(commit, this._config.mongodb.collection);

    const isReplay = "_replayFallback" in resumeToken;
    const state: ResumeState = {
      changeStreamToken: isReplay ? undefined : resumeToken,
      lastCommitToken: (commit.token as string) ?? undefined,
      updatedAt: new Date(),
    };

    await this._config.resumeTokenStore.save(state);
    this.emit("dispatched", commit);
  }

  /**
   * Graceful shutdown: close the change stream, drain in-flight publishes,
   * then close RabbitMQ and MongoDB connections.
   */
  async stop(): Promise<void> {
    await this._watcher.stop();
    await this._publisher.close();
    this.emit("stopped");
  }
}
