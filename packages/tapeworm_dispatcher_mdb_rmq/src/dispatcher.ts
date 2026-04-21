import { EventEmitter } from "events";
import { UUID } from "mongodb";
import type { Document } from "mongodb";
import type { ICommit } from "tapeworm";
import { LoggerFactory } from "slf";
import type { DispatcherConfig, DispatcherEvents, ResumeState } from "./types";
import { ChangeStreamWatcher } from "./watcher";
import type { ICommitWatcher } from "./watcher";
import { OplogWatcher } from "./oplog-watcher";
import { CommitPublisher } from "./publisher";

const LOG = LoggerFactory.getLogger("tapeworm-dispatcher");

/**
 * Dispatcher: tails a MongoDB commits collection and publishes each
 * new commit to a RabbitMQ headers exchange.
 *
 * Supports two watch modes:
 *   - "changeStream" (default): majority-safe, uses MongoDB change streams
 *   - "oplog": lowest latency, tails local.oplog.rs directly
 *
 * Both modes provide at-least-once delivery with two-level resume:
 *   1. Primary token (change stream resume token or oplog Timestamp)
 *   2. UUID v7 .token field on commits (fallback when oplog expires)
 */
export class Dispatcher extends EventEmitter {
  private readonly _config: DispatcherConfig;
  private readonly _watcher: ICommitWatcher;
  private readonly _publisher: CommitPublisher;

  private _lastFailedCommitId: string | null = null;
  private _consecutiveFailures = 0;
  private static readonly MAX_COMMIT_RETRIES = 3;

  private _heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private _dispatchCount = 0;
  private _startedAt: Date | null = null;

  constructor(config: DispatcherConfig) {
    super();
    this._config = config;

    this._watcher =
      config.watchMode === "oplog"
        ? new OplogWatcher(config.mongodb)
        : new ChangeStreamWatcher(config.mongodb);
    this._publisher = new CommitPublisher(config.rabbitmq, config.tenant);

    this._watcher.on("fallback", () => this.emit("fallback"));
    this._watcher.on("error", (err) => this.emit("error", err));
    this._watcher.on("fatal", (err) => this.emit("fatal", err));
  }

  /**
   * Start the dispatcher:
   * 1. Connect to MongoDB and RabbitMQ
   * 2. Load resume state
   * 3. Begin tailing (change stream or oplog based on config)
   *
   * Note: start() is long-running — blocks until stop() is called or stream ends.
   */
  async start(): Promise<void> {
    LOG.info("starting dispatcher");
    await this._watcher.connect();
    await this._publisher.connect();

    const resumeState = await this._config.resumeTokenStore.load();
    if (resumeState) {
      this.emit("resumed", resumeState);
    }

    this.emit("started");

    this._startedAt = new Date();
    this._dispatchCount = 0;
    this._heartbeatTimer = setInterval(() => {
      LOG.info(
        "heartbeat dispatched=%d uptime=%ds",
        this._dispatchCount,
        Math.floor((Date.now() - this._startedAt!.getTime()) / 1000),
      );
    }, 60000);
    this._heartbeatTimer.unref();

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
    try {
      await this._publisher.publish(commit, this._config.mongodb.collection);
    } catch (err: any) {
      if (commit.id === this._lastFailedCommitId) {
        this._consecutiveFailures++;
      } else {
        this._lastFailedCommitId = commit.id;
        this._consecutiveFailures = 1;
      }

      if (this._consecutiveFailures >= Dispatcher.MAX_COMMIT_RETRIES) {
        LOG.error(
          "skipping poison commit commitId=%s streamId=%s failures=%d: %s",
          commit.id,
          commit.streamId,
          this._consecutiveFailures,
          err.message,
        );
        this.emit(
          "error",
          new Error(`skipped poison commit ${commit.id}: ${err.message}`),
        );
        this._lastFailedCommitId = null;
        this._consecutiveFailures = 0;
      } else {
        throw err;
      }
    }

    this._lastFailedCommitId = null;
    this._consecutiveFailures = 0;

    const isReplay = "_replayFallback" in resumeToken;
    const state: ResumeState = {
      changeStreamToken: isReplay ? undefined : resumeToken,
      lastCommitToken:
        commit.token instanceof UUID
          ? commit.token.toHexString()
          : ((commit.token as string) ?? undefined),
      updatedAt: new Date(),
    };

    await this._config.resumeTokenStore.save(state);
    LOG.debug("commit dispatched token=%s", String(commit.token));
    this.emit("dispatched", commit);
    this._dispatchCount++;
  }

  /**
   * Graceful shutdown: close the watcher, drain in-flight publishes,
   * then close RabbitMQ connection.
   */
  async stop(): Promise<void> {
    LOG.info("stopping dispatcher");
    if (this._heartbeatTimer) {
      clearInterval(this._heartbeatTimer);
      this._heartbeatTimer = null;
    }
    await this._watcher.stop();
    await this._publisher.close();
    this.emit("stopped");
  }
}
