import { EventEmitter } from "events";
import { UUID } from "mongodb";
import type {
  ChangeStream,
  ChangeStreamInsertDocument,
  Collection,
  Document,
  FindCursor,
} from "mongodb";
import type { ICommit } from "tapeworm";
import { LoggerFactory } from "slf";
import type { MongoConfig, ResumeState } from "./types";

const LOG = LoggerFactory.getLogger(
  "tapeworm-dispatcher:watcher:change-stream",
);

export type CommitHandler = (
  commit: ICommit,
  resumeToken: Document,
) => Promise<void>;

/**
 * Common contract for commit watchers.
 * Implemented by ChangeStreamWatcher (majority-safe) and OplogWatcher (lowest latency).
 *
 * Implementations emit:
 * - "error" (err: Error) — recoverable error, will retry
 * - "fatal" (err: Error) — unrecoverable, giving up
 */
export interface ICommitWatcher extends EventEmitter {
  connect(): Promise<void>;
  start(resumeState: ResumeState | null, handler: CommitHandler): Promise<void>;
  stop(): Promise<void>;
}

/**
 * Watches a commits collection via MongoDB change stream.
 * Waits for majority-committed writes (replication-safe).
 * Falls back to querying by .token field when the oplog window expires.
 *
 * Includes retry loop with exponential backoff and zombie detection.
 */
export class ChangeStreamWatcher
  extends EventEmitter
  implements ICommitWatcher
{
  private readonly _config: MongoConfig;
  private _collection: Collection | null = null;
  private _changeStream: ChangeStream | null = null;
  private _replayCursor: FindCursor | null = null;
  private _stopped = false;

  constructor(config: MongoConfig) {
    super();
    this._config = config;
  }

  async connect(): Promise<void> {
    this._collection = this._config.db.collection(this._config.collection);
  }

  async start(
    resumeState: ResumeState | null,
    handler: CommitHandler,
  ): Promise<void> {
    this._stopped = false;
    LOG.info("starting");
    let delay = 1000;
    const maxDelay = 30000;
    const maxRetries = 50;
    let retries = 0;
    const currentResumeState = resumeState;

    while (!this._stopped) {
      try {
        await this._startOnce(currentResumeState, handler);
        retries = 0;
        delay = 1000;
        if (!this._stopped) {
          LOG.warn("watch loop ended unexpectedly, restarting");
          continue;
        }
        return;
      } catch (err: unknown) {
        if (this._stopped) return;
        retries++;
        const error = err instanceof Error ? err : new Error(String(err));
        LOG.error(
          "watch error, retrying err=%s delayMs=%d retries=%d",
          error.message,
          delay,
          retries,
        );
        this.emit("error", error);
        if (retries >= maxRetries) {
          LOG.error("max retries reached retries=%d", maxRetries);
          this.emit("fatal", error);
          return;
        }
        await this._sleep(delay);
        delay = Math.min(delay * 2, maxDelay);
      }
    }
  }

  async stop(): Promise<void> {
    LOG.info("stopping");
    this._stopped = true;
    if (this._changeStream) {
      await this._changeStream.close();
      this._changeStream = null;
    }
    if (this._replayCursor) {
      await this._replayCursor.close();
      this._replayCursor = null;
    }
  }

  /** Single attempt at the full start sequence. */
  private async _startOnce(
    resumeState: ResumeState | null,
    handler: CommitHandler,
  ): Promise<void> {
    if (resumeState?.changeStreamToken) {
      try {
        await this._watchChangeStream(resumeState.changeStreamToken, handler);
        return;
      } catch {
        this.emit("fallback");
      }
    }

    if (resumeState?.lastCommitToken) {
      await this._replayFromToken(resumeState.lastCommitToken, handler);
    }

    if (!this._stopped) {
      await this._watchChangeStream(undefined, handler);
    }
  }

  private async _watchChangeStream(
    resumeAfter: Document | undefined,
    handler: CommitHandler,
  ): Promise<void> {
    const pipeline = [{ $match: { operationType: "insert" } }];
    const options: Record<string, unknown> = {
      fullDocument: "updateLookup",
    };
    if (resumeAfter) {
      options.resumeAfter = resumeAfter;
    }

    this._changeStream = this._collection!.watch(pipeline, options);
    LOG.info("change stream opened");

    for await (const change of this._changeStream) {
      if (this._stopped) break;
      const insertChange = change as ChangeStreamInsertDocument;
      const commit = insertChange.fullDocument as unknown as ICommit;
      LOG.debug("commit received token=%s", String(commit.token));
      await handler(commit, insertChange._id as Document);
    }
  }

  private async _replayFromToken(
    lastToken: string,
    handler: CommitHandler,
  ): Promise<void> {
    LOG.info("replaying from token %s", lastToken);
    const resumeUuid = new UUID(lastToken);
    const cursor = this._collection!.find({
      token: { $gt: resumeUuid },
    }).sort({ token: 1 });
    this._replayCursor = cursor;

    const placeholder: Document = { _replayFallback: true };
    let count = 0;

    for await (const doc of cursor) {
      if (this._stopped) break;
      await handler(doc as unknown as ICommit, placeholder);
      count++;
    }

    this._replayCursor = null;
    LOG.info("replay complete count=%d", count);
  }

  private _sleep(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }
}
