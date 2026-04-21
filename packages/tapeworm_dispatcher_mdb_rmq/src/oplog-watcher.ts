import { EventEmitter } from "events";
import { UUID, Timestamp } from "mongodb";
import type { Collection, Db, Document, FindCursor } from "mongodb";
import type { ICommit } from "tapeworm";
import { LoggerFactory } from "slf";
import type { MongoConfig, ResumeState } from "./types";
import type { CommitHandler, ICommitWatcher } from "./watcher";

const LOG = LoggerFactory.getLogger("tapeworm-dispatcher:watcher:oplog");

/**
 * Watches commits by tailing the MongoDB oplog (local.oplog.rs) directly.
 * Sees inserts immediately on the primary — does NOT wait for majority replication.
 *
 * Trade-off: lowest possible latency, but writes may be rolled back
 * if the primary loses election before the write replicates.
 *
 * Includes retry loop with exponential backoff and zombie detection.
 */
export class OplogWatcher extends EventEmitter implements ICommitWatcher {
  private readonly _config: MongoConfig;
  private _db: Db | null = null;
  private _oplog: Collection | null = null;
  private _namespace: string | null = null;
  private _cursor: FindCursor | null = null;
  private _replayCursor: FindCursor | null = null;
  private _stopped = false;

  constructor(config: MongoConfig) {
    super();
    this._config = config;
  }

  async connect(): Promise<void> {
    this._db = this._config.db;
    const dbName = this._db.databaseName;
    this._namespace = `${dbName}.${this._config.collection}`;
    this._oplog = this._db.client.db("local").collection("oplog.rs");
  }

  async start(
    resumeState: ResumeState | null,
    handler: CommitHandler,
  ): Promise<void> {
    this._stopped = false;
    if (!this._db) {
      throw new Error("connect() must be called before start()");
    }
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
    if (this._cursor) {
      await this._cursor.close();
      this._cursor = null;
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
      const ts = resumeState.changeStreamToken.ts as Timestamp | undefined;
      if (ts) {
        try {
          await this._tailOplog(ts, handler);
          return;
        } catch {
          this.emit("fallback");
        }
      }
    }

    if (resumeState?.lastCommitToken) {
      await this._replayFromToken(resumeState.lastCommitToken, handler);
    }

    if (!this._stopped) {
      const now = Timestamp.fromNumber(Math.floor(Date.now() / 1000));
      await this._tailOplog(now, handler);
    }
  }

  /**
   * Open a tailable-await cursor on oplog.rs, filtering for inserts
   * on the target namespace.
   */
  private async _tailOplog(
    afterTs: Timestamp,
    handler: CommitHandler,
  ): Promise<void> {
    this._cursor = this._oplog!.find(
      {
        ts: { $gt: afterTs },
        op: "i",
        ns: this._namespace,
      },
      {
        tailable: true,
        awaitData: true,
        noCursorTimeout: true,
      },
    );

    LOG.info("oplog tail opened afterTs=%s", afterTs.toString());

    for await (const doc of this._cursor) {
      if (this._stopped) break;
      const commit = doc.o as unknown as ICommit;
      const resumeToken: Document = { ts: doc.ts };
      LOG.debug("commit received token=%s", String(commit.token));
      await handler(commit, resumeToken);
    }

    if (!this._stopped) {
      throw new Error("oplog cursor ended unexpectedly");
    }
  }

  /**
   * Fallback replay: query the commits collection by .token > lastSeen.
   */
  private async _replayFromToken(
    lastToken: string,
    handler: CommitHandler,
  ): Promise<void> {
    LOG.info("replaying from token %s", lastToken);
    const collection = this._db!.collection(this._config.collection);
    const resumeUuid = new UUID(lastToken);
    const cursor = collection
      .find({ token: { $gt: resumeUuid } })
      .sort({ token: 1 });
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
