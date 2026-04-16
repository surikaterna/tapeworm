import { EventEmitter } from "events";
import { UUID, Timestamp } from "mongodb";
import type { Collection, Db, Document, FindCursor } from "mongodb";
import type { ICommit } from "tapeworm";
import type { MongoConfig, ResumeState } from "./types";
import type { CommitHandler, ICommitWatcher } from "./watcher";

/**
 * Watches commits by tailing the MongoDB oplog (local.oplog.rs) directly.
 * Sees inserts immediately on the primary — does NOT wait for majority replication.
 *
 * Trade-off: lowest possible latency, but writes may be rolled back
 * if the primary loses election before the write replicates.
 */
export class OplogWatcher extends EventEmitter implements ICommitWatcher {
  private readonly _config: MongoConfig;
  private _db: Db | null = null;
  private _oplog: Collection | null = null;
  private _namespace: string | null = null;
  private _cursor: FindCursor | null = null;
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

    // Try resuming from saved oplog timestamp
    if (resumeState?.changeStreamToken) {
      try {
        const ts = resumeState.changeStreamToken.ts as Timestamp | undefined;
        if (ts) {
          await this._tailOplog(ts, handler);
          return;
        }
      } catch {
        this.emit("fallback");
      }
    }

    // Fallback: replay from last commit .token, then switch to oplog tail
    if (resumeState?.lastCommitToken) {
      await this._replayFromToken(resumeState.lastCommitToken, handler);
    }

    // Start tailing from now (no historical position)
    if (!this._stopped) {
      const now = Timestamp.fromNumber(Math.floor(Date.now() / 1000));
      await this._tailOplog(now, handler);
    }
  }

  async stop(): Promise<void> {
    this._stopped = true;
    if (this._cursor) {
      await this._cursor.close();
      this._cursor = null;
    }
  }

  /**
   * Open a tailable-await cursor on oplog.rs, filtering for inserts
   * on the target namespace. Each matching oplog entry contains the
   * full inserted document in the `o` field.
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

    for await (const doc of this._cursor) {
      if (this._stopped) break;

      // oplog insert entries carry the full document in `o`
      const commit = doc.o as unknown as ICommit;

      // Save the oplog timestamp as the resume token
      const resumeToken: Document = { ts: doc.ts };
      await handler(commit, resumeToken);
    }
  }

  /**
   * Fallback replay: query the commits collection by .token > lastSeen,
   * then hand off to oplog tailing.
   */
  private async _replayFromToken(
    lastToken: string,
    handler: CommitHandler,
  ): Promise<void> {
    const collection = this._db!.collection(this._config.collection);
    const resumeUuid = new UUID(lastToken);
    const cursor = collection
      .find({ token: { $gt: resumeUuid } })
      .sort({ token: 1 });

    const placeholder: Document = { _replayFallback: true };

    for await (const doc of cursor) {
      if (this._stopped) break;
      await handler(doc as unknown as ICommit, placeholder);
    }
  }
}
