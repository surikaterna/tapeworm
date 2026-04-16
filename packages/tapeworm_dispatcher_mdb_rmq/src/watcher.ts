import { EventEmitter } from "events";
import type {
  ChangeStream,
  ChangeStreamInsertDocument,
  Collection,
  Document,
} from "mongodb";
import type { ICommit } from "tapeworm";
import type { MongoConfig, ResumeState } from "./types";

type CommitHandler = (commit: ICommit, resumeToken: Document) => Promise<void>;

/**
 * Tails a MongoDB commits collection via change stream.
 * Falls back to querying by .token field when the oplog window expires.
 */
export class CommitWatcher extends EventEmitter {
  private readonly _config: MongoConfig;
  private _collection: Collection | null = null;
  private _changeStream: ChangeStream | null = null;
  private _stopped = false;

  constructor(config: MongoConfig) {
    super();
    this._config = config;
  }

  async connect(): Promise<void> {
    this._collection = this._config.db.collection(this._config.collection);
  }

  /**
   * Start watching for new commits.
   * Blocks until stop() is called or the stream ends.
   */
  async start(
    resumeState: ResumeState | null,
    handler: CommitHandler,
  ): Promise<void> {
    this._stopped = false;

    if (resumeState?.changeStreamToken) {
      try {
        await this._watchChangeStream(resumeState.changeStreamToken, handler);
        return;
      } catch {
        // Token expired — fall back to .token cursor
        this.emit("fallback");
      }
    }

    // Fallback: replay from lastCommitToken, then switch to change stream
    if (resumeState?.lastCommitToken) {
      await this._replayFromToken(resumeState.lastCommitToken, handler);
    }

    if (!this._stopped) {
      await this._watchChangeStream(undefined, handler);
    }
  }

  async stop(): Promise<void> {
    this._stopped = true;
    if (this._changeStream) {
      await this._changeStream.close();
      this._changeStream = null;
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

    for await (const change of this._changeStream) {
      if (this._stopped) break;
      const insertChange = change as ChangeStreamInsertDocument;
      const commit = insertChange.fullDocument as unknown as ICommit;
      await handler(commit, insertChange._id as Document);
    }
  }

  private async _replayFromToken(
    lastToken: string,
    handler: CommitHandler,
  ): Promise<void> {
    const cursor = this._collection!.find({
      token: { $gt: lastToken },
    }).sort({ token: 1 });

    const placeholder: Document = { _replayFallback: true };

    for await (const doc of cursor) {
      if (this._stopped) break;
      await handler(doc as unknown as ICommit, placeholder);
    }
  }
}
