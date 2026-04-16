import { EventEmitter } from "events";
import { UUID } from "mongodb";
import type {
  ChangeStream,
  ChangeStreamInsertDocument,
  Collection,
  Document,
} from "mongodb";
import type { ICommit } from "tapeworm";
import type { MongoConfig, ResumeState } from "./types";

export type CommitHandler = (
  commit: ICommit,
  resumeToken: Document,
) => Promise<void>;

/**
 * Common contract for commit watchers.
 * Implemented by ChangeStreamWatcher (majority-safe) and OplogWatcher (lowest latency).
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
 */
export class ChangeStreamWatcher
  extends EventEmitter
  implements ICommitWatcher
{
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
    const resumeUuid = new UUID(lastToken);
    const cursor = this._collection!.find({
      token: { $gt: resumeUuid },
    }).sort({ token: 1 });

    const placeholder: Document = { _replayFallback: true };

    for await (const doc of cursor) {
      if (this._stopped) break;
      await handler(doc as unknown as ICommit, placeholder);
    }
  }
}
