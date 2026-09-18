import { UUID } from "mongodb";
import type { Collection, FindCursor, Timestamp } from "mongodb";
import type { ICommit } from "tapeworm";
import type { MongoConfig, RecoveryPosition } from "../types";
import { decodeCommit, record, timestamp, uuid } from "../validation";

export interface HistoryPort {
  boundary(): Promise<Timestamp>;
  upper(): Promise<string | undefined>;
  scan(position: RecoveryPosition): AsyncIterable<ICommit, void, unknown>;
  close(): Promise<void>;
}

export class MongoHistory implements HistoryPort {
  private readonly collection: Collection<Record<string, unknown>>;
  private cursor?: FindCursor<Record<string, unknown>>;

  constructor(private readonly config: MongoConfig) {
    this.collection = config.db.collection(config.collection);
    if (!Number.isSafeInteger(config.batchSize ?? 256) || (config.batchSize ?? 256) < 1) {
      throw new Error("batchSize must be a positive integer");
    }
  }

  async boundary(): Promise<Timestamp> {
    const session = this.config.db.client.startSession({ causalConsistency: true });
    try {
      // A primary majority read initializes operationTime even for an empty collection.
      await this.collection.findOne({}, { session, projection: { _id: 1 },
        readConcern: { level: "majority" }, readPreference: "primary" });
      return timestamp(session.operationTime);
    } finally { await session.endSession(); }
  }

  async upper(): Promise<string | undefined> {
    const indexes: unknown[] = await this.collection.listIndexes().toArray();
    const indexed = indexes.some((value) => {
      const index = record(value);
      const key = record(index.key);
      return Object.keys(key).length === 1 && key.token === 1 && !index.partialFilterExpression && !index.sparse;
    });
    if (!indexed) throw new Error("Recovery requires an existing complete {token:1} index; provision offline");
    const doc = await this.collection.findOne({}, { sort: { token: -1 }, hint: { token: 1 },
      projection: { token: 1 }, readConcern: { level: "majority" }, readPreference: "primary" });
    return doc ? uuid(doc.token) : undefined;
  }

  async *scan(position: RecoveryPosition): AsyncIterable<ICommit, void, unknown> {
    if (!position.upper) return;
    const lower = position.cursor ? { $gte: new UUID(position.cursor) } : { $gt: new UUID(position.lower) };
    const cursor = this.collection.find({ token: { ...lower, $lte: new UUID(position.upper) } }, {
      hint: { token: 1 }, sort: { token: 1 }, batchSize: this.config.batchSize ?? 256,
      readConcern: { level: "majority" }, readPreference: "primary",
    });
    this.cursor = cursor;
    try { for await (const doc of cursor) yield decodeCommit(doc); }
    finally { await cursor.close(); this.cursor = undefined; }
  }

  async close(): Promise<void> { await this.cursor?.close(); }
}
