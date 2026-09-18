import type { ChangeStream, Collection, FindCursor, Timestamp } from "mongodb";
import type { ICommit } from "tapeworm";
import type { MongoConfig, PrimaryPosition, WatchMode } from "../types";
import { cursorNext, HistoryExpired } from "./recovery-errors";
import { decodeCommit, record, timestamp } from "../validation";

export interface LiveItem { commit: ICommit; position: PrimaryPosition }
export interface LivePort {
  readonly mode: WatchMode;
  watch(position: PrimaryPosition): AsyncIterable<LiveItem, void, unknown>;
  close(): Promise<void>;
}

export class ChangeStreamSource implements LivePort {
  readonly mode = "changeStream";
  private stream?: ChangeStream<Record<string, unknown>>;
  constructor(private readonly config: MongoConfig) {}

  async *watch(position: PrimaryPosition): AsyncIterable<LiveItem, void, unknown> {
    if (position.kind === "oplog" || (position.kind === "boundary" && position.mode !== this.mode)) {
      throw new Error("Checkpoint watch mode mismatch");
    }
    const options = position.kind === "changeStream"
      ? { resumeAfter: position.token } : { startAtOperationTime: position.ts };
    const stream = this.config.db.collection<Record<string, unknown>>(this.config.collection)
      .watch([{ $match: { operationType: "insert" } }], { ...options, maxAwaitTimeMS: 1000 });
    this.stream = stream;
    try {
      while (!stream.closed) {
        const value: unknown = await cursorNext(() => stream.next());
        const change = record(value);
        yield { commit: decodeCommit(change.fullDocument),
          position: { kind: "changeStream", token: record(change._id) } };
      }
    } finally { await stream.close(); this.stream = undefined; }
  }

  async close(): Promise<void> { await this.stream?.close(); }
}

export class OplogSource implements LivePort {
  readonly mode = "oplog";
  private readonly oplog: Collection<Record<string, unknown>>;
  private cursor?: FindCursor<Record<string, unknown>>;
  constructor(private readonly config: MongoConfig) {
    this.oplog = config.db.client.db("local").collection("oplog.rs");
  }

  private async retained(ts: Timestamp): Promise<void> {
    const first = await this.oplog.findOne({}, { sort: { $natural: 1 }, readPreference: "primary" });
    if (!first || timestamp(first.ts).greaterThan(ts)) throw new HistoryExpired("Oplog retention gap");
  }

  async *watch(position: PrimaryPosition): AsyncIterable<LiveItem, void, unknown> {
    if (position.kind === "changeStream" || (position.kind === "boundary" && position.mode !== this.mode)) {
      throw new Error("Checkpoint watch mode mismatch");
    }
    await this.retained(position.ts);
    const comparison = position.kind === "boundary" ? { $gte: position.ts } : { $gt: position.ts };
    const cursor = this.oplog.find({ ts: comparison }, { tailable: true, awaitData: true,
      maxAwaitTimeMS: 1000, batchSize: this.config.batchSize ?? 256, readPreference: "primary" });
    this.cursor = cursor;
    const namespace = `${this.config.db.databaseName}.${this.config.collection}`;
    try {
      while (!cursor.closed) {
        const doc = await cursorNext(() => cursor.next());
        if (!doc) throw new Error("Oplog cursor ended");
        if (doc.ns !== namespace || doc.op !== "i") continue;
        yield { commit: decodeCommit(doc.o), position: { kind: "oplog", ts: timestamp(doc.ts) } };
      }
    } finally { await cursor.close(); this.cursor = undefined; }
  }

  async close(): Promise<void> { await this.cursor?.close(); }
}
