import { ObjectId, MongoServerError, type Collection, type Db } from "mongodb";
import type { RejectionCode } from "../publication-policy";
import type { AttemptResult, DiagnosticCode, QuarantineScope, QuarantineStore, RedriveRequest,
  SourceReference, QuarantineRecord, QuarantineListOptions, QuarantinePage, ClaimResult } from "./types";
import { assertReference, assertScope, validateRejection } from "./validation";
import { cursor, inspection, listQuery, validateReference, type QuarantineDocument } from "./document";
import { MongoQuarantineClaims } from "./mongodb-claims";
import { initializeQuarantineIndexes, simpleCollation } from "./mongodb-indexes";

export interface MongoQuarantineStoreOptions extends QuarantineScope { leaseMs?: number }
export class MongoQuarantineStore implements QuarantineStore {
  readonly scope: Readonly<QuarantineScope>;
  private readonly collection: Collection<QuarantineDocument>;
  private readonly claims: MongoQuarantineClaims;
  private ready?: Promise<void>;

  constructor(db: Db, collection: string, options: MongoQuarantineStoreOptions) {
    const leaseMs = options.leaseMs ?? 60000;
    if (!Number.isSafeInteger(leaseMs) || leaseMs < 1 || leaseMs > 3600000) throw new Error("Invalid quarantine lease");
    this.scope = Object.freeze({ feed: options.feed, sourceCollection: options.sourceCollection });
    this.collection = db.collection<QuarantineDocument>(collection, { writeConcern: { w: "majority", j: true },
      readConcern: { level: "majority" }, readPreference: "primary" });
    this.claims = new MongoQuarantineClaims(this.collection, this.scope, leaseMs);
  }
  initialize(): Promise<void> {
    this.ready ??= initializeQuarantineIndexes(this.collection).catch((error: unknown) => { this.ready = undefined; throw error; });
    return this.ready;
  }
  async find(reference: SourceReference): Promise<QuarantineRecord | null> {
    assertScope(reference, this.scope);
    validateReference(reference);
    await this.initialize();
    const doc = await this.collection.findOne({ ...this.scope, commitId: reference.commitId },
      { hint: "quarantine_identity", collation: simpleCollation });
    if (!doc) return null;
    assertReference(doc.reference, reference);
    return inspection(doc);
  }
  async capture(reference: SourceReference, code: RejectionCode): Promise<QuarantineRecord> {
    validateRejection(code);
    assertScope(reference, this.scope);
    validateReference(reference);
    await this.initialize();
    const identity = { ...this.scope, commitId: reference.commitId };
    try {
      await this.collection.updateOne({ ...identity, reference }, { $setOnInsert: { ...identity, reference,
        _id: new ObjectId(), version: 1, code, status: "quarantined", observations: 0, createdAt: new Date(),
        attempts: [], attemptCount: 0 } }, { upsert: true, collation: simpleCollation });
    } catch (error: unknown) {
      if (!(error instanceof MongoServerError) || error.code !== 11000) throw error;
    }
    const existing = await this.find(reference);
    if (!existing) throw new Error("Quarantine capture missing");
    await this.collection.updateOne({ ...identity, observations: { $lt: 2147483647 } },
      { $inc: { observations: 1 } }, { collation: simpleCollation });
    const current = await this.find(reference);
    if (!current) throw new Error("Quarantine capture missing");
    return current;
  }
  async list(options: QuarantineListOptions = {}): Promise<QuarantinePage> {
    const { query, limit } = listQuery(options, this.scope);
    await this.initialize();
    const docs = await this.collection.find(query, { collation: simpleCollation })
      .hint(options.status ? "quarantine_status_id" : "quarantine_scope_id")
      .sort({ _id: 1 }).limit(limit + 1).toArray();
    const records = docs.slice(0, limit).map(inspection);
    const last = records.at(-1);
    return { records, ...(docs.length > limit && last ? { after: cursor(last.id, this.scope, options.status) } : {}) };
  }
  async claim(id: string, request: RedriveRequest): Promise<ClaimResult> {
    await this.initialize();
    return this.claims.claim(id, request);
  }
  async finish(id: string, token: string, result: AttemptResult, diagnostic?: DiagnosticCode): Promise<boolean> {
    await this.initialize();
    return this.claims.finish(id, token, result, diagnostic);
  }
}
