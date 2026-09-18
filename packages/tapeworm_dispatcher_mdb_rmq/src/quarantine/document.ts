import { BSON, ObjectId } from "mongodb";
import { createHash } from "node:crypto";
import type { QuarantineListOptions, QuarantineRecord, QuarantineScope, SourceReference } from "./types";
import { boundedText } from "./validation";
import { uuid } from "../validation";

export interface QuarantineDocument extends Omit<QuarantineRecord, "id">, QuarantineScope {
  _id: ObjectId;
  commitId: string;
  attemptCount: number;
  claimToken?: string;
  claimExpiresAt?: Date;
}
export function inspection(doc: QuarantineDocument): QuarantineRecord {
  return { id: doc._id.toHexString(), version: doc.version, reference: doc.reference, code: doc.code,
    status: doc.status, observations: doc.observations, createdAt: doc.createdAt, attempts: doc.attempts };
}
export function validateReference(reference: SourceReference): void {
  uuid(reference.token);
  for (const value of [reference.feed, reference.sourceCollection, reference.commitId, reference.partitionId, reference.streamId]) {
    boundedText(value, 4096);
  }
  if (BSON.calculateObjectSize(reference) > 4096 || !/^[a-f0-9]{64}$/.test(reference.fingerprint) ||
    !Number.isSafeInteger(reference.commitSequence) || reference.commitSequence < 0) throw new Error("Invalid source reference");
}
export function objectId(id: string): ObjectId {
  if (!/^[a-f0-9]{24}$/.test(id)) throw new Error("Invalid quarantine id");
  return new ObjectId(id);
}
function cursorScope(scope: QuarantineScope, status: QuarantineListOptions["status"]): string {
  return createHash("sha256").update(JSON.stringify([scope.feed, scope.sourceCollection, status ?? null])).digest("hex");
}
export function cursor(id: string, scope: QuarantineScope, status: QuarantineListOptions["status"]): string {
  return Buffer.from(`${cursorScope(scope, status)}:${id}`).toString("base64url");
}
export function listQuery(options: QuarantineListOptions, scope: QuarantineScope) {
  const limit = options.limit ?? 25;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) throw new Error("Invalid quarantine list limit");
  const status = options.status;
  if (status !== undefined && !["quarantined", "claimed", "published"].includes(status)) throw new Error("Invalid quarantine status");
  const query = { ...scope, ...(status ? { status } : {}) };
  if (options.after === undefined) return { query, limit };
  if (typeof options.after !== "string" || options.after.length > 256) throw new Error("Invalid quarantine cursor");
  const [signature, id] = Buffer.from(options.after, "base64url").toString().split(":");
  if (signature !== cursorScope(scope, status) || !id || cursor(id, scope, status) !== options.after) {
    throw new Error("Invalid scoped quarantine cursor");
  }
  return { query: { ...query, _id: { $gt: objectId(id) } }, limit };
}
