import { BSON } from "mongodb";
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import type { ICommit } from "tapeworm";
import { decodeCommit, record, text, uuid } from "../validation";
import type { QuarantineScope, RedriveRequest, SourceReference } from "./types";

export function boundedText(value: string, bytes: number): void {
  if (typeof value !== "string" || !value.trim() || Buffer.byteLength(value) > bytes) {
    throw new Error("Invalid bounded quarantine metadata");
  }
}
export function validateRequest(request: RedriveRequest): void {
  boundedText(request.actor, 128);
  boundedText(request.reason, 1024);
}
export function validateRejection(code: unknown): void {
  if (code !== "message-too-large") throw new Error("Invalid quarantine rejection code");
}
export function validateCompletion(result: unknown, diagnostic: unknown): void {
  if (result !== "published" && result !== "rejected" && result !== "outcome-unknown") throw new Error("Invalid quarantine result");
  const codes: unknown[] = ["message-too-large", "source-invalid", "publication-failed", "lease-expired"];
  if (diagnostic !== undefined && !codes.includes(diagnostic)) throw new Error("Invalid quarantine diagnostic");
}
export function assertScope(actual: QuarantineScope, expected: QuarantineScope): void {
  if (actual.feed !== expected.feed || actual.sourceCollection !== expected.sourceCollection) {
    throw new Error("Quarantine scope mismatch");
  }
}
export function validateRetention(value: unknown): void {
  if (value !== "immutable-until-resolved") throw new Error("Immutable source retention required");
}
export function validateQuarantine(value: unknown, scope: QuarantineScope): void {
  if (value === undefined) return;
  const config = record(value);
  if ("acceptOrderingGaps" in config && config.acceptOrderingGaps !== true) {
    throw new Error('acceptOrderingGaps is deprecated and only accepts true when supplied; use mode: "pause" to stop');
  }
  if (config.mode !== undefined && config.mode !== "pause" && config.mode !== "continue") {
    throw new Error("Invalid quarantine mode");
  }
  if (config.enabled === false) return;
  if (config.enabled !== true || config.sourceRetention !== "immutable-until-resolved") {
    throw new Error("Invalid quarantine configuration");
  }
  const actual = record(record(config.store).scope);
  assertScope({ feed: text(actual.feed), sourceCollection: text(actual.sourceCollection) }, scope);
}

function canonical(value: unknown): unknown {
  if (Array.isArray(value)) { const values: unknown[] = value; return values.map(canonical); }
  if (!value || typeof value !== "object" || Object.getPrototypeOf(value) !== Object.prototype) return value;
  return Object.fromEntries(Object.entries(value).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([key, item]) => [key, canonical(item)]));
}
export function sourceReference(commit: ICommit, scope: QuarantineScope): SourceReference {
  const validated = decodeCommit(commit);
  const fields = Object.fromEntries(Object.entries(validated).filter(([key]) => key !== "_id")
    .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([key, value]) => [key, canonical(value)]));
  const reference: SourceReference = { ...scope, commitId: validated.id, partitionId: validated.partitionId,
    streamId: validated.streamId, commitSequence: validated.commitSequence, token: uuid(validated.token),
    fingerprint: createHash("sha256").update(BSON.serialize(fields)).digest("hex") };
  if (BSON.calculateObjectSize(reference) > 4096) throw new Error("Quarantine reference too large");
  return reference;
}
export function assertReference(actual: SourceReference, expected: SourceReference): void {
  if (!isDeepStrictEqual(actual, expected)) throw new Error("Quarantine source identity or fingerprint mismatch");
}
