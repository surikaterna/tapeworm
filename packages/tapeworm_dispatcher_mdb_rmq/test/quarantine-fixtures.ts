import type { ICommit } from "tapeworm";
import type { PublisherPort } from "../src/delivery";
import { encodePublication, type PublicationPolicy, type RejectionCode } from "../src/publication-policy";
import type { AttemptResult, ClaimResult, DiagnosticCode, QuarantineRecord, QuarantineScope,
  QuarantineStore, RedriveRequest, SourceReference } from "../src/quarantine/types";
import { assertReference } from "../src/quarantine/validation";

export const scope: QuarantineScope = { feed: "feed", sourceCollection: "commits" };
export const request = { actor: "operator", reason: "schema repaired" };
export const rejectPolicy: PublicationPolicy = { validateRecord: () => ({ kind: "reject", code: "unsupported-schema" }) };
export class PolicyPublisher implements PublisherPort {
  published: ICommit[] = [];
  calls = 0;
  closed = false;
  failure?: Error;
  constructor(readonly policy?: PublicationPolicy) {}
  connect(): Promise<void> { return Promise.resolve(); }
  publish(commit: ICommit): Promise<void> {
    this.calls++;
    if (this.failure) return Promise.reject(this.failure);
    encodePublication(commit, this.policy);
    this.published.push(commit);
    return Promise.resolve();
  }
  close(): Promise<void> { this.closed = true; return Promise.resolve(); }
}
export class MemoryQuarantine implements QuarantineStore {
  record?: QuarantineRecord;
  lookups = 0;
  writes = 0;
  failure = false;
  finishFailure = false;
  constructor(readonly scope: QuarantineScope) {}
  initialize(): Promise<void> { return Promise.resolve(); }
  find(reference: SourceReference): Promise<QuarantineRecord | null> {
    this.lookups++;
    if (this.record && this.record.reference.commitId !== reference.commitId) return Promise.resolve(null);
    if (this.record) assertReference(this.record.reference, reference);
    return Promise.resolve(this.record ?? null);
  }
  capture(reference: SourceReference, code: RejectionCode): Promise<QuarantineRecord> {
    this.writes++;
    if (this.failure) return Promise.reject(new Error("Capture failed"));
    this.record ??= { id: "000000000000000000000001", version: 1, reference, code,
      status: "quarantined", createdAt: new Date(), observations: 1, attempts: [] };
    return Promise.resolve(this.record);
  }
  list() { return Promise.resolve({ records: this.record ? [this.record] : [] }); }
  claim(_id: string, details: RedriveRequest): Promise<ClaimResult> {
    if (!this.record) return Promise.resolve({ kind: "missing" });
    if (this.record.status === "published") return Promise.resolve({ kind: "already-published" });
    if (this.record.status === "claimed") return Promise.resolve({ kind: "busy" });
    this.record = { ...this.record, status: "claimed", attempts: [{ ...details, token: "claim",
      startedAt: new Date(), expiresAt: new Date(Date.now() + 60000) }] };
    return Promise.resolve({ kind: "claimed", record: this.record, token: "claim" });
  }
  finish(_id: string, _token: string, result: AttemptResult, diagnostic?: DiagnosticCode): Promise<boolean> {
    if (this.finishFailure) return Promise.reject(new Error("credentials must not escape"));
    if (!this.record) return Promise.resolve(false);
    this.record = { ...this.record, status: result === "published" ? "published" : "quarantined",
      attempts: this.record.attempts.map((attempt) => ({ ...attempt, result, diagnostic, finishedAt: new Date() })) };
    return Promise.resolve(true);
  }
}
