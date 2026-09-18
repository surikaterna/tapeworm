import type { ICommit } from "tapeworm";
import type { RejectionCode } from "../rabbitmq/encoding";

export interface QuarantineScope { feed: string; sourceCollection: string }
export interface SourceReference extends QuarantineScope {
  commitId: string;
  partitionId: string;
  streamId: string;
  commitSequence: number;
  token: string;
  fingerprint: string;
}
export type QuarantineStatus = "quarantined" | "claimed" | "published";
export type AttemptResult = "published" | "rejected" | "outcome-unknown";
export type DiagnosticCode = RejectionCode | "source-invalid" | "publication-failed" | "lease-expired";
export interface RedriveRequest { actor: string; reason: string }
export interface QuarantineAttempt extends RedriveRequest {
  token: string;
  startedAt: Date;
  expiresAt: Date;
  finishedAt?: Date;
  result?: AttemptResult;
  diagnostic?: DiagnosticCode;
}
export interface QuarantineRecord {
  id: string;
  version: 1;
  reference: SourceReference;
  code: RejectionCode;
  status: QuarantineStatus;
  observations: number;
  createdAt: Date;
  attempts: readonly QuarantineAttempt[];
}
export interface QuarantineListOptions { status?: QuarantineStatus; after?: string; limit?: number }
export interface QuarantinePage { records: readonly QuarantineRecord[]; after?: string }
export type ClaimResult = { kind: "claimed"; record: QuarantineRecord; token: string }
  | { kind: "already-published" | "busy" | "missing" | "attempt-limit" };
export type RedriveOutcome = { kind: "published" | "already-published" | "busy" | "missing" | "outcome-unknown" }
  | { kind: "rejected"; code: DiagnosticCode | "attempt-limit" };

/** Implementations must durably persist capture/claim/finish and fence stale claim tokens. */
export interface QuarantineStore {
  readonly scope: Readonly<QuarantineScope>;
  initialize(): Promise<void>;
  find(reference: SourceReference): Promise<QuarantineRecord | null>;
  capture(reference: SourceReference, code: RejectionCode): Promise<QuarantineRecord>;
  list(options?: QuarantineListOptions): Promise<QuarantinePage>;
  claim(id: string, request: RedriveRequest): Promise<ClaimResult>;
  finish(id: string, token: string, result: AttemptResult, diagnostic?: DiagnosticCode): Promise<boolean>;
}
export interface QuarantineSourceReader {
  readonly scope: Readonly<QuarantineScope>;
  initialize(): Promise<void>;
  read(reference: SourceReference): Promise<ICommit>;
}
type Enabled = { enabled: true; store: QuarantineStore; sourceRetention: "immutable-until-resolved" };
export type QuarantineConfig = ({ enabled: false } | (Enabled & { mode?: "continue" | "pause" })) & {
  /** @deprecated Enabled quarantine continues by default; use mode:"pause" to stop instead. */
  acceptOrderingGaps?: true;
};
export interface QuarantinedEvent {
  id: string;
  code: RejectionCode;
  checkpointAdvanced: boolean;
  resolution: "unresolved" | "published";
}
