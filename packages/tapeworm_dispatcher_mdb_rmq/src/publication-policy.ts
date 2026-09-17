import type { ICommit } from "tapeworm";
import { decodeCommit, record } from "./validation";

export type PublicationDecision = { kind: "allow" } | { kind: "reject"; code: "unsupported-schema" };
export interface PublicationPolicy {
  maxMessageBytes?: number;
  validateRecord?: (commit: Readonly<ICommit>) => PublicationDecision;
}
export type RejectionCode = "unsupported-schema" | "message-too-large";

// Only this module can mint eligibility, before a network publication is attempted.
class PrepublicationRejection extends Error {
  constructor(readonly code: RejectionCode) { super(code); }
}
export function rejectionCode(error: unknown): RejectionCode | undefined {
  return error instanceof PrepublicationRejection ? error.code : undefined;
}

export function validatePublicationPolicy(policy: PublicationPolicy = {}): void {
  if (policy.maxMessageBytes !== undefined &&
    (!Number.isSafeInteger(policy.maxMessageBytes) || policy.maxMessageBytes < 1)) {
    throw new Error("maxMessageBytes must be a positive integer");
  }
  if (policy.validateRecord !== undefined && typeof policy.validateRecord !== "function") {
    throw new Error("validateRecord must be a function");
  }
}

export function encodePublication(commit: ICommit, policy: PublicationPolicy = {}): Buffer {
  try { decodeCommit(commit); } catch { throw new Error("Invalid publication source identity"); }
  if (policy.validateRecord) {
    const decision = validateDecision(commit, policy.validateRecord);
    if (decision.kind === "reject" && decision.code === "unsupported-schema" && Object.keys(decision).length === 2) {
      throw new PrepublicationRejection("unsupported-schema");
    }
    if (decision.kind !== "allow" || Object.keys(decision).length !== 1) throw new Error("Invalid publication decision");
  }
  let body: Buffer;
  try { body = Buffer.from(JSON.stringify(commit)); } catch { throw new Error("Publication serialization failed"); }
  if (policy.maxMessageBytes !== undefined && body.length > policy.maxMessageBytes) {
    throw new PrepublicationRejection("message-too-large");
  }
  return body;
}

function validateDecision(commit: ICommit, validate: NonNullable<PublicationPolicy["validateRecord"]>): Record<string, unknown> {
  try { return record(validate(commit)); } catch { throw new Error("Publication validator failed"); }
}
