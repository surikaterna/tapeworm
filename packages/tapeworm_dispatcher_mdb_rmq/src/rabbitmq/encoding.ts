import type { ICommit } from "tapeworm";

export interface PublicationPolicy {
  maxMessageBytes?: number;
}
export type RejectionCode = "message-too-large";

// Only this module can mint eligibility, before a network publication is attempted.
class PrepublicationRejection extends Error {
  constructor(readonly code: RejectionCode) { super(code); }
}
export function rejectionCode(error: unknown): RejectionCode | undefined {
  return error instanceof PrepublicationRejection ? error.code : undefined;
}

export function validatePublicationPolicy(value: unknown = {}): void {
  if (typeof value !== "object" || value === null) throw new Error("Invalid publication policy");
  if (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) {
    throw new Error("Publication policy must be a plain options object");
  }
  for (const key of Reflect.ownKeys(value)) {
    if (key !== "maxMessageBytes") throw new Error(`Unsupported publication option: ${String(key)}`);
  }
  const maxMessageBytes: unknown = Reflect.get(value, "maxMessageBytes");
  if (maxMessageBytes !== undefined &&
    (typeof maxMessageBytes !== "number" || !Number.isSafeInteger(maxMessageBytes) || maxMessageBytes < 1)) {
    throw new Error("maxMessageBytes must be a positive integer");
  }
}

/** The caller supplies an already validated transport envelope, not untrusted input. */
export function encodePublication(commit: ICommit, policy: PublicationPolicy = {}): Buffer {
  let body: Buffer;
  try { body = Buffer.from(JSON.stringify(commit)); } catch { throw new Error("Publication serialization failed"); }
  if (policy.maxMessageBytes !== undefined && body.length > policy.maxMessageBytes) {
    throw new PrepublicationRejection("message-too-large");
  }
  return body;
}
