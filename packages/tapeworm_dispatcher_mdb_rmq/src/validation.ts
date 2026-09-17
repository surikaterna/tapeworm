import { Binary, Timestamp, UUID } from "mongodb";
import type { IBaseEvent, ICommit } from "tapeworm";
import type { PrimaryPosition, RecoveryPosition, ResumeState } from "./types";

export function record(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Expected object");
  }
  return Object.fromEntries(Object.entries(value));
}

export function text(value: unknown): string {
  if (typeof value !== "string" || !value) throw new Error("Expected nonempty string");
  return value;
}

export function timestamp(value: unknown): Timestamp {
  if (!(value instanceof Timestamp)) throw new Error("Expected BSON Timestamp");
  return value;
}

export function uuid(value: unknown): string {
  if (value instanceof Binary && value.sub_type === Binary.SUBTYPE_UUID) {
    return value.toUUID().toHexString();
  }
  return new UUID(text(value)).toHexString();
}

function event(value: unknown): IBaseEvent {
  const doc = record(value);
  if (doc.version !== undefined && !Number.isSafeInteger(doc.version)) {
    throw new Error("Invalid event version");
  }
  return { ...doc, id: text(doc.id), type: text(doc.type) };
}

export function decodeCommit(value: unknown): ICommit {
  const doc = record(value);
  const sequence = doc.commitSequence;
  if (typeof sequence !== "number" || !Number.isSafeInteger(sequence) || sequence < 0) {
    throw new Error("Invalid commitSequence");
  }
  if (!Array.isArray(doc.events)) throw new Error("Invalid events");
  if (doc.isDispatched !== undefined && typeof doc.isDispatched !== "boolean") {
    throw new Error("Invalid isDispatched");
  }
  uuid(doc.token);
  const events: unknown[] = doc.events;
  return { ...doc, id: text(doc.id), partitionId: text(doc.partitionId),
    streamId: text(doc.streamId), commitSequence: sequence, events: events.map(event) };
}

function date(value: unknown): Date {
  if (!(value instanceof Date) || !Number.isFinite(value.getTime())) throw new Error("Invalid date");
  return value;
}

function primary(value: unknown): PrimaryPosition {
  const doc = record(value);
  if (doc.kind === "changeStream") return { kind: doc.kind, token: resumeToken(doc.token) };
  if (doc.kind === "oplog") return { kind: doc.kind, ts: timestamp(doc.ts) };
  if (doc.kind === "boundary" && (doc.mode === "changeStream" || doc.mode === "oplog")) {
    return { kind: doc.kind, mode: doc.mode, ts: timestamp(doc.ts) };
  }
  throw new Error("Invalid primary position");
}

function resumeToken(value: unknown): Record<string, unknown> {
  const token = record(value);
  if ("_replayFallback" in token) throw new Error("Synthetic replay token requires operator repair");
  if (!Object.keys(token).length) throw new Error("Empty primary token");
  return token;
}

function recovery(value: unknown): RecoveryPosition {
  const doc = record(value);
  if (doc.phase !== "scan" && doc.phase !== "cutover") throw new Error("Invalid recovery phase");
  return { phase: doc.phase, boundary: timestamp(doc.boundary), lower: uuid(doc.lower),
    upper: doc.upper == null ? undefined : uuid(doc.upper),
    cursor: doc.cursor == null ? undefined : uuid(doc.cursor), startedAt: date(doc.startedAt) };
}

export function decodeState(value: unknown): ResumeState {
  const doc = record(value);
  if (doc.version !== undefined && doc.version !== 1) throw new Error("Unknown checkpoint version");
  const token = doc.changeStreamToken == null ? undefined : resumeToken(doc.changeStreamToken);
  const state: ResumeState = { updatedAt: date(doc.updatedAt), version: doc.version === 1 ? 1 : undefined,
    feed: doc.feed == null ? undefined : text(doc.feed), changeStreamToken: token,
    lastCommitToken: doc.lastCommitToken == null ? undefined : uuid(doc.lastCommitToken),
    primary: doc.primary == null ? undefined : primary(doc.primary),
    recovery: doc.recovery == null ? undefined : recovery(doc.recovery) };
  validateRecovery(state);
  return state;
}

function validateRecovery(state: ResumeState): void {
  const scan = state.recovery;
  if (!scan) return;
  if (state.version !== 1) throw new Error("Recovery requires version 1");
  if (scan.cursor && (!scan.upper || scan.cursor <= scan.lower || scan.cursor > scan.upper || scan.cursor !== state.lastCommitToken)) {
    throw new Error("Inconsistent replay cursor");
  }
  if (scan.phase === "cutover" && (state.primary?.kind !== "boundary" || !state.primary.ts.equals(scan.boundary))) {
    throw new Error("Cutover must retain original live boundary");
  }
}
