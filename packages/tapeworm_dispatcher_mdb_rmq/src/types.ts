import type { Db, Timestamp } from "mongodb";
import type { ICommit } from "tapeworm";
import type { IResumeTokenStore } from "./checkpoints/types";
import type { PublicationPolicy } from "./publication-policy";
import type { DeliveryFailureHandler } from "./delivery-failure";

/** MongoDB connection config for the dispatcher. */
export interface MongoConfig {
  db: Db;
  collection: string;
  batchSize?: number;
  maxRetries?: number;
  retryDelayMs?: number;
}

/** RabbitMQ connection config. */
export interface RabbitConfig {
  uri: string;
  exchange: string;
  confirmTimeoutMs?: number;
  maxPending?: number;
}

/**
 * Watch strategy for observing new commits in MongoDB.
 *
 * - "changeStream" (default): Uses MongoDB change streams. Waits for
 *   majority-committed writes. Replication-safe, no rollback risk.
 *
 * - "oplog": Tails local.oplog.rs directly. Sees writes immediately
 *   on the primary. Lowest latency, but writes may be rolled back if
 *   the primary loses election before replication completes.
 */
export type WatchMode = "changeStream" | "oplog";

/** Persisted resume state for at-least-once delivery. */
export interface ResumeState {
  changeStreamToken?: Record<string, unknown>;
  lastCommitToken?: string;
  updatedAt: Date;
  version?: 1;
  feed?: string;
  primary?: PrimaryPosition;
  recovery?: RecoveryPosition;
}

export type PrimaryPosition =
  | { kind: "changeStream"; token: Record<string, unknown> }
  | { kind: "oplog"; ts: Timestamp }
  | { kind: "boundary"; mode: WatchMode; ts: Timestamp };

export interface RecoveryPosition {
  phase: "scan" | "cutover";
  boundary: Timestamp;
  lower: string;
  upper?: string;
  cursor?: string;
  startedAt: Date;
}

export type DurableProgress =
  | { kind: "live"; position: PrimaryPosition; state: ResumeState }
  | { kind: "replay"; state: ResumeState }
  | { kind: "transition"; state: ResumeState };

export interface RecoveryEvent {
  phase: "started" | "scan" | "cutover" | "live";
  reason?: "history-expired" | "legacy-uuid";
  state: ResumeState;
}

/** Full dispatcher configuration. */
export interface DispatcherConfig {
  mongodb: MongoConfig;
  rabbitmq: RabbitConfig;
  resumeTokenStore: IResumeTokenStore;
  tenant?: string;
  /** Watch strategy — defaults to "changeStream" if omitted. */
  watchMode?: WatchMode;
  /** Stable source-cluster/destination identity; never include credentials. */
  feedId?: string;
  /** Explicit operator assertion that an unidentifiable legacy checkpoint belongs here. */
  adoptLegacyCheckpoint?: boolean;
  publication?: PublicationPolicy;
  failureHandler?: DeliveryFailureHandler;
}

/** Typed event map for the Dispatcher EventEmitter. */
export interface DispatcherEvents {
  started: [];
  stopped: [];
  dispatched: [commit: ICommit];
  resumed: [state: ResumeState];
  fallback: [];
  recovery: [event: RecoveryEvent];
  error: [err: Error];
  fatal: [err: Error];
}
