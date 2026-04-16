import type { Db, Document } from "mongodb";
import type { ICommit } from "tapeworm";
import type { IResumeTokenStore } from "./resume/types";

/** MongoDB connection config for the dispatcher. */
export interface MongoConfig {
  db: Db;
  collection: string;
}

/** RabbitMQ connection config. */
export interface RabbitConfig {
  uri: string;
  exchange: string;
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
  changeStreamToken?: Document;
  lastCommitToken?: string;
  updatedAt: Date;
}

/** Full dispatcher configuration. */
export interface DispatcherConfig {
  mongodb: MongoConfig;
  rabbitmq: RabbitConfig;
  resumeTokenStore: IResumeTokenStore;
  tenant?: string;
  /** Watch strategy — defaults to "changeStream" if omitted. */
  watchMode?: WatchMode;
}

/** Typed event map for the Dispatcher EventEmitter. */
export interface DispatcherEvents {
  started: [];
  stopped: [];
  dispatched: [commit: ICommit];
  resumed: [state: ResumeState];
  fallback: [];
  error: [err: Error];
  fatal: [err: Error];
}
