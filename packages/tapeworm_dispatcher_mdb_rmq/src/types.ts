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
