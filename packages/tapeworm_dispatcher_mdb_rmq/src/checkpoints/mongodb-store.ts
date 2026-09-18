import type { Db } from "mongodb";
import type { ResumeState } from "../types";
import { decodeState } from "../validation";
import type { IResumeTokenStore } from "./types";

export interface MongoResumeStoreOptions {
  checkpointKey?: string;
  feedId?: string;
  adoptLegacyCheckpoint?: boolean;
}

/** One externally fenced owner per key. No distributed lease is implied. */
export class MongoResumeTokenStore implements IResumeTokenStore {
  constructor(private readonly db: Db, private readonly collectionName: string,
    private readonly options: MongoResumeStoreOptions = {}) {}

  private collection() {
    return this.db.collection<{ _id: string; [key: string]: unknown }>(this.collectionName);
  }

  async load(): Promise<ResumeState | null> {
    const doc = await this.collection().findOne(
      { _id: this.options.checkpointKey ?? "dispatcher_resume" },
      { readConcern: { level: "majority" }, readPreference: "primary" },
    );
    if (!doc) return null;
    return this.validate(decodeState(doc));
  }

  private validate(state: ResumeState): ResumeState {
    const feed = this.options.feedId;
    if (feed && state.feed !== feed && !(state.feed === undefined && this.options.adoptLegacyCheckpoint)) {
      throw new Error("Checkpoint feed mismatch; explicit legacy migration required");
    }
    return state;
  }

  async save(state: ResumeState): Promise<void> {
    const validated = this.validate(decodeState(state));
    await this.collection().replaceOne(
      { _id: this.options.checkpointKey ?? "dispatcher_resume" },
      { ...validated, feed: this.options.feedId ?? validated.feed,
        _id: this.options.checkpointKey ?? "dispatcher_resume" },
      { upsert: true, ignoreUndefined: true, writeConcern: { w: "majority" } },
    );
  }
}
