import type { Db } from "mongodb";
import type { ResumeState } from "../types";
import type { IResumeTokenStore } from "./types";

const STORE_ID = "dispatcher_resume";

/**
 * Stores dispatcher resume state in a MongoDB collection.
 * Uses a single document with a fixed _id for upsert semantics.
 */
export class MongoResumeTokenStore implements IResumeTokenStore {
  private readonly _db: Db;
  private readonly _collectionName: string;

  constructor(db: Db, collectionName: string) {
    this._db = db;
    this._collectionName = collectionName;
  }

  async load(): Promise<ResumeState | null> {
    const doc = await this._db
      .collection(this._collectionName)
      .findOne({ _id: STORE_ID as any });

    if (!doc) return null;

    return {
      changeStreamToken: doc.changeStreamToken ?? undefined,
      lastCommitToken: doc.lastCommitToken ?? undefined,
      updatedAt: doc.updatedAt,
    };
  }

  async save(state: ResumeState): Promise<void> {
    await this._db.collection(this._collectionName).updateOne(
      { _id: STORE_ID as any },
      {
        $set: {
          changeStreamToken: state.changeStreamToken ?? null,
          lastCommitToken: state.lastCommitToken ?? null,
          updatedAt: state.updatedAt,
        },
      },
      { upsert: true },
    );
  }
}
