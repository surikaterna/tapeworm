import { MongoServerError, type Collection } from "mongodb";
import { record } from "../validation";
import type { QuarantineDocument } from "./document";

export const simpleCollation = Object.freeze({ locale: "simple" });
const ownedIndexes = new Set(["quarantine_identity", "quarantine_scope_id", "quarantine_status_id"]);

export function isSimpleCollation(value: unknown): boolean {
  return value === undefined || record(value).locale === "simple";
}

async function validateIndexes(collection: Collection<QuarantineDocument>): Promise<void> {
  let indexes: unknown[];
  try { indexes = await collection.listIndexes().toArray(); }
  catch (error: unknown) {
    if (error instanceof MongoServerError && error.code === 26) return;
    throw error;
  }
  for (const value of indexes) {
    const index = record(value);
    // ObjectId _id uniqueness is unaffected by a collection's string collation.
    if (index.name === "_id_" || isSimpleCollation(index.collation)) continue;
    if (index.unique === true || (typeof index.name === "string" && ownedIndexes.has(index.name))) {
      throw new Error("Quarantine requires operator index migration: inspect and replace incompatible non-simple indexes while owners are stopped; see README");
    }
  }
}

export async function initializeQuarantineIndexes(collection: Collection<QuarantineDocument>): Promise<void> {
  // Adding a binary index cannot undo a legacy linguistic unique constraint.
  await validateIndexes(collection);
  await collection.createIndex({ feed: 1, sourceCollection: 1, commitId: 1 },
    { unique: true, name: "quarantine_identity", collation: simpleCollation });
  await collection.createIndex({ feed: 1, sourceCollection: 1, _id: 1 },
    { name: "quarantine_scope_id", collation: simpleCollation });
  await collection.createIndex({ feed: 1, sourceCollection: 1, status: 1, _id: 1 },
    { name: "quarantine_status_id", collation: simpleCollation });
}
