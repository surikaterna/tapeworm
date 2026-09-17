import type { Collection, Db } from "mongodb";
import type { ICommit } from "tapeworm";
import { decodeCommit, record } from "../validation";
import type { QuarantineScope, QuarantineSourceReader, SourceReference } from "./types";
import { assertReference, assertScope, sourceReference } from "./validation";
import { isSimpleCollation, simpleCollation } from "./mongodb-indexes";

export class MongoQuarantineSourceReader implements QuarantineSourceReader {
  readonly scope: Readonly<QuarantineScope>;
  private readonly collection: Collection;
  private index?: string;
  constructor(db: Db, collection: string, feed: string) {
    this.scope = Object.freeze({ feed, sourceCollection: collection });
    this.collection = db.collection(collection, { readConcern: { level: "majority" }, readPreference: "primary" });
  }
  async initialize(): Promise<void> {
    const indexes: unknown[] = await this.collection.listIndexes().toArray();
    const index = indexes.map(record).find((item) => item.unique === true && Object.keys(record(item.key)).length === 1 &&
      record(item.key).id === 1 && !item.partialFilterExpression && !item.sparse && isSimpleCollation(item.collation));
    if (typeof index?.name !== "string") throw new Error("Quarantine requires an existing unique simple id index");
    this.index = index.name;
  }
  async read(reference: SourceReference): Promise<ICommit> {
    assertScope(reference, this.scope);
    if (!this.index) throw new Error("Quarantine source not initialized");
    const source: unknown = await this.collection.findOne({ id: reference.commitId }, { hint: this.index, collation: simpleCollation });
    const commit = decodeCommit(source);
    assertReference(sourceReference(commit, this.scope), reference);
    return commit;
  }
}
