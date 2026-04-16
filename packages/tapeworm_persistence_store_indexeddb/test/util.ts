import { IDBKeyRange } from "fake-indexeddb";
import FDBFactory from "fake-indexeddb/lib/FDBFactory";

// Make IDBKeyRange available globally for idb_partition
(globalThis as Record<string, unknown>).IDBKeyRange = IDBKeyRange;

export default function getDb(): IDBFactory {
  return new FDBFactory() as unknown as IDBFactory;
}
