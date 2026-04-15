import { describe, it, expect } from "vitest";
import { v4 as uuid } from "uuid";
import Promise from "bluebird";

import EventStore, {
  Commit,
  Event,
  ConcurrencyError,
  DuplicateCommitError,
} from "tapeworm";

import Store from "../..";

import getDb from "../util";

describe("indexeddb_persistence", () => {
  describe("#commit", () => {
    it("should accept a commit and store it", async () => {
      var store = new Store(getDb());
      var partition = await store.openPartition("1");
      var events = [new Event(uuid(), "type1", { test: 11 })];
      var commit = new Commit(uuid(), "master", "1", 0, events);
      await partition.append(commit);
      var x = await partition.queryAll();
      expect(x.length).toBe(1);
    });

    it("should open the database version 4 and have all objectStores", async () => {
      var store = new Store(getDb(), "tenantDbKey");
      var partition = await store.openPartition("master");
      expect(partition._db.version).toBe(4);
      expect(partition._db.objectStoreNames.contains("commits")).toBe(true);
      expect(partition._db.objectStoreNames.contains("snapshots")).toBe(true);
      expect(partition._db.objectStoreNames.contains("truncated")).toBe(true);
    });

    it("should open and create missing object stores", async () => {
      var indexedDb = getDb();
      var request = indexedDb.open("tw_tenantDbKey_master", 3);
      await new Promise<void>((resolve) => {
        request.onsuccess = () => {
          var db = request.result;
          db.close();
          resolve();
        };
      });

      var store = new Store(indexedDb, "tenantDbKey");
      var partition = await store.openPartition("master");
      expect(partition._db.version).toBe(4);
      expect(partition._db.objectStoreNames.contains("commits")).toBe(true);
      expect(partition._db.objectStoreNames.contains("snapshots")).toBe(true);
      expect(partition._db.objectStoreNames.contains("truncated")).toBe(true);
    });

    it("commit in one stream is not visible in other", async () => {
      var store = new Store(getDb());
      var partition = await store.openPartition("1");
      var events1 = [new Event(uuid(), "type1", { test: 11 })];
      var commit1 = new Commit(uuid(), "master", "1", 0, events1);
      await partition.append(commit1);

      var events2 = [new Event(uuid(), "type2", { test: 22 })];
      var commit2 = new Commit(uuid(), "master", "2", 0, events2);
      await partition.append(commit2);

      var [r1, r2] = await Promise.all([
        partition.queryStream("1"),
        partition.queryStream("2"),
      ]);
      expect(r1.length).toBe(1);
      expect(r2.length).toBe(1);
    });

    it("two commits in one stream are visible", async () => {
      var store = new Store(getDb());
      var partition = await store.openPartition("1");
      var events1 = [new Event(uuid(), "type1", { test: 11 })];
      var commit1 = new Commit(uuid(), "master", "1", 0, events1);
      await partition.append(commit1);
      var events2 = [new Event(uuid(), "type2", { test: 22 })];
      var commit2 = new Commit(uuid(), "master", "1", 1, events2);
      await partition.append(commit2);
      var res = await partition.queryAll();
      expect(res.length).toBe(2);
    });
  });
  describe("#concurrency", () => {
    // Note: skipped test with original `parition` typo preserved
    it.skip("same commit sequence twice should throw", async () => {
      var store = new Store(getDb());
      var partition = await store.openPartition("1");
      var events = [new Event(uuid(), "type1", { test: 11 })];
      var commit = new Commit(uuid(), "master", "1", 0, events);
      var commit2 = new Commit(uuid(), "master", "1", 0, events);
      await partition.append(commit);
      await expect(partition.append(commit2)).rejects.toThrow();
    });
  });
  describe("#duplicateEvents", () => {
    it("same commit twice should throw", async () => {
      var store = new Store(getDb());
      var partition = await store.openPartition("1");
      var events = [new Event(uuid(), "type1", { test: 11 })];
      var commit = new Commit(uuid(), "master", "1", 0, events);
      await partition.append(commit);
      await expect(partition.append(commit)).rejects.toThrow();
    });
  });
  describe("#partition", () => {
    it("getting the same partition twice should return same instance", async () => {
      var store = new Store(getDb());
      var [p1, p2] = await Promise.all([
        store.openPartition("1"),
        store.openPartition("1"),
      ]);
      expect(p1).toBe(p2);
    });
    it("not indicating partition name should give master partition", async () => {
      var store = new Store(getDb());
      var [p1, p2] = await Promise.all([
        store.openPartition(),
        store.openPartition("master"),
      ]);
      expect(p1).toBe(p2);
    });
  });
});
