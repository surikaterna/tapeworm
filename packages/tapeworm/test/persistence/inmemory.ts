import Promise from "bluebird";
import { v4 as uuid } from "uuid";
import { describe, expect, it } from "vitest";
import Event from "../../src/event";
import Commit from "../../src/persistence/commit";
import Store from "../../src/persistence/inmemory/inmemory_persistence";

describe("inmemory_persistence", () => {
  describe("#commit", () => {
    it("should accept a commit and store it", async () => {
      const store = new Store();
      const partition = await store.openPartition("1");
      const events = [new Event(uuid(), "type1", { test: 11 })];
      const commit = new Commit(uuid(), "master", "1", 0, events);

      await partition.append(commit);
      const x = await partition.queryAll();
      expect(x).toHaveLength(1);
    });

    it("commit in one stream is not visible in other", async () => {
      const store = new Store();
      const partition = await store.openPartition("1");

      let events = [new Event(uuid(), "type1", { test: 11 })];
      let commit = new Commit(uuid(), "master", "1", 0, events);
      partition.append(commit);

      events = [new Event(uuid(), "type2", { test: 22 })];
      commit = new Commit(uuid(), "master", "2", 0, events);
      partition.append(commit);

      const [r1, r2] = await Promise.all([partition.queryStream("1"), partition.queryStream("2")]);

      expect(r1).toHaveLength(1);
      expect(r2).toHaveLength(1);
    });

    it("two commits in one stream are visible", async () => {
      const store = new Store();
      const partition = await store.openPartition("1");

      let events = [new Event(uuid(), "type1", { test: 11 })];
      let commit = new Commit(uuid(), "master", "1", 0, events);
      partition.append(commit);

      events = [new Event(uuid(), "type2", { test: 22 })];
      commit = new Commit(uuid(), "master", "1", 1, events);
      partition.append(commit);

      const res = await partition.queryAll();
      expect(res).toHaveLength(2);
    });

    it("should skip events", async () => {
      const store = new Store();
      const partition = await store.openPartition("1");

      let events = [new Event(uuid(), "type1", { test: 11 }), new Event(uuid(), "type1", { test: 12 })];
      let commit = new Commit(uuid(), "master", "1", 0, events);
      partition.append(commit);

      events = [new Event(uuid(), "type2", { test: 22 })];
      commit = new Commit(uuid(), "master", "1", 1, events);
      partition.append(commit);

      const res = await partition.queryStream("1", 2);
      expect(res).toHaveLength(1);
    });

    it("should skip events and split commit if inbetween", async () => {
      const store = new Store();
      const partition = await store.openPartition("1");

      let events = [new Event(uuid(), "type1", { test: 11 }), new Event(uuid(), "type1", { test: 12 })];
      let commit = new Commit(uuid(), "master", "1", 0, events);
      partition.append(commit);

      events = [new Event(uuid(), "type2", { test: 22 })];
      commit = new Commit(uuid(), "master", "1", 1, events);
      partition.append(commit);

      const res = await partition.queryStream("1", 1);
      expect(res).toHaveLength(2);
      expect(res[0].events).toHaveLength(1);
    });
  });
  describe("#concurrency", () => {
    it("same commit sequence twice should throw", async () => {
      const store = new Store();
      const partition = await store.openPartition("1");

      const events = [new Event(uuid(), "type1", { test: 11 })];
      const commit = new Commit(uuid(), "master", "1", 0, events);
      const commit2 = new Commit(uuid(), "master", "1", 0, events);

      await expect(async () => await Promise.all(partition.append(commit), partition.append(commit2))).rejects.toThrow("Concurrency error on stream 1");
    });
  });

  describe("#duplicateEvents", () => {
    it("same commit twice should throw", async () => {
      const store = new Store();
      const partition = await store.openPartition("1");

      const events = [new Event(uuid(), "type1", { test: 11 })];
      const commit = new Commit(uuid(), "master", "1", 0, events);

      await partition.append(commit);
      await expect(async () => partition.append(commit)).rejects.toThrow(`Duplicate commit of ${commit.id}`);
    });
  });

  describe("#partition", () => {
    it("getting the same partition twice should return same instance", async () => {
      const store = new Store();
      const [p1, p2] = await Promise.all([store.openPartition("1"), store.openPartition("1")]);
      expect(p1).toBe(p2);
    });

    it("not indicating partition name should give master partition", async () => {
      const store = new Store();
      const [p1, p2] = await Promise.all([store.openPartition(), store.openPartition("master")]);
      expect(p1).toBe(p2);
    });
  });

  describe("#storeSnapshot", () => {
    it("should return previously stored snapshot", async () => {
      const store = new Store();
      const part = await store.openPartition("1");

      const snapshot = await part.storeSnapshot("stream1", { iAmSnapshot: true }, 10);
      const newSnapshot = await part.loadSnapshot("stream1");

      expect(newSnapshot).toBe(snapshot);
    });
  });
});
