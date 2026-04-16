import { describe, it, expect } from "vitest";

import EventStore, { Commit } from "tapeworm";
import IdbPersistenceStore from "..";

import getDb from "./util";

describe("Partition", () => {
  describe("#append", () => {
    it("should return commit if added", async () => {
      var es = new EventStore(new IdbPersistenceStore(getDb()));
      var partition = await es.openPartition("location");
      var c = await partition.append(new Commit("1", "location", "1", 0, []));
      expect(c[0].events.length).toBe(0);
    });
    it("should return after all commits are persisted", async () => {
      var es = new EventStore(new IdbPersistenceStore(getDb()));
      var partition = await es.openPartition("location");
      var c = await partition.append([
        new Commit("1", "location", "1", 0, []),
        new Commit("2", "location", "1", 1, []),
      ]);
      expect(c[0].events.length).toBe(0);
    });
  });
  describe("#_truncateStreamFrom", () => {
    it("should remove all commits ", async () => {
      var es = new EventStore(new IdbPersistenceStore(getDb()));
      var partition = await es.openPartition("location");
      await partition.append([
        new Commit("1", "location", "1", 0, []),
        new Commit("2", "location", "1", 1, []),
      ]);
      await partition._truncateStreamFrom("1", 0);
      var stream = await partition.openStream("1");
      expect(stream.getVersion()).toBe(-1);
    });
    it("should remove all commits after commitSequence", async () => {
      var es = new EventStore(new IdbPersistenceStore(getDb()));
      var partition = await es.openPartition("location");
      await partition.append([
        new Commit("1", "location", "1", 0, [{}]),
        new Commit("2", "location", "1", 1, [{}]),
      ]);
      await partition._truncateStreamFrom("1", 1);
      var stream = await partition.openStream("1");
      expect(stream.getVersion()).toBe(1);
    });
  });
  describe("#_applyCommitHeader", () => {
    it("should add to commit ", async () => {
      var es = new EventStore(new IdbPersistenceStore(getDb()));
      var partition = await es.openPartition("location");
      var commit = new Commit("1", "location", "1", 0, []);
      await partition.append([commit, new Commit("2", "location", "1", 1, [])]);
      var result = await partition._applyCommitHeader(commit.id, {
        authorative: true,
      });
      expect(result.authorative).toBeTruthy();
    });
    it("should throw if commit id is unknown", async () => {
      var es = new EventStore(new IdbPersistenceStore(getDb()));
      var partition = await es.openPartition("location");
      var commit = new Commit("1", "location", "1", 0, []);
      await partition.append([commit, new Commit("2", "location", "1", 1, [])]);
      await expect(
        partition._applyCommitHeader("ID MISSING", { authorative: true }),
      ).rejects.toThrow();
    });
  });
  describe("#querySnapshots", () => {
    it("Should return the snapshots within time range", async () => {
      var store = new IdbPersistenceStore(getDb(), "foo");
      var partition = await store.openPartition("location");
      var streamId = "abc";
      var snapshot = { id: "abc" };
      var version = 1;
      var maxDate = new Date();
      maxDate.setMinutes(maxDate.getMinutes() + 30);
      await partition.storeSnapshot(streamId, snapshot, version);
      var snapshots = await partition.querySnapshotsOlderThanMaxDate(
        maxDate.toISOString(),
      );
      expect(snapshots.length).toBe(1);
      expect(snapshots[0].id).toBe(streamId);
    });

    it("Should return not return any snapshot", async () => {
      var store = new IdbPersistenceStore(getDb());
      var partition = await store.openPartition("location");
      var streamId = "abc";
      var snapshot = { id: "abc" };
      var version = 1;
      var maxDate = new Date();
      maxDate.setMinutes(maxDate.getMinutes() - 30);

      await partition.storeSnapshot(streamId, snapshot, version);
      var snapshots = await partition.querySnapshotsOlderThanMaxDate(
        maxDate.toISOString(),
      );
      expect(snapshots.length).toBe(0);
    });
  });
});
