import { v4 as uuid } from "uuid";
import { describe, expect, it } from "vitest";
import EventStore, { Commit } from "..";

describe("Partition", () => {
  describe("#append", () => {
    it("should return commit if added", async (done) => {
      const es = new EventStore();
      const partition = await es.openPartition("location");
      const c = await partition.append(new Commit("1", "location", "1", 0, []));
      expect(c[0].events).toHaveLength(0);
    });

    it("should return after all commits are persisted", async () => {
      const es = new EventStore();
      const partition = await es.openPartition("location");
      const c = await partition.append([new Commit("1", "location", "1", 0, []), new Commit("2", "location", "1", 1, [])]);
      expect(c).toHaveLength(2);
    });
  });

  describe("#_truncateStreamFrom", () => {
    it("should remove all commits ", async () => {
      const es = new EventStore();
      const partition = await es.openPartition("location");
      await partition.append([new Commit("1", "location", "1", 0, []), new Commit("2", "location", "1", 1, [])]);
      await partition._truncateStreamFrom("1", 0);
      const stream = await partition.openStream("1");
      expect(stream.getVersion()).toBe(-1);
    });

    it("should remove all commits after commitSequence", async () => {
      const es = new EventStore();
      const partition = await es.openPartition("location");
      await partition.append([new Commit("1", "location", "1", 0, [{}]), new Commit("2", "location", "1", 1, [{}])]);
      await partition._truncateStreamFrom("1", 1);
      const stream = await partition.openStream("1");
      expect(stream.getVersion()).toBe(1);
    });
  });

  describe("#_applyCommitHeader", () => {
    it("should add to commit ", async () => {
      const es = new EventStore();
      const partition = await es.openPartition("location");
      let commit = new Commit("1", "location", "1", 0, []);
      await partition.append([commit, new Commit("2", "location", "1", 1, [])]);
      commit = await partition._applyCommitHeader(commit.id, {
        authorative: true,
      });
      expect(commit.authorative).toBeTruthy();
    });

    it("should throw if commit id is unknown", async () => {
      const es = new EventStore();
      const partition = await es.openPartition("location");
      const commit = new Commit("1", "location", "1", 0, []);
      await partition.append([commit, new Commit("2", "location", "1", 1, [])]);

      expect(() => partition._applyCommitHeader("ID MISSING", { authorative: true })).toThrow("Trying to apply header to missing commit: ID MISSING");
    });
  });

  describe("#queryStreamWithSnapshot", () => {
    it("queryStreamWithSnapshot should return snapshot and missing commits", async () => {
      const es = new EventStore();
      const streamId = "1";
      const stream = await es.openPartition("location").call("openStream", streamId);

      stream.append({ event: "123" });
      stream.append({ event: "999" });
      await stream.commit(uuid());

      stream.append({ event: "666" });
      stream.append({ event: "777" });
      await stream.commit(uuid());

      const part = await es.openPartition("location");
      part.storeSnapshot(streamId, { test: "snapshot" }, 2);
      const res = await part.queryStreamWithSnapshot(streamId);

      expect(res.snapshot.version).toBe(2);
      expect(res.commits).toHaveLength(1);
      expect(res.commits[0].events).toHaveLength(2);
      expect(res.commits[0].events[0].version).toBe(2);
    });

    it("queryStreamWithSnapshot should return snapshot and no commit if up to date", async () => {
      const es = new EventStore();
      const streamId = "1";
      const stream = await es.openPartition("location").call("openStream", streamId);

      stream.append({ event: "123" });
      stream.append({ event: "999" });
      await stream.commit(uuid());

      const part = await es.openPartition("location");
      part.storeSnapshot(streamId, { test: "snapshot" }, 2);

      const res = await part.queryStreamWithSnapshot(streamId);
      expect(res.snapshot.version).toBe(2);
      expect(res.commits).toHaveLength(0);
    });
  });

  describe("#delete", () => {
    it("should delete stream and all commits", async () => {
      let didIGetaDeleteEvent = false;

      const es = new EventStore(null, (commit) => {
        if (commit.events[0].type === "$stream.deleted.event") {
          didIGetaDeleteEvent = true;
        }
      });

      const partition = await es.openPartition("location");
      await partition.append([new Commit("1", "location", "1", 0, [{ type: "dummy.event" }]), new Commit("2", "location", "1", 1, [{ type: "dummy2.event" }])]);
      await partition.delete("1", {
        some: "header-value",
        payload: { test: true },
        type: "fail",
      });

      await expect(partition.openStream("1")).rejects.toThrow("Stream is deleted");
      expect(didIGetaDeleteEvent).toBeTruthy();
    });

    it("should delete and placeholder commit should have an id", async () => {
      let didIGetaDeleteEvent = false;

      const es = new EventStore(null, (commit) => {
        if (commit.events[0].type === "$stream.deleted.event" && commit.id) {
          didIGetaDeleteEvent = true;
        }

        expect(commit.id).not.toBeNull();
      });

      const partition = await es.openPartition("location");
      await partition.append([new Commit("1", "location", "1", 0, [{ type: "dummy.event" }]), new Commit("2", "location", "1", 1, [{ type: "dummy2.event" }])]);
      await partition.delete("1", {
        some: "header-value",
        payload: { test: true },
        type: "fail",
      });

      await expect(partition.openStream("1")).rejects.toThrow("Stream is deleted");
      expect(didIGetaDeleteEvent).toBeTruthy();
    });
  });
});
